"""
fetch_gp_history.py
===================

Download the FULL historical orbital catalog from Space-Track.org
(gp_history class) and save it as Apache Parquet files partitioned by
year.

Scope
-----
* Every tracked object (all NORAD_CAT_IDs).
* All time: from 1960-01-01 up to today.
* All OMM columns exposed by the gp_history class (see README for the
  full list of columns).

How it works
------------
1. Reads your Space-Track credentials from the project `.env` file.
2. Logs in via the `spacetrack` client (cookie/session handled for you).
3. Divides the whole time span into time "windows" (yearly for old eras,
   monthly for recent dense eras such as Starlink).
4. Fetches each window with `format=csv` (compressed, allowed by the API).
5. If a window is too big and the server times out/errors, the window is
   split in half recursively until the request succeeds.
6. Streams every window line-by-line, batches the rows in memory and
   writes `data/gp_history/year=YYYY/part-<date>-<batch>.parquet`.

Resume support
--------------
Completed windows are stored in `data/.progress.json`. Any window already
downloaded is skipped on the next run, so the script can be restarted
safely after a network drop or a crash.

Usage
-----
    python scripts/fetch_gp_history.py
    python scripts/fetch_gp_history.py --start 2020-01-01 --limit 5   # test run
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import queue
import sys
import time
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from spacetrack import SpaceTrackClient
from spacetrack.operators import inclusive_range

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Root of the repository (parent of the scripts/ folder).
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data output directory (git-ignored).
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = DATA_DIR / "gp_history"
PROGRESS_FILE = DATA_DIR / ".progress.json"

# Environment file holding the credentials.
ENV_FILE = PROJECT_ROOT / ".env"

# Default range: from the dawn of the catalog era to today.
DEFAULT_START = datetime(1960, 1, 1, tzinfo=timezone.utc)

# Columns returned by gp_history, in API order (from the Space-Track
# model definition page). These are assigned to the CSV data we download.
COLUMNS = [
    "CCSDS_OMM_VERS",
    "COMMENT",
    "CREATION_DATE",
    "ORIGINATOR",
    "OBJECT_NAME",
    "OBJECT_ID",
    "CENTER_NAME",
    "REF_FRAME",
    "TIME_SYSTEM",
    "MEAN_ELEMENT_THEORY",
    "EPOCH",
    "MEAN_MOTION",
    "ECCENTRICITY",
    "INCLINATION",
    "RA_OF_ASC_NODE",
    "ARG_OF_PERICENTER",
    "MEAN_ANOMALY",
    "EPHEMERIS_TYPE",
    "CLASSIFICATION_TYPE",
    "NORAD_CAT_ID",
    "ELEMENT_SET_NO",
    "REV_AT_EPOCH",
    "BSTAR",
    "MEAN_MOTION_DOT",
    "MEAN_MOTION_DDOT",
    "SEMIMAJOR_AXIS",
    "PERIOD",
    "APOAPSIS",
    "PERIAPSIS",
    "OBJECT_TYPE",
    "RCS_SIZE",
    "COUNTRY_CODE",
    "LAUNCH_DATE",
    "SITE",
    "DECAY_DATE",
    "FILE",
    "GP_ID",
    "TLE_LINE0",
    "TLE_LINE1",
    "TLE_LINE2",
]

# Numeric columns converted to a dedicated type when writing Parquet.
FLOAT_COLUMNS = [
    "MEAN_MOTION",
    "ECCENTRICITY",
    "INCLINATION",
    "RA_OF_ASC_NODE",
    "ARG_OF_PERICENTER",
    "MEAN_ANOMALY",
    "BSTAR",
    "MEAN_MOTION_DOT",
    "MEAN_MOTION_DDOT",
    "SEMIMAJOR_AXIS",
    "PERIOD",
    "APOAPSIS",
    "PERIAPSIS",
]
INT_COLUMNS = [
    "EPHEMERIS_TYPE",
    "NORAD_CAT_ID",
    "ELEMENT_SET_NO",
    "REV_AT_EPOCH",
    "FILE",
    "GP_ID",
]
DATETIME_COLUMNS = [
    "EPOCH",
    "CREATION_DATE",
]

# Smallest window we are willing to request before giving up on a range.
MIN_WINDOW_DAYS = 1

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("fetch_gp_history")


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------


def initial_window_days(year: int) -> int:
    """Pick a starting window size for a given year.

    Recent years (Starlink era) hold millions of element sets, so they must
    be fetched in smaller windows to avoid server time-outs. Older eras are
    fetched in bigger windows to reduce the number of requests.
    """
    if year >= 2020:
        return 30          # ~monthly
    if year >= 2016:
        return 91          # ~quarterly
    if year >= 2008:
        return 182         # ~bi-annual
    return 365             # yearly (or larger) for the old catalog


def make_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Split [start, end] into contiguous, non-overlapping windows.

    Every window is a closed interval [window_start, window_end]. The end of
    each window is set to one microsecond before the start of the next window
    so there are no gaps and no overlaps.
    """
    windows: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor <= end:
        step = timedelta(days=initial_window_days(cursor.year))
        window_end = min(cursor + step, end)
        # Make sure the window does not spill into the next calendar year.
        if window_end.year != cursor.year and window_end != end:
            window_end = datetime(
                cursor.year, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc
            )
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(microseconds=1)
    return windows


def split_window(
    start: datetime, end: datetime
) -> tuple[tuple[datetime, datetime], tuple[datetime, datetime]]:
    """Split a closed window in half at its midpoint."""
    midpoint = start + (end - start) / 2
    left = (start, midpoint - timedelta(microseconds=1))
    right = (midpoint, end)
    return left, right


def window_key(start: datetime, end: datetime) -> str:
    """Stable identifier for a window, used for the progress tracker."""
    return f"{start.isoformat()}|{end.isoformat()}"


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------


def load_progress() -> set[str]:
    """Load the set of completed windows from disk."""
    if not PROGRESS_FILE.exists():
        return set()
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        return set(data.get("completed", []))
    except (json.JSONDecodeError, OSError):
        LOG.warning("Could not read %s, starting from scratch.", PROGRESS_FILE)
        return set()


def save_progress(completed: set[str]) -> None:
    """Persist the set of completed windows to disk."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(
        json.dumps({"completed": sorted(completed)}, indent=2), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Data conversion
# ---------------------------------------------------------------------------


def parse_batch(lines: list[str]) -> pd.DataFrame:
    """Parse a batch of CSV lines (already serialized) into a DataFrame.

    Spaces-Track returns RFC-4180 quoted CSV, so pandas' default CSV parser
    can safely handle commas that appear inside quoted fields.
    """
    raw = "\n".join(lines)
    df = pd.read_csv(
        io.StringIO(raw),
        header=None,
        names=COLUMNS,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        engine="python",
    )
    return normalize_dtypes(df)


def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Convert string columns into proper pandas/Parquet types."""
    for col in DATETIME_COLUMNS:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    for col in FLOAT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in INT_COLUMNS:
        series = pd.to_numeric(df[col], errors="coerce")
        df[col] = series.astype("Int64")
    return df


def first_line_is_header(first_line: str) -> bool:
    """Detect whether the API prefixed the stream with the field-name row."""
    probe = first_line.strip().strip('""').split(",")[0].strip('"')
    return probe == COLUMNS[0]


# ---------------------------------------------------------------------------
# Core fetch logic
# ---------------------------------------------------------------------------


def fetch_window(
    st: SpaceTrackClient,
    start: datetime,
    end: datetime,
    fmt: str = "csv",
    batch_size: int = 200_000,
) -> tuple[int, int]:
    """Fetch one window and write it to Parquet parts.

    Returns (rows_written, batches_written). Raises on API failure so the
    caller can decide whether to split the window.
    """
    year = start.year
    year_dir = OUT_DIR / f"year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"part-{start:%Y%m%d}"

    # Filter value understood by the API: [start;end].
    epoch_range = inclusive_range(start, end)

    lines: Iterator[str] = st.gp_history(
        epoch=epoch_range,
        format=fmt,
        orderby="EPOCH",
        iter_lines=True,
    )

    batch: list[str] = []
    batch_index = 0
    total_rows = 0
    total_batches = 0

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Skip the header row on the very first line, if present.
        if total_rows == 0 and len(batch) == 0 and first_line_is_header(line):
            continue
        batch.append(line)
        if len(batch) >= batch_size:
            total_rows += flush_batch(batch, year_dir, prefix, batch_index, year)
            batch_index += 1
            total_batches += 1

    if batch:
        total_rows += flush_batch(batch, year_dir, prefix, batch_index, year)
        total_batches += 1

    if total_rows == 0:
        LOG.info("  window %s -> %s empty", start.date(), end.date())
    return total_rows, total_batches


def flush_batch(
    batch: list[str], year_dir: Path, prefix: str, batch_index: int, year: int
) -> int:
    """Write one batch of CSV lines to a Parquet part file."""
    df = parse_batch(batch)
    df["year"] = year
    part_path = year_dir / f"{prefix}-{batch_index:05d}.parquet"
    df.to_parquet(part_path, engine="pyarrow", index=False)
    batch.clear()
    return len(df)


def fetch_window_with_subdivision(
    st: SpaceTrackClient,
    start: datetime,
    end: datetime,
    fmt: str,
    batch_size: int,
    completed: set[str],
    interval: float,
) -> int:
    """Fetch a window, splitting it recursively when it is too large.

    Returns the number of rows downloaded for this top-level window.
    """
    pending: queue.Queue[tuple[datetime, datetime]] = queue.Queue()
    pending.put((start, end))
    rows_total = 0

    while not pending.empty():
        win_start, win_end = pending.get()

        # Already done during a previous run?
        key = window_key(win_start, win_end)
        if key in completed:
            continue

        # Respect a voluntary pause between API requests.
        if interval > 0:
            time.sleep(interval)

        span_days = (win_end - win_start).total_seconds() / 86_400
        LOG.info(
            "Fetching %s -> %s (%.1f days)",
            win_start.isoformat(),
            win_end.isoformat(),
            span_days,
        )

        try:
            rows, batches = fetch_window(st, win_start, win_end, fmt, batch_size)
            if batches == 0:
                # Empty result is still a successful download.
                completed.add(key)
                save_progress(completed)
            else:
                completed.add(key)
                save_progress(completed)
            rows_total += rows
        except Exception as exc:  # noqa: BLE001 - we need to catch any API failure
            LOG.warning("  Request failed: %s", exc)

            # Don't split below the minimum window size we allow.
            too_small = span_days <= MIN_WINDOW_DAYS
            if too_small:
                LOG.error(
                    "  Window too small to split (%s days); skipping.", span_days
                )
                continue

            left, right = split_window(win_start, win_end)
            LOG.info(
                "  Splitting into %s -> %s and %s -> %s",
                left[0].isoformat(),
                left[1].isoformat(),
                right[0].isoformat(),
                right[1].isoformat(),
            )
            pending.put(left)
            pending.put(right)

    return rows_total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Download the full gp_history catalog from Space-Track.org as "
            "Parquet files partitioned by year."
        )
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START.strftime("%Y-%m-%d"),
        help="Start date (YYYY-MM-DD, UTC). Default: 1960-01-01.",
    )
    parser.add_argument(
        "--end",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD, UTC). Default: today.",
    )
    parser.add_argument(
        "--format",
        default="csv",
        choices=["csv", "json"],
        help="API format to request. 'csv' is the most compact.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200_000,
        help="Number of records buffered in memory before writing a Parquet file.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after this many top-level time windows (for testing).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Extra sleep in seconds between API requests.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Ignore existing progress and re-download everything.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    # --- Credentials ----------------------------------------------------
    load_dotenv(ENV_FILE)
    identity = os.getenv("SPACE_TRACK_EMAIL")
    password = os.getenv("SPACE_TRACK_PASSWORD")
    if not identity or not password:
        LOG.error(
            "Missing credentials. Set SPACE_TRACK_EMAIL and "
            "SPACE_TRACK_PASSWORD in %s", ENV_FILE
        )
        return 1

    # --- Date range -----------------------------------------------------
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    if end.date() >= datetime.now(timezone.utc).date():
        end = datetime.now(timezone.utc)
    if start >= end:
        LOG.error("--start must be before --end.")
        return 1

    # --- Progress -------------------------------------------------------
    completed = set() if args.reset else load_progress()
    LOG.info("Output directory: %s", OUT_DIR)
    LOG.info("Completed windows so far: %d", len(completed))

    # --- Login in a context-managed session -----------------------------
    try:
        with SpaceTrackClient(identity, password) as st:
            LOG.info("Logged in to Space-Track as %s", identity)

            windows = make_windows(start, end)
            LOG.info("Planned %d time windows from %s to %s.",
                     len(windows), start.date(), end.date())

            processed = 0
            for win_start, win_end in windows:
                if args.limit is not None and processed >= args.limit:
                    LOG.info("--limit reached; stopping.")
                    break
                fetch_window_with_subdivision(
                    st,
                    win_start,
                    win_end,
                    args.format,
                    args.batch_size,
                    completed,
                    args.interval,
                )
                processed += 1

            LOG.info("Done.")
            return 0
    except Exception as exc:  # noqa: BLE001 - report the cause clearly
        LOG.error("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())