"""
fetch_omni.py
=============

Download the hourly OMNI merged solar-wind / magnetosphere dataset from
NASA GSFC SPDF (dataset OMNI_COHO1HR_MERGED_MAG_PLASMA, 1963 -> today)
through the HAPI protocol and save it as Apache Parquet partitioned by year.

Scope
-----
* One row per hour: 1963-01-01 -> today (~565,000 rows).
* The dataset carries the IMF (BX_GSE, BY_GSM, BZ_GSM, BGT), solar wind
  (flow_speed, proton_density, proton_temperature, flow_pressure),
  geomagnetic indices (DST, Kp, Sunspot_Number, AE/AL/AU, ap, F10_INDEX)
  and energetic proton fluxes (>1/>2/>4/>10/>30/>60 MeV). The exact
  parameter set is enumerated at runtime from the HAPI /info endpoint and
  logged; when no explicit --parameters list is given every parameter of
  the dataset is fetched.

How it works
------------
1. Queries HAPI `/info` to enumerate the dataset parameters.
2. Splits the requested range into calendar-year windows.
3. Fetches each window once with `format=csv` and writes
   `data/omni/year=YYYY/part-00000.parquet`.
4. HAPI fill values (large negative numbers such as -1e31, used across the
   OMNI archive) are converted to NaN.

Resume support
--------------
Completed years are recorded in `data/.progress.json` under keys
`omni:YYYY` so interrupted runs are skipped on restart. `--reset` forces a
full re-download.

Usage
-----
    python scripts/fetch_omni.py
    python scripts/fetch_omni.py --start 2022-01-01 --end 2022-12-31
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = DATA_DIR / "omni"
PROGRESS_FILE = DATA_DIR / ".progress.json"

HAPI_BASE = "https://cdaweb.gsfc.nasa.gov/hapi"
DATASET_ID = "OMNI_COHO1HR_MERGED_MAG_PLASMA"

DEFAULT_START = datetime(1963, 1, 1, tzinfo=timezone.utc)

MAX_RETRIES = 3
DEFAULT_MIN_INTERVAL = 3.0
FILL_THRESHOLD = 1e20

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("fetch_omni")


def load_progress() -> set[str]:
    if not PROGRESS_FILE.exists():
        return set()
    try:
        return set(json.loads(PROGRESS_FILE.read_text(encoding="utf-8")).get("completed", []))
    except (json.JSONDecodeError, OSError):
        LOG.warning("Could not read %s, starting from scratch.", PROGRESS_FILE)
        return set()


def save_progress(completed: set[str]) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(
        json.dumps({"completed": sorted(completed)}, indent=2), encoding="utf-8"
    )


def hapi_get(
    session: requests.Session, path: str, params: dict[str, str] | None, min_interval: float
) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(min_interval)
        try:
            resp = session.get(HAPI_BASE + path, params=params, timeout=180)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status < 500:
                raise
            backoff = min_interval * (2 ** attempt)
            LOG.warning(
                "  Attempt %d/%d failed: %s - retrying in %.1fs",
                attempt, MAX_RETRIES, exc, backoff,
            )
            time.sleep(backoff)
        except requests.RequestException as exc:
            backoff = min_interval * (2 ** attempt)
            LOG.warning(
                "  Attempt %d/%d failed: %s - retrying in %.1fs",
                attempt, MAX_RETRIES, exc, backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(f"GET {HAPI_BASE}{path} failed after {MAX_RETRIES} retries")


def enumerate_parameters(session: requests.Session, min_interval: float) -> list[str]:
    resp = hapi_get(session, "/info", {"id": DATASET_ID}, min_interval)
    parameters = resp.json().get("parameters", [])
    if isinstance(parameters, list):
        return [p.get("name") for p in parameters if isinstance(p, dict)]
    return []


def normalize_dtypes(df: pd.DataFrame, year: int) -> pd.DataFrame:
    if "Time" in df.columns:
        df["EPOCH"] = pd.to_datetime(df.pop("Time"), errors="coerce", utc=True)
    for col in df.columns:
        if col == "EPOCH":
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().any():
            df[col] = numeric.mask(numeric.abs() > FILL_THRESHOLD)
    df["year"] = year
    return df


def fetch_year(
    session: requests.Session, year: int, parameters: list[str], min_interval: float
) -> int:
    start = datetime(year, 1, 1, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    params: dict[str, str] = {
        "id": DATASET_ID,
        "time.min": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "time.max": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format": "csv",
    }
    if parameters:
        params["parameters"] = ",".join(parameters)
    LOG.info("Fetching year %d (%s -> %s)", year, start.date(), end.date())
    resp = hapi_get(session, "/data", params, min_interval)
    df = pd.read_csv(
        io.StringIO(resp.text),
        comment="#",
        skipinitialspace=True,
        dtype=str,
        keep_default_na=False,
    )
    if df.empty:
        LOG.info("  year %d empty", year)
        return 0
    df = normalize_dtypes(df, year)
    year_dir = OUT_DIR / f"year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    path = year_dir / "part-00000.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)
    LOG.info("  OK: %d rows -> %s", len(df), path)
    return len(df)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the hourly OMNI merged dataset (1963 -> today) as "
            "Parquet files partitioned by year."
        )
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START.strftime("%Y-%m-%d"),
        help="Start date (YYYY-MM-DD, UTC). Default: 1963-01-01.",
    )
    parser.add_argument(
        "--end",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD, UTC). Default: today.",
    )
    parser.add_argument(
        "--parameters",
        default=None,
        help="Comma-separated HAPI parameter names to fetch. Default: all parameters.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after this many years (for testing).",
    )
    parser.add_argument(
        "--min-interval",
        type=float,
        default=DEFAULT_MIN_INTERVAL,
        help="Minimum sleep in seconds between API requests (default: 3.0).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Ignore existing progress and re-download everything.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    if end.date() >= datetime.now(timezone.utc).date():
        end = datetime.now(timezone.utc)
    if start >= end:
        LOG.error("--start must be before --end.")
        return 1

    completed = set() if args.reset else load_progress()
    LOG.info("Output directory: %s", OUT_DIR)
    LOG.info("Completed years so far: %d", sum(1 for k in completed if k.startswith("omni:")))

    session = requests.Session()
    session.headers.update({"User-Agent": "cme-sentinel/0.1 (research download)"})

    parameters: list[str] = []
    if args.parameters:
        parameters = [p.strip() for p in args.parameters.split(",") if p.strip()]
    else:
        try:
            parameters = enumerate_parameters(session, args.min_interval)
            shown = ", ".join(parameters[:10]) + ("..." if len(parameters) > 10 else "")
            LOG.info("Dataset parameters (%d): %s", len(parameters), shown)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Could not enumerate parameters (%s); fetching all columns.", exc)

    processed = 0
    for year in range(start.year, end.year + 1):
        if args.limit is not None and processed >= args.limit:
            LOG.info("--limit reached; stopping.")
            break
        key = f"omni:{year}"
        if key in completed:
            LOG.info("Skipping year %d (already done).", year)
            processed += 1
            continue
        try:
            rows = fetch_year(session, year, parameters, args.min_interval)
            completed.add(key)
            save_progress(completed)
            processed += 1
            _ = rows
        except Exception as exc:  # noqa: BLE001
            LOG.error("Year %d failed: %s", year, exc)
            return 1

    LOG.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())