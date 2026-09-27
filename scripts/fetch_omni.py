"""
fetch_omni.py
=============

Download the OMNI merged solar-wind / magnetosphere dataset from NASA GSFC
SPDF (dataset OMNI_HRO_5MIN) through the HAPI protocol and save it as
Apache Parquet partitioned by year.

Scope
-----
* One row per hour, resampled from the native 5-minute cadence. The
  project analyzes storm windows (hours to days), so hourly is the target
  resolution; keeping 5-minute rows for decades would be ~23M rows for
  15 years with no analytical gain.
* What this dataset actually carries (verified against HAPI /info, 45
  parameters): IMF in GSE and GSM (BX_GSE, BY_GSE, BZ_GSE, BY_GSM,
  BZ_GSM), solar wind (flow_speed, proton_density, T, Pressure), electric
  field, plasma beta, Mach numbers, GSM position, geomagnetic indices
  (AE_INDEX, AL_INDEX, AU_INDEX, SYM_D, SYM_H, ASY_D, ASY_H,
  PC_N_INDEX) and three energetic-proton channels (PR-FLX_10, PR-FLX_30,
  PR-FLX_60 MeV).
* It does **not** carry Dst, Kp, ap, sunspot number or F10.7. SYM_H is
  the modern successor of Dst (ring-current index) and is the storm
  driver used downstream; AE_INDEX covers the Kp-like "how disturbed"
  role. The exact parameter set is enumerated at runtime from the HAPI
  /info endpoint and logged.

How it works
------------
1. Queries HAPI `/info` to enumerate the dataset parameters.
2. Splits the requested range into calendar-year windows.
3. Fetches each window once with `format=csv` and writes
   `data/omni/year=YYYY/part-00000.parquet`, resampled to hourly.
4. HAPI fill values are converted to NaN. `OMNI_HRO_5MIN` uses small
   per-parameter sentinels (99999.9 in `flow_speed`, 99999 in `SYM_H`, 99.99
   in `Pressure`, ...), not the -1e31 of the COHO series, so the exact
   value declared in the `fill` field of `/info` is used per column.

Hourly aggregation rule
-----------------------
5-minute samples are reduced to hourly with an explicit per-column rule,
because the average hides the physically relevant extreme in three cases:
mean for the continuous fields, `min` for the ring-current indices
(SYM_H/SYM_D, the storm minimum) and `max` for BZ_GSM (the southward IMF
excursion that drives coupling) and the proton fluxes (SEP peak). Columns
carrying such an extreme get an explicit `_min`/`_max` suffix, so the
aggregation is never ambiguous downstream.

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
DATASET_ID = "OMNI_HRO_5MIN"

DEFAULT_START = datetime(1963, 1, 1, tzinfo=timezone.utc)

MAX_RETRIES = 3
DEFAULT_MIN_INTERVAL = 3.0
FILL_THRESHOLD = 1e20

# Columns whose hourly aggregate is the extreme, not the mean. The
# ring-current index is quoted by its minimum (storm depth), the southward
# IMF by its most negative excursion (coupling driver) and the SEP
# channels by their peak (event severity).
AGG_MIN = ("SYM_H", "SYM_D", "ASY_H", "ASY_D")
AGG_MAX = ("BZ_GSM", "AE_INDEX", "AL_INDEX", "AU_INDEX", "PC_N_INDEX") + (
    "PR-FLX_10",
    "PR-FLX_30",
    "PR-FLX_60",
)

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


def enumerate_parameters(
    session: requests.Session, min_interval: float
) -> dict[str, float | None]:
    resp = hapi_get(session, "/info", {"id": DATASET_ID}, min_interval)
    parameters = resp.json().get("parameters", [])
    spec: dict[str, float | None] = {}
    for item in parameters:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if not name:
            continue
        fill = item.get("fill")
        try:
            spec[name] = float(fill) if fill is not None else None
        except (TypeError, ValueError):
            spec[name] = None
    return spec


def hourly_agg_funcs(columns: list[str]) -> dict[str, str]:
    funcs: dict[str, str] = {}
    for col in columns:
        if col in AGG_MIN:
            funcs[col] = "min"
        elif col in AGG_MAX:
            funcs[col] = "max"
        else:
            funcs[col] = "mean"
    return funcs


def normalize_dtypes(
    df: pd.DataFrame, fills: dict[str, float | None] | None = None
) -> pd.DataFrame:
    if "Time" in df.columns:
        df["EPOCH"] = pd.to_datetime(df.pop("Time"), errors="coerce", utc=True)
    for col in df.columns:
        if col == "EPOCH":
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().any():
            masked = numeric.mask(numeric.abs() > FILL_THRESHOLD)
            fill = (fills or {}).get(col)
            if fill is not None:
                masked = masked.mask(numeric == fill)
            df[col] = masked
    return df


def resample_hourly(df: pd.DataFrame) -> pd.DataFrame:
    if "EPOCH" not in df.columns:
        raise ValueError("OMNI payload has no Time column; cannot build EPOCH.")
    if df.empty or df["EPOCH"].isna().all():
        return pd.DataFrame()
    funcs = hourly_agg_funcs([c for c in df.columns if c != "EPOCH"])
    out = df.dropna(subset=["EPOCH"]).set_index("EPOCH").resample("1h").agg(funcs)
    out = out.rename(columns={c: f"{c}_max" for c in AGG_MAX if c in out.columns})
    return out.dropna(how="all").reset_index()


def fetch_year(
    session: requests.Session,
    year: int,
    parameters: list[str],
    min_interval: float,
    fills: dict[str, float | None] | None = None,
    allow_narrow: bool = False,
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
        header=None,
        names=parameters or None,
        comment="#",
        skipinitialspace=True,
        dtype=str,
        keep_default_na=False,
    )
    if df.empty:
        LOG.info("  year %d empty", year)
        return 0
    df = normalize_dtypes(df, fills)
    df = resample_hourly(df)
    df["year"] = year
    year_dir = OUT_DIR / f"year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    path = year_dir / "part-00000.parquet"
    if path.exists():
        # A --parameters run fetches a column subset. Writing it over a
        # full partition would silently drop the other columns, so require an
        # explicit --reset before narrowing an existing year.
        prev = pd.read_parquet(path)
        if not allow_narrow and len(prev.columns) > len(df.columns):
            raise RuntimeError(
                f"refusing to overwrite {path} ({len(prev.columns)} columns) with a "
                f"{len(df.columns)}-column fetch; re-run with --reset to replace it"
            )
    df.to_parquet(path, engine="pyarrow", index=False)
    LOG.info("  OK: %d hourly rows -> %s", len(df), path)
    return len(df)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the OMNI merged solar-wind/magnetosphere dataset "
            "(OMNI_HRO_5MIN) from NASA GSFC SPDF and store it resampled to "
            "hourly as Parquet files partitioned by year."
        )
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START.strftime("%Y-%m-%d"),
        help="Start date (YYYY-MM-DD, UTC). Default: 1963-01-01 (the dataset "
        "start; the project currently holds 2012-01-01 -> 2026-09-03, so pass "
        "--start 2012-01-01 to only extend that range).",
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

    try:
        spec = enumerate_parameters(session, args.min_interval)
    except Exception as exc:  # noqa: BLE001
        LOG.error("Could not enumerate parameters from HAPI /info: %s", exc)
        LOG.error(
            "The HAPI CSV payload carries no header row, so the column names "
            "must come from /info. Refusing to write a Parquet whose columns "
            "would be the first data row."
        )
        return 1
    if not spec:
        LOG.error("HAPI /info returned no parameters for %s.", DATASET_ID)
        return 1

    parameters: list[str] = list(spec)
    if args.parameters:
        wanted = [p.strip() for p in args.parameters.split(",") if p.strip()]
        unknown = [p for p in wanted if p not in spec]
        if unknown:
            LOG.error("Unknown --parameters for %s: %s", DATASET_ID, unknown)
            return 1
        # HAPI only returns the requested subset, and the time axis is not
        # included unless asked for, so request it explicitly. HAPI rejects a
        # subset that is not in the dataset's native order (error 1411), so
        # the selection is re-expanded in /info order. Time is dropped in
        # normalize_dtypes() and never reaches the Parquet.
        selection = set(wanted) | {"Time"}
        parameters = [name for name in spec if name in selection]
    fills = {name: spec[name] for name in parameters}

    shown = ", ".join(parameters[:10]) + ("..." if len(parameters) > 10 else "")
    LOG.info("Dataset: %s", DATASET_ID)
    LOG.info("Parameters (%d): %s", len(parameters), shown)

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
            rows = fetch_year(
                session, year, parameters, args.min_interval, fills, args.reset
            )
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