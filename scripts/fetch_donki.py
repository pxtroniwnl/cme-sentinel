"""
fetch_donki.py
==============

Download discrete space-weather events from NASA's DONKI API (Space Weather
Database Of Notifications, Knowledge, Information) and save the records as
Parquet, partitioned by source endpoint and year.

Scope
-----
* Endpoints (the causal core): CME, CMEAnalysis, GST, FLR, SEP, IPS, HSS.
* One row per event (e.g. one CME, one geomagnetic storm, one SEP). The
  records are JSON objects with nested structures (linkedEvents,
  allKpIndex, cmeAnalyses, instruments, sentNotifications) that get
  flattened into scalar columns for analysis; the nested arrays themselves
  are kept serialized as JSON so nothing is lost.
* Availability: DONKI catalogs events from ~2010 (default start), through
  today.

How it works
------------
1. Reads the NASA API key from the project `.env` (or --api-key).
2. Splits the requested range into calendar-year windows.
3. Fetches each (endpoint, year) window once and writes
   `data/donki/<endpoint>/year=YYYY/part-00000.parquet`.

Resume support
--------------
Completed (endpoint, year) pairs are recorded in `data/.progress.json`
under keys `donki:<endpoint>:YYYY`. Interrupted runs are skipped on
restart; `--reset` forces a full re-download.

Usage
-----
    python scripts/fetch_donki.py
    python scripts/fetch_donki.py --endpoints GST,SEP
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = DATA_DIR / "donki"
PROGRESS_FILE = DATA_DIR / ".progress.json"
ENV_FILE = PROJECT_ROOT / ".env"

BASE_URL = "https://api.nasa.gov"
ENDPOINTS = {
    "CME": "/DONKI/CME",
    "CMEAnalysis": "/DONKI/CMEAnalysis",
    "GST": "/DONKI/GST",
    "FLR": "/DONKI/FLR",
    "SEP": "/DONKI/SEP",
    "IPS": "/DONKI/IPS",
    "HSS": "/DONKI/HSS",
}

DEFAULT_START = datetime(2010, 1, 1, tzinfo=timezone.utc)

MAX_RETRIES = 3
DEFAULT_MIN_INTERVAL = 5.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("fetch_donki")


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


def get_json(
    session: requests.Session, url: str, params: dict[str, str], min_interval: float
) -> list[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(min_interval)
        try:
            resp = session.get(url, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            logged = str(data)[:200]
            raise RuntimeError(f"Unexpected DONKI payload: {logged}")
        except (requests.RequestException, RuntimeError) as exc:
            backoff = min_interval * (2 ** attempt)
            LOG.warning(
                "  Attempt %d/%d failed: %s - retrying in %.1fs",
                attempt, MAX_RETRIES, exc, backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(f"GET {url} failed after {MAX_RETRIES} retries")


def flatten_record(rec: dict) -> dict:
    out: dict = {}
    for key, value in rec.items():
        if isinstance(value, list):
            out[f"{key}_count"] = len(value)
            if key == "linkedEvents":
                out["linked_activity_ids"] = [
                    e.get("activityID") for e in value if isinstance(e, dict)
                ]
            if key == "allKpIndex":
                kps = [e.get("kpIndex") for e in value if isinstance(e, dict)]
                nums = [k for k in kps if isinstance(k, (int, float))]
                out["all_kp_max"] = max(nums) if nums else None
                out["all_kp_times"] = [
                    e.get("observedTime") for e in value if isinstance(e, dict)
                ]
            if key == "instruments":
                out["instruments_displayName"] = [
                    e.get("displayName") for e in value if isinstance(e, dict)
                ]
            out[key] = json.dumps(value, default=str)
        elif isinstance(value, dict):
            out[key] = json.dumps(value, default=str)
        else:
            out[key] = value
    return out


def fetch_endpoint_year(
    session: requests.Session,
    endpoint: str,
    year: int,
    api_key: str,
    min_interval: float,
) -> int:
    start = datetime(year, 1, 1, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, tzinfo=timezone.utc)
    params = {
        "startDate": start.strftime("%Y-%m-%d"),
        "endDate": end.strftime("%Y-%m-%d"),
        "api_key": api_key,
    }
    url = BASE_URL + ENDPOINTS[endpoint]
    LOG.info("Fetching DONKI %s year %d (%s -> %s)", endpoint, year, start.year, end.year)
    records = get_json(session, url, params, min_interval)
    if not records:
        LOG.info("  %s year %d empty", endpoint, year)
        return 0
    df = pd.DataFrame([flatten_record(r) for r in records])
    df["source_endpoint"] = endpoint
    df["fetched_at"] = datetime.now(timezone.utc)
    year_dir = OUT_DIR / f"{endpoint}/year={year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    path = year_dir / "part-00000.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)
    LOG.info("  OK: %d rows -> %s", len(df), path)
    return len(df)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download discrete space-weather events from the NASA DONKI API "
            "as Parquet files."
        )
    )
    parser.add_argument(
        "--endpoints",
        default=",".join(ENDPOINTS),
        help="Comma-separated subset of endpoints. Default: all "
             f"({', '.join(ENDPOINTS)}).",
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START.strftime("%Y-%m-%d"),
        help="Start date (YYYY-MM-DD, UTC). Default: 2010-01-01.",
    )
    parser.add_argument(
        "--end",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD, UTC). Default: today.",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="NASA API key. Overrides NASA_API_KEY from the .env file.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after this many (endpoint, year) pairs (for testing).",
    )
    parser.add_argument(
        "--min-interval",
        type=float,
        default=DEFAULT_MIN_INTERVAL,
        help="Minimum sleep in seconds between API requests (default: 5.0).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Ignore existing progress and re-download everything.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    load_dotenv(ENV_FILE)
    api_key = args.api_key or os.getenv("NASA_API_KEY")
    if not api_key:
        LOG.error(
            "Missing NASA API key. Set NASA_API_KEY in %s (get one free at "
            "https://api.nasa.gov) or pass --api-key.", ENV_FILE,
        )
        return 1

    endpoints = [e.strip() for e in args.endpoints.split(",") if e.strip()]
    unknown = [e for e in endpoints if e not in ENDPOINTS]
    if unknown:
        LOG.error("Unknown endpoints: %s", ", ".join(unknown))
        return 1

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    if end.date() >= datetime.now(timezone.utc).date():
        end = datetime.now(timezone.utc)
    if start >= end:
        LOG.error("--start must be before --end.")
        return 1

    completed = set() if args.reset else load_progress()
    LOG.info("Output directory: %s", OUT_DIR)
    LOG.info("Endpoints: %s", ", ".join(endpoints))
    LOG.info("Completed (endpoint, year) pairs so far: %d",
             sum(1 for k in completed if k.startswith("donki:")))

    session = requests.Session()
    session.headers.update({"User-Agent": "cme-sentinel/0.1 (research download)"})

    processed = 0
    for endpoint in endpoints:
        for year in range(start.year, end.year + 1):
            if args.limit is not None and processed >= args.limit:
                LOG.info("--limit reached; stopping.")
                break
            key = f"donki:{endpoint}:{year}"
            if key in completed:
                LOG.info("Skipping %s (already done).", key)
                processed += 1
                continue
            try:
                rows = fetch_endpoint_year(session, endpoint, year, api_key, args.min_interval)
                completed.add(key)
                save_progress(completed)
                processed += 1
                _ = rows
            except Exception as exc:  # noqa: BLE001
                LOG.error("%s year %d failed: %s", endpoint, year, exc)
                return 1

    LOG.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())