"""
fetch_gunter.py
===============

Crawl Gunter's Space Page (space.skyrocket.de) and save the site data
locally.

Scope
-----
* Full-site crawl: every reachable HTML page on the domain is downloaded.
* Two kinds of content are extracted:
  - Index tables (satellite directory tables whose header includes COSPAR
    and Launch Vehicle): rows saved to `data/gunter/tables.parquet`.
  - Comsat failure narratives (pages under doc_sat/comsat_failures): the
    heading + narrative-text pairs saved to `data/gunter/incidents.parquet`.
* Raw HTML and extracted text of every visited page are kept under
  `data/gunter/pages/`. A crawl metadata table is kept in
  `data/gunter/meta/pages.parquet`.

Why raw content is kept
-----------------------
Most of the failure information on the site is free prose ("failed in 1998
due to momentum wheel problems"). Keeping the raw text lets downstream
parsing (regex / NLP) be improved without re-crawling.

Courtesy
--------
* Respects robots.txt when present.
* Sleeps --delay seconds (default 1.5 s) between requests.
* Resume via `data/.progress.json` (keys `gunter:page:<sha1(url)>`).

Usage
-----
    python scripts/fetch_gunter.py                 # full crawl, resume-safe
    python scripts/fetch_gunter.py --max-pages 100 --delay 2.0
    python scripts/fetch_gunter.py --satellites    # satellite sections only
    python scripts/fetch_gunter.py --path-prefix doc_sdat
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from collections import deque
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag, urlunsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = DATA_DIR / "gunter"
PAGES_DIR = OUT_DIR / "pages"
META_DIR = OUT_DIR / "meta"
PROGRESS_FILE = DATA_DIR / ".progress.json"

TABLES_PATH = OUT_DIR / "tables.parquet"
INCIDENTS_PATH = OUT_DIR / "incidents.parquet"
PAGES_META_PATH = META_DIR / "pages.parquet"

SITE = "https://space.skyrocket.de"
SEED = SITE + "/index.html"
HOST = urlparse(SITE).hostname

MAX_RETRIES = 3
DEFAULT_DELAY = 1.5
DEFAULT_MAX_PAGES = 8000
SATELLITE_PREFIXES = ("/doc_sdat/", "/doc_sat/", "/directories/sat")
SKIP_SUFFIX = (
    ".gif", ".jpg", ".jpeg", ".png", ".css", ".js", ".pdf", ".zip",
    ".ico", ".svg", ".webp", ".mp4", ".mov", ".ppt", ".kml", ".csv", ".dat",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("fetch_gunter")


def load_progress() -> tuple[set[str], deque[str]]:
    if not PROGRESS_FILE.exists():
        return set(), deque()
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        completed = set(data.get("completed", []))
        frontier = deque(data.get("frontier", []))
        return completed, frontier
    except (json.JSONDecodeError, OSError):
        LOG.warning("Could not read %s, starting from scratch.", PROGRESS_FILE)
        return set(), deque()


def save_progress(completed: set[str], frontier: deque[str]) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(
        json.dumps(
            {"completed": sorted(completed), "frontier": list(dict.fromkeys(frontier))},
            indent=2,
        ),
        encoding="utf-8",
    )


def load_existing(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        df = pd.read_parquet(path)
        return df.to_dict("records") if not df.empty else []
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Could not load %s: %s", path, exc)
        return []


def flush(tables_rows: list[dict], incidents_rows: list[dict], pages_meta: list[dict]) -> None:
    if tables_rows:
        TABLES_PATH.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(tables_rows).drop_duplicates().to_parquet(
            TABLES_PATH, engine="pyarrow", index=False
        )
    if incidents_rows:
        INCIDENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(incidents_rows).drop_duplicates().to_parquet(
            INCIDENTS_PATH, engine="pyarrow", index=False
        )
    if pages_meta:
        META_DIR.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(pages_meta).drop_duplicates(subset=["url"]).to_parquet(
            PAGES_META_PATH, engine="pyarrow", index=False
        )


def page_key(url: str) -> str:
    return "gunter:page:" + hashlib.sha1(url.encode("utf-8")).hexdigest()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    parts = parsed.path.split("/")
    collapsed = []
    prev = None
    for part in parts:
        if part == prev:
            continue
        collapsed.append(part)
        prev = part
    return urlunsplit((parsed.scheme, parsed.netloc, "/".join(collapsed), parsed.query, ""))


def rebuild_frontier(completed: set[str], frontier: deque[str]) -> deque[str]:
    known = set(completed)
    rebuilt = deque()
    for url in frontier:
        canonical = normalize_url(url)
        key = page_key(canonical)
        if key in known:
            continue
        known.add(key)
        rebuilt.append(canonical)
    if len(rebuilt) != len(frontier):
        LOG.info("Dropped %d alias/duplicate URLs from the frontier.", len(frontier) - len(rebuilt))
    return rebuilt


def prune_aliases() -> None:
    meta = load_existing(PAGES_META_PATH)
    if not meta:
        return
    canonical_urls = {row["url"] for row in meta if normalize_url(row["url"]) == row["url"]}
    alias_ids = {row["page_id"] for row in meta if normalize_url(row["url"]) != row["url"]}
    dropped = len(alias_ids)
    if not dropped:
        LOG.info("No alias pages to prune.")
        return
    LOG.info("Pruning %d alias pages and their parquet rows...", dropped)
    for pid in alias_ids:
        for ext in ("html", "txt"):
            page_file = PAGES_DIR / f"{pid}.{ext}"
            if page_file.exists():
                page_file.unlink()
    kept_meta = [row for row in meta if row["url"] in canonical_urls]
    META_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(kept_meta).to_parquet(PAGES_META_PATH, engine="pyarrow", index=False)
    for label, path in (("tables", TABLES_PATH), ("incidents", INCIDENTS_PATH)):
        rows = load_existing(path)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        clean = df[[normalize_url(u) == u for u in df["source_url"]]]
        path.parent.mkdir(parents=True, exist_ok=True)
        clean.to_parquet(path, engine="pyarrow", index=False)
        LOG.info("  %s: %d -> %d rows", label, len(df), len(clean))


def load_disallow(session: requests.Session, delay: float) -> list[str]:
    try:
        resp = fetch(session, SITE + "/robots.txt", delay)
        rules = []
        for line in resp.text.splitlines():
            line = line.strip().lower()
            if line.startswith("disallow"):
                value = line.split(":", 1)[1].strip().strip("/") if ":" in line else ""
                if value:
                    rules.append("/" + value)
        return rules
    except Exception as exc:  # noqa: BLE001
        LOG.warning("No usable robots.txt (%s); crawling without restrictions.", exc)
        return []


def fetch(session: requests.Session, url: str, delay: float) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(delay)
        try:
            resp = session.get(url, timeout=120, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status < 500:
                raise
            backoff = delay * (2 ** attempt)
            LOG.warning(
                "  Attempt %d/%d for %s failed: %s - retrying in %.1fs",
                attempt, MAX_RETRIES, url, exc, backoff,
            )
            time.sleep(backoff)
        except requests.RequestException as exc:
            backoff = delay * (2 ** attempt)
            LOG.warning(
                "  Attempt %d/%d for %s failed: %s - retrying in %.1fs",
                attempt, MAX_RETRIES, url, exc, backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(f"GET {url} failed after {MAX_RETRIES} retries")


def extract_index_tables(soup: BeautifulSoup, url: str) -> list[dict]:
    rows: list[dict] = []
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
        lowered = [h.lower() for h in headers]
        if not headers or "cospar" not in lowered:
            continue
        if not any("launch vehicle" in h or h == "vehicle" for h in lowered):
            continue
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) >= len(headers):
                record = dict(zip(headers, cells))
                record["source_url"] = url
                rows.append(record)
    return rows


def extract_incidents(soup: BeautifulSoup, url: str) -> list[dict]:
    rows: list[dict] = []
    path = urlparse(url).path

    if "comsat_failures" in url:
        for table in soup.find_all("table"):
            if table.get("id") == "satflist":
                for tr in table.find_all("tr"):
                    cells = tr.find_all("td")
                    if len(cells) >= 2:
                        name = cells[0].get_text(" ", strip=True)
                        text = cells[1].get_text(" ", strip=True)
                        if name and text:
                            rows.append({
                                "satellite": name,
                                "text": text,
                                "source_url": url,
                                "kind": "comsat_failure",
                            })
        return rows

    if path.startswith("/doc_sdat/"):
        desc = soup.find("div", id="satdescription")
        if desc is not None:
            title = soup.find("h1")
            name = title.get_text(" ", strip=True) if title else ""
            text = desc.get_text(" ", strip=True)
            if name and text:
                rows.append({
                    "satellite": name,
                    "text": text,
                    "source_url": url,
                    "kind": "description",
                })

    return rows


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl Gunter's Space Page and save raw + extracted data."
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Sleep in seconds between requests (default: 1.5).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help="Stop after this many downloaded pages (default: 8000).",
    )
    parser.add_argument(
        "--path-prefix",
        action="append",
        default=None,
        help="Only visit paths starting with this prefix (repeatable).",
    )
    parser.add_argument(
        "--satellites",
        action="store_true",
        help="Crawl only the satellite sections: doc_sdat/, doc_sat/, directories/sat*.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Ignore existing progress and re-crawl everything.",
    )
    parser.add_argument(
        "--prune-aliases",
        action="store_true",
        help="Delete duplicate pages reached through redirect aliases and rebuild parquets, then exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    PAGES_DIR.mkdir(parents=True, exist_ok=True)

    if args.prune_aliases:
        prune_aliases()
        return 0

    session = requests.Session()
    session.headers.update({"User-Agent": "cme-sentinel/0.1 research crawler (+politeness)"})

    disallow = load_disallow(session, args.delay)
    completed, old_frontier = (set(), deque()) if args.reset else load_progress()
    frontier = deque() if args.reset else rebuild_frontier(completed, old_frontier)
    seen = set(completed) | set(frontier)

    prefixes = list(SATELLITE_PREFIXES) if args.satellites else (args.path_prefix or [])

    if not frontier:
        initial_seeds = [SEED]
        if args.satellites:
            initial_seeds.append(SITE + "/doc_sat/comsat_failures.htm")
        for seed in initial_seeds:
            seed_key = page_key(seed)
            if seed_key not in seen:
                seen.add(seed_key)
                frontier.append(seed)

    tables_rows = load_existing(TABLES_PATH)
    incidents_rows = load_existing(INCIDENTS_PATH)
    pages_meta = load_existing(PAGES_META_PATH)

    handled = 0
    failed = 0

    while frontier and handled < args.max_pages:
        url = frontier.popleft()
        key = page_key(url)
        if key in completed:
            continue
        seen.add(key)

        path = urlparse(url).path
        if any(path.startswith(rule) for rule in disallow):
            LOG.info("Skipping disallowed path: %s", url)
            completed.add(key)
            continue

        try:
            resp = fetch(session, url, args.delay)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Failed %s: %s", url, exc)
            completed.add(key)
            save_progress(completed, frontier)
            failed += 1
            continue

        final_url = normalize_url(resp.url)
        if final_url != url:
            final_key = page_key(final_url)
            if final_key in completed:
                continue
        url = final_url
        key = page_key(url)

        soup = BeautifulSoup(resp.text, "html.parser")
        page_id = key.split(":")[-1][:8]

        (PAGES_DIR / f"{page_id}.html").write_bytes(resp.content)
        (PAGES_DIR / f"{page_id}.txt").write_text(soup.get_text("\n"), encoding="utf-8")

        before = len(tables_rows)
        tables_rows.extend(extract_index_tables(soup, url))
        has_index = len(tables_rows) > before

        before = len(incidents_rows)
        incidents_rows.extend(extract_incidents(soup, url))
        has_incident = len(incidents_rows) > before

        pages_meta.append({
            "url": url,
            "page_id": page_id,
            "title": soup.title.get_text(strip=True) if soup.title else "",
            "has_index_table": has_index,
            "has_incident": has_incident,
            "size_bytes": len(resp.content),
            "fetched_at": datetime.now(timezone.utc),
        })

        completed.add(key)
        handled += 1
        if handled % 50 == 0:
            save_progress(completed, frontier)
            flush(tables_rows, incidents_rows, pages_meta)
            LOG.info("  crawled %d pages...", handled)

        for a in soup.find_all("a", href=True):
            link = normalize_url(urljoin(url, urldefrag(a["href"])[0]))
            parsed = urlparse(link)
            if parsed.scheme not in ("http", "https") or parsed.hostname != HOST:
                continue
            if parsed.path.lower().endswith(SKIP_SUFFIX):
                continue
            if prefixes and not any(parsed.path.startswith(p) for p in prefixes):
                continue
            if any(parsed.path.startswith(rule) for rule in disallow):
                continue
            link_key = page_key(link)
            if link_key not in seen:
                seen.add(link_key)
                frontier.append(link)

    save_progress(completed, frontier)
    flush(tables_rows, incidents_rows, pages_meta)
    LOG.info(
        "Done: %d pages crawled, %d failed, %d queued, "
        "%d index-table rows, %d incidents, %d pages on disk",
        handled, failed, len(frontier), len(tables_rows), len(incidents_rows), len(pages_meta),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())