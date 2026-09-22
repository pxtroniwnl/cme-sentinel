"""
build_gunter_failures.py
========================

Derive satellite-failure records from the Gunter's Space Page crawl.

Inputs (written by fetch_gunter.py):
* data/gunter/tables.parquet   -- launch registry (Satellite, COSPAR, Date,
                                  Launch Vehicle, Remarks, source_url)
* data/gunter/incidents.parquet -- narrative blocks per mission family
                                  (description) and the comsat_failures list.

Method
------
For each mission family, the free-form narrative ("#satdescription") and the
launch-table Remarks are scanned sentence by sentence for failure signals and
year mentions. A sentence counts as a failure signal when it matches an
unambiguous disposition phrase ("stopped operating", "went out of service",
"no longer operational", "failed to deploy", ...) or a weaker mention
("failed", "failure", "malfunction", "lost command", "anomaly", "drifted",
loss of a component wheel/array/thruster) provided it does not also describe an
instrument or scientific objective (monitoring, detection, shielding,
prototypes, studies of failure mechanisms...). A record is emitted when at
least one failure signal is found. Space-weather drivers (and
propulsion/power/attitude/comms terms) never trigger detection on their own;
they only feed cause_category, which is scored on the matched sentences only.

* failure_year: the last year mentioned next to a failure signal, preferring
  years >= --min-year when present (heuristic, documented as such).
* window_2012_plus: failure_year >= 2012, or (no failure_year parsed AND the
  mission launched in a year >= 2012 AND a failure signal exists).
* recovered: narrative signals that the spacecraft came back (e.g. "regained
  control", "operating normally").
* cause_category: first matching category from a keyword map
  (space_weather / propulsion / power / attitude / comms / other).

Caveats
-------
Gunter's data is free prose, not a structured failure table. The comsat_failures
list only covers ~pre-2004 events; recent failures live in per-mission prose.
Coverage is therefore partial and heuristic -- review the flagged rows before
quoting counts. Not every retired satellite carries an explicit failure line.

Attribution
-----------
Content is (c) Gunter Dirk Krebs 1996-2026, Gunter's Space Page. Keep
source_url on every row and cite the page in any output.

Usage
-----
    python scripts/build_gunter_failures.py                       # all rows
    python scripts/build_gunter_failures.py --min-year 2012       # default
    python scripts/build_gunter_failures.py --out data/gunter/failures.parquet
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
GUNTER_DIR = DATA_DIR / "gunter"
TABLES_PATH = GUNTER_DIR / "tables.parquet"
INCIDENTS_PATH = GUNTER_DIR / "incidents.parquet"
OUT_PATH = GUNTER_DIR / "failures.parquet"

DEFAULT_MIN_YEAR = 2012

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

DISPOSITION_PATTERNS = (
    re.compile(r"\bstopped\s+(?:working|operating|functioning|responding)\b"),
    re.compile(r"\bceased\s+(?:working|operating|functioning|responding)\b"),
    re.compile(r"\bdid\s+not\s+(?:deploy|activate|respond|separate)\b"),
    re.compile(r"\bfailed\s+to\s+(?:deploy|separate|activate|respond)\b"),
    re.compile(r"\bwas\s+not\s+(?:deployed|activated|recovered)\b"),
    re.compile(r"\b(?:went|goes?)\s+out\s+of\s+(?:service|order)\b"),
    re.compile(r"\bout\s+of\s+(?:service|order)\b"),
    re.compile(r"\binoperative\b"),
    re.compile(r"\bunresponsive\b"),
    re.compile(r"\bno\s+longer\s+(?:operational|operating|functioning|worked|functioned|operated)\b"),
    re.compile(r"\bdecommission\w*\b"),
    re.compile(r"\bdeorbited\b"),
    re.compile(r"\bcrashed\b"),
    re.compile(r"\bdestroyed\b"),
    re.compile(r"\bexploded\b"),
    re.compile(r"\bknocked\s+out\b"),
    re.compile(r"\bpower\s+outage\b"),
    re.compile(r"\bend\s+of\s+(?:its\s+|the\s+)?mission\b"),
    re.compile(r"\bmission\s+ended\b"),
    re.compile(r"\bdied\b"),
)

MENTION_PATTERNS = (
    re.compile(r"\bfailed\b"),
    re.compile(r"\bfailure\b"),
    re.compile(r"\bmalfunction\w*\b"),
    re.compile(r"\bdamaged\b"),
    re.compile(r"\boutage\b"),
    re.compile(r"\b(?:lost|loss\s+of)\s+(?:the\s+ability|command\w*|control\w*|link\w*|contact\w*|telemetry|signals?)\b"),
    re.compile(r"\b(?:lost|loss\s+of)\s+(?:one\s+of\s+(?:its\s+|the\s+)?|two\s+|three\s+|four\s+)?(?:momentum|reaction)?\s*wheels?\b"),
    re.compile(r"\b(?:lost|loss\s+of)\s+(?:a\s+)?(?:thruster\w*|solar\s+array|solar\s+panel|batter\w*|transponder\w*|power\w*)\b"),
    re.compile(r"\banomal\w*\b"),
    re.compile(r"\bdrifte?d\b|\bdrifting\b"),
    re.compile(r"\bhad\s+to\s+be\s+(?:retired|replaced|switched|taken\s+out)\b"),
)

NEGATIVE_CONTEXT = (
    "monitor", "measure", "study", "characteriz", "assess", "observe",
    "detect", "instrument", "payload", "sensor", "scientific", "objective",
    "demonstrate", "evaluate", "test", "prototype", "validation", "experiment",
    "radiation-resistant", "radiation-hardened", "shield", "hardened",
    "background", "mitigation", "toolkit", "simulation", "model",
    "can lead to", "may result", "could", "risk of", "prone to",
    "vulnerable to", "essential to", "important to", "aims to", "helps",
    "help", "understand", "leads to", "cosmic", "ray", "flux", "particle",
    "energetic", "environment", "sep", "detector", "telescope", "spectromet",
)

ALL_FAILURE_PATTERNS = DISPOSITION_PATTERNS + MENTION_PATTERNS

DRIVER_PATTERNS = {
    "space_weather": (
        "solar flare", "solar storm", "space weather", "geomagnetic",
        "magnetic storm", "coronal mass ejection", "cme", "solar radiation",
        "solar activity", "solar energetic",
    ),
    "propulsion": (
        "thruster", "propulsion", "ion engine", "propellant", "stationkeeping",
        "pressur", "fuel",
    ),
    "power": (
        "solar array", "solar panel", "battery", "power subsystem", "bus voltage",
    ),
    "attitude": (
        "momentum wheel", "reaction wheel", "gyro", "attitude control",
        "star tracker", "attitude",
    ),
    "comms_command": (
        "commanding", "command link", "telemetry link", "spacecraft control",
        "scp", "processor", "flight computer", "controller",
    ),
}

RECOVERY_KEYWORDS = (
    "recovered", "regained", "regain", "operating normally", "returned to",
    "back in service", "restored", "was regained", "nominal", "recovered control",
    "resumed", "back to normal", "was restored",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("build_gunter_failures")


def split_sentences(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+|\n+", text) if s.strip()]


def years_in(text: str) -> list[int]:
    return [int(y) for y in re.findall(r"\b(?:19|20)\d{2}\b", text)]


def detect_failure(text: str) -> dict:
    lowered = text.lower()
    sentences = split_sentences(text)
    hits: list[tuple[str, list[int]]] = []
    for sentence in sentences:
        s_low = sentence.lower()
        negative = any(term in s_low for term in NEGATIVE_CONTEXT)
        if any(p.search(s_low) for p in DISPOSITION_PATTERNS):
            hits.append((sentence, years_in(sentence)))
        elif any(p.search(s_low) for p in MENTION_PATTERNS) and not negative:
            hits.append((sentence, years_in(sentence)))

    if not hits:
        return {"has_failure": False, "reason": "", "failure_year": None,
                "all_years": years_in(text), "recovered": False,
                "cause_category": "none"}

    all_failure_years = sorted({y for _, ys in hits for y in ys})
    recent = [y for y in all_failure_years if y >= DEFAULT_MIN_YEAR]
    failure_year = recent[-1] if recent else (all_failure_years[-1] if all_failure_years else None)

    reason = " | ".join(dict.fromkeys(h[0] for h in hits))
    recovered = any(kw in lowered for kw in RECOVERY_KEYWORDS)
    hit_text = " ".join(h[0] for h in hits).lower()
    cause = "other"
    for category, terms in DRIVER_PATTERNS.items():
        if any(term in hit_text for term in terms):
            cause = category
            break
    match_pats = [p for p in ALL_FAILURE_PATTERNS if p.search(lowered)]
    keywords = sorted({p.pattern for p in match_pats})

    return {"has_failure": True, "reason": reason, "failure_year": failure_year,
            "all_years": years_in(text), "recovered": recovered,
            "cause_category": cause, "keywords": keywords}


INDEX_MARKERS = ("/doc_sdat/directories/", "/doc_sat/directories/", "/directories/sat")

def is_index_page(url: str) -> bool:
    return any(mark in url for mark in INDEX_MARKERS)


def build_records() -> list[dict]:
    tables_df = pd.read_parquet(TABLES_PATH) if TABLES_PATH.exists() else pd.DataFrame()
    incidents_df = pd.read_parquet(INCIDENTS_PATH) if INCIDENTS_PATH.exists() else pd.DataFrame()

    if tables_df.empty and incidents_df.empty:
        LOG.error("No data found in %s / %s", TABLES_PATH, INCIDENTS_PATH)
        return []

    tables_df = tables_df.rename(columns={"": "Success"})
    launch_rows: list[dict] = []
    for row in tables_df.to_dict("records"):
        row = dict(row)
        source_url = str(row.get("source_url") or "").strip()
        if is_index_page(source_url):
            continue
        launch_rows.append({
            "satellite": str(row.get("Satellite") or "").strip(),
            "cospar": str(row.get("COSPAR") or "").strip(),
            "launch_date": str(row.get("Date") or "").strip(),
            "launch_vehicle": str(row.get("Launch Vehicle") or "").strip(),
            "remarks": str(row.get("Remarks") or "").strip(),
            "success": str(row.get("Success") or "").strip(),
            "source_url": source_url,
        })

    incident_rows: list[dict] = []
    for row in incidents_df.to_dict("records"):
        source_url = str(row.get("source_url") or "").strip()
        if is_index_page(source_url):
            continue
        incident_rows.append({
            "kind": str(row.get("kind") or "description"),
            "satellite": str(row.get("satellite") or "").strip(),
            "text": str(row.get("text") or "").strip(),
            "source_url": source_url,
        })

    by_mission: dict[str, dict] = {}
    for inc in incident_rows:
        mission = inc["satellite"] or inc["source_url"]
        entry = by_mission.setdefault(mission, {
            "satellite": inc["satellite"], "kind": inc["kind"], "text": "",
            "remarks": "", "cospars": [], "launch_years": [],
            "source_urls": set(),
        })
        entry["text"] = ((entry["text"] + " " + inc["text"]).strip())
        entry["source_urls"].add(inc["source_url"])

    for lr in launch_rows:
        mission = lr["satellite"] or lr["source_url"]
        entry = by_mission.setdefault(mission, {
            "satellite": lr["satellite"], "kind": "launch", "text": "",
            "remarks": "", "cospars": [], "launch_years": [],
            "source_urls": set(),
        })
        if lr["cospar"]:
            entry["cospars"].append(lr["cospar"])
        launch_year = None
        m = re.match(r"^(\d{4})-", lr["cospar"])
        if m:
            launch_year = int(m.group(1))
        else:
            m = re.search(r"\b(?:19|20)\d{2}\b", lr["launch_date"])
            launch_year = int(m.group(0)) if m else None
        if launch_year:
            entry["launch_years"].append(launch_year)
        if lr["remarks"]:
            entry["remarks"] = ((entry["remarks"] + " " + lr["remarks"]).strip())
        entry["source_urls"].add(lr["source_url"])

    records: list[dict] = []
    for mission, entry in by_mission.items():
        combined = f"{entry['text']} {entry['remarks']}".strip()
        det = detect_failure(combined)
        launch_years = sorted(set(entry["launch_years"]))
        launch_year = launch_years[0] if launch_years else None
        cospars = sorted(set(entry["cospars"]))
        recent_launch = launch_year is not None and launch_year >= DEFAULT_MIN_YEAR

        if det["has_failure"]:
            records.append({
                "satellite": entry["satellite"],
                "kind": entry["kind"],
                "cospars": ", ".join(cospars[:20]),
                "launch_year": launch_year,
                "failure_year": det["failure_year"],
                "window_2012_plus": (det["failure_year"] is not None and det["failure_year"] >= DEFAULT_MIN_YEAR)
                                    or (det["failure_year"] is None and recent_launch),
                "recovered": det["recovered"],
                "cause_category": det["cause_category"],
                "reason": det["reason"],
                "source_url": next(iter(entry["source_urls"])) if entry["source_urls"] else "",
                "all_text": combined,
            })
    return records


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a satellite-failure dataset from the Gunter's crawler output."
    )
    parser.add_argument(
        "--min-year", type=int, default=DEFAULT_MIN_YEAR,
        help="Window year for window_2012_plus (default: 2012).",
    )
    parser.add_argument(
        "--out", type=Path, default=OUT_PATH,
        help="Output parquet path (default: data/gunter/failures.parquet).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    records = build_records()
    if not records:
        LOG.error("No failure records produced.")
        return 1

    out = args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records).sort_values(
        ["failure_year", "satellite"], na_position="last"
    )
    df.to_parquet(out, engine="pyarrow", index=False)

    in_window = df[df["window_2012_plus"]]
    LOG.info("Total failure records: %d", len(df))
    LOG.info("In window (>=%d): %d", args.min_year, len(in_window))
    LOG.info("By cause category: %s",
             df["cause_category"].value_counts().to_dict())
    LOG.info("Output written to %s", out)
    LOG.info("Attribution: content (c) Gunter Dirk Krebs 1996-2026, Gunter's Space Page.")
    return 0


if __name__ == "__main__":
    sys.exit(main())