# CME Sentinel

Historical **coronal mass ejection (CME) → satellite effect** research
project: establishing, from historical records, whether solar eruptions
measurably affect tracked objects and spacecraft — **orbital decay** via
storm-driven atmospheric drag and **electronics failures** — as a first step
toward **early warning** of such events.

This repository currently holds the **full historical orbital catalog** from
[Space-Track.org](https://www.space-track.org) (the `gp_history` API class)
stored locally as **Apache Parquet** files partitioned by year — every tracked
object (all `NORAD_CAT_ID`s), all time from **1960-01-01 to today**, with the
complete set of Orbit Mean-Elements Message (OMM) columns exposed by the API
(orbital elements, satellite-catalog metadata, and the raw TLE lines).

The space-weather side of the causal chain (CME catalogs, geomagnetic
indices, solar wind) is **written but not yet downloaded** — the collector
scripts are ready to run and documented in the
[causal study data sources](#causal-study-data-sources) section and the
[data collection runbook](#ready-to-run-collection-scripts).

---

## Table of contents

- [Why this project](#why-this-project)
- [Causal study data sources](#causal-study-data-sources)
  - [The causal chain](#the-causal-chain)
  - [Source comparison](#source-comparison)
  - [ESA Anomaly Dataset (documented reference)](#esa-anomaly-dataset-documented-reference)
  - [Satellite status & failure reason (extra layers)](#satellite-status--failure-reason-extra-layers)
  - [Ready-to-run collection scripts](#ready-to-run-collection-scripts)
    - [OMNI](#omni-hourly-solar-wind--indices)
    - [DONKI](#donki-space-weather-events)
    - [Gunter's Space Page](#gunters-space-page-satellite-status--failure)
  - [Design decisions & caveats](#design-decisions--caveats)
- [How the data source works](#how-the-data-source-works)
- [Requirements](#requirements)
- [Setup](#setup)
- [How the script works (how the data was obtained)](#how-the-script-works-how-the-data-was-obtained)
  - [1. Login and session](#1-login-and-session)
  - [2. Time windows](#2-time-windows)
  - [3. Streaming the download](#3-streaming-the-download)
  - [4. Auto-subdivision for large windows](#4-auto-subdivision-for-large-windows)
  - [5. Resume support](#5-resume-support)
  - [6. Parquet output](#6-parquet-output)
- [Sample data by era](#sample-data-by-era)
  - [1960 – the dawn of the space age](#1960--the-dawn-of-the-space-age)
  - [1980 – the cold war catalog](#1980--the-cold-war-catalog)
  - [2010 – the pre-megaconstellation era](#2010--the-pre-megaconstellation-era)
  - [2024 – the Starlink era](#2024--the-starlink-era)
- [Data dictionary (what every variable means)](#data-dictionary-what-every-variable-means)
  - [1. Object identification](#1-object-identification)
  - [2. Classic orbital elements](#2-classic-orbital-elements)
  - [3. SGP4 propagation coefficients](#3-sgp4-propagation-coefficients)
  - [4. Derived orbital parameters](#4-derived-orbital-parameters)
  - [5. Mission metadata](#5-mission-metadata)
  - [6. API / data-frame metadata](#6-api--data-frame-metadata)
  - [7. Raw TLE lines](#7-raw-tle-lines)
  - [8. Partition column](#8-partition-column)
- [Reading the data](#reading-the-data)
- [Verification](#verification)
- [Considerations, rate limits and policy](#considerations-rate-limits-and-policy)
- [Project layout](#project-layout)

---

## Why this project

Space-Track's `gp_history` endpoint exposes the complete historical archive
of US Space Force element sets. A single API query cannot return all of it:
the server times out on huge date ranges, and the API guidelines throttle
how fast you may query.

This tool solves that by:

1. Breaking the 1960→today range into **time windows** (small enough that
   each request succeeds).
2. Downloading every window **as CSV** (the most compact format) and
   streaming it **line by line**, so memory usage stays bounded.
3. Automatically **splitting any window that is too big** until it fits.
4. Saving the result as **Parquet partitioned by year**, ready for fast
   analysis with pandas/polars.

No filtering is applied: **everything is downloaded**. Filtering (e.g.
LEO-only, payload-only, decayed objects) can be done later at query time.

This repository currently contains a download of **67.2 million element
sets** covering **63,762 distinct objects** across **67 years of data**
(1960–2026), split into **455 Parquet part files** (~12 GB on disk).

---

## Causal study data sources

The long-term goal of this repo is to test whether **CMEs causally affect
tracked satellites** and, ultimately, to give **early warning**. Four families
of data are involved; each plays a fixed role in the causal chain.

### The causal chain

Two physically distinct pathways connect a CME at the Sun to an effect on a
satellite:

| Pathway | Chain | Observable in the catalog |
|---------|-------|---------------------------|
| Orbit decay | CME → reaches Earth → **geomagnetic storm** (Dst ↓, Kp ↑) → **thermospheric heating / density ↑** → **drag** ↑ → **orbital decay** | `MEAN_MOTION` ↑, `PERIAPSIS` ↓, `DECAY_DATE` set (re-entry) |
| Electronics failure | CME / SEP → **solar energetic particles** reach orbit → radiation damage, **single-event upsets** | telemetry anomalies / loss of signal (not visible in TLEs alone) |

The canonical validation case for the pipeline is the **Starlink batch lost in
February 2022**: a CME-driven geomagnetic storm thickened the thermosphere and
~40 Starlink satellites re-entered within days — visible in the catalog as
clustered `DECAY_DATE`s right after a strong storm.

### Source comparison

| Source | Measures | Coverage | Role in the chain | Access |
|--------|----------|----------|-------------------|--------|
| **Space-Track `gp_history`** (already collected) | Orbital state of every object (TLE/OMM) | 1960 → today | **Effect**: drag, decay, re-entry | CSV → Parquet (this repo) |
| **ESA Anomaly Dataset** | On-board telemetry (housekeeping) + curated anomaly labels | 3 missions, ~25.5 y (**timestamps anonymised**) | ❌ **Not usable** for correlation (see below) | Zenodo zip (~11.6 GB) |
| **DONKI** (NASA CCMC) | Discrete space-weather events: CME (3D cone), geomagnetic storms (GST), SEP, solar flares, HSS | ≈2010/2013 → today | **Event framework**: what happened and when | REST JSON (free NASA API key) → `scripts/fetch_donki.py` |
| **OMNI** (NASA GSFC) | Continuous solar wind + **Dst, Kp, proton fluxes** | Hourly **1963** → today; 5-min 1995 → today | **Continuous driver** + geomagnetic response | HAPI CSV (SPDF) → `scripts/fetch_omni.py` |

### ESA Anomaly Dataset (documented reference)

The [ESA Anomaly Dataset](https://github.com/esa/anomaly-dataset) is the first
large-scale dataset of **real satellite telemetry** (housekeeping: currents,
voltages, temperatures, states) with **anomaly annotations curated by ESA
mission-operations engineers**. It was produced by Airbus Defence and Space,
KP Labs and ESA/ESOC under the A²I roadmap.

- Data: Zenodo `10.5281/zenodo.12528696` (3 zips, ~11.6 GB)
- Paper: arXiv [`2406.17826`](https://arxiv.org/abs/2406.17826); journal
  version [DMLR](https://data.mlr.press/assets/pdf/v03-23.pdf)
- Benchmark code: [`kplabs-pl/ESA-ADB`](https://github.com/kplabs-pl/ESA-ADB)
  (TimeEval-based pipeline; Kaggle: `esa-adb-challenge`)

| | Mission1 | Mission2 | Mission3 |
|---|---|---|---|
| Channels (target) | 76 (58) | 100 (47) | 48 (24) |
| Telecommands | 698 | 123 | 0 |
| Duration (anonymised) | 14 y | 3.5 y | 8 y |
| Data points | ~775 M | ~777 M | ~745 M |
| Annotated (%) | 1.80 | 0.58 | 1.03 |
| Events | 200 | 644 | 586 |
| Anomalies | 118 | 31 | 8 |
| Rare nominal events | 78 | 613 | 25 |
| Gaps / invalid | 4 / 0 | 0 / 0 | 397 / 156 |

Each mission folder ships `channels/<param>.zip` (per-channel time series),
`telecommands/<tc>.zip`, `labels.csv` and `anomaly_types.csv` (`Anomaly` /
`Rare Event` / `Gap`). Mission3 is excluded from the benchmark upstream
(trivial anomalies).

> **Why it is not in the causal pipeline**: the dataset is fully anonymised —
> channel names, mission identity **and the time axis**. The paper states the
> anonymisation *prevents expecting anomalies at specific times, e.g. during
> increased solar activity*. Because the timestamps cannot be aligned with
> DONKI/OMNI storm times, this dataset **cannot** contribute to the CME
> correlation study. It is documented here as the reference benchmark for
> spacecraft-telemetry anomaly detection, in case a telemetry-anomaly module
> is ever added.

### Satellite status & failure reason (extra layers)

Chosen as verification layers rather than bulk-cut into the pipeline:

| Source | Adds | Role |
|--------|------|------|
| **Gunter's Space Page** (`space.skyrocket.de`) | Per-satellite **status and failure cause** (e.g. "failed in 1998 due to momentum wheel problems") | Curated **secondary** reference to validate targeted events (e.g. the Feb-2022 Starlink case); HTML, no API — full-site crawl via `scripts/fetch_gunter.py` |
| **DISCOSweb** (ESA) | Object metadata + `reentryEpoch`, launches, fragmentations, re-entries | REST API (account) — complements Space-Track object metadata |
| **UCS Satellite Database** | Current operational status (Operational / Non-operational) | Snapshot of "alive today", no failure history |

Space-weather attribution itself comes from **DONKI (GST/SEP) + OMNI
(Dst/Kp/protons) + decay events in Space-Track**; these layers only answer
"why did this specific satellite stop working".

### Ready-to-run collection scripts

Three downloaders are written, follow the same conventions as
`fetch_gp_history.py`, and are **documented below with nothing executed
yet**. Run them in the order given (OMNI first — it is the continuous
driver — then DONKI, then Gunter's, which is the slowest). Every script
resumes safely from `data/.progress.json`; `--reset` is only for
intentional re-downloads.

| Script | Source | Command | Needs |
|--------|--------|---------|-------|
| `fetch_omni.py` | OMNI hourly merged (NASA SPDF, HAPI) | `python scripts/fetch_omni.py` | nothing (open data) |
| `fetch_donki.py` | DONKI (NASA public API) | `python scripts/fetch_donki.py` | `NASA_API_KEY` in `.env` |
| `fetch_gunter.py` | Gunter's Space Page (HTML crawl) | `python scripts/fetch_gunter.py` | nothing |

Add `--help` to any script for the full options list. Test runs:
`--limit 1` (OMNI/DONKI) or `--max-pages 10` (Gunter's).

#### OMNI (hourly solar wind & indices)

**Source**: NASA GSFC Space Physics Data Facility, CDAWeb HAPI server,
dataset `OMNI_COHO1HR_MERGED_MAG_PLASMA`
(https://cdaweb.gsfc.nasa.gov/hapi) — DOI `10.48322/6ffx-3441`
(King & Papitashvili). Open data, CC0, no key required.

**What it brings**: 1 row per hour, **1963 → today** (~565,000 rows) — the
continuous geomagnetic/generic response that drives the orbital-decay
pathway.

| EPOCH | BZ_GSM (nT) | BGT (nT) | flow_speed (km/s) | proton_density (#/cm³) | DST (nT) | Kp | AE_INDEX (nT) | P<10MeV_flux |
|---|---|---|---|---|---|---|---|---|
| 2022-02-03 11:00 | −8.2 | 12.5 | 615 | 7.4 | −42 | 37 | 618 | 0.02 |
| 2022-02-03 12:00 | −15.4 | 18.9 | 689 | 9.1 | −85 | 57 | 1248 | 0.04 |
| 2022-02-03 13:00 | −19.8 | 22.1 | 702 | 10.2 | −112 | 73 | 1866 | 0.03 |

*Illustrative rows; the exact parameter set is resolved at runtime from the
HAPI `/info` endpoint (IMF, plasma, Dst/Kp/AE/AL/AU/ap, sunspot, F10.7,
energetic protons).*

**Why this source**: OMNI is the standard "L1-monopole" dataset used by the
space-weather community: it time-shifts solar-wind measurements to the
Earth's bow-shock nose and stitches in the definitive **Dst**, **Kp** and
**proton flux** indices on the same hourly grid — one table you can join
everything against.

**How to run**:
```bash
python scripts/fetch_omni.py          # everything, 1963 → today
python scripts/fetch_omni.py --limit 1                      # one year, test
python scripts/fetch_omni.py --start 2022-01-01 --end 2022-12-31
```

**Output**: `data/omni/year=YYYY/part-00000.parquet` (one part per year,
`year` int column, HAPI fill values e.g. `-1e31` → `NaN`).
Estimated volume: a few hundred MB total; a few minutes to an hour of
download time depending on throttling (default 3 s per request).

#### DONKI (space-weather events)

**Source**: NASA public API `https://api.nasa.gov/DONKI/...` (Space Weather
Database Of Notifications, Knowledge, Information). US Government work,
public domain. **Requires a free key** (https://api.nasa.gov) in `.env`:

```dotenv
NASA_API_KEY=your_nasa_api_key_here
```

**What it brings**: 1 row per **event** — the discrete causal framework
(what happened and when). Endpoints fetched: `CME`, `CMEAnalysis`, `GST`,
`FLR`, `SEP`, `IPS`, `HSS`. Records are JSON with nested arrays that the
script flattens into columns (the arrays are also kept serialized as JSON).

| activityID | startTime | sourceLocation | activeRegionNum | cmeAnalyses_speed_3d | cmeAnalyses_count | linked_activity_ids |
|---|---|---|---|---|---|---|
| 2022-02-01T17:00:00-CME-001 | 2022-02-01 17:00 | S24 | 12955 | 708 | 1 | [] |

| gstID | startTime | kpIndex | all_kp_max | linked_activity_ids |
|---|---|---|---|---|
| 2022-02-03T22:00:00-GST-001 | 2022-02-03 22:00 | 6.0 | 6.0 | [2022-02-01T17:00:00-CME-001] |

*(Illustrative rows; `linked_activity_ids` chains CME → GST/SEP for
attribution.)*

**Why this source**: DONKI adds what OMNI cannot — CME catalogs with **3D
direction** (`cmeAnalyses[].speed_3d`, `isEarthDirected`), geomagnetic
storm declarations, SEP/flare events and the **`linkedEvents` cause→effect
chains**, timestamped in real UTC.

**How to run**:
```bash
python scripts/fetch_donki.py                       # all endpoints, 2010 → today
python scripts/fetch_donki.py --endpoints GST,SEP   # subset
python scripts/fetch_donki.py --limit 1             # one (endpoint, year) pair
```

**Output**: `data/donki/<endpoint>/year=YYYY/part-00000.parquet`.
Estimated volume: hundreds of events per endpoint; small (a few MB).

#### Gunter's Space Page (satellite status & failure)

**Source**: https://space.skyrocket.de (G. Krebs) — HTML pages, no API.
Curated **secondary** reference: per-satellite status and failure cause,
including the free-text comsat-failure narratives.

**What it brings**: two extracted tables plus the raw pages:

1. **Index tables** → `data/gunter/tables.parquet` (rows scraped from the
   satellite-directory tables):

| satellite | cospar | date | ls | launch vehicle | remarks | source_url |
|---|---|---|---|---|---|---|
| GOES 9 (GOES J) | 1995-025A | 23.05.1995 | CC LC-36B | Atlas-1 | | https://space.skyrocket.de/doc_sdat/goes-i.htm |

2. **Comsat failure narratives** → `data/gunter/incidents.parquet`
   (heading + text pairs from `doc_sat/comsat_failures*` pages):

| satellite | text | source_url |
|---|---|---|
| DirecTV-6 | "…fell victim to a solar flare in April 1997 which knocked out three transponders…" | https://space.skyrocket.de/doc_sat/comsat_failures.htm |

3. **Raw content** → `data/gunter/pages/<page_id>.html` and `.txt`, plus a
   crawl map `data/gunter/meta/pages.parquet` — so future parsing can be
   improved without re-crawling.

**Why this source**: it is the closest thing to a "why did this specific
satellite stop working" record (dates are real but coarse, e.g. "April
1997", and causes are prose) — useful to **illustrate** cases, not for the
population-level statistics. It is the extra layer for the canonical
Feb-2022 Starlink validation. It is deliberately kept **out** of the core
event-study model.

**How to run**:
```bash
python scripts/fetch_gunter.py                        # full-site crawl (resume-safe)
python scripts/fetch_gunter.py --max-pages 100 --delay 2.0   # small test range
python scripts/fetch_gunter.py --path-prefix doc_sdat          # mission pages only
```

**Output**: `data/gunter/{tables,incidents}.parquet`,
`data/gunter/pages/*.{html,txt}`, `data/gunter/meta/pages.parquet`.
The crawl respects `robots.txt`, sleeps `--delay` (default 1.5 s) per
request and resumes via `data/.progress.json`; at 1.5 s one full run of
`--max-pages 5000` takes roughly two hours and ~100–200 MB on disk. Scale
`--max-pages` up and re-run to finish the whole site.

### Design decisions & caveats

Decisions recorded so the analysis (-phase) stays consistent:

- **ESA Anomaly Dataset is excluded from the causal pipeline** (see above):
  its time axis is anonymised, so its anomalies cannot be aligned with
  DONKI/OMNI storm times. Documented only.
- **Exact orbital position via SGP4**: the user chose propagating the
  TLEs with SGP4 rather than just orbit *type*. Caveat: propagation error
  grows with TLE age, so Phase-3 propagation should use the object's TLE
  closest to the storm (±7 days; Starlink updates TLEs ~6×/day, older
  catalog objects far less often).
- **Population-level event study**: compare decay rate inside post-storm
  windows vs. baseline across all objects (fixed-effects OLS), with the
  Starlink Feb-2022 re-entry cluster as a validation case (75 objects
  decayed in Feb-2022 in this catalog, incl. Starlink).
- **Real-data gotchas** to remember:
  - In `data/gp_history` `DECAY_DATE` of active objects is the **empty
    string `""`**, not `NaN` (the downloader used `keep_default_na=False`).
    Filter with `df["DECAY_DATE"].astype(str).ne("")`, not `.isna()`.
  - The OMNI HAPI endpoint that works is **`cdaweb.gsfc.nasa.gov/hapi`**;
    the `spdf.gsfc.nasa.gov/hapi` host returns 403.
  - OMNI fill values (`-1e31`, or `999.9` in some legacy outputs) are
    converted to `NaN` by `fetch_omni.py`; DONKI `Kp` is 10× the usual
    index in OMNI (mapped 0,0+→3, 1→10, …).
- **Gunter's role**: narrative layer for targeted validation (e.g. Starlink
  Feb-2022), never the causal engine.

Other historical sources were scoped earlier (CDAW/SOHO-LASCO CME catalog
1996+, CACTus 1997–2017, HELCATS/STEREO 2007–2017) and remain candidates.
DONKI/OMNI were chosen as the core because DONKI adds real 3D direction plus
SEP/GST events, and OMNI carries the continuous Dst/Kp/proton series.

---

## How the data source works

Space-Track.org is the US Space Force / 18th Space Defense Squadron site that
publishes unclassified space situational awareness data. Its REST API:

- **Login**: `POST https://www.space-track.org/ajaxauth/login` with
  `identity` + `password` (form fields). The response sets a session cookie
  that must be sent on subsequent requests.
- **Query**: `GET /basicspacedata/query/class/gp_history/<PREDICATE>.../<format>`
- **Predicates** support operators: `>` (greater than), `<` (less than),
  `--` (range), comma-separated lists, and relative times such as
  `>now-10`. A date range filter is written as `EPOCH/1960-01-01--1960-12-31/`.
- **Formats**: `xml`, `kvn`, `json`, `csv`, `tle`, `3le`, `html`.

The `spacetrack` Python package (used by this project) wraps all of the
above: it performs the login, keeps the session cookie, validates query
predicates, and enforces the **30 requests / minute** rate limit
automatically.

---

## Requirements

- Python **3.10+** (the repo pins 3.14).
- A registered, approved **Space-Track.org account**. New users must accept
  the Space-Track user agreement and wait for approval.
- A free **NASA API key** (for `fetch_donki.py` only; it already ships a
  `DEMO_KEY` fallback on the server side, but that is heavily rate-limited).
- Enough disk space for the full history (see
  [Considerations](#considerations-rate-limits-and-policy)).

---

## Setup

```bash
# 1. Create your virtual environment (if not already present) and activate it.
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies.
pip install -r requirements.txt

# 3. Provide your Space-Track credentials.
cp .env.example .env      # then edit .env
```

`.env` must contain:

```dotenv
SPACE_TRACK_EMAIL=your_email@example.com
SPACE_TRACK_PASSWORD=your_password_here
NASA_API_KEY=your_nasa_api_key_here        # optional for now (DONKI only)
```

> Note: `.env` and `data/` are already git-ignored, so credentials and
> downloaded raw data will never be committed.

---

## How the script works (how the data was obtained)

Run it with:

```bash
python scripts/fetch_gp_history.py
```

### 1. Login and session

`SpaceTrackClient(identity, password)` logs in lazily on the first request
and stores the session cookie. The script wraps the client in a context
manager (`with ... as st:`) so the session is closed cleanly at the end.

### 2. Time windows

The whole range (default `1960-01-01` → today) is divided into contiguous,
non-overlapping windows. The window size depends on the era:

| Era                   | Window size | Reason                                                    |
| --------------------- | ----------- | --------------------------------------------------------- |
| 2020 → today          | ~1 month    | Starlink era: millions of element sets per month.         |
| 2016 – 2019           | ~3 months   | Large constellations, heavy debris fields.                |
| 2008 – 2015           | ~6 months   | Moderately populated catalog.                            |
| before 2008 (and 1960)| ~1 year     | Sparse catalog, few launches, few events per year.       |

Every window is a closed interval `[start, end]`. Consecutive windows share
no records and leave no gaps (each window ends one microsecond before the
next begins).

### 3. Streaming the download

Each window is requested as:

```python
st.gp_history(epoch=op.inclusive_range(start, end),
              format="csv",
              orderby="EPOCH",
              iter_lines=True)
```

`iter_lines=True` streams the HTTP response line by line instead of loading
the whole result into memory. Lines are accumulated into batches of
`--batch-size` records (default 200 000); every batch is converted to a
DataFrame and written immediately as its own Parquet part file:

```
data/gp_history/year=YYYY/part-<window_start_date>-<batch>.parquet
```

To be a polite API consumer and stay well under the rate limits, every
window also sleeps a minimum `--min-interval` (default 2 s) before its
request and retries up to 3 times with exponential backoff on transient
failures (429, timeouts, resets).

### 4. Auto-subdivision for large windows

If a request keeps failing, the failing window is **split in half** at its
midpoint and both halves are retried. This repeats down to a minimum window
of 1 day and a maximum split depth of 10. In practice you are back-rated to
month/quarter windows from the start, so splitting is only a fallback for
dense months.

### 5. Resume support

Completed windows are recorded in `data/.progress.json`. On every run the
script loads this file and **skips windows that are already done**, so an
interrupted download (network drop, crash, machine sleep) can simply be
restarted with the same command. Use `--reset` to ignore the saved progress.

### 6. Parquet output

```
data/gp_history/
├── year=1960/part-19600101-00000.parquet
├── year=1960/part-19600101-00001.parquet
├── ...
├── year=2024/part-20240101-00000.parquet
└── ...
```

- One folder per calendar year (`year=YYYY`).
- One part file per batch of records, all with identical schemas.
- A convenience `year` integer column is added to every part.

---

## Sample data by era

The five columns `ECCENTRICITY`, `INCLINATION`, `MEAN_MOTION`, `PERIAPSIS`
and `APOAPSIS` are the most useful for a quick glance at how a satellite
moves. Below are **real rows** from the actual downloaded data.

### 1960 – the dawn of the space age

Only **51 distinct objects** were tracked in 1960 (737 element sets). All of
them are early US probes, launcher upper stages, and their debris.

| EPOCH | OBJECT_NAME | NORAD_CAT_ID | OBJECT_TYPE | OBJECT_ID | COUNTRY | ECCENTRICITY | INCLINATION (°) | MEAN_MOTION (rev/day) | PERIAPSIS (km) | APOAPSIS (km) | LAUNCH_DATE | DECAY_DATE | RCS_SIZE | SITE   |
|-------|-------------|--------------|-------------|-----------|---------|--------------|-----------------|-----------------------|----------------|---------------|-------------|------------|----------|--------|
| 1960-01-01 23:52:39 | VANGUARD R/B | 12 | ROCKET BODY | 1959-001B | US | 0.184 | 32.9 | 11.09 | 554 | 3,681 | 1959-02-17 | | MEDIUM | AFETR |
| 1960-01-02 11:15:40 | JUNO II R/B | 23 | ROCKET BODY | 1959-009B | US | 0.037 | 50.3 | 14.22 | 552 | 1,084 | 1959-10-13 | 1989-07-16 | MEDIUM | AFETR |
| 1960-01-02 12:02:07 | EXPLORER 7 | 22 | PAYLOAD | 1959-009A | US | 0.037 | 50.3 | 14.20 | 560 | 1,087 | 1959-10-13 | | MEDIUM | AFETR |

Note the high eccentricity of the Vanguard upper stage (0.184 → apogee of
3,681 km) and orbits are elliptical; most 1960s objects were still low
inclination (32–50°).

### 1980 – the cold war catalog

By 1980 the US, USSR (`CIS`), and France (`FR`) were all in space — **4,580
distinct objects** (501,840 rows). Debris fields were growing fast after the
1960s-70s breakup events.

| EPOCH | OBJECT_NAME | NORAD_CAT_ID | OBJECT_TYPE | OBJECT_ID | COUNTRY | ECCENTRICITY | INCLINATION (°) | MEAN_MOTION (rev/day) | PERIAPSIS (km) | APOAPSIS (km) | LAUNCH_DATE | DECAY_DATE | RCS_SIZE | SITE   |
|-------|-------------|--------------|-------------|-----------|---------|--------------|-----------------|-----------------------|----------------|---------------|-------------|------------|----------|--------|
| 1980-01-01 00:00:09 | SCOUT X-3 DEB (YO) | 523 | DEBRIS | 1962-071D | US | 0.005 | 90.5 | 14.84 | 583 | 648 | 1962-12-19 | 1980-11-30 | SMALL | AFWTR |
| 1980-01-01 00:00:18 | DELTA 1 R/B | 3094 | ROCKET BODY | 1968-002B | US | 0.032 | 105.8 | 12.84 | 1,081 | 1,570 | 1968-01-11 | | MEDIUM | AFWTR |
| 1980-01-01 00:01:10 | COSMOS 469 | 5721 | PAYLOAD | 1971-117A | CIS | 0.002 | 64.5 | 13.75 | 966 | 995 | 1971-12-25 | | LARGE | TTMTR |

Notice the near-circular orbits (eccentricities of 0.002–0.032), the
retrograde Delta stage at 105°, and the Soviet `COSMOS` payload — the catalog
is now truly international and includes long-lived debris (SCOUT X-3 debris
was still tracked 18 years after launch).

### 2010 – the pre-megaconstellation era

2010 holds **14,951 distinct objects** (1,015,278 rows). GEO satellites,
highly-elliptical science missions, and the debris cloud from the 2007
Chinese ASAT test (Fengyun-1C) were all being tracked.

| EPOCH | OBJECT_NAME | NORAD_CAT_ID | OBJECT_TYPE | OBJECT_ID | COUNTRY | ECCENTRICITY | INCLINATION (°) | MEAN_MOTION (rev/day) | PERIAPSIS (km) | APOAPSIS (km) | LAUNCH_DATE | DECAY_DATE | RCS_SIZE | SITE   |
|-------|-------------|--------------|-------------|-----------|---------|--------------|-----------------|-----------------------|----------------|---------------|-------------|------------|----------|--------|
| 2010-01-01 00:00:00 | THEMIS D | 30797 | PAYLOAD | 2007-004D | US | 0.751 | 3.6 | 1.00 | 4,125 | 67,437 | 2007-02-17 | | MEDIUM | AFETR |
| 2010-01-01 00:00:00 | INTELSAT 15 | 36106 | PAYLOAD | 2009-067A | ITSO | 0.000 | 0.0 | 1.01 | 35,664 | 35,688 | 2009-11-30 | | LARGE | TTMTR |
| 2010-01-01 00:00:13 | FENGYUN 1C DEB | 33710 | DEBRIS | 1999-025DGX | PRC | 0.006 | 99.3 | 13.98 | 856 | 947 | 1999-05-10 | | SMALL | TSC |

This era has it all: a highly-elliptical science orbit (THEMIS D:
eccentricity 0.75, apogee ~67,000 km), a **geostationary** satellite at
35,700 km with `MEAN_MOTION ≈ 1 rev/day`, and debris from the Fengyun-1C ASAT
test (Chinese `PRC` designation).

### 2024 – the Starlink era

By 2024 the catalog exploded to **29,814 distinct objects** (6.36 million
rows). Most new objects are Starlink payloads — small, near-circular LEO
satellites launched only months before.

| EPOCH | OBJECT_NAME | NORAD_CAT_ID | OBJECT_TYPE | OBJECT_ID | COUNTRY | ECCENTRICITY | INCLINATION (°) | MEAN_MOTION (rev/day) | PERIAPSIS (km) | APOAPSIS (km) | LAUNCH_DATE | DECAY_DATE | RCS_SIZE | SITE   |
|-------|-------------|--------------|-------------|-----------|---------|--------------|-----------------|-----------------------|----------------|---------------|-------------|------------|----------|--------|
| 2024-01-01 00:00:00 | STARLINK-6068 | 56798 | PAYLOAD | 2023-078AH | US | 0.000 | 70.0 | 14.98 | 570 | 574 | 2023-05-31 | | LARGE | AFWTR |
| 2024-01-01 00:00:00 | STARLINK-30550 | 58045 | PAYLOAD | 2023-156T | US | 0.000 | 53.0 | 15.92 | 296 | 298 | 2023-10-09 | | LARGE | AFWTR |
| 2024-01-01 00:00:00 | STARLINK-30951 | 58439 | PAYLOAD | 2023-183C | US | 0.000 | 43.0 | 15.26 | 487 | 489 | 2023-11-28 | | LARGE | AFETR |

Starlink sats are essentially circular (eccentricity ~0.0001-0.0003), at
various shell inclinations (43°, 53°, 70°), with mean motions of 15–16
rev/day (periods around 90–96 minutes) and altitudes of roughly 300–600 km.

---

## Data dictionary (what every variable means)

Every row in the catalog is one **Orbit Mean-elements Message (CCSDS OMM)**:
a snapshot of one object's orbit at `EPOCH`. The 41 columns fall into eight
semantic groups.

### 1. Object identification

| Column | Parquet type | Meaning |
|--------|--------------|---------|
| `OBJECT_NAME` | string | Public common name, e.g. `ISS (ZARYA)`, `STARLINK-6068`, `COSMOS 469`. |
| `OBJECT_ID` | string | International designator (COSPAR): `YYYY-NNNx`, e.g. `1998-067A`. `YYYY-NNN` = launch year + launch number of that year; trailing letter = the piece in sequence from that launch (`A` = payload, `B`/`C`/... = upper stages and debris). |
| `NORAD_CAT_ID` | int | Unique **catalog number** issued by US Space Force (e.g. 25544 = ISS). This is the primary key of the object. |
| `OBJECT_TYPE` | string | What the object is: `PAYLOAD`, `ROCKET BODY` (launcher upper stage), `DEBRIS`, `TBA`/`UNKNOWN`. |
| `CLASSIFICATION_TYPE` | string | Security classification, almost always `U` (unclassified). |

### 2. Classic orbital elements

The six Keplerian elements that describe the orbit at the epoch time, plus
the epoch itself. With `EPOCH` and these five they fully define the state;
`MEAN_ANOMALY` is the "where along the orbit" parameter.

| Column | Parquet type | Meaning |
|--------|--------------|---------|
| `EPOCH` | datetime (UTC) | **Time of the element set** — the instant these elements are valid. |
| `MEAN_MOTION` | float | **Mean motion**, revolutions per day (1/day). ~15.9 = ISS (LEO), ~1.0 = GEO, ~14.8 = Starlink. |
| `ECCENTRICITY` | float | **Orbital eccentricity**, 0 = perfect circle, 0-1. LEO objects often ~0.001; HEO like THEMIS D can be 0.75. |
| `INCLINATION` | float | **Inclination**, degrees. 0° = equatorial, 51.6° = ISS, 97°+ = Sun-synchronous, >90° = retrograde. |
| `RA_OF_ASC_NODE` | float | **Right ascension of the ascending node**, degrees (0–360). Where the orbit crosses the equator going north. |
| `ARG_OF_PERICENTER` | float | **Argument of perigee**, degrees (0–360). Angle along the orbit from node to perigee. |
| `MEAN_ANOMALY` | float | **Mean anomaly**, degrees (0–360). Where along the ellipse (measured from perigee) the object is at epoch. |

### 3. SGP4 propagation coefficients

These are used by the standard SGP4 propagator to evolve the orbit forward
in time (e.g. to compute ground tracks, future position, re-entry windows).

| Column | Parquet type | Meaning |
|--------|--------------|---------|
| `BSTAR` | float | **Drag / radiation-pressure coefficient** (1/earth-radii). Measures how strongly atmospheric drag decays the orbit. Negative values indicate radiation pressure. |
| `MEAN_MOTION_DOT` | float | **First derivative** of mean motion, rev/day². Shows how fast the orbit is shrinking (drag). |
| `MEAN_MOTION_DDOT` | float | **Second derivative** of mean motion, rev/day³. Almost always 0 for operational TLEs. |
| `EPHEMERIS_TYPE` | int | Ephemeris model used in generation (0 = SGP4 in practice). |
| `ELEMENT_SET_NO` | int | Incrementing revision number of this element set for the object. |
| `REV_AT_EPOCH` | int | **Orbit number at epoch** — how many full revs the object has complete since launch. |

### 4. Derived orbital parameters

These are *computed* by Space-Track from the classic elements — you can
recompute them yourself but they are provided for convenience.

| Column | Parquet type | Meaning |
|--------|--------------|---------|
| `SEMIMAJOR_AXIS` | float | **Semi-major axis**, km. Half the longest axis of the orbital ellipse. |
| `PERIOD` | float | **Orbital period**, minutes. Mean motion converted to minutes. ~90-96 min LEO, 1436 min GEO. |
| `APOAPSIS` | float | **Apogee altitude**, km above Earth's sea-level surface (closest extreme altitude). |
| `PERIAPSIS` | float | **Perigee altitude**, km above sea level (lowest altitude). Perigee ≤ 2,000 km ≈ LEO. |

### 5. Mission metadata

Identity facts about the mission the object came from.

| Column | Parquet type | Meaning |
|--------|--------------|---------|
| `LAUNCH_DATE` | string (`YYYY-MM-DD`) | When the object's launch vehicle lifted off. |
| `DECAY_DATE` | string (`YYYY-MM-DD`) | When the object re-entered (blank if still on orbit). |
| `SITE` | string | Launch site code, e.g. `AFETR` (Cape Canaveral), `AFWTR` (Vandenberg), `TTMTR` (Tyuratam/Baikonur), `TSC` (Taiyuan). |
| `COUNTRY_CODE` | string | Country/organization operating the object, e.g. `US`, `CIS` (USSR/Russia), `PRC` (China), `ITSO` (International Telecom Sat. Org). |
| `RCS_SIZE` | string | Radar cross-section class: `SMALL`, `MEDIUM`, `LARGE` — a proxy for object size. |

### 6. API / data-frame metadata

Technical metadata about the element-set record itself.

| Column | Parquet type | Meaning |
|--------|--------------|---------|
| `CCSDS_OMM_VERS` | string | OMM format version (e.g. `3.0`). |
| `COMMENT` | string | Free-text comment, e.g. `GENERATED VIA SPACE-TRACK.ORG API`. |
| `CREATION_DATE` | datetime (UTC) | When 18 SPCS generated/published this element set. |
| `ORIGINATOR` | string | Creating agency, e.g. `18 SPCS`. |
| `CENTER_NAME` | string | Central body for the elements, `EARTH`. |
| `REF_FRAME` | string | Reference frame, `TEME` (True Equator, Mean Equinox). |
| `TIME_SYSTEM` | string | Time system of `EPOCH`, `UTC`. |
| `MEAN_ELEMENT_THEORY` | string | Propagator theory used to draw the elements, `SGP4`. |
| `FILE` | int | Source file ID of the upload on Space-Track (higher = more recent batch). |
| `GP_ID` | int | **Unique row identifier** in the GP archive. Every element set in history has its own GP_ID. |

### 7. Raw TLE lines

The classic **Two-Line Element set** (the human-readable form all current
systems print TLEs in). These are exact and most useful for running your own
SGP4 propagation.

| Column | Parquet type | Meaning |
|--------|--------------|---------|
| `TLE_LINE0` | string | Line 0: the object name line (`0 NAME`). |
| `TLE_LINE1` | string | Line 1: catalog number, classification, epoch data, drag terms, element set number, checksum. |
| `TLE_LINE2` | string | Line 2: inclination, RAAN, eccentricity (decimal squashed), argument of perigee, mean anomaly, mean motion, checksum. |

### 8. Partition column

| Column | Parquet type | Meaning |
|--------|--------------|---------|
| `year` | int | Convenience column added by the downloader, equal to `EPOCH`'s year. Used for the `year=YYYY` folder partitioning. |

In the Parquet files the numeric columns are stored as 64-bit floats / Int64
(nullable) integers and `EPOCH` / `CREATION_DATE` as timezone-aware
`datetime64[ns, UTC]`. Timestamp precision may alternate between microseconds
and milliseconds across year boundaries; both unify cleanly on read.

---

## Reading the data

```python
import pandas as pd

# Read the whole catalog as one DataFrame (may take a while / a lot of RAM
# for the full history). For large analysis, read one year at a time.
df = pd.read_parquet("data/gp_history")

# Only one year:
df_2024 = pd.read_parquet("data/gp_history/year=2024")

# Only the latest elset of the ISS:
iss = df[df["NORAD_CAT_ID"] == 25544].sort_values("EPOCH").tail(1)

# LEO example (perigee below 2000 km):
leo = df[df["PERIAPSIS"] <= 2000.0]

# Only Starlink payloads:
starlink = df[df["OBJECT_NAME"].str.startswith("STARLINK")]

# Only active (non-decayed) objects: remember DECAY_DATE is "" for active
# objects (see "Design decisions & caveats"), so:
active = df[df["DECAY_DATE"].astype(str).ne("")]
```

The other datasets read the same way:

```python
omni = pd.read_parquet("data/omni")              # hourly solar wind + indices
donki_cme = pd.read_parquet("data/donki/CME")    # one row per CME
donki_gst = pd.read_parquet("data/donki/GST")    # one row per storm
gunter_tables = pd.read_parquet("data/gunter/tables.parquet")
gunter_incidents = pd.read_parquet("data/gunter/incidents.parquet")
```

---

## Verification

After downloading you can sanity-check the archive:

```bash
python - <<'EOF'
import pandas as pd
df = pd.read_parquet("data/gp_history")
print(df.groupby("year")["NORAD_CAT_ID"].count())          # rows per year
print(df.groupby("year")["EPOCH"].agg(["min", "max"]))     # coverage per year
print(df.groupby("year")["NORAD_CAT_ID"].nunique().max())  # distinct objects
EOF
```

Optional deep check: recompute the modulo-10 checksum of `TLE_LINE1`/`TLE_LINE2`
to confirm raw-TLE integrity (Space-Track assigns 0 to letters/blanks/`.`/`+`
and 1 to `-`).

---

## Considerations, rate limits and policy

- **Volume**: the full archive is **~138M+ element sets** → dozens of GB of
  Parquet, and a download that may take **hours to a few days**, depending on
  your connection and Space-Track load. The current download here totals
  **67.2M rows / ~12 GB**.
- **API throttle**: Space-Track limits you to **<30 requests/minute** and
  **<300 requests/hour**. The `spacetrack` client enforces the per-minute
  limit; the script is designed so the total request count stays well below
  300 (one request per window).
- **Data class policy**: the guidelines label `GP_HISTORY` as
  "1 / lifetime" — meaning you should download data once and **store it on
  your own servers** (which is exactly what this project does). Do not re-run
  full downloads; `--reset` should only be used intentionally.
- **Recommended**: before a huge bulk download, contact
  [Space-Track](https://www.space-track.org/documentation) (Contact Us) to
  announce your plan and confirm acceptable usage. Their test server is
  available for development.
- **Re-distribution**: USSPACECOM provides blanket approval to redistribute
  basic SSA data with appropriate citation (`USSPACECOM/18 SDS`).
- **Time zone**: all timestamps are **UTC**.

---

## Project layout

```
cme-sentinel/
├── .env                     # credentials (git-ignored)
├── .env.example             # template for the credentials file
├── AGENTS.md                # project instructions for AI agents (Spanish)
├── requirements.txt          # Python dependencies
├── scripts/
│   ├── fetch_gp_history.py  # the orbital-catalog downloader (already run)
│   ├── fetch_omni.py        # OMNI hourly solar wind & indices (ready, not run)
│   ├── fetch_donki.py       # DONKI space-weather events (ready, needs NASA_API_KEY)
│   └── fetch_gunter.py      # Gunter's Space Page full-site crawl (ready, not run)
├── notebooks/
│   └── 01_validate_and_explore.ipynb  # catalog validation & exploration
└── data/
    ├── .progress.json       # download progress (git-ignored)
    ├── gp_history/
    │   └── year=YYYY/       # Parquet partitions (git-ignored)
    ├── omni/                # created by fetch_omni.py (git-ignored)
    ├── donki/               # created by fetch_donki.py (git-ignored)
    └── gunter/              # created by fetch_gunter.py (git-ignored)
```