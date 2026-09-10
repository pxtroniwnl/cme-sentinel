# CME Sentinel

Download the **full historical orbital catalog** from
[Space-Track.org](https://www.space-track.org) (the `gp_history` API class)
and store it locally as **Apache Parquet** files partitioned by year.

The download includes **every tracked object** (all `NORAD_CAT_ID`s) and
**all time** from **1960-01-01 to today**, with the complete set of
Orbit Mean-Elements Message (OMM) columns exposed by the API — orbital
elements, satellite-catalog metadata, and the raw TLE lines.

---

## Table of contents

- [Why this project](#why-this-project)
- [How the data source works](#how-the-data-source-works)
- [Requirements](#requirements)
- [Setup](#setup)
- [How the script works](#how-the-script-works)
  - [1. Login and session](#1-login-and-session)
  - [2. Time windows](#2-time-windows)
  - [3. Streaming the download](#3-streaming-the-download)
  - [4. Auto-subdivision for large windows](#4-auto-subdivision-for-large-windows)
  - [5. Resume support](#5-resume-support)
  - [6. Parquet output](#6-parquet-output)
- [Columns returned by the API](#columns-returned-by-the-api)
- [Usage](#usage)
- [Reading the data](#reading-the-data)
- [Verification](#verification)
- [Considerations, rate limits and policy](#considerations-rate-limits-and-policy)

---

## Why this project

Space-Track's `gp_history` endpoint exposes the complete historical archive
of US Space Force element sets — **over 138 million records**. A single
API query cannot return all of it: the server times out on huge date
ranges, and the API guidelines throttle how fast you may query.

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
```

> Note: `.env` and `data/` are already git-ignored, so credentials and
> downloaded raw data will never be committed.

---

## How the script works

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

### 4. Auto-subdivision for large windows

If a request fails (server timeout, connection reset, "result too large"
error), the failing window is **split in half** at its midpoint and both
halves are retried. This repeats down to a minimum window of 1 day. In
practice you are back-rated to month/quarter windows from the start, so
splitting is only a fallback for dense months.

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

## Columns returned by the API

This is the exact, ordered list of fields the `gp_history` class returns
(taken from Space-Track's model definition). The script assigns these names
to every downloaded row.

| # | Column                 | Type                  | Description                                                              |
|---|------------------------|-----------------------|--------------------------------------------------------------------------|
| 1 | `CCSDS_OMM_VERS`       | `varchar(3)`          | CCSDS OMM standard version (e.g. `3.0`).                                 |
| 2 | `COMMENT`              | `varchar(33)`         | Free-text comment, e.g. "GENERATED VIA SPACE-TRACK.ORG API".             |
| 3 | `CREATION_DATE`        | `datetime`            | When 18 SPCS generated/published this element set (UTC).                 |
| 4 | `ORIGINATOR`           | `varchar(7)`          | Creating agency, e.g. `18 SPCS`.                                         |
| 5 | `OBJECT_NAME`          | `varchar(25)`         | Common object name, e.g. `ISS (ZARYA)`.                                  |
| 6 | `OBJECT_ID`            | `varchar(12)`         | International designator (COSPAR), e.g. `1998-067A`.                     |
| 7 | `CENTER_NAME`          | `varchar(5)`          | Central body, `EARTH`.                                                   |
| 8 | `REF_FRAME`            | `varchar(4)`          | Reference frame, `TEME`.                                                 |
| 9 | `TIME_SYSTEM`          | `varchar(3)`          | Time system, `UTC`.                                                      |
| 10| `MEAN_ELEMENT_THEORY`  | `varchar(4)`          | Propagator used, `SGP4`.                                                 |
| 11| `EPOCH`                | `datetime(6)`         | Epoch time of the element set (UTC).                                     |
| 12| `MEAN_MOTION`          | `decimal(13,8)`       | Mean motion in revolutions/day.                                          |
| 13| `ECCENTRICITY`         | `decimal(13,8)`       | Orbital eccentricity.                                                    |
| 14| `INCLINATION`          | `decimal(7,4)`        | Orbital inclination in degrees.                                          |
| 15| `RA_OF_ASC_NODE`       | `decimal(7,4)`        | Right ascension of ascending node in degrees.                            |
| 16| `ARG_OF_PERICENTER`    | `decimal(7,4)`        | Argument of perigee in degrees.                                          |
| 17| `MEAN_ANOMALY`         | `decimal(7,4)`        | Mean anomaly in degrees.                                                 |
| 18| `EPHEMERIS_TYPE`       | `tinyint unsigned`    | Ephemeris type (0 externally).                                           |
| 19| `CLASSIFICATION_TYPE`  | `char(1)`             | Security classification (`U` = unclassified).                            |
| 20| `NORAD_CAT_ID`         | `int unsigned`        | Satellite catalog number.                                                |
| 21| `ELEMENT_SET_NO`       | `smallint unsigned`   | Element set number (999 in practice).                                    |
| 22| `REV_AT_EPOCH`         | `mediumint unsigned`  | Orbit number at epoch.                                                   |
| 23| `BSTAR`                | `decimal(19,14)`      | SGP4 drag coefficient / radiation-pressure coefficient.                  |
| 24| `MEAN_MOTION_DOT`      | `decimal(9,8)`        | First derivative of mean motion (rev/day²).                               |
| 25| `MEAN_MOTION_DDOT`     | `decimal(22,13)`      | Second derivative of mean motion (rev/day³).                             |
| 26| `SEMIMAJOR_AXIS`       | `double(12,3)`        | Semi-major axis in km.                                                   |
| 27| `PERIOD`               | `double(12,3)`        | Orbital period in minutes.                                               |
| 28| `APOAPSIS`             | `double(12,3)`        | Apogee altitude in km.                                                   |
| 29| `PERIAPSIS`            | `double(12,3)`        | Perigee altitude in km.                                                  |
| 30| `OBJECT_TYPE`          | `varchar(12)`         | `PAYLOAD`, `ROCKET BODY` (R/B), `DEBRIS`, ...                          |
| 31| `RCS_SIZE`             | `varchar(6)`          | Radar cross-section class: `SMALL`, `MEDIUM`, `LARGE`.                   |
| 32| `COUNTRY_CODE`         | `char(6)`             | Country/org that operates/owns the object.                               |
| 33| `LAUNCH_DATE`          | `varchar(10)`         | Launch date `YYYY-MM-DD` (UTC).                                          |
| 34| `SITE`                 | `char(5)`             | Launch site code (e.g. `TTMTR`).                                         |
| 35| `DECAY_DATE`           | `varchar(10)`         | Re-entry date `YYYY-MM-DD`; blank if still on orbit.                     |
| 36| `FILE`                 | `bigint unsigned`     | Source file ID of the upload (higher = more recent).                     |
| 37| `GP_ID`                | `int unsigned`        | Unique row identifier in the GP archive.                                 |
| 38| `TLE_LINE0`            | `varchar(27)`         | TLE line 0 (object name line, `"0 NAME"`).                               |
| 39| `TLE_LINE1`            | `varchar(71)`         | Raw TLE line 1 (catalog number, epoch, drag terms, checksum).             |
| 40| `TLE_LINE2`            | `varchar(71)`         | Raw TLE line 2 (orbital elements, mean motion, checksum).                 |

In the Parquet files the numeric columns are stored as 64-bit floats / Int64
(nullable) integers and `EPOCH` / `CREATION_DATE` as timezone-aware
`datetime64[ns, UTC]`. Columns 36–40 are most valuable for SGP4
propagation using the raw TLE lines.

---

## Usage

```bash
# Full download (1960 → today), resumable.
python scripts/fetch_gp_history.py

# Quick test: only the first few windows of 2020.
python scripts/fetch_gp_history.py --start 2020-01-01 --limit 3

# Custom range.
python scripts/fetch_gp_history.py --start 2000-01-01 --end 2004-12-31

# Ignore existing progress and start over.
python scripts/fetch_gp_history.py --reset

# All options
python scripts/fetch_gp_history.py --help
```

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
  your connection and Space-Track load.
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
├── requirements.txt          # Python dependencies
├── scripts/
│   └── fetch_gp_history.py  # the downloader
└── data/
    ├── .progress.json       # download progress (git-ignored)
    └── gp_history/
        └── year=YYYY/       # Parquet partitions (git-ignored)
```