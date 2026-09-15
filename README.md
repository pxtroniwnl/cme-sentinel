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
- [Next step: CME data sources](#next-step-cme-data-sources)
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

## Next step: CME data sources

So far this repository holds **only satellite orbital data** (an element-set
catalog). The planned follow-up is to also source the **coronal mass ejection
(CME)** data itself — i.e. downloading the actual CME events from one of the
portals below (or another source with similar data). That CME catalog is what
will let us probe whether solar eruptions measurably affect tracked objects.

The candidates, with their time coverage:

| Source | Covers | Content | Access format |
|--------|--------|---------|---------------|
| **CDAW / SOHO-LASCO CME Catalog** (`cdaw.gsfc.nasa.gov/CME_list`) | Jan **1996** → today (SOHO era, ~43k CMEs) | date/time, direction (central position angle), angular width, speed, acceleration, mass, kinetic energy, halo flag | monthly HTML; official parquet mirror in Hugging Face (`juliensimon/cdaw-lasco-cme-catalog`) |
| **DONKI — NASA CCMC** (`api.nasa.gov/DONKI/CME`) | **~2012** → today (curated, smaller volume) | CMEs with real **3D direction** (WSA-ENLIL cone-model fits: speed, half-angle, source lat/lon) + linked events | free REST API (JSON) |
| **CACTus** (SIDC Belgium, `sidc.be/cactus`) | **1997** → **2017** (automatic detection) | same basic LASCO parameters, useful as cross-check | web CSV |
| **OMNI solar wind** (NASA GSFC, `omniweb.gsfc.nasa.gov`) | **1963** → today (continuous) | in-situ solar wind at L1: speed, density, IMF Bz, etc. (the "big" time series) | multi-index ASCII |
| **Geomagnetic indices Dst / Kp / Ap** (OMNI, GFZ, NOAA) | Dst: **1957** → today; Kp: **1932** → today | geomagnetic storm strength (the causal link between CMEs and orbit drag) | ASCII |
| **HELCATS** (STEREO, `helcats-fp7.eu`) | **2007** → **2017** | CMEs as seen by STEREO-A/B, true 3D direction | CSV / ASCII |

Coverage notes that matter for the planned analysis:

- A complete CME catalog only exists from **1996** (SOHO/LASCO). For the
  1960–1995 part of the satellite archive there is no exhaustive CME catalog;
  the continuous series that do cover it are **OMNI solar wind (1963+)** and
  the **Dst / Kp indices (1957+ / 1932+)**.
- CDAW's "direction" is **2D** on the plane of the sky (position angle), not a
  3D vector. Halo CMEs (angular width = 360°) are the Earth-directed subset —
  the basic geo-effective signal. Real 3D direction is only available from
  **DONKI** and **HELCATS**.
- The mechanism that connects CMEs to the satellite catalog is:
  *CME → reaches Earth → geomagnetic storm → thermospheric heating → higher
  atmospheric drag → orbital decay*. The TLE data can only reveal effects that
  change the orbit (drag/decay), not electronic failures.

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

# Only active (non-decayed) objects:
active = df[df["DECAY_DATE"].isna()]
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
├── requirements.txt          # Python dependencies
├── scripts/
│   └── fetch_gp_history.py  # the downloader
└── data/
    ├── .progress.json       # download progress (git-ignored)
    └── gp_history/
        └── year=YYYY/       # Parquet partitions (git-ignored)
```