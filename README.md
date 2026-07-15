# Everpure Azure Managed Disk Visualization Tool — Containerized

A Flask web app that estimates and compares the multi-year **Total Cost of Ownership
(TCO)** of running storage on **Azure managed disks** versus **Pure Storage / Everpure
(EC)**. Upload a workload inventory CSV, map its columns, run a cost analysis, and
review multi-year cost, growth, migration, and **consolidation** breakdowns.

This is the **containerized, cross-platform** edition. It runs the same on **Linux,
macOS, and Windows**, needs no AWS account by default (Local Storage on a mounted
volume), parallelizes the analysis, and paginates the large data tables.

---

> **Full deployment guide:** see [DEPLOYMENT.md](DEPLOYMENT.md) — installing Docker
> (macOS / Windows / Linux), build & run, configuration, operations, native (no-Docker)
> deployment, and troubleshooting.

## Quick start (Docker — Linux / macOS / Windows)

```bash
docker compose up --build
```

Open **http://localhost:5000**, choose a username/password (see **Default credentials**),
and go. Uploaded files, parsed runs, and generated TCOs persist in the `everpure_data`
Docker volume.

That's it — the image includes headless Chromium (for PDF export) and seeds the engine
config files, so a fresh **Local Storage** deployment works with no AWS account.

To stop and remove the data volume: `docker compose down -v`.

### Plain `docker run`

```bash
docker build -t everpure-tco .
docker run --rm -p 5000:5000 \
  -v everpure_data:/data \
  -e FLASK_SECRET_KEY="$(openssl rand -hex 32)" \
  everpure-tco
```

---

## Run on a Mac

**Option A — Docker Desktop (recommended):** install Docker Desktop for Mac, then run
`docker compose up --build` as above. Nothing else to install.

**Option B — natively (no Docker):**

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export FLASK_SECRET_KEY="$(openssl rand -hex 32)"
export EVERPURE_STORAGE=local
export EVERPURE_LOCAL_ROOT="$HOME/everpure-data"     # data goes under here/EverpureTCO
# seed the engine configs once (first run only):
mkdir -p "$HOME/everpure-data/EverpureTCO/TCO-GUI/_config"
cp notes/ec_config.json "$HOME/everpure-data/EverpureTCO/TCO-GUI/_config/"
cp notes/ecan_config.json "$HOME/everpure-data/EverpureTCO/TCO-GUI/_config/"
python app.py     # serves http://127.0.0.1:5000
```

For PDF export natively on a Mac, install Google Chrome (the app auto-detects it) — or
just use the Docker image, which bundles Chromium.

---

## Storage backends

Chosen once, either via the login screen or (for containers/headless) via env vars.

| Backend | What it is | How to select |
|---|---|---|
| **Local Storage** | a folder on the host (a mounted volume in Docker) | `EVERPURE_STORAGE=local` + `EVERPURE_LOCAL_ROOT=/data`, or the login screen |
| **MikeS3** | the shared, pre-configured Amazon S3 bucket | `EVERPURE_STORAGE=mikes3` |
| **Other S3** | your own S3 bucket | `EVERPURE_STORAGE=others3` + `EVERPURE_S3_BUCKET` + AWS creds |

On Linux/macOS, **Local Storage** takes a **folder path**; on Windows it also accepts a
**drive letter** (e.g. `D`). In the container it's a folder path on the mounted volume.

### AWS credentials

Provide via the standard env vars: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_DEFAULT_REGION`. (The old hardcoded `aws.arch` file path is now optional and, if
used, configurable via `AWS_ARCH_FILE`.)

---

## What's new in this edition

- **Containerized & cross-platform.** One `docker compose up`; runs on Mac/Linux/Windows.
  All previously Windows-only paths (credentials file, Chromium, local storage root) are
  now configurable / auto-detected per OS.
- **Multithreaded analysis.** The dominant cost of an analysis — the many Azure
  retail-price lookups — is fanned out across a thread pool (per region×product for
  Azure disk pricing, and per region for the EC infrastructure pricing). Tune with
  `AZURE_PRICE_WORKERS` (default 8).
- **Paginated data tables.** The **Results → Group Breakdown** table and the
  **TCO Review → Data** view now load **50 rows per page** by default, with a
  **Rows/page** control and Prev/Next paging.
- **Production server.** The container serves via **waitress**, not Flask's dev server.

---

## Configuration reference (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `FLASK_SECRET_KEY` | insecure placeholder | signs the session cookie — **set your own** |
| `EVERPURE_STORAGE` | (unset → login-screen setup) | `local` / `mikes3` / `others3` — configures storage headlessly |
| `EVERPURE_LOCAL_ROOT` | `/data` | Local Storage root (data under `<root>/EverpureTCO`) |
| `EVERPURE_S3_BUCKET` | — | bucket for `others3` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION` | — | AWS credentials for S3 backends |
| `AWS_ARCH_FILE` | Windows path | optional path to the zlib `aws.arch` credentials file |
| `CHROMIUM_PATH` | auto-detect | explicit Chromium/Chrome/Edge binary for PDF export |
| `AZURE_PRICE_WORKERS` | `8` | thread-pool size for the Azure price lookups |
| `HOST` / `PORT` | `127.0.0.1` / `5000` | bind address/port for `python app.py` (container uses waitress on `0.0.0.0:5000`) |

---

## Default credentials

Demo accounts live in `VALID_USERS` in `app.py`:

| Username | Password |
|---|---|
| `admin` | `password123` |
| `demo`  | `demo` |

**Change these before any real use.**

---

## Live Azure pricing

Cost analyses fetch **live Azure retail prices** over HTTPS from the public Azure Retail
Prices API, so the app needs outbound internet when you click *Run Analysis*.

---

## Project layout

```
app.py                     # entire Flask backend (routes + cost engine)
templates/index.html       # entire single-page frontend (inline JS/CSS)
requirements.txt           # Python dependencies (Flask, boto3, pandas, requests, waitress)
Dockerfile                 # container image (Python 3.12 + Chromium + waitress)
docker-compose.yml         # one-command run with a persistent data volume
notes/                     # ec_config.json / ecan_config.json (engine configs) + architecture
tools/                     # workload inventory CSV generator
static/                    # (empty)
```
