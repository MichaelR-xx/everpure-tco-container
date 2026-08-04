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

## Quick start (Mac / Windows, no licensed software)

No Docker Desktop license needed — bundled scripts check/install every
prerequisite and deploy with a free container engine:

```bash
./deploy_mac.sh            # macOS — uses Colima
```
```powershell
.\deploy_windows.ps1       # Windows — uses Podman + WSL2
```

Each builds and runs the app, waits until it's healthy, and opens
**http://localhost:5000**. See **[DEPLOYMENT.md § 0](DEPLOYMENT.md)** for options
(`-Yes`/`--yes`, `-Down`, `-Destroy`, `-Logs`) and how to verify it's running.

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

## How to use — detailed guide

The app flows left-to-right across the top tabs: **sign in → pick a customer/scenario →
get your data in → run an analysis → review the results**. Every upload, parsed dataset,
and TCO run is namespaced under `<customer>/<scenario>/` at your storage location.

### 1. Customer & scenario

Select or create a **customer**, then set a **scenario** name (defaults to `default`) — use
scenarios to keep separate analyses for one customer (e.g. `prod`, `dr`). **Save Customer &
Scenario** to make it active; the active selection drives the Upload, Workload Builder,
Results, and TCO Review tabs.

### 2. Get your data in — two ways

**A. Data Upload (a real inventory).** Drop a workload inventory **CSV** and map its columns.
**Disk Type** and **Disk Size** are required. Tick **Searchable** on text columns
(host/VM names, tags, resource groups) so you can filter on them later. Save/load a
**mapping template** for similar files, then **Parse Data** to produce the dataset.

**B. Workload Builder (a synthetic workload).** Assemble a workload from reusable models
instead of uploading — useful for what-if sizing.

- **Size by capacity (session-wide):** choose whether capacity inputs mean **Original
  capacity** or **Licensed capacity (Everpure)**. In licensed mode, original is derived as
  `licensed ÷ efficiency` using the **Efficiency rate (%)** you set (default 65%).
- **Simple sizing:** enter a **Total capacity (TiB)**, a **Disk type**, and a **Number of
  volumes**. Per-volume size = `capacity ÷ volumes`, rounded **up** to the nearest valid
  Azure disk size for that type (max **32 TiB** for tiered types, **64 TiB** for
  PremiumV2/Ultra); oversized requests are refused. **＋ Add to workload** adds them with the
  current placement.
- **Models & placement:** load a **saved workload** or build models in the **Model library**,
  then set **Placement** (region / availability zone / VNet) — change it between adds to
  spread capacity across zones and VNets.
- **Save workload** against the active customer. This parses it into a dataset (reusable
  across customers) that shows up on the Results tab, exactly like an uploaded inventory.

### 3. Results — run the analysis

Pick a parsed dataset, review its summary, set the assumptions, and **Run Analysis**.

- **Deployment model:** **Dedicated** (EC array sizing) or **Azure Native** (capacity +
  throughput model).
- **Growth (yearly):** a percentage (0–50% in 5% steps), modeled as **yearly compounding
  growth**. This single value drives both the growth projection and the compounding
  migration model in TCO Review.
- Plus **Data Reduction Ratio**, **Monthly Snapshot Rate**, **Initial Max Size**, **Usage
  Efficiency**, default **SKU**, **Years**, and **Projection Cycle**; Advanced settings
  expose pricing term and per-tier limits.
- **Efficiency sweep (optional):** set start/end/step to generate a batch of runs across a
  range of usage-efficiency settings at once.
- **🔎 Use-case filter (optional):** include/exclude rows matching terms (e.g. `sql`, `db`)
  over the searchable columns. **Preview** the split, **Apply to summary** to re-scope the
  metrics, or **Save filtered dataset** as a new dataset.

### 4. TCO Review — compare & explore

Browse generated TCOs and tune **commercial adjustments** (minimum savings rate, Everpure
discount, partner margin — capped at the Everpure discount — and Azure discount), plus a
region filter. Every view updates live. Mark a run **primary** so others inherit its
included-group set, or **compare** runs side by side.

**Data** shows the per-group table and the **Overall Results for Year 1** summary (annual
figures, monthly in parentheses). **Graphs**, **Migration**, **Ramp**, and **Consolidation**
are detailed below. **Advanced** is the raw per-group sizing data (CSV-downloadable);
**Downloads** holds saved PDF reports.

#### Migration (compounding growth)

Model a phased migration where capacity keeps **compounding** while groups move from Azure to
Everpure one cohort at a time.

1. **Pace it:** set **Months to complete** *or* **Capacity / month (TiB)** — the two are
   linked (capacity/month = total ÷ months). Set per-group **precedence** (early / middle /
   late) and an explicit **order** number to control the sequence.
2. **Evaluate:** click **▶ Evaluate**. Each period, capacity grows at the run's yearly rate;
   each group flips from Azure to Everpure when it migrates; a **one-time migration cost**
   (`migrated capacity × $/TiB`, default $260/TiB) is applied in the month the group begins
   migrating. Everpure cost starts that same month; the group's Azure cost is dropped the
   period after it finishes.
3. **Save** named plans to reuse. Saved plans — and the compounding run itself — are
   selectable in the Graphs view.

#### Graphs & Ramp

- The **Growth Projection** compounds capacity at the yearly growth rate. By default it shows
  growth with **no migration**.
- **Include migration plan** folds in a saved plan or this run's compounding migration:
  Everpure ramps in with each group's timing, unmigrated capacity stays on Azure, and the
  one-time migration cost is applied.
- With a plan added, the **N-Year TCO Summary** also shows the one-time **Migration Cost** and
  Everpure/Savings totals that include it (matching the projection's total). With no plan,
  no migration costs are shown.
- **⬇ CSV (cost by period)** exports just the per-period cost table as raw numbers; graph
  views also export to **PDF**.
- The **Ramp** view presents the same phased trajectory as cost/savings ramping over the
  growth projection, with cumulative savings vs. staying on Azure.

#### Consolidation

Re-home **negative-savings** groups into **positive-savings** groups to lift the overall TCO.

1. **▶ Consolidate & Re-run** moves every negative-savings group into a positive one in the
   **same region** (balanced by EC licensed capacity), builds a new *consolidated* dataset,
   and re-runs the TCO. The negative/positive split uses the discounts currently set above.
2. It prices the intra-region **VNet peering** the moves require. Set **Peered traffic per
   licensed TiB (GiB/mo)** (default 512) to control that cost — it updates live and is folded
   into the Everpure total and savings.
3. Optionally **exclude negative groups** instead of re-homing them. Consolidated datasets are
   hidden on the Results tab unless you tick **Show consolidation datasets**.

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
- **Compounding growth & migration.** Growth is modeled as yearly compounding, and the
  Migration view builds a phased, capacity-metered migration with a one-time per-TiB cost;
  the Graphs/Ramp views show the ramped trajectory and an N-year summary. See
  **[How to use](#how-to-use--detailed-guide)**.
- **Workload Builder & Consolidation.** Assemble synthetic workloads from reusable models
  (incl. licensed-capacity sizing), and re-home negative-savings groups with priced VNet
  peering.
- **CSV/PDF exports.** Export the growth-projection cost-by-period table (CSV), the raw
  per-group data (CSV), and graph views (PDF).

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
