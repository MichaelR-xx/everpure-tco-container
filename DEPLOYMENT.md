# Deployment Guide — Everpure TCO Tool (Containerized)

How to install the prerequisites, build the image, run it, configure it, keep it
running, and troubleshoot — for **macOS**, **Windows**, and **Linux**. The app ships
as a Docker image (Python 3.12 + bundled Chromium + waitress) and also runs natively
(no Docker) if you prefer.

- **TL;DR (any machine with Docker):** `cd everpure-tco-container && docker compose up --build` → open <http://localhost:5000>.
- **macOS, no licensed software:** `./deploy_mac.sh` (uses Colima — see the section below).
- One-command build + self-test: `./build_and_test.sh`.

---

## 0. macOS — one command, no licensed software (recommended for Mac)

Docker Desktop is free for personal use and small businesses but needs a **paid
subscription** for larger organizations. This repo ships **`deploy_mac.sh`**, which
deploys the tool using **[Colima](https://github.com/abiosoft/colima)** — a free,
open-source Docker runtime — and installs anything you're missing. No Docker
Desktop, no license.

```bash
git clone https://github.com/MichaelR-xx/everpure-tco-container.git
cd everpure-tco-container
./deploy_mac.sh
```

**What it does:** checks every prerequisite — Xcode Command Line Tools, Homebrew,
Colima, the `docker` CLI, and `docker compose` — and offers to install each missing
one; then starts the Colima Docker VM, builds and runs the container, waits until
it's healthy on <http://localhost:5000>, and opens it in your browser.

**Options:**

| Command | What it does |
|---|---|
| `./deploy_mac.sh` | Interactive — asks before installing anything |
| `./deploy_mac.sh --yes` | Non-interactive — auto-installs any missing prerequisites |
| `./deploy_mac.sh --logs` | Follow the container logs |
| `./deploy_mac.sh --down` | Stop the app (keeps its data) |
| `./deploy_mac.sh --destroy` | Stop the app **and** delete its data volume |

> If the script isn't executable after cloning: `chmod +x deploy_mac.sh`.
> First run downloads the Colima VM image and builds the container — allow a few minutes.

### Verify it's running (with Colima / docker)

After the script finishes, from the repo directory:

```bash
colima status                              # -> Colima is "running"
docker compose ps                          # -> service "everpure-tco"  ...  Up  0.0.0.0:5000->5000/tcp
curl -s localhost:5000/api/auth/status     # -> {"storage_kind":"local",...}  (HTTP 200 = serving)
docker compose logs --tail 20              # startup log: waitress serving on 0.0.0.0:5000
```

Confirm a login works end to end (optional):

```bash
curl -s -X POST localhost:5000/api/auth/login \
  -H 'content-type: application/json' \
  -d '{"username":"admin","password":"password123"}'
# -> {"ok":true,"username":"admin"}
```

Then open <http://localhost:5000> and sign in (`admin` / `password123` — **change before real use**).
For a fuller end-to-end check, run `./build_and_test.sh` (builds a throwaway image, health-checks it, prints PASS/FAIL, and cleans up).

**Stop / clean up:**

```bash
./deploy_mac.sh --down      # stop the app (data kept in the everpure_data volume)
colima stop                 # shut down the Docker VM entirely
./deploy_mac.sh --destroy   # stop AND delete the data volume
```

---

## 1. Install Docker (alternative to the Colima script above)

### macOS (Docker Desktop)
> For a **license-free** setup, prefer the `./deploy_mac.sh` (Colima) path in section 0 above.
> Docker Desktop is fine for personal/small-business use but needs a paid subscription for larger orgs.

1. Install **Docker Desktop for Mac**: <https://www.docker.com/products/docker-desktop/>
   (choose the Apple-silicon or Intel build to match your Mac), or with Homebrew:
   ```bash
   brew install --cask docker
   ```
2. Launch **Docker Desktop** once and let it finish starting (whale icon in the menu bar).
3. Verify:
   ```bash
   docker --version && docker run --rm hello-world
   ```

### Linux
```bash
curl -fsSL https://get.docker.com | sh          # installs Docker Engine
sudo usermod -aG docker "$USER"                 # run docker without sudo (re-login after)
docker run --rm hello-world
```

### Windows 10/11 (Docker Desktop + WSL2)
Docker Desktop on Windows runs Linux containers via **WSL2**, which requires **hardware
virtualization**. Do these **in an elevated (Administrator) PowerShell**, and note the
reboots.

> ⚠️ **On this machine specifically**, a readiness check found **hardware virtualization
> DISABLED in firmware** and **WSL not installed**. You must enable virtualization in
> BIOS/UEFI first (step 0) — Docker Desktop cannot start without it.

0. **Enable virtualization in BIOS/UEFI** (one-time): reboot → enter firmware setup
   (usually `F2`/`F10`/`Del` at boot, or Windows *Settings → System → Recovery →
   Advanced startup → Restart now → Troubleshoot → UEFI Firmware Settings*). Enable
   **Intel VT-x / AMD-V** (may be called "Virtualization Technology", "SVM Mode", or
   "Intel VMX"). Save and reboot. Confirm afterward:
   ```powershell
   (Get-CimInstance Win32_Processor).VirtualizationFirmwareEnabled   # must be True
   ```
1. **Install WSL2** (admin PowerShell; reboots):
   ```powershell
   wsl --install
   ```
2. **Install Docker Desktop** (admin), then launch it and accept the license:
   ```powershell
   winget install -e --id Docker.DockerDesktop
   ```
   Or download the installer: <https://www.docker.com/products/docker-desktop/>.
   In Docker Desktop → *Settings → General*, ensure **"Use the WSL 2 based engine"** is on.
3. **Verify** (normal terminal, after Docker Desktop is running):
   ```powershell
   docker --version ; docker run --rm hello-world
   ```

> Behind a corporate TLS proxy? Docker/winget/pip downloads may fail with certificate
> errors. Configure Docker Desktop → *Settings → Resources → Proxies*, and if needed set
> the proxy CA in the image (see §6, `AWS_CA_BUNDLE` / `S3_INSECURE_TLS` for the app's
> own S3/Azure calls).

---

## 2. Build & run

From the repository root (`everpure-tco-container/`):

### Option A — docker compose (recommended)
```bash
docker compose up --build            # build + run; data persists in the everpure_data volume
# open http://localhost:5000
docker compose up -d --build         # ...or run detached
docker compose logs -f               # follow logs
docker compose down                  # stop (keeps data)
docker compose down -v               # stop AND delete the data volume
```

### Option B — plain docker
```bash
docker build -t everpure-tco .
docker run -d --name everpure-tco -p 5000:5000 \
  -v everpure_data:/data \
  -e FLASK_SECRET_KEY="$(openssl rand -hex 32)" \
  everpure-tco
```

### Option C — one-command build + smoke test
```bash
./build_and_test.sh                  # builds, runs, health-checks, prints PASS/FAIL, cleans up
```

Then sign in with a demo account (**change these before real use** — see §6):

| Username | Password |
|---|---|
| `admin` | `password123` |
| `demo`  | `demo` |

---

## 3. Storage backends

Data (uploads, parsed runs, generated TCOs, consolidations) lives in one backend,
selected either on the login screen or — for headless/containers — via env vars.

| Backend | What it is | Configure with |
|---|---|---|
| **Local Storage** (default) | a folder on the host (a Docker volume) | `EVERPURE_STORAGE=local` + `EVERPURE_LOCAL_ROOT=/data` |
| **MikeS3** | the shared pre-configured S3 bucket | `EVERPURE_STORAGE=mikes3` + AWS creds |
| **Other S3** | your own S3 bucket | `EVERPURE_STORAGE=others3` + `EVERPURE_S3_BUCKET` + AWS creds |

The image defaults to **Local Storage** on the `/data` volume and seeds the required
engine config files (`ec_config.json`, `ecan_config.json`), so it runs with **no AWS
account**. Data survives restarts as long as you keep the `everpure_data` volume.

**Use S3 instead** (compose example — edit `docker-compose.yml` or pass `-e`):
```yaml
environment:
  EVERPURE_STORAGE: others3
  EVERPURE_S3_BUCKET: your-bucket
  AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
  AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
  AWS_DEFAULT_REGION: us-east-1
```

---

## 4. Configuration reference (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `FLASK_SECRET_KEY` | insecure placeholder | **set your own** — signs the session cookie |
| `EVERPURE_STORAGE` | `local` (in the image) | `local` / `mikes3` / `others3` |
| `EVERPURE_LOCAL_ROOT` | `/data` | Local Storage root (data under `<root>/EverpureTCO`) |
| `EVERPURE_S3_BUCKET` | — | bucket for `others3` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION` | — | AWS creds for S3 backends |
| `AWS_ARCH_FILE` | (Windows path) | optional path to a zlib `aws.arch` creds file |
| `AWS_CA_BUNDLE` | — | CA bundle path for TLS-intercepting proxies (S3/Azure calls) |
| `S3_INSECURE_TLS` | — | `1` to skip S3 TLS verification (last resort, insecure) |
| `CHROMIUM_PATH` | `/usr/bin/chromium` (image) | Chromium/Chrome/Edge binary for PDF export |
| `AZURE_PRICE_WORKERS` | `8` | thread-pool size for the Azure price lookups (analysis speed) |
| `HOST` / `PORT` | container serves `0.0.0.0:5000` via waitress | bind for native `python app.py` |

**Change the demo logins** by editing `VALID_USERS` in `app.py` (rebuild the image after).

---

## 5. Operations

- **Update to a new version:** `git pull` (or drop in new source) → `docker compose up -d --build`.
  The `everpure_data` volume is preserved across rebuilds.
- **Back up data:** `docker run --rm -v everpure_data:/data -v "$PWD":/backup alpine tar czf /backup/everpure-data.tgz -C /data .`
- **Restore:** `docker run --rm -v everpure_data:/data -v "$PWD":/backup alpine sh -c "cd /data && tar xzf /backup/everpure-data.tgz"`
- **Logs:** `docker compose logs -f` (or `docker logs -f everpure-tco`).
- **Health check:** `curl -s localhost:5000/api/auth/status` → `{"storage_kind":"local",...}`.
- **Outbound internet is required** when running an analysis (live Azure retail pricing).

---

## 6. Run natively (no Docker)

### macOS / Linux
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export FLASK_SECRET_KEY="$(openssl rand -hex 32)"
export EVERPURE_STORAGE=local EVERPURE_LOCAL_ROOT="$HOME/everpure-data"
mkdir -p "$HOME/everpure-data/EverpureTCO/TCO-GUI/_config"
cp notes/ec_config.json notes/ecan_config.json "$HOME/everpure-data/EverpureTCO/TCO-GUI/_config/"
python app.py                      # http://127.0.0.1:5000  (install Chrome for PDF export)
```

### Windows (PowerShell)
```powershell
py -3 -m venv .venv ; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:FLASK_SECRET_KEY = "replace-with-random"
# Local Storage accepts a drive letter (D) or a folder path; set it on the login screen,
# or run S3 via env. Then:
python app.py
```

---

## 7. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `docker build` fails pulling the base image or on `pip install` (TLS/cert errors) | Corporate proxy intercepting TLS. Set Docker Desktop proxies + a CA bundle; retry. |
| Docker Desktop won't start on Windows | Virtualization not enabled in BIOS/UEFI, or WSL2 missing — see §1 step 0/1. |
| Page won't load at `http://localhost:5061`/`5060` | Browsers block those ports (SIP/SIPS). Use `5000` (default) or another normal port. |
| *"ec_config.json was not found…"* on Run Analysis | Engine configs missing for the backend. The image seeds them for Local; for S3 place them under `TCO-GUI/_config/`. |
| PDF export produces an empty/zero-byte file | Chromium missing/blocked. The image bundles it; natively install Chrome or set `CHROMIUM_PATH`. |
| Analysis is slow | It fetches live Azure prices; increase `AZURE_PRICE_WORKERS` (default 8) and ensure good outbound bandwidth. |
| Data disappeared after `down -v` | `-v` deletes the volume. Use `docker compose down` (no `-v`) to keep data; back up per §5. |

---

## 8. What the image contains

- **Base:** `python:3.12-slim` (Debian bookworm).
- **System:** `chromium` (headless PDF export), `fonts-liberation`, `ca-certificates`, `tini`.
- **Python:** Flask, boto3, pandas, requests, **waitress** (production WSGI server).
- **Serves** via `waitress-serve --host=0.0.0.0 --port=5000 app:app`.
- **Volume:** `/data` (Local Storage + seeded engine configs).
- **Analysis** parallelizes the Azure retail-price lookups across a thread pool.
- **Data tables** (Results Group Breakdown, TCO Data view) paginate at 50 rows/page.
