# Upgrading the Everpure TCO app

This app runs locally in Docker (usually via **Colima** on a Mac). There are two
ways to upgrade to the latest version. **The host rebuild is the recommended,
durable method and works from any older version** — including versions too old to
have the in‑app updater.

> ⚠️ **Never** run `docker compose down -v`, and never delete the
> `everpure-tco-container_everpure_data` Docker volume. All customers, parsed
> datasets, and generated TCOs live in that volume. A normal rebuild
> (`docker compose up --build`) preserves it; only `-v` / deleting the volume
> destroys it.

---

## Option A — Host rebuild (recommended, durable)

Run these from the repo clone on the machine hosting the app.

```bash
export PATH="/opt/homebrew/bin:$PATH"   # Colima/Docker via Homebrew

# 0) Make sure Docker is running (Colima). Harmless if already up.
colima start 2>/dev/null || true

# 1) Go to the repo (see "Finding the repo" below if you don't know where it is).
cd /path/to/everpure-tco-container

# 2) Back up the data volume first (safe to run while the app is up).
./backup_everpure.sh

# 3) Pull the latest source and rebuild + restart the container.
git pull
docker compose up --build -d

# 4) Verify: the running version should match the repo's VERSION file.
cat VERSION
curl -s http://localhost:5000/api/app/current
```

Then open http://localhost:5000 and confirm the login page loads. The
**Software update** section shows the running version code.

This rebuilds the Docker **image** itself, so the image, the running container,
and the source tree all end up on the same version.

### Finding the repo

If you don't know where the clone lives, ask the running container:

```bash
docker inspect everpure-tco-container-everpure-tco-1 \
  --format '{{ index .Config.Labels "com.docker.compose.project.working_dir" }}'
```

That prints the directory `docker compose` was launched from — the repo clone.
If the container isn't running, search for it:

```bash
find "$HOME" -name docker-compose.yml -path '*everpure*' 2>/dev/null
```

If there is **no local clone at all** (e.g. you only have the image), back up the
old data first (`./backup_everpure.sh` if you can, or export from the login page),
then:

```bash
git clone https://github.com/MichaelR-xx/everpure-tco-container.git
cd everpure-tco-container
docker compose up --build -d
# restore your data into the fresh volume:
./backup_everpure.sh --restore /path/to/your_backup.tgz
```

---

## Option B — In‑app updater (fast, in place)

If the login page already shows a **Software update** section:

1. Open http://localhost:5000.
2. Click **⟳ Check for updates**. If you're behind it shows
   `current → latest` and an **⬆ Update to <version>** button.
3. Enter the admin **username/password** in the fields above.
4. Click **⬆ Update to <version>**, confirm, and wait — the server pulls the
   latest code, restarts, and the page reloads onto the new version.

This is an **in‑place** update: it overwrites the code inside the running
container. It's durable across restarts, but a later `docker compose up --build`
from a **stale** local checkout would revert it. So if you use Option B, still run
a `git pull` on the host clone at some point so the image stays in sync.

---

## Letting Claude Code drive the upgrade

On the hosting Mac, open Claude Code and paste:

> I'm running the Everpure TCO app (github.com/MichaelR-xx/everpure-tco-container)
> locally in Docker and want to upgrade to the latest version. Read UPGRADING.md
> in my clone and follow **Option A** (host rebuild). Locate the repo via
> `docker inspect` of the running container if needed, make sure Colima is
> running, back up the data volume first, then `git pull` and
> `docker compose up --build -d`. Do **not** run `down -v` or touch the
> `everpure-tco-container_everpure_data` volume. Finish by confirming the running
> version (`curl -s http://localhost:5000/api/app/current`) matches the repo's
> `VERSION`.

---

## Prerequisites

- **Colima + Docker** (or Docker Desktop) installed. The `docker compose`
  commands are identical either way.
- **git** and network access to GitHub (the repo is public — no auth needed).
- For Option B, the **admin credentials** used to sign in.
