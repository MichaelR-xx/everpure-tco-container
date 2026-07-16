#!/usr/bin/env bash
# deploy_mac.sh — one-shot macOS deploy for the Everpure TCO tool using Colima,
# a free, open-source Docker Desktop alternative (no licensed software required).
#
# It checks EVERY prerequisite and offers to install anything missing
# (Xcode CLT, Homebrew, Colima, the docker CLI, docker compose), starts the
# Docker VM, builds + runs the container, waits until it's healthy, and opens it.
#
# Usage:
#   ./deploy_mac.sh          # interactive — asks before installing anything
#   ./deploy_mac.sh --yes    # non-interactive — auto-install missing prereqs
#   ./deploy_mac.sh --down    # stop the app (keeps its data volume)
#   ./deploy_mac.sh --destroy # stop the app AND delete its data volume
#   ./deploy_mac.sh --logs    # follow the container logs
set -euo pipefail

# ── config ───────────────────────────────────────────────────────────────────
PORT=5000
URL="http://localhost:${PORT}"
HEALTH="${URL}/api/auth/status"
APP_NAME="Everpure TCO"
COLIMA_CPU=2          # cores
COLIMA_MEM=4          # GiB
COLIMA_DISK=20        # GiB

# ── ui helpers ─────────────────────────────────────────────────────────────────
bold(){ printf '\033[1m%s\033[0m\n' "$*"; }
ok(){   printf '  \033[32m\xe2\x9c\x93\033[0m %s\n' "$*"; }
warn(){ printf '  \033[33m!\033[0m %s\n' "$*"; }
err(){  printf '  \033[31m\xe2\x9c\x97\033[0m %s\n' "$*" >&2; }
step(){ printf '\n\033[1m==> %s\033[0m\n' "$*"; }
have(){ command -v "$1" >/dev/null 2>&1; }

ARG="${1:-}"
AUTO_YES=0
case "$ARG" in --yes|-y) AUTO_YES=1 ;; esac
ask(){ # ask "question"  -> 0 if yes
  [ "$AUTO_YES" = 1 ] && return 0
  local a; read -r -p "  $1 [Y/n] " a </dev/tty || a=""
  case "$a" in ""|y|Y|yes|YES) return 0 ;; *) return 1 ;; esac
}

cd "$(cd "$(dirname "$0")" && pwd)"   # always run from the repo root (script lives here)

# docker compose v2 plugin if present, else the standalone docker-compose
compose(){ if docker compose version >/dev/null 2>&1; then docker compose "$@"; else docker-compose "$@"; fi; }

# ── management shortcuts ───────────────────────────────────────────────────────
case "$ARG" in
  --down)    step "Stopping ${APP_NAME}"; compose down; ok "stopped (data kept)"; exit 0 ;;
  --destroy) step "Stopping ${APP_NAME} and deleting its data"; compose down -v; ok "stopped, data volume removed"; exit 0 ;;
  --logs)    compose logs -f; exit 0 ;;
esac

# ── 1. platform ────────────────────────────────────────────────────────────────
step "Platform"
[ "$(uname -s)" = "Darwin" ] || { err "This script is for macOS. On Linux/Windows just run: docker compose up --build"; exit 1; }
ok "macOS ($(uname -m))"

# ── 2. Xcode Command Line Tools (Homebrew needs them) ──────────────────────────
step "Xcode Command Line Tools"
if xcode-select -p >/dev/null 2>&1; then ok "present"; else
  warn "not installed (Homebrew needs them)."
  if ask "Install now (a macOS dialog will open)?"; then
    xcode-select --install || true
    err "Finish the Command Line Tools install, then re-run this script."; exit 1
  else err "Required. Install with: xcode-select --install"; exit 1; fi
fi

# ── 3. Homebrew ────────────────────────────────────────────────────────────────
step "Homebrew"
if ! have brew; then
  [ -x /opt/homebrew/bin/brew ] && eval "$(/opt/homebrew/bin/brew shellenv)" || true
  [ -x /usr/local/bin/brew ]  && eval "$(/usr/local/bin/brew shellenv)"  || true
fi
if ! have brew; then
  warn "Homebrew (free package manager) is not installed."
  if ask "Install Homebrew now?"; then
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    [ -x /opt/homebrew/bin/brew ] && eval "$(/opt/homebrew/bin/brew shellenv)"
    [ -x /usr/local/bin/brew ]  && eval "$(/usr/local/bin/brew shellenv)"
  else err "Required. Install from https://brew.sh then re-run."; exit 1; fi
fi
ok "Homebrew $(brew --version | head -1 | awk '{print $2}')"

# ── 4. colima + docker CLI + docker compose (all open source) ──────────────────
for f in colima docker docker-compose; do
  step "Package: $f"
  if brew list "$f" >/dev/null 2>&1 || have "$f"; then ok "installed"; else
    warn "not installed."
    if ask "Run 'brew install $f'?"; then brew install "$f"; ok "installed $f"
    else err "$f is required."; exit 1; fi
  fi
done
# make sure the compose v2 plugin is discoverable by the docker CLI
mkdir -p "$HOME/.docker/cli-plugins"
if ! docker compose version >/dev/null 2>&1; then
  BP="$(brew --prefix)/lib/docker/cli-plugins/docker-compose"
  [ -x "$BP" ] && ln -sf "$BP" "$HOME/.docker/cli-plugins/docker-compose" || true
fi
compose version >/dev/null 2>&1 && ok "compose: $(compose version 2>/dev/null | head -1)" || warn "using fallback docker-compose"

# ── 5. Colima VM ───────────────────────────────────────────────────────────────
step "Colima (Docker engine VM)"
if colima status >/dev/null 2>&1; then ok "already running"; else
  echo "  starting a Linux VM: ${COLIMA_CPU} CPU / ${COLIMA_MEM} GiB RAM / ${COLIMA_DISK} GiB disk (first run pulls an image)…"
  colima start --cpu "$COLIMA_CPU" --memory "$COLIMA_MEM" --disk "$COLIMA_DISK"
  ok "started"
fi

# ── 6. Docker engine reachable ─────────────────────────────────────────────────
step "Docker engine"
docker info >/dev/null 2>&1 || { err "Docker engine not reachable. Try: colima start"; exit 1; }
ok "reachable via Colima"

# ── 7. repo sanity ─────────────────────────────────────────────────────────────
[ -f docker-compose.yml ] || { err "docker-compose.yml not found — run this from the repo root."; exit 1; }

# ── 8. build + run ─────────────────────────────────────────────────────────────
step "Building and starting ${APP_NAME} (this can take a few minutes the first time)"
compose up -d --build
ok "container is up"

# ── 9. wait for health ─────────────────────────────────────────────────────────
step "Waiting for ${URL} to respond"
UP=0
for _ in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$HEALTH" 2>/dev/null || echo 000)" = "200" ] && { UP=1; break; }
  sleep 2
done
if [ "$UP" = 1 ]; then ok "serving on ${URL}"
else warn "app hasn't responded yet — recent logs:"; compose logs --tail 40; fi

# ── 10. open + summary ─────────────────────────────────────────────────────────
open "$URL" >/dev/null 2>&1 || true
echo
bold "${APP_NAME} is running → ${URL}"
echo "  Default logins: admin / password123  (or demo / demo) — change before real use."
warn "docker-compose.yml ships a placeholder FLASK_SECRET_KEY; set a real one for production."
echo
echo "  Manage it:"
echo "    ./deploy_mac.sh --logs      # follow logs"
echo "    ./deploy_mac.sh --down      # stop (keeps data)"
echo "    ./deploy_mac.sh --destroy   # stop + delete the data volume"
echo "    colima stop                 # stop the whole Docker VM"
