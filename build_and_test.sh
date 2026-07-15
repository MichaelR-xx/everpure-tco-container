#!/usr/bin/env bash
# Build the Everpure TCO Docker image and smoke-test it end to end.
# Usage (on any machine with Docker — e.g. your Mac):
#   ./build_and_test.sh
# Exits non-zero if the build or any check fails. Prints everything it does so the
# full output can be shared back for review.
set -uo pipefail

IMAGE="everpure-tco:test"
NAME="everpure_tco_test"
PORT="${PORT:-5055}"        # host port (avoid Chrome's blocked 5060/5061 if you open it in a browser)
FAIL=0
say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
ok()  { printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAIL=1; }

command -v docker >/dev/null 2>&1 || { echo "docker not found on PATH"; exit 2; }

say "docker version"; docker --version

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

say "docker build"
if docker build -t "$IMAGE" .; then ok "image built: $IMAGE"; else bad "docker build failed"; exit 1; fi

say "verify chromium + waitress-serve are present in the image"
docker run --rm --entrypoint sh "$IMAGE" -c 'command -v chromium && command -v waitress-serve' \
  && ok "chromium + waitress-serve on PATH" || bad "chromium or waitress-serve missing"

say "verify seeded engine configs exist in the image"
docker run --rm --entrypoint sh "$IMAGE" -c 'ls -1 /data/EverpureTCO/TCO-GUI/_config/' \
  | grep -q ec_config.json && ok "ec/ecan configs seeded" || bad "engine configs not seeded"

say "run the container (Local Storage on a fresh volume)"
docker run -d --name "$NAME" -p "${PORT}:5000" \
  -e FLASK_SECRET_KEY=smoketest -e EVERPURE_STORAGE=local -e EVERPURE_LOCAL_ROOT=/data \
  "$IMAGE" >/dev/null && ok "container started" || { bad "container did not start"; exit 1; }

say "wait for HTTP readiness"
UP=0
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${PORT}/" 2>/dev/null || echo 000)
  [ "$code" = "200" ] && { UP=1; break; }
  sleep 1
done
[ "$UP" = "1" ] && ok "app serving 200 on :${PORT}" || bad "app never returned 200"

say "check auth-status + env storage recognized (no setup screen)"
STATUS=$(curl -s "http://localhost:${PORT}/api/auth/status" 2>/dev/null || true)
echo "  $STATUS"
echo "$STATUS" | grep -q '"storage_kind":"local"' && ok "storage_kind=local (env storage works)" || bad "env storage not recognized"

say "check login works"
LOGIN=$(curl -s -X POST "http://localhost:${PORT}/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"password123"}' 2>/dev/null || true)
echo "  $LOGIN"
echo "$LOGIN" | grep -q '"ok":true' && ok "login succeeded" || bad "login failed"

say "container logs (last 30 lines)"
docker logs --tail 30 "$NAME" 2>&1 | sed 's/^/  /'

say "RESULT"
if [ "$FAIL" = "0" ]; then echo "  ✅ ALL CHECKS PASSED"; else echo "  ❌ SOME CHECKS FAILED"; fi
exit $FAIL
