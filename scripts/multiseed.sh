#!/bin/sh
# Diff the Rust binary against the Python oracle on many generator seeds.
# Usage: scripts/multiseed.sh FIRST_SEED LAST_SEED [EVENTS] ; needs GEN (path to `generate`) in env.
set -eu
FIRST=${1:-2}; LAST=${2:-10}; EVENTS=${3:-100000}
CLICKS=$((EVENTS / 4))
GEN=${GEN:-generate}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${PY:-$ROOT/.venv/bin/python3}
BIN=${BIN:-$ROOT/target/release/shopping}
W=$(mktemp -d)
bad=0
for seed in $(seq "$FIRST" "$LAST"); do
  rm -rf "$W/d" "$W/rust" "$W/oracle"
  "$GEN" --seed "$seed" --events "$EVENTS" --clicks "$CLICKS" --output "$W/d" >/dev/null
  mkdir -p "$W/rust"
  DATA_DIR="$W/d" OUTPUT_DIR="$W/rust" "$BIN"
  "$PY" "$ROOT/scripts/oracle.py" "$W/d" "$W/oracle"
  if "$PY" "$ROOT/scripts/compare.py" "$W/rust" "$W/oracle" >"$W/log" 2>&1; then
    echo "seed $seed: MATCH"
  else
    echo "seed $seed: DIFFER"; tail -20 "$W/log"; bad=1
  fi
done
rm -rf "$W"
exit $bad
