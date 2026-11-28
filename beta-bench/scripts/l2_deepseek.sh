#!/usr/bin/env bash
# L2 semi-real reconciliation driver for P3a exit criterion.
#
# Drives runbayd (shadow scheduler + decision ledger) with fixture workers that
# make REAL LLM calls through the secretless gateway (shared hot prefixes +
# varied tails), then runs bench_recon to reconcile predicted vs actual cache
# hits from real provider telemetry (DeepSeek prompt_cache_hit_tokens).
#
# Usage (WSL2):
#   export RUNBAY_LLM_API_KEY=sk-...        # DeepSeek key (never logged, never committed)
#   ./beta-bench/scripts/l2_deepseek.sh     # default: 2 batches x 3 clusters x 12 runs
#   SMOKE=1 ./beta-bench/scripts/l2_deepseek.sh   # ~6 cheap calls, pipeline check only
#
# Cross-TTL (expiry) samples: either run with BATCH_GAP_S=14400 (4h between
# batches), or simply run this script again hours later — the cache table and
# ledger persist in STATE_DIR across restarts, and the gap buckets in the
# report separate the regimes.
#
# Tunables (env): RUNBAY_LLM_BASE_URL (default https://api.deepseek.com)
#                 RUNBAY_LLM_MODEL    (default deepseek-chat)
#                 RUNBAY_L2_STATE     (default ~/.runbay-l2)
#                 RUNBAY_L2_WS        (default ~/runbay-l2-ws; must be OUTSIDE the state dir)
#                 RUNBAY_L2_PORT      (default 7317)
#                 BATCHES BATCH_GAP_S PER_CLUSTER PACE_S
set -euo pipefail

: "${RUNBAY_LLM_API_KEY:?export RUNBAY_LLM_API_KEY=sk-... first (DeepSeek platform key)}"

BASE_URL="${RUNBAY_LLM_BASE_URL:-https://api.deepseek.com}"
MODEL="${RUNBAY_LLM_MODEL:-deepseek-chat}"
STATE_DIR="${RUNBAY_L2_STATE:-$HOME/.runbay-l2}"
WS_DIR="${RUNBAY_L2_WS:-$HOME/runbay-l2-ws}"   # MUST be outside STATE_DIR (daemon security boundary)
PORT="${RUNBAY_L2_PORT:-7317}"
TOKEN="rb_l2_$$"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

if [[ "${SMOKE:-0}" == "1" ]]; then
  BATCHES=1; PER_CLUSTER=2; BATCH_GAP_S=0; PACE_S=0; N_COLD=3
fi
BATCHES="${BATCHES:-2}"
PER_CLUSTER="${PER_CLUSTER:-12}"
BATCH_GAP_S="${BATCH_GAP_S:-300}"
PACE_S="${PACE_S:-2}"
N_COLD="${N_COLD:-30}"   # cold clusters x 2 runs: guaranteed predicted-miss samples

command -v python3 >/dev/null || { echo "python3 is required for payload building"; exit 1; }
mkdir -p "$STATE_DIR"

echo "== L2 driver =="
echo "   upstream:  $BASE_URL  model: $MODEL"
echo "   state:     $STATE_DIR   (ledger persists across restarts)"
echo "   plan:      $BATCHES batch(es) x 3 clusters x $PER_CLUSTER runs, gap ${BATCH_GAP_S}s + $N_COLD cold clusters x 2"
echo

echo "[1/4] building runbayd + bench_recon..."
cargo build --quiet -p runbayd
cargo build --quiet -p runbay-scheduler --bin bench_recon
BIN="$REPO_ROOT/target/debug/runbayd"
RECON="$REPO_ROOT/target/debug/bench_recon"

echo "[2/4] starting runbayd (secretless gateway -> $BASE_URL)..."
RUNBAY_UPSTREAM_API_KEY="$RUNBAY_LLM_API_KEY" "$BIN" serve \
  --port "$PORT" --state-dir "$STATE_DIR" --token "$TOKEN" \
  --upstream-url "$BASE_URL" --enable-fixture-adapter \
  --allow-workspace-root "$WS_DIR" \
  --max-concurrent-runs 4 --max-queued-runs 64 \
  >"$STATE_DIR/daemon.log" 2>&1 &
DAEMON_PID=$!
trap 'kill "$DAEMON_PID" 2>/dev/null || true' EXIT

AUTH="Authorization: Bearer $TOKEN"
API="http://127.0.0.1:$PORT"
for _ in $(seq 1 20); do
  curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/v1/status" >/dev/null && break
  sleep 0.5
done
curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/v1/status" >/dev/null || {
  echo "runbayd failed to start; see $STATE_DIR/daemon.log"; exit 1; }

mkdir -p "$WS_DIR"
if ! curl -sSf --noproxy '*' --max-time 15 -X POST -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"agent_id\":\"l2bench\",\"runtime\":\"fixture\",\"workspace_dir\":\"$WS_DIR\"}" \
  "$API/agents" >/dev/null; then
  curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/agents/l2bench" >/dev/null 2>&1 \
    && echo "      note: agent l2bench already registered (previous run); continuing" \
    || { echo "FATAL: register failed and agent l2bench does not exist; see errors above"; exit 1; }
fi
echo "      daemon up (pid $DAEMON_PID), fixture agent registered"

# Shared hot prefix per cluster (~2.5K tokens, MUST exceed the ledger's
# min-cacheable threshold of 1024 tokens): first call warms the provider
# cache, subsequent calls with the same head should report cached tokens.
PREFIX_C1="$(python3 -c 'print("The runbay cache ledger reconciler treats each session cluster as one shared hot prefix. " * 150)')"
PREFIX_C2="$(python3 -c 'print("Guarded dual admission compares remaining TTL against estimated service time before deferring a job. " * 150)')"
PREFIX_C3="$(python3 -c 'print("Session aware scheduling keeps warm prompt prefixes alive through sliding renewal on every cache hit. " * 150)')"

submit_one() { # $1 cluster  $2 prefix  $3 tail marker
  local payload
  payload="$(python3 - "$1" "$2" "$3" "$MODEL" <<'PY'
import json, sys
cluster, prefix, marker, model = sys.argv[1:5]
directive = {
    "action": "call_gateway",
    "model": model,
    "check_direct_net": True,
    "content": prefix + "\nTASK_MARKER: " + marker + "\nReply with OK only.",
    "max_tokens": 16,
}
print(json.dumps({
    "prompt": json.dumps(directive),
    "priority": "interactive",
    "allowed_model": model,
    "cluster_id": cluster,
    "tenant_id": "l2",
    "timeout_secs": 90,
}))
PY
)"
  curl -sSf --noproxy '*' --max-time 15 -X POST -H "$AUTH" -H 'Content-Type: application/json' \
    -d "$payload" "$API/agents/l2bench/run" >/dev/null
}

# Serialize submissions: decision is evaluated at submit time (shadow), so run
# N+1 of a cluster must be submitted only after run N's gateway call has landed
# in the cache ledger -- otherwise every decision sees a cold table and every
# LLM call races its sibling (both quadrants go cold, smoke shows 0% hits).
wait_terminal() {
  local deadline=$((SECONDS + 300)) remaining
  while (( SECONDS < deadline )); do
    remaining="$(curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/runs?agent_id=l2bench" | python3 -c '
import json, sys
runs = json.load(sys.stdin).get("runs", [])
print(sum(1 for r in runs if r.get("state") not in ("succeeded", "failed", "cancelled")))'
    )" || return 1
    [[ "$remaining" == "0" ]] && return 0
    sleep 2
  done
  echo "      warn: runs did not all reach terminal state within 300s; continuing"
  return 0
}

echo "[3/4] driving traffic (serial: one run in flight at a time)..."
for b in $(seq 1 "$BATCHES"); do
  for cluster_prefix in "c1:$PREFIX_C1" "c2:$PREFIX_C2" "c3:$PREFIX_C3"; do
    cluster="${cluster_prefix%%:*}"
    prefix="${cluster_prefix#*:}"
    for i in $(seq 1 "$PER_CLUSTER"); do
      submit_one "$cluster" "$prefix" "b${b}-${cluster}-${i}"
      wait_terminal
    done
  done
  echo "      batch $b submitted"
  if [[ "$b" -lt "$BATCHES" && "$BATCH_GAP_S" -gt 0 ]]; then
    echo "      sleeping ${BATCH_GAP_S}s before next batch (cross-TTL sample window)..."
    sleep "$BATCH_GAP_S"
  fi
done

# Cold clusters: nonce-unique prefix heads -> first run is a GENUINE cold miss
# (predicted cold x actual miss), second run exercises the warm path on a fresh
# cluster. Fills the >=30 predicted-miss side of the acceptance bar.
NONCE="$(date +%s)-$$"
echo "      cold phase: $N_COLD clusters x 2 runs (nonce $NONCE)..."
for k in $(seq 1 "$N_COLD"); do
  cold_prefix="$(python3 -c "print('Unique cold nonce ${NONCE}-${k} keeps this prompt head uncached and fresh for the shadow ledger test. ' * 150)")"
  submit_one "x$k" "$cold_prefix" "cold-x$k-1"
  wait_terminal
  submit_one "x$k" "$cold_prefix" "cold-x$k-2"
  wait_terminal
done
echo "      cold phase submitted"

echo "      waiting for runs to reach terminal state..."
deadline=$((SECONDS + 900))
while (( SECONDS < deadline )); do
  remaining="$(curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/runs?agent_id=l2bench" | python3 -c '
import json, sys
runs = json.load(sys.stdin).get("runs", [])
print(sum(1 for r in runs if r.get("state") not in ("succeeded", "failed", "cancelled")))'
  )"
  [[ "$remaining" == "0" ]] && break
  sleep 5
done
curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/runs?agent_id=l2bench" | python3 -c '
import json, sys, collections
runs = json.load(sys.stdin).get("runs", [])
c = collections.Counter(r.get("state") for r in runs)
print("      run states:", dict(c))'

echo "[4/4] reconciliation..."
kill "$DAEMON_PID" 2>/dev/null || true
trap - EXIT
"$RECON" "$STATE_DIR"
echo
echo "done. ledger: $STATE_DIR/scheduler/decisions.jsonl"
echo "      report: $STATE_DIR/scheduler/recon_report.json"
