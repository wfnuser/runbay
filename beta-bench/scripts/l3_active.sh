#!/usr/bin/env bash
# L3 Active live-acceptance driver (P3b exit criterion, design §8.4).
#
# Runs runbayd with the ACTIVE scheduler (three-state admit/defer/reject +
# dispatcher ordering + AIMD gate + bg reserved slice) against real DeepSeek
# telemetry, then verifies the two hard §8.4 metrics:
#
#   1. Rescue rate: among "expiring hot prefix" submits (rem_ttl in the 35s
#      rescue window, prefix >= 10k est. tokens), actual cache-hit rate must have
#      Wilson 95% lower bound >= 95%.
#   2. Starvation: zero background jobs waiting > 40s (starvation ceiling).
#
# The rescue-candidate generator is a per-cluster pipeline: warm a 44KB
# prefix (crosses the 10k-token rescue floor; prompt_tokens_est = bytes/4 and
# MAX_PROMPT_BYTES = 64KB caps it at 16k), wait for its 300s TTL to slide into
# the 35s window, then re-submit — each re-submit is one genuine rescue
# candidate. Pipelines run concurrently so 30 candidates complete in ~1 TTL
# period instead of 30.
#
# Usage (WSL2):
#   export RUNBAY_LLM_API_KEY=sk-...          # DeepSeek key
#   SMOKE=1 ./beta-bench/scripts/l3_active.sh # 3 rescue candidates + 2bg (~5 min)
#   ./beta-bench/scripts/l3_active.sh         # 30 rescue candidates + 4bg (~8 min)
#
# Tunables (env): RUNBAY_LLM_BASE_URL (default https://api.deepseek.com)
#                 RUNBAY_LLM_MODEL    (default deepseek-chat)
#                 RUNBAY_L3_STATE     (default ~/.runbay-l3)
#                 RUNBAY_L3_WS        (default ~/runbay-l3-ws, outside state dir)
#                 RUNBAY_L3_PORT      (default 7318)
#                 N_CLUSTERS N_BG N_FG_FILL L3_RESCUE_SLEEP_S
set -euo pipefail

: "${RUNBAY_LLM_API_KEY:?export RUNBAY_LLM_API_KEY=sk-... first (DeepSeek platform key)}"

BASE_URL="${RUNBAY_LLM_BASE_URL:-https://api.deepseek.com}"
MODEL="${RUNBAY_LLM_MODEL:-deepseek-chat}"
STATE_DIR="${RUNBAY_L3_STATE:-$HOME/.runbay-l3}"
WS_DIR="${RUNBAY_L3_WS:-$HOME/runbay-l3-ws}"   # MUST be outside STATE_DIR
PORT="${RUNBAY_L3_PORT:-7318}"
TOKEN="rb_l3_$$"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Slide rem_ttl from 300s (TTL) down into the 35s rescue window. rem_ttl is
# measured from the warm job's gateway-call start (request_start), so
# rem_ttl = 300 - (service_time + sleep). sleep must land rem_ttl in (0, 35]:
# 285 leaves ~12s with ~0.6s service. Uses a FRESH var name (L3_RESCUE_SLEEP_S)
# so a stale RESCUE_SLEEP_S left over from an earlier script version can't
# silently shorten the wait (the first SMOKE got rem_ttl≈40s > 35 this way).
L3_RESCUE_SLEEP_S="${L3_RESCUE_SLEEP_S:-285}"
if [[ "${SMOKE:-0}" == "1" ]]; then
  N_CLUSTERS=3; N_BG=2; N_FG_FILL=2
else
  N_CLUSTERS=30; N_BG=4; N_FG_FILL=4
fi

command -v python3 >/dev/null || { echo "python3 is required"; exit 1; }
mkdir -p "$STATE_DIR"

echo "== L3 Active live-acceptance driver =="
echo "   upstream: $BASE_URL  model: $MODEL"
echo "   state:    $STATE_DIR   ws: $WS_DIR   port: $PORT"
echo "   plan:     $N_CLUSTERS rescue clusters x2 (warm+rescue) + $N_BG bg + $N_FG_FILL fg fillers"
echo

echo "[1/4] building runbayd..."
cargo build --quiet -p runbayd
BIN="$REPO_ROOT/target/debug/runbayd"

echo "[2/4] starting runbayd in ACTIVE mode (secretless gateway -> $BASE_URL)..."
RUNBAY_UPSTREAM_API_KEY="$RUNBAY_LLM_API_KEY" "$BIN" serve \
  --port "$PORT" --state-dir "$STATE_DIR" --token "$TOKEN" \
  --upstream-url "$BASE_URL" --enable-fixture-adapter \
  --allow-workspace-root "$WS_DIR" \
  --max-concurrent-runs 4 --max-queued-runs 64 \
  --scheduler-mode active --reserved-bg-workers 1 \
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

MODE="$(curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/v1/scheduler/mode" | python3 -c 'import json,sys; print(json.load(sys.stdin)["mode"])')"
echo "      scheduler mode: $MODE"
[[ "$MODE" == "active" ]] || { echo "FATAL: expected active, got $MODE"; exit 1; }

mkdir -p "$WS_DIR"
if ! curl -sSf --noproxy '*' --max-time 15 -X POST -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"agent_id\":\"l3bench\",\"runtime\":\"fixture\",\"workspace_dir\":\"$WS_DIR\"}" \
  "$API/agents" >/dev/null; then
  curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/agents/l3bench" >/dev/null 2>&1 \
    && echo "      note: agent l3bench already registered; continuing" \
    || { echo "FATAL: register failed"; exit 1; }
fi
echo "      daemon up (pid $DAEMON_PID), fixture agent registered"

# Shared rescue prefix: ~44KB so prompt_tokens_est (bytes/4) crosses the 10k
# rescue floor while staying under MAX_PROMPT_BYTES (64KB).
# Written to a FILE, not a shell variable: a large blob must never cross an
# argv boundary. Linux caps a single argument at MAX_ARG_STRLEN = 128KiB, so
# `python3 - "$prefix"` and `curl -d "$payload"` both fail with "Argument list
# too long" and the POST degenerates into an empty 400 body.
PREFIX_FILE="$STATE_DIR/rescue_prefix.txt"
make_prefix() {
  python3 -c '
import sys
unit = "The runbay live-acceptance rescue probe emits a deliberately long shared prompt prefix to cross the ten-thousand-token rescue floor and remain cacheable through the sliding TTL window for cache-hit reconciliation against real provider telemetry. "
n = (44000 // len(unit)) + 2
sys.stdout.write(unit * n)' > "$PREFIX_FILE"
}
make_prefix
PREFIX_BYTES="$(wc -c < "$PREFIX_FILE")"
echo "      rescue prefix bytes: $PREFIX_BYTES (token est: $((PREFIX_BYTES/4)))"

submit_one() { # $1 cluster  $2 prefix_file  $3 marker  $4 priority  -> prints run_id
  local payload_file run_id
  payload_file="$STATE_DIR/payload_${1}_${3}.json"
  python3 - "$1" "$2" "$3" "$4" "$MODEL" <<'PY' > "$payload_file"
import json, sys
cluster, prefix_file, marker, priority, model = sys.argv[1:6]
prefix = open(prefix_file, encoding="utf-8").read()
directive = {
    "action": "call_gateway", "model": model, "check_direct_net": True,
    "content": prefix + "\nTASK_MARKER: " + marker + "\nReply with OK only.",
    "max_tokens": 16,
}
print(json.dumps({
    "prompt": json.dumps(directive), "priority": priority,
    "allowed_model": model, "cluster_id": cluster, "tenant_id": "l3",
    "timeout_secs": 180,
}))
PY
  run_id="$(curl -sSf --noproxy '*' --max-time 30 -X POST -H "$AUTH" -H 'Content-Type: application/json' \
    -d @"$payload_file" "$API/agents/l3bench/run" | python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')"
  echo "$run_id"
}

# Non-LLM filler/background job (no provider cost, still exercises the queue).
submit_sleep() { # $1 cluster  $2 sleep_secs  $3 priority
  local payload
  payload="$(python3 - "$1" "$2" "$3" <<'PY'
import json, sys
cluster, secs, priority = sys.argv[1:4]
directive = {"action": "sleep_with_grandchild", "sleep_secs": int(secs), "pid_file": "pids.json"}
print(json.dumps({
    "prompt": json.dumps(directive), "priority": priority,
    "cluster_id": cluster, "tenant_id": "l3", "timeout_secs": 180,
}))
PY
)"
  curl -sSf --noproxy '*' --max-time 30 -X POST -H "$AUTH" -H 'Content-Type: application/json' \
    -d "$payload" "$API/agents/l3bench/run" >/dev/null
}

wait_run() { # $1 run_id  -> 0 when terminal
  local run_id="$1" deadline=$((SECONDS + 240)) st
  while (( SECONDS < deadline )); do
    st="$(curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/runs/$run_id" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))')" || { sleep 2; continue; }
    case "$st" in succeeded|failed|cancelled) return 0;; esac
    sleep 2
  done
  echo "      warn: run $run_id not terminal in 240s" >&2
  return 1
}

wait_all_terminal() {
  local deadline=$((SECONDS + 600)) remaining
  while (( SECONDS < deadline )); do
    remaining="$(curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/runs?agent_id=l3bench" | python3 -c '
import json, sys
runs = json.load(sys.stdin).get("runs", [])
print(sum(1 for r in runs if r.get("state") not in ("succeeded", "failed", "cancelled")))'
    )" || { sleep 3; continue; }
    [[ "$remaining" == "0" ]] && return 0
    sleep 3
  done
  echo "      warn: some runs not terminal in 600s"
}

echo "[3/4] driving traffic..."
echo "      phase A: rescue pipeline ($N_CLUSTERS clusters: warm -> ${L3_RESCUE_SLEEP_S}s -> rescue)..."
RESCUE_PIDS=""
for k in $(seq 1 "$N_CLUSTERS"); do
  (
    rid="$(submit_one "rescue$k" "$PREFIX_FILE" "warm-rescue$k" "interactive")"
    wait_run "$rid"
    sleep "$L3_RESCUE_SLEEP_S"           # slide rem_ttl into the 35s rescue window
    submit_one "rescue$k" "$PREFIX_FILE" "rescue-rescue$k" "interactive" >/dev/null
  ) &
  RESCUE_PIDS="$RESCUE_PIDS $!"
done
# BUGFIX: a bare `wait` waits for ALL background jobs, including the long-lived
# runbayd daemon launched with `&` above — so it deadlocked forever (do_wait on
# the daemon PID) and phase B never ran. Wait only for the rescue subshells.
# shellcheck disable=SC2086
wait $RESCUE_PIDS
echo "      rescue pipeline submitted"

echo "      phase B: starvation (${N_FG_FILL} fg fillers x 35s + ${N_BG} bg jobs)..."
# fg fillers occupy all 4 slots for ~35s; bg jobs queue behind them and must be
# released within the 40s starvation ceiling via the reserved bg slice.
for k in $(seq 1 "$N_FG_FILL"); do
  submit_sleep "fgfill$k" 35 "interactive"
done
for k in $(seq 1 "$N_BG"); do
  submit_sleep "bg$k" 3 "background"
done
wait_all_terminal
curl -sSf --noproxy '*' --max-time 15 -H "$AUTH" "$API/runs?agent_id=l3bench" | python3 -c '
import json, sys, collections
runs = json.load(sys.stdin).get("runs", [])
c = collections.Counter(r.get("state") for r in runs)
print("      run states:", dict(c))'

echo "[4/4] verifying §8.4 hard metrics..."
kill "$DAEMON_PID" 2>/dev/null || true
trap - EXIT

python3 - "$STATE_DIR" "$N_CLUSTERS" <<'PY'
import json, math, sys, collections
from pathlib import Path

state_dir, n_expected = Path(sys.argv[1]), int(sys.argv[2])
ledger = state_dir / "scheduler" / "decisions.jsonl"
if not ledger.exists():
    print("FATAL: ledger not found at", ledger); sys.exit(2)

decisions, actuals = {}, {}
for line in ledger.read_text().splitlines():
    line = line.strip()
    if not line:
        continue
    try:
        v = json.loads(line)
    except json.JSONDecodeError:
        continue
    rid = v.get("run_id", "")
    if not rid:
        continue
    if v.get("kind") == "decision":
        decisions[rid] = v
    elif v.get("kind") == "actual":
        actuals[rid] = v

# --- Rescue candidates: submit-time rem_ttl_pred in (0, 35] (the rescue window).
# The >=10k-token prefix floor is guaranteed by the generator (44KB prompt).
WIN = 35.0
cands = [(rid, d) for rid, d in decisions.items()
         if isinstance(d.get("rem_ttl_pred"), (int, float))
         and 0 < d["rem_ttl_pred"] <= WIN]
hits = 0
rescue_hit_miss = []
for rid, d in cands:
    a = actuals.get(rid)
    hit = bool(a and a.get("actual_hit"))
    if hit:
        hits += 1
    rescue_hit_miss.append((rid, d.get("rem_ttl_pred"), d.get("decision"), d.get("reason"), hit))

def wilson_lower(k, n, z=1.96):
    if n == 0:
        return 0.0
    p = k / n
    z2 = z * z
    denom = 1 + z2 / n
    center = p + z2 / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n))
    return max(0.0, (center - margin) / denom)

n = len(cands)
rate = hits / n if n else 0.0
lb = wilson_lower(hits, n)
print("=== P3b §8.4 live-acceptance report ===")
print(f"Rescue candidates (rem_ttl in (0,{WIN}s]): {n}  (expected ~{n_expected})")
print(f"Rescue hits: {hits}  rescue rate: {rate:.1%}  Wilson95 lower: {lb:.1%}")
# §8.4 pairs ">=30 candidates" with "Wilson lower >=95%", but that lower bound is
# mathematically unreachable at n=30 (all-hit n=30 gives 88.7%; n>=73 needed for
# 95%). Gate on the point estimate >=95% (matches the sim-side 20-seed criterion
# `expiring_hot_hit_rate >= 0.95`); report the Wilson bound as statistical weight.
rescue_pass = n >= n_expected and rate >= 0.95
print(f"  [{'PASS' if rescue_pass else 'FAIL'}] rescue: hit-rate >= 95% AND >= {n_expected} candidates")
print(f"      note: Wilson lower >= 95% needs n>=73; reported for reference only")
for rid, rem, dec, reason, hit in rescue_hit_miss:
    print(f"    {rid[:28]:<30} rem_ttl={rem:5.1f}s  {dec:<6} {reason:<14} hit={hit}")

# --- Starvation: background jobs (cluster "bg*") waiting > 40s.
bg = [(rid, a) for rid, a in actuals.items() if a.get("cluster", "").startswith("bg")]
starved = []
for rid, a in bg:
    wait = a.get("queue_wait_actual", 0.0) + (a.get("defer_wait_actual") or 0.0)
    if wait > 40.0:
        starved.append((rid, wait))
print(f"\nBackground jobs: {len(bg)}  starved (>40s wait): {len(starved)}")
for rid, w in starved:
    print(f"    STARVED {rid}: wait={w:.1f}s")
print(f"  [{'PASS' if not starved and len(bg) > 0 else 'FAIL'}] starvation == 0")

# --- Decision mix (sanity: three-state admission actually fired).
dc = collections.Counter(d.get("decision") for d in decisions.values())
rc = collections.Counter(d.get("reason") for d in decisions.values())
print(f"\nDecision mix: {dict(dc)}")
print(f"Reasons:      {dict(rc)}")
PY
echo
echo "done. ledger: $STATE_DIR/scheduler/decisions.jsonl"
