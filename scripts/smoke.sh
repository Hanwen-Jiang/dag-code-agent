#!/usr/bin/env bash
# End-to-end smoke test for the three-layer stack.
# Each service is started without external infrastructure and must degrade
# gracefully (local adapters / in-memory stores).
#
#   make build && ./scripts/smoke.sh
set -euo pipefail

cd "$(dirname "$0")/.."

BIN=bin
PASS=0
FAIL=0
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

wait_http() { # url name timeout_s
  local url=$1 name=$2 timeout=${3:-15} i=0
  while [ $i -lt $timeout ]; do
    if curl -sf -o /dev/null "$url"; then
      echo "  ok: $name ready"
      return 0
    fi
    i=$((i+1))
    sleep 1
  done
  echo "  FAIL: $name did not become ready ($url)"
  return 1
}

check() { # name condition...
  local name=$1; shift
  if "$@" >/dev/null 2>&1; then
    echo "  ok: $name"; PASS=$((PASS+1))
  else
    echo "  FAIL: $name"; FAIL=$((FAIL+1))
  fi
}

check_match() { # name pattern text
  local name=$1 pattern=$2 text=$3
  if printf '%s' "$text" | grep -q "$pattern"; then
    echo "  ok: $name"; PASS=$((PASS+1))
  else
    echo "  FAIL: $name"; FAIL=$((FAIL+1))
  fi
}

echo "=== 1. agent-harness (control plane) ==="
HARNESS_DB_PATH=$(mktemp -d)/harness.sqlite \
  "$BIN/agent-harness" -mode server -addr 127.0.0.1:18080 >/tmp/harness.log 2>&1 &
PIDS+=($!)
wait_http http://127.0.0.1:18080/health "agent-harness" || true
check "health endpoint"    curl -sf http://127.0.0.1:18080/health
check "model status"       curl -sf http://127.0.0.1:18080/api/model/status
check "offline demo run"   curl -sf http://127.0.0.1:18080/api/harness/demo
# create a durable session and inject a message
TASK_ID=$(curl -sf -X POST http://127.0.0.1:18080/api/harness/runs \
  -H 'Content-Type: application/json' \
  -d '{"task_id":"smoke-task-1","goal":"verify the control plane","tenant_id":"t1","subject_id":"s1"}' \
  | sed -E 's/.*"task_id":"([^"]+)".*/\1/')
check "session created"    test -n "$TASK_ID"
curl -sf -X POST "http://127.0.0.1:18080/api/harness/runs/$TASK_ID/messages" \
  -H 'Content-Type: application/json' -d '{"message":"hello"}' >/dev/null || true
check "session events"     curl -sf "http://127.0.0.1:18080/api/harness/runs/$TASK_ID/events"
check "dag manifest"       curl -sf http://127.0.0.1:18080/api/dag/manifest

echo "=== 2. llm-service (model layer, offline) ==="
PORT=18081 "$BIN/llm-service" >/tmp/llm.log 2>&1 &
PIDS+=($!)
wait_http http://127.0.0.1:18081/health "llm-service" || true
check "health"  curl -sf http://127.0.0.1:18081/health
check "embeddings (local)" curl -sf -X POST http://127.0.0.1:18081/embeddings \
  -H 'Content-Type: application/json' -d '{"texts":["hello world","goodbye"],"model":"local"}'
check "offline completion" curl -sf -X POST http://127.0.0.1:18081/complete \
  -H 'Content-Type: application/json' -d '{"query":"ping","model_tier":"offline"}'

echo "=== 3. agent-core (execution layer, gRPC + HTTP mgmt) ==="
AGENT_CORE_GRPC_PORT=18051 AGENT_CORE_HTTP_PORT=18052 "$BIN/agent-core" >/tmp/agent-core.log 2>&1 &
PIDS+=($!)
wait_http http://127.0.0.1:18052/health "agent-core" || true
check "health"    curl -sf http://127.0.0.1:18052/health
check "discover"  curl -sf http://127.0.0.1:18052/api/tools
# WASM round-trip through the sandbox (real .wasm module, base64-encoded)
if [ -f testdata/hello.wasm ]; then
  B64=$(base64 < testdata/hello.wasm | tr -d '\n')
  check "wasi execute" bash -c "curl -sf -X POST http://127.0.0.1:18052/api/execute -H 'Content-Type: application/json' -d '{\"tool\":\"wasi\",\"wasm_b64\":\"$B64\"}'"
else
  echo "  skip: testdata/hello.wasm not found"
fi

echo "=== 4. xai-dashboard (transparency layer) ==="
PORT=18030 "$BIN/xai-dashboard" >/tmp/xai.log 2>&1 &
PIDS+=($!)
wait_http http://127.0.0.1:18030/health "xai-dashboard" || true
check "health"     curl -sf http://127.0.0.1:18030/health
check "traces api" curl -sf http://127.0.0.1:18030/api/traces
check "static ui"  curl -sf http://127.0.0.1:18030/index.html

echo "=== 5. orchestrator (workflow layer -> execution layer) ==="
# Point the orchestrator at the agent-core started above. Without this the link
# is never exercised: the default address (50051) has nothing behind it, so every
# step would report a degradation and the wiring would be untested.
AGENT_CORE_ADDR=127.0.0.1:18051 ORCH_PORT=18083 TEMPORAL_PORT=19999 "$BIN/orchestrator" >/tmp/orch.log 2>&1 &
PIDS+=($!)
wait_http http://127.0.0.1:18083/health "orchestrator" || true
check "health" curl -sf http://127.0.0.1:18083/health
check "local-mode query" curl -sf -X POST http://127.0.0.1:18083/agent/query \
  -H 'Content-Type: application/json' -d '{"query":"hello","model_tier":"offline"}'

# The startup handshake must have found the execution layer, and its advertised
# capabilities must be visible; this is what distinguishes "wired" from
# "silently degraded".
ORCH_HEALTH=$(curl -sf http://127.0.0.1:18083/health || true)
check_match "agent-core handshake reachable" '"reachable": *true' "$ORCH_HEALTH"

# A sandbox step must actually be executed BY agent-core over gRPC, not by the
# local fallback. openssl is used for base64 because macOS and GNU base64 take
# incompatible flags.
WASM_B64=$(openssl base64 -A -in testdata/hello.wasm 2>/dev/null || true)
if [ -n "$WASM_B64" ]; then
  SANDBOX_RESPONSE=$(curl -sf -X POST http://127.0.0.1:18083/agent/query \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"run this code in the sandbox\",\"context\":{\"wasm_b64\":\"$WASM_B64\"}}" || true)
  check_match "sandbox step executed by agent-core" '"agent-core"' "$SANDBOX_RESPONSE"
  check_match "agent-core returned real WASI output" 'hello from wasi' "$SANDBOX_RESPONSE"
else
  echo "  FAIL: encode testdata/hello.wasm"; FAIL=$((FAIL+1))
fi

echo "=== 6. self-evolution (offline baseline) ==="
# macOS has no `timeout`; fall back to a background kill.
if command -v timeout >/dev/null 2>&1; then
  SELF_EVOLUTION_ONESHOT=1 timeout 60 "$BIN/self-evolution" >/tmp/selfevo.log 2>&1
  SEVO_RC=$?
else
  SELF_EVOLUTION_ONESHOT=1 "$BIN/self-evolution" >/tmp/selfevo.log 2>&1 &
  SEVO_PID=$!
  ( sleep 60; kill "$SEVO_PID" 2>/dev/null ) &
  WAIT_PID=$!
  wait "$SEVO_PID"; SEVO_RC=$?
  kill "$WAIT_PID" 2>/dev/null || true
fi
check "baseline run finished" test "$SEVO_RC" -eq 0

echo
echo "=============================="
echo "PASS: $PASS   FAIL: $FAIL"
echo "=============================="
[ "$FAIL" -eq 0 ]
