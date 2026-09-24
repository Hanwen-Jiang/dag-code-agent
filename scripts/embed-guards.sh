#!/usr/bin/env bash
# Embeddability guards.
#
# The design doc's acceptance criteria (C1-C4) are only real if a machine
# enforces them. Each guard below corresponds to one criterion:
#
#   G1 -> C4  the core builds without CGO
#   G2 -> C4  the core does not drag in wasmtime/temporal/qdrant/pgx
#   G3 -> C3  library packages never read the process environment
#   G4 -> C3  library packages never probe relative filesystem paths
#   G5 -> C1  an unrelated Go module can import the public packages
#
# Usage:
#   ./scripts/embed-guards.sh              # run every guard
#   ./scripts/embed-guards.sh --self-test  # prove the guards can actually fail
set -uo pipefail

cd "$(dirname "$0")/.."

PASS=0
FAIL=0
CORE_PACKAGES="./harness/... ./httpapi/... ./modelenv/..."

ok()   { printf '  ok: %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  FAIL: %s\n' "$1"; FAIL=$((FAIL + 1)); }

# guard <name> <command...>
guard() {
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$name"; else bad "$name"; fi
}

# reversed <name> <command...> : passes when the command FAILS to find anything
reversed() {
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then bad "$name"; else ok "$name"; fi
}

echo "== G1: core builds with CGO disabled (C4) =="
guard "CGO_ENABLED=0 go build $CORE_PACKAGES" env CGO_ENABLED=0 go build $CORE_PACKAGES

echo
echo "== G2: core has no heavy dependencies (C4) =="
# go list -deps prints every package in the build closure; a match means the
# core would force an embedder to accept that dependency.
reversed "core does not depend on wasmtime/temporal/qdrant/pgx" \
  bash -c "go list -deps $CORE_PACKAGES 2>/dev/null | grep -qE 'wasmtime|temporal|qdrant|pgx'"

echo
echo "== G3: library packages do not read the environment (C3) =="
reversed "no os.Getenv / os.LookupEnv / signal.Notify in harness, httpapi" \
  bash -c "grep -rn --include='*.go' -E 'os\.Getenv|os\.LookupEnv|signal\.Notify' harness httpapi | grep -v '_test.go' | grep -q ."

echo
echo "== G4: library packages do not probe relative paths (C3) =="
reversed "no relative-path probing in harness, httpapi" \
  bash -c "grep -rn --include='*.go' -E 'os\.Stat\(\"\.\.|filepath\.Join\(\"\.\.' harness httpapi | grep -v '_test.go' | grep -q ."

echo
echo "== G5: an unrelated module can import the public packages (C1) =="
guard "external consumer module builds" \
  bash -c "cd examples/embed_external && go build -o /dev/null ."

echo
echo "== G6: the conformance suite passes for every backend (abstraction) =="
guard "storetest passes for SQLiteStore and InMemoryStore" go test ./harness/storetest/

echo
echo "== G6b: strategy conformance suite =="
guard "looptest passes for the built-in and a custom topology" go test ./harness/looptest/

echo
echo "== G7: dependency budget (regression guard) =="
# Design 9.2: at most 4 direct third-party dependencies in the core (currently
# 2: jsonschema + modernc.org/sqlite). A new heavy import must be a deliberate
# decision rather than an accident.
direct=$(go list -f '{{range .Imports}}{{println .}}{{end}}' ./harness 2>/dev/null \
  | grep -E '\.' | grep -v '^github.com/Hanwen-Jiang/dag-code-agent/' | sort -u | wc -l | tr -d ' ')
if [[ "$direct" -le 4 ]]; then
  ok "core has $direct direct third-party dependencies (budget 4)"
else
  bad "core has $direct direct third-party dependencies (budget 4)"
fi

echo
echo "== G8: multi-instance embedding contract (lifecycle) =="
guard "two runtimes in one process stay isolated" go test -count=1 -run TestMultipleInstancesInOneProcess ./harness/

echo
echo "== self-test (guards must be able to fail) =="
if [[ "${1:-}" == "--self-test" ]]; then
  # Plant a real violation, prove G3 notices it, then remove it even on ^C.
  probe="harness/zz_guard_selftest.go"
  cleanup() { rm -f "$probe"; }
  trap cleanup EXIT INT TERM
  cat > "$probe" <<'EOF'
package harness

import "os"

func guardSelfTestProbe() string { return os.Getenv("GUARD_SELF_TEST") }
EOF
  if bash -c "grep -rn --include='*.go' -E 'os\.Getenv|os\.LookupEnv|signal\.Notify' harness httpapi | grep -v '_test.go' | grep -q ."; then
    ok "G3 detects a planted os.Getenv violation"
  else
    bad "G3 failed to detect a planted os.Getenv violation"
  fi
  cleanup
  trap - EXIT INT TERM
else
  printf '  (skipped: pass --self-test to verify the guards can fail)\n'
fi

echo
echo "=============================="
printf 'PASS: %d   FAIL: %d\n' "$PASS" "$FAIL"
echo "=============================="
[[ "$FAIL" -eq 0 ]]
