#!/usr/bin/env bash
# Release check: prove a tagged version is installable by a consumer.
#
# Tagging is a local act; being installable is not. The in-repo example
# (examples/embed_external) uses a replace directive, so it validates the
# packages but says nothing about whether a published version resolves. This
# script builds a throwaway module in a temp directory and fetches the real
# tag, which is the only way to know the release is usable.
#
# Usage:
#   ./scripts/release-check.sh            # checks the latest tag
#   ./scripts/release-check.sh v0.1.0     # checks a specific tag
#
# The repository is private, so go must fetch it over git with the local
# credentials rather than through the public module proxy. GOPRIVATE arranges
# that; the credential helper comes from the developer's git config.
set -uo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
  VERSION="$(git describe --tags --abbrev=0 2>/dev/null || true)"
fi
if [[ -z "$VERSION" ]]; then
  echo "no tag found: tag a release first (git tag -a v0.1.0 -m ...)" >&2
  exit 1
fi

MODULE="$(head -1 go.mod | awk '{print $2}')"
echo "== release check: $MODULE@$VERSION =="

WORKDIR="$(mktemp -d)"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

export GOPRIVATE="${GOPRIVATE:-github.com/Hanwen-Jiang/*}"
export GONOSUMDB="$GOPRIVATE"
export GOFLAGS=-mod=mod
export GOWORK=off

cd "$WORKDIR"
cat > go.mod <<EOF
module example.com/release-consumer

go 1.26.0
EOF

cat > main.go <<'EOF'
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// A release consumer uses the published version with no replace directive and
// no knowledge of the repository's internals.
func main() {
	runtime, err := harness.New()
	if err != nil {
		log.Fatalf("harness.New: %v", err)
	}
	defer func() { _ = runtime.Close() }()
	auth := harness.AuthContext{TenantID: "release", SubjectID: "consumer"}
	result, err := runtime.Run(context.Background(), harness.TaskRequest{
		TaskID: "release-check", Goal: "prove the tag installs", TenantID: auth.TenantID, SubjectID: auth.SubjectID,
	}, auth)
	if err != nil {
		log.Fatalf("Run: %v", err)
	}
	if _, err := runtime.Journal().ReplayState(result.TaskID); err != nil {
		log.Fatalf("ReplayState: %v", err)
	}
	fmt.Printf("installed %s: status=%s events=%d\n", harness.Version(), result.Status, len(result.Events))
}
EOF

fail=0
step() {
  local name="$1"; shift
  if "$@" >/tmp/release-check.log 2>&1; then
    echo "  ok: $name"
  else
    echo "  FAIL: $name"
    tail -20 /tmp/release-check.log | sed 's/^/        /'
    fail=1
  fi
}

step "go get $MODULE@$VERSION" go get "$MODULE@$VERSION"
step "go build" go build ./...
if [[ "$fail" -eq 0 ]]; then
  step "go run" go run .
else
  echo "  (skipped go run: the module did not resolve)"
fi

echo
if [[ "$fail" -eq 0 ]]; then
  echo "RELEASE CHECK PASSED: $MODULE@$VERSION is installable without a replace directive."
else
  echo "RELEASE CHECK FAILED: $MODULE@$VERSION is not installable." >&2
fi
exit "$fail"
