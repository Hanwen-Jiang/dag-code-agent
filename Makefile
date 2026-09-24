.PHONY: test race vet build build-bin demo dag rework server proto clean \
        guards conformance \
        agent-core orchestrator llm-service self-evolution xai-dashboard infra-up infra-down

GOFLAGS ?=

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

# Enforce the embeddability acceptance criteria (C1-C4). See
# docs/embeddability-design.md section 9.
guards:
	./scripts/embed-guards.sh --self-test

# Run the store conformance suite against every available backend.
conformance:
	go test -count=1 -v ./harness/storetest/
	go test -count=1 -v ./backends/...

# Build every binary in the three-layer stack.
build:
	mkdir -p bin
	go build -trimpath -o bin/agent-harness    ./cmd/agent-harness
	go build -trimpath -o bin/agent-dag        ./cmd/agent-dag
	go build -trimpath -o bin/agent-core       ./cmd/agent-core
	go build -trimpath -o bin/orchestrator     ./cmd/orchestrator
	go build -trimpath -o bin/llm-service      ./cmd/llm-service
	go build -trimpath -o bin/self-evolution   ./cmd/self-evolution
	go build -trimpath -o bin/xai-dashboard    ./cmd/xai-dashboard

build-bin: build

demo:
	go run ./cmd/agent-harness -mode demo

dag:
	go run ./cmd/agent-dag -run dag-demo

rework:
	go run ./cmd/agent-dag -run dag-rework -rework

server:
	go run ./cmd/agent-harness -mode server

# Regenerate gRPC/protobuf code from cmd/proto/agent_core.proto.
proto:
	protoc --go_out=. --go_opt=module=github.com/Hanwen-Jiang/dag-code-agent \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/Hanwen-Jiang/dag-code-agent \
	       cmd/proto/agent_core.proto

# Three-layer services (each degrades gracefully without external infra).
agent-core:
	go run ./cmd/agent-core

orchestrator:
	go run ./cmd/orchestrator

llm-service:
	go run ./cmd/llm-service

self-evolution:
	go run ./cmd/self-evolution

xai-dashboard:
	go run ./cmd/xai-dashboard

# Real external infrastructure (Temporal + Qdrant). Optional.
infra-up:
	docker compose up -d

infra-down:
	docker compose down

clean:
	rm -rf bin coverage.out
