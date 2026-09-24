FROM golang:1.26-alpine AS build
WORKDIR /src/dag-code-agent
COPY dag-code-agent/go.mod dag-code-agent/go.sum ./
RUN go mod download
COPY dag-code-agent/cmd ./cmd
COPY dag-code-agent/internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/agent-harness ./cmd/agent-harness

FROM alpine:3.22
WORKDIR /app
RUN addgroup -S app && adduser -S -G app app
USER app
COPY --from=build /out/agent-harness /usr/local/bin/agent-harness
COPY dag-code-agent/configs ./configs
EXPOSE 8080
ENTRYPOINT ["agent-harness", "-mode", "server", "-addr", "0.0.0.0:8080"]
