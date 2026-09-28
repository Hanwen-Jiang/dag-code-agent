package mcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

const (
	mcpSessionHeader = "Mcp-Session-Id"
	sseContentType   = "text/event-stream"
	maxHTTPErrorBody = 4 << 10
)

// HTTPTransport speaks MCP "streamable HTTP": every JSON-RPC message is POSTed
// to Endpoint, and the reply arrives either as a JSON body or as an SSE stream
// (design 6.2). It is a Transport, not a client: no JSON-RPC knowledge beyond
// framing lives here.
type HTTPTransport struct {
	Endpoint string
	Headers  map[string]string
	Client   *http.Client

	once    sync.Once
	frames  chan []byte
	done    chan struct{}
	client  *http.Client
	bodies  map[io.Closer]struct{}
	session string
	closed  bool
	closeOn sync.Once
	mu      sync.Mutex
}

func (t *HTTPTransport) init() {
	t.once.Do(func() {
		t.frames = make(chan []byte, receiveBuffer)
		t.done = make(chan struct{})
		t.bodies = map[io.Closer]struct{}{}
		t.client = t.Client
		if t.client == nil {
			// Per-request contexts carry the deadlines; a global client
			// timeout would also cut long-lived SSE streams.
			t.client = &http.Client{}
		}
	})
}

// Send POSTs one message and queues whatever the server answers.
func (t *HTTPTransport) Send(ctx context.Context, payload []byte) error {
	t.init()
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return ErrClosed
	}
	endpoint := strings.TrimSpace(t.Endpoint)
	if endpoint == "" {
		return errors.New("mcp: HTTPTransport.Endpoint is required")
	}
	body := bytes.TrimSpace(payload)
	if len(body) == 0 {
		return nil
	}
	req, err := http.NewRequestWithContext(orBackground(ctx), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("mcp: build http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, "+sseContentType)
	for key, value := range t.Headers {
		req.Header.Set(key, value)
	}
	t.mu.Lock()
	session := t.session
	t.mu.Unlock()
	if session != "" {
		req.Header.Set(mcpSessionHeader, session)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("mcp: http post %s: %w", endpoint, err)
	}
	if id := resp.Header.Get(mcpSessionHeader); id != "" {
		t.mu.Lock()
		t.session = id
		t.mu.Unlock()
	}
	return t.consume(resp)
}

func (t *HTTPTransport) consume(resp *http.Response) error {
	contentType := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type")))
	switch {
	case resp.StatusCode == http.StatusAccepted:
		// Notifications are acknowledged without a body.
		drainAndClose(resp.Body)
		return nil
	case strings.HasPrefix(contentType, sseContentType):
		t.track(resp.Body)
		go t.readSSE(resp.Body)
		return nil
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxHTTPErrorBody))
		_ = resp.Body.Close()
		return fmt.Errorf("mcp: http %s: %s", resp.Status, strings.TrimSpace(string(snippet)))
	default:
		data, err := io.ReadAll(io.LimitReader(resp.Body, defaultMaxMessageBytes+1))
		_ = resp.Body.Close()
		if err != nil {
			return fmt.Errorf("mcp: read http response: %w", err)
		}
		if len(data) > defaultMaxMessageBytes {
			return fmt.Errorf("mcp: http response exceeds %d bytes", defaultMaxMessageBytes)
		}
		return t.push(bytes.TrimSpace(data))
	}
}

// readSSE decodes an event stream into frames. It runs in its own goroutine so
// a server that keeps the stream open cannot block Send.
func (t *HTTPTransport) readSSE(body io.ReadCloser) {
	defer func() {
		t.untrack(body)
		_ = body.Close()
	}()
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64<<10), defaultMaxMessageBytes)
	var data bytes.Buffer
	flush := func() {
		if data.Len() == 0 {
			return
		}
		frame := append([]byte(nil), bytes.TrimSpace(data.Bytes())...)
		data.Reset()
		if len(frame) > 0 {
			_ = t.push(frame)
		}
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		if field != "data" {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		if data.Len() > 0 {
			data.WriteByte('\n')
		}
		data.WriteString(value)
	}
	flush()
}

// Receive returns the next frame the server produced.
func (t *HTTPTransport) Receive(ctx context.Context) ([]byte, error) {
	t.init()
	select {
	case frame := <-t.frames:
		return frame, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.done:
		return nil, ErrClosed
	}
}

// Close stops accepting work and closes any in-flight SSE stream.
func (t *HTTPTransport) Close() error {
	t.init()
	t.closeOn.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.mu.Unlock()
		close(t.done)
		t.mu.Lock()
		for body := range t.bodies {
			_ = body.Close()
		}
		t.bodies = map[io.Closer]struct{}{}
		t.mu.Unlock()
	})
	return nil
}

func (t *HTTPTransport) push(frame []byte) error {
	if len(frame) == 0 {
		return nil
	}
	select {
	case t.frames <- frame:
		return nil
	case <-t.done:
		return ErrClosed
	}
}

func (t *HTTPTransport) track(body io.Closer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		_ = body.Close()
		return
	}
	t.bodies[body] = struct{}{}
}

func (t *HTTPTransport) untrack(body io.Closer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.bodies, body)
}

func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxHTTPErrorBody))
	_ = body.Close()
}
