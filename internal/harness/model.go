package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"
)

type ModelError struct {
	Message   string
	Retryable bool
	Provider  string
	Status    int
}

func (e *ModelError) Error() string { return e.Message }

type StructuredOutputError struct{ Message string }

func (e *StructuredOutputError) Error() string { return e.Message }

type ModelAdapter interface {
	Name() string
	Complete(context.Context, ModelRequest) (ModelResponse, error)
	Stream(context.Context, ModelRequest) (<-chan StreamEvent, error)
}

func EstimateTokens(value any) int {
	var text string
	switch v := value.(type) {
	case string:
		text = v
	default:
		b, _ := json.Marshal(v)
		text = string(b)
	}
	if len(text) < 1 {
		return 1
	}
	return (len([]byte(text)) + 3) / 4
}

func RequestFingerprint(request ModelRequest) string {
	digest, _ := Digest(map[string]any{"messages": request.Messages, "tools": request.Tools, "schema": request.ResponseSchema, "model": request.Model, "reasoning_effort": request.ReasoningEffort})
	return digest
}

// ScriptedModel makes the entire control flow deterministic for tests and
// demos.  It still records every request, which makes context and cache
// behavior inspectable without a provider key.
type ScriptedModel struct {
	ModelName string
	mu        sync.Mutex
	responses []ModelResponse
	Requests  []ModelRequest
}

func NewScriptedModel(items []ModelResponse) *ScriptedModel {
	return &ScriptedModel{ModelName: "scripted", responses: append([]ModelResponse(nil), items...)}
}
func (m *ScriptedModel) Name() string { return m.ModelName }
func (m *ScriptedModel) Complete(_ context.Context, req ModelRequest) (ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Requests = append(m.Requests, req)
	if len(m.responses) == 0 {
		return ModelResponse{}, &ModelError{Message: "scripted model exhausted", Retryable: false, Provider: m.ModelName}
	}
	r := m.responses[0]
	m.responses = m.responses[1:]
	if r.FinishReason == "" {
		r.FinishReason = "stop"
	}
	return r, nil
}
func (m *ScriptedModel) Stream(ctx context.Context, req ModelRequest) (<-chan StreamEvent, error) {
	r, err := m.Complete(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make(chan StreamEvent, 4)
	go func() {
		defer close(out)
		if r.Text != "" {
			for _, chunk := range strings.Fields(r.Text + " ") {
				select {
				case out <- StreamEvent{Type: "text_delta", Data: map[string]any{"delta": chunk + " "}}:
				case <-ctx.Done():
					return
				}
			}
		}
		out <- StreamEvent{Type: "done", Data: map[string]any{"finish_reason": r.FinishReason}}
	}()
	return out, nil
}

// ConversationModel is the local no-key adapter used by the chat surface. It
// deliberately keeps the ModelAdapter boundary intact: replacing it with
// OpenAICompatibleAdapter (or another provider) does not change session,
// policy, tool, or replay code. Its deterministic answer makes the UI useful
// in an offline checkout while still exercising a real multi-turn loop.
type ConversationModel struct {
	ModelName string
	mu        sync.Mutex
	Requests  []ModelRequest
}

func NewConversationModel() *ConversationModel {
	return &ConversationModel{ModelName: "local-conversation"}
}

func (m *ConversationModel) Name() string {
	if m.ModelName == "" {
		return "local-conversation"
	}
	return m.ModelName
}

func (m *ConversationModel) Complete(ctx context.Context, req ModelRequest) (ModelResponse, error) {
	select {
	case <-ctx.Done():
		return ModelResponse{}, ctx.Err()
	default:
	}
	m.mu.Lock()
	m.Requests = append(m.Requests, req)
	m.mu.Unlock()
	message := lastConversationMessage(req.Messages)
	if message == "" {
		message = "继续检查当前项目"
	}
	if looksLikeWorkspaceTask(message) {
		lower := strings.ToLower(message)
		if strings.Contains(message, "测试") || strings.Contains(lower, "test") {
			if hasModelTool(req.Tools, "workspace_test") && !hasToolObservation(req.Messages, "workspace_test") {
				return ModelResponse{ToolCalls: []ToolCall{{CallID: "workspace-test-1", Name: "workspace_test", Arguments: map[string]any{"command": "go test ./..."}}}, FinishReason: "tool_calls", Usage: ModelUsage{InputTokens: EstimateTokens(req.Messages)}}, nil
			}
		}
		if (strings.Contains(message, "搜索") || strings.Contains(lower, "search")) && hasModelTool(req.Tools, "workspace_search") && !hasToolObservation(req.Messages, "workspace_search") {
			return ModelResponse{ToolCalls: []ToolCall{{CallID: "workspace-search-1", Name: "workspace_search", Arguments: map[string]any{"query": message}}}, FinishReason: "tool_calls", Usage: ModelUsage{InputTokens: EstimateTokens(req.Messages)}}, nil
		}
		if hasModelTool(req.Tools, "workspace_list") && !hasToolObservation(req.Messages, "workspace_list") {
			return ModelResponse{ToolCalls: []ToolCall{{CallID: "workspace-list-1", Name: "workspace_list", Arguments: map[string]any{"path": ".", "depth": 2}}}, FinishReason: "tool_calls", Usage: ModelUsage{InputTokens: EstimateTokens(req.Messages)}}, nil
		}
	}
	text := "已收到你的请求：\n\n" + message + "\n\n我会把它作为当前 session 的下一步，先检查上下文和已有证据，再给出可落地的代码变更与验证结果。"
	if observation := lastToolObservation(req.Messages); observation != "" {
		text += "\n\n已提交工具证据：\n" + observation
	}
	return ModelResponse{Text: text, FinishReason: "stop", Usage: ModelUsage{InputTokens: EstimateTokens(req.Messages), OutputTokens: EstimateTokens(text)}}, nil
}

func (m *ConversationModel) Stream(ctx context.Context, req ModelRequest) (<-chan StreamEvent, error) {
	response, err := m.Complete(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make(chan StreamEvent, 8)
	go func() {
		defer close(out)
		for _, chunk := range strings.Fields(response.Text + " ") {
			select {
			case out <- StreamEvent{Type: "text_delta", Data: map[string]any{"delta": chunk + " "}}:
			case <-ctx.Done():
				return
			}
		}
		out <- StreamEvent{Type: "done", Data: map[string]any{"finish_reason": response.FinishReason}}
	}()
	return out, nil
}

func lastConversationMessage(messages []map[string]any) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if stringValue(messages[i]["role"]) != "user" {
			continue
		}
		content := strings.TrimSpace(stringValue(messages[i]["content"]))
		if content == "" || strings.HasPrefix(content, "[") {
			continue
		}
		return content
	}
	return ""
}

func hasModelTool(tools []map[string]any, name string) bool {
	for _, tool := range tools {
		function, _ := tool["function"].(map[string]any)
		if stringValue(function["name"]) == name {
			return true
		}
	}
	return false
}

func hasToolObservation(messages []map[string]any, name string) bool {
	for _, message := range messages {
		if stringValue(message["role"]) == "user" && strings.Contains(stringValue(message["content"]), `"tool":"`+name+`"`) {
			return true
		}
	}
	return false
}

func lastToolObservation(messages []map[string]any) string {
	for i := len(messages) - 1; i >= 0; i-- {
		content := stringValue(messages[i]["content"])
		if strings.HasPrefix(content, "[observation]") && strings.Contains(content, `"tool"`) {
			if len(content) > 700 {
				return content[:700] + "..."
			}
			return content
		}
	}
	return ""
}

func looksLikeWorkspaceTask(message string) bool {
	lower := strings.ToLower(message)
	for _, term := range []string{"代码", "仓库", "项目", "文件", "实现", "修复", "检查", "测试", "code", "repo", "file", "implement", "fix", "test"} {
		if strings.Contains(message, term) || strings.Contains(lower, term) {
			return true
		}
	}
	return false
}

type OpenAICompatibleAdapter struct {
	BaseURL    string
	APIKey     string
	ModelName  string
	Client     *http.Client
	MaxRetries int
	RetryBase  time.Duration
}

func (a *OpenAICompatibleAdapter) Name() string {
	if a.ModelName != "" {
		return a.ModelName
	}
	return "openai-compatible"
}
func (a *OpenAICompatibleAdapter) endpoint() string {
	base := strings.TrimRight(a.BaseURL, "/")
	if strings.HasSuffix(base, "/chat/completions") {
		return base
	}
	if !strings.HasSuffix(base, "/v1") {
		base += "/v1"
	}
	return base + "/chat/completions"
}

func (a *OpenAICompatibleAdapter) Complete(ctx context.Context, req ModelRequest) (ModelResponse, error) {
	model := req.Model
	if model == "" {
		model = a.ModelName
	}
	body := map[string]any{"model": model, "messages": req.Messages, "stream": false}
	if len(req.Tools) > 0 {
		body["tools"] = req.Tools
	}
	if req.ReasoningEffort != "" {
		body["reasoning_effort"] = providerReasoningEffort(req.ReasoningEffort)
	}
	if req.MaxOutputTokens > 0 {
		body["max_tokens"] = req.MaxOutputTokens
	}
	if req.ResponseSchema != nil {
		body["response_format"] = map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "agent_output", "strict": true, "schema": req.ResponseSchema}}
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return ModelResponse{}, err
	}
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	max := a.MaxRetries
	if max < 0 {
		max = 0
	}
	base := a.RetryBase
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	var last error
	for attempt := 0; attempt <= max; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ModelResponse{}, ctx.Err()
			case <-time.After(base * time.Duration(1<<(attempt-1))):
			}
		}
		hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint(), bytes.NewReader(payload))
		if err != nil {
			return ModelResponse{}, err
		}
		hreq.Header.Set("Content-Type", "application/json")
		if a.APIKey != "" {
			hreq.Header.Set("Authorization", "Bearer "+a.APIKey)
		}
		resp, err := client.Do(hreq)
		if err != nil {
			last = err
			continue
		}
		raw, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			last = readErr
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			retry := resp.StatusCode == 429 || resp.StatusCode >= 500
			last = &ModelError{Message: fmt.Sprintf("provider HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw))), Retryable: retry, Provider: a.Name(), Status: resp.StatusCode}
			if !retry {
				return ModelResponse{}, last
			}
			continue
		}
		result, err := parseOpenAIResponse(raw)
		if err != nil {
			return ModelResponse{}, err
		}
		return result, nil
	}
	if last == nil {
		last = errors.New("model request failed")
	}
	return ModelResponse{}, last
}

func parseOpenAIResponse(raw []byte) (ModelResponse, error) {
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return ModelResponse{}, err
	}
	result := ModelResponse{Raw: body, FinishReason: "stop"}
	if usage, ok := body["usage"].(map[string]any); ok {
		result.Usage.InputTokens = intNumber(usage["prompt_tokens"])
		result.Usage.OutputTokens = intNumber(usage["completion_tokens"])
		if details, ok := usage["prompt_tokens_details"].(map[string]any); ok {
			result.Usage.CachedInputTokens = intNumber(details["cached_tokens"])
		} else {
			result.Usage.CachedInputTokens = intNumber(usage["cached_tokens"])
		}
	}
	choices, _ := body["choices"].([]any)
	if len(choices) == 0 {
		return result, nil
	}
	choice, _ := choices[0].(map[string]any)
	if reason, ok := choice["finish_reason"].(string); ok && reason != "" {
		result.FinishReason = reason
	}
	message, _ := choice["message"].(map[string]any)
	if text, ok := message["content"].(string); ok {
		result.Text = text
	}
	if calls, ok := message["tool_calls"].([]any); ok {
		for _, rawCall := range calls {
			c, _ := rawCall.(map[string]any)
			fn, _ := c["function"].(map[string]any)
			args := map[string]any{}
			if argText, ok := fn["arguments"].(string); ok && strings.TrimSpace(argText) != "" {
				if err := json.Unmarshal([]byte(argText), &args); err != nil {
					return ModelResponse{}, &ModelError{Message: "malformed tool arguments", Retryable: false}
				}
			}
			result.ToolCalls = append(result.ToolCalls, ToolCall{CallID: stringValue(c["id"]), Name: stringValue(fn["name"]), Arguments: args})
		}
	}
	return result, nil
}

// providerReasoningEffort keeps the product-level "ultra" setting usable
// with gateways that expose the OpenAI-compatible vocabulary and call their
// highest level "max". The requested value remains in the durable TaskRequest;
// only the outbound provider payload is normalized.
func providerReasoningEffort(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "ultra") {
		return "max"
	}
	return value
}

func intNumber(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}
func stringValue(v any) string { s, _ := v.(string); return s }

func (a *OpenAICompatibleAdapter) Stream(ctx context.Context, req ModelRequest) (<-chan StreamEvent, error) {
	model := req.Model
	if model == "" {
		model = a.ModelName
	}
	body := map[string]any{"model": model, "messages": req.Messages, "stream": true}
	if len(req.Tools) > 0 {
		body["tools"] = req.Tools
	}
	if req.ReasoningEffort != "" {
		body["reasoning_effort"] = providerReasoningEffort(req.ReasoningEffort)
	}
	if req.MaxOutputTokens > 0 {
		body["max_tokens"] = req.MaxOutputTokens
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/event-stream")
	if a.APIKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+a.APIKey)
	}
	resp, err := client.Do(hreq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, &ModelError{Message: fmt.Sprintf("provider HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw))), Retryable: resp.StatusCode == 429 || resp.StatusCode >= 500, Provider: a.Name(), Status: resp.StatusCode}
	}
	out := make(chan StreamEvent, 16)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 4096), 2<<20)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "[DONE]" {
				out <- StreamEvent{Type: "done"}
				return
			}
			var chunk map[string]any
			if json.Unmarshal([]byte(data), &chunk) != nil {
				continue
			}
			choices, _ := chunk["choices"].([]any)
			if len(choices) == 0 {
				continue
			}
			choice, _ := choices[0].(map[string]any)
			delta, _ := choice["delta"].(map[string]any)
			if text, ok := delta["content"].(string); ok && text != "" {
				select {
				case out <- StreamEvent{Type: "text_delta", Data: map[string]any{"delta": text}}:
				case <-ctx.Done():
					return
				}
			}
			if calls, ok := delta["tool_calls"].([]any); ok {
				select {
				case out <- StreamEvent{Type: "tool_call_delta", Data: map[string]any{"tool_calls": calls}}:
				case <-ctx.Done():
					return
				}
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case out <- StreamEvent{Type: "error", Data: map[string]any{"error": err.Error()}}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func ValidateStructured(text string, schema map[string]any) error {
	if schema == nil {
		return nil
	}
	var value any
	if err := json.Unmarshal([]byte(text), &value); err != nil {
		return &StructuredOutputError{Message: "output is not valid JSON"}
	}
	if err := validateJSONSchemaValue(value, schema); err != nil {
		return &StructuredOutputError{Message: err.Error()}
	}
	return nil
}

func validateJSONSchemaValue(value any, schemaValue map[string]any) error {
	schemaBytes, err := json.Marshal(schemaValue)
	if err != nil {
		return fmt.Errorf("invalid JSON schema: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("inline-schema.json", bytes.NewReader(schemaBytes)); err != nil {
		return fmt.Errorf("invalid JSON schema: %w", err)
	}
	schema, err := compiler.Compile("inline-schema.json")
	if err != nil {
		return fmt.Errorf("invalid JSON schema: %w", err)
	}
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("value is not JSON-compatible: %w", err)
	}
	var normalized any
	if err := json.Unmarshal(valueBytes, &normalized); err != nil {
		return fmt.Errorf("value is not JSON-compatible: %w", err)
	}
	if err := schema.Validate(normalized); err != nil {
		return fmt.Errorf("JSON schema validation failed: %w", err)
	}
	return nil
}

type ModelRouter struct {
	Primary  ModelAdapter
	Fallback ModelAdapter
}

func (r ModelRouter) Name() string {
	if r.Primary != nil {
		return r.Primary.Name()
	}
	if r.Fallback != nil {
		return r.Fallback.Name()
	}
	return "router"
}
func (r ModelRouter) Complete(ctx context.Context, req ModelRequest) (ModelResponse, error) {
	if r.Primary != nil {
		v, err := r.Primary.Complete(ctx, req)
		if err == nil {
			return v, nil
		}
		if me, ok := err.(*ModelError); !ok || !me.Retryable {
			return ModelResponse{}, err
		}
	}
	if r.Fallback != nil {
		return r.Fallback.Complete(ctx, req)
	}
	return ModelResponse{}, errors.New("no model adapter")
}
func (r ModelRouter) Stream(ctx context.Context, req ModelRequest) (<-chan StreamEvent, error) {
	if r.Primary != nil {
		v, err := r.Primary.Stream(ctx, req)
		if err == nil {
			return v, nil
		}
		if me, ok := err.(*ModelError); !ok || !me.Retryable {
			return nil, err
		}
	}
	if r.Fallback != nil {
		return r.Fallback.Stream(ctx, req)
	}
	return nil, errors.New("no model adapter")
}

type CircuitBreaker struct {
	mu        sync.Mutex
	Failures  int
	Threshold int
	OpenUntil time.Time
	Cooldown  time.Duration
}

func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.OpenUntil.IsZero() || time.Now().After(b.OpenUntil) {
		return true
	}
	return false
}
func (b *CircuitBreaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Failures = 0
	b.OpenUntil = time.Time{}
}
func (b *CircuitBreaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Failures++
	threshold := b.Threshold
	if threshold <= 0 {
		threshold = 3
	}
	if b.Failures >= threshold {
		cooldown := b.Cooldown
		if cooldown <= 0 {
			cooldown = time.Second
		}
		b.OpenUntil = time.Now().Add(cooldown)
	}
}

type BudgetLedger struct {
	mu            sync.Mutex
	MaxTokens     int
	MaxToolCalls  int
	UsedTokens    int
	UsedToolCalls int
	MaxCostUSD    float64
	UsedCostUSD   float64
}

func (b *BudgetLedger) Charge(usage ModelUsage, toolCalls int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.MaxTokens > 0 && b.UsedTokens+usage.TotalTokens() > b.MaxTokens {
		return errors.New("TOKEN_BUDGET_EXCEEDED")
	}
	if b.MaxToolCalls > 0 && b.UsedToolCalls+toolCalls > b.MaxToolCalls {
		return errors.New("TOOL_BUDGET_EXCEEDED")
	}
	if b.MaxCostUSD > 0 && b.UsedCostUSD+usage.CostUSD > b.MaxCostUSD {
		return errors.New("COST_BUDGET_EXCEEDED")
	}
	b.UsedTokens += usage.TotalTokens()
	b.UsedToolCalls += toolCalls
	b.UsedCostUSD += usage.CostUSD
	return nil
}

type PrefixCache struct {
	mu           sync.Mutex
	MaxEntries   int
	entries      map[string]time.Time
	hits, misses int
}

// PrefixCacheIdentity contains every boundary that can change the meaning of
// a stable prefix.  The cache tracks reusable-prefix accounting; it never
// claims ownership of provider GPU KV tensors.
type PrefixCacheIdentity struct {
	Model           string `json:"model,omitempty"`
	ModelRevision   string `json:"model_revision,omitempty"`
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
	TenantID        string `json:"tenant_id,omitempty"`
	SubjectID       string `json:"subject_id,omitempty"`
	ScopeDigest     string `json:"scope_digest,omitempty"`
	PolicyVersion   string `json:"policy_version,omitempty"`
	CompilerVersion string `json:"compiler_version,omitempty"`
	SkillDigest     string `json:"skill_digest,omitempty"`
}

func NewPrefixCache(max int) *PrefixCache {
	if max <= 0 {
		max = 128
	}
	return &PrefixCache{MaxEntries: max, entries: map[string]time.Time{}}
}
func (c *PrefixCache) Key(messages []map[string]any, tools []map[string]any) string {
	return c.KeyWithIdentity(messages, tools, PrefixCacheIdentity{})
}

func (c *PrefixCache) KeyWithIdentity(messages []map[string]any, tools []map[string]any, identity PrefixCacheIdentity) string {
	stable := []map[string]any{}
	for _, m := range messages {
		if v, ok := m["stable"].(bool); ok && v {
			stable = append(stable, m)
		}
	}
	d, _ := Digest(map[string]any{"identity": identity, "messages": stable, "tools": tools})
	return d
}
func (c *PrefixCache) Observe(key string, tokens int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[key]; ok {
		c.hits++
		c.entries[key] = time.Now()
		return true
	}
	c.misses++
	if len(c.entries) >= c.MaxEntries {
		var oldest string
		var at time.Time
		for k, v := range c.entries {
			if oldest == "" || v.Before(at) {
				oldest = k
				at = v
			}
		}
		delete(c.entries, oldest)
	}
	c.entries[key] = time.Now()
	return false
}
func (c *PrefixCache) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}
func (c *PrefixCache) Stats() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]int{"hits": c.hits, "misses": c.misses, "entries": len(c.entries)}
}
