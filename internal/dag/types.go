package dag

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Contract struct {
	ContractID   string `json:"contract_id"`
	ArtifactKind string `json:"artifact_kind"`
	MediaType    string `json:"media_type,omitempty"`
	Required     bool   `json:"required"`
	DeliveryMode string `json:"delivery_mode,omitempty"`
	MaxTokens    int    `json:"max_tokens,omitempty"`
	TrustClass   string `json:"trust_class,omitempty"`
}
type Node struct {
	NodeID          string         `json:"node_id"`
	Title           string         `json:"title"`
	Objective       string         `json:"objective"`
	WorkType        string         `json:"work_type"`
	ExecutorRole    string         `json:"executor_role"`
	InputContracts  []Contract     `json:"input_contracts"`
	OutputContracts []Contract     `json:"output_contracts"`
	Activation      map[string]any `json:"activation"`
	GatePolicy      map[string]any `json:"gate_policy"`
	ExecutionPolicy map[string]any `json:"execution_policy"`
	ToolProfile     any            `json:"tool_profile,omitempty"`
	WriteScope      map[string]any `json:"write_scope,omitempty"`
}

func (n Node) GateMode() string { v, _ := n.GatePolicy["mode"].(string); return v }
func (n Node) ActivationMode() string {
	v, _ := n.Activation["mode"].(string)
	if v == "" {
		return "ALL"
	}
	return v
}
func (n Node) MaxAttempts() int {
	p, _ := n.ExecutionPolicy["retry_policy"].(map[string]any)
	v := number(p["max_attempts"])
	if v <= 0 {
		v = 1
	}
	return v
}
func number(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

type Predicate struct {
	Source    string `json:"source"`
	SubjectID string `json:"subject_id"`
	Operator  string `json:"operator"`
	Value     string `json:"value"`
}
type Edge struct {
	EdgeID              string         `json:"edge_id"`
	FromNode            string         `json:"from_node"`
	ToNode              string         `json:"to_node"`
	EdgeType            string         `json:"edge_type"`
	Gating              bool           `json:"gating"`
	Condition           map[string]any `json:"condition"`
	ArtifactContractIDs []string       `json:"artifact_contract_ids"`
	Description         string         `json:"description,omitempty"`
}

func (e Edge) Predicate() Predicate {
	var p Predicate
	if raw, ok := e.Condition["predicate"]; ok {
		b, _ := json.Marshal(raw)
		_ = json.Unmarshal(b, &p)
	}
	return p
}

type Manifest struct {
	SchemaVersion string           `json:"schema_version"`
	ManifestID    string           `json:"manifest_id"`
	SpecHash      string           `json:"spec_hash"`
	Plan          map[string]any   `json:"plan"`
	Nodes         []Node           `json:"nodes"`
	Edges         []Edge           `json:"edges"`
	Attempts      []map[string]any `json:"attempts,omitempty"`
	Artifacts     []map[string]any `json:"artifacts,omitempty"`
	Evidence      []map[string]any `json:"evidence,omitempty"`
	Events        []map[string]any `json:"events,omitempty"`
	Raw           map[string]any   `json:"-"`
}

func (m Manifest) Objective() string {
	if v, ok := m.Plan["objective"].(string); ok {
		return v
	}
	return m.ManifestID
}
func (m Manifest) Node(id string) (Node, bool) {
	for _, n := range m.Nodes {
		if n.NodeID == id {
			return n, true
		}
	}
	return Node{}, false
}
func (m Manifest) Incoming(id string) []Edge {
	out := []Edge{}
	for _, e := range m.Edges {
		if e.ToNode == id && e.Gating && e.EdgeType != "DATA" {
			out = append(out, e)
		}
	}
	return out
}
func (m Manifest) TopologicalOrder() ([]string, error) {
	indegree := map[string]int{}
	out := map[string][]string{}
	for _, n := range m.Nodes {
		indegree[n.NodeID] = 0
	}
	for _, e := range m.Edges {
		if !e.Gating || e.EdgeType == "DATA" {
			continue
		}
		if _, ok := indegree[e.FromNode]; !ok {
			return nil, fmt.Errorf("unknown edge source %s", e.FromNode)
		}
		if _, ok := indegree[e.ToNode]; !ok {
			return nil, fmt.Errorf("unknown edge target %s", e.ToNode)
		}
		indegree[e.ToNode]++
		out[e.FromNode] = append(out[e.FromNode], e.ToNode)
	}
	queue := []string{}
	for _, n := range m.Nodes {
		if indegree[n.NodeID] == 0 {
			queue = append(queue, n.NodeID)
		}
	}
	order := []string{}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)
		for _, next := range out[node] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if len(order) != len(m.Nodes) {
		return nil, errors.New("DAG contains cycle")
	}
	return order, nil
}
