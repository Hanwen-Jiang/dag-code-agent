package dag

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"
)

var ErrInvalidManifest = errors.New("invalid manifest")

func LoadManifest(path string) (Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	schemaPath := filepath.Join(filepath.Dir(path), "agent-dag-runtime-v0.2.schema.json")
	if _, statErr := os.Stat(schemaPath); statErr == nil {
		if err := ValidateJSONSchema(raw, schemaPath); err != nil {
			return Manifest{}, err
		}
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	if err := ValidateManifest(m); err != nil {
		return m, err
	}
	m.Raw = map[string]any{}
	_ = json.Unmarshal(raw, &m.Raw)
	return m, nil
}

func ValidateJSONSchema(document []byte, schemaPath string) error {
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return err
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("runtime-schema.json", bytes.NewReader(schemaBytes)); err != nil {
		return err
	}
	schema, err := compiler.Compile("runtime-schema.json")
	if err != nil {
		return err
	}
	var value any
	if err := json.Unmarshal(document, &value); err != nil {
		return err
	}
	if err := schema.Validate(value); err != nil {
		return fmt.Errorf("%w: schema: %v", ErrInvalidManifest, err)
	}
	return nil
}
func ValidateManifest(m Manifest) error {
	if m.SchemaVersion == "" || m.ManifestID == "" || m.SpecHash == "" {
		return fmt.Errorf("%w: missing identity", ErrInvalidManifest)
	}
	if len(m.Nodes) == 0 {
		return fmt.Errorf("%w: no nodes", ErrInvalidManifest)
	}
	seen := map[string]bool{}
	for _, n := range m.Nodes {
		if n.NodeID == "" || seen[n.NodeID] {
			return fmt.Errorf("%w: duplicate/empty node", ErrInvalidManifest)
		}
		seen[n.NodeID] = true
		if len(n.OutputContracts) == 0 && n.GateMode() == "NONE" {
			return fmt.Errorf("%w: node %s has no output contract", ErrInvalidManifest, n.NodeID)
		}
	}
	for _, e := range m.Edges {
		if e.FromNode == e.ToNode || !seen[e.FromNode] || !seen[e.ToNode] {
			return fmt.Errorf("%w: invalid edge %s", ErrInvalidManifest, e.EdgeID)
		}
		if e.EdgeType == "" {
			e.EdgeType = "CONTROL"
		}
	}
	_, err := m.TopologicalOrder()
	return err
}

func ConditionSatisfied(edge Edge, nodeStates map[string]string) bool {
	p := edge.Predicate()
	if p.Source == "" || p.SubjectID == "" {
		return false
	}
	actual := nodeStates[p.SubjectID]
	switch p.Operator {
	case "EQ", "EQUALS", "":
		return actual == p.Value
	case "NEQ":
		return actual != p.Value
	}
	return false
}

func ActivationDecision(m Manifest, nodeID string, nodeStates map[string]string) string {
	node, ok := m.Node(nodeID)
	if !ok {
		return "BLOCKED"
	}
	incoming := m.Incoming(nodeID)
	if len(incoming) == 0 {
		return "READY"
	}
	satisfied := 0
	possible := 0
	for _, e := range incoming {
		if ConditionSatisfied(e, nodeStates) {
			satisfied++
			continue
		}
		source := nodeStates[e.FromNode]
		if source != "FAILED" && source != "BLOCKED" && source != "CANCELLED" && source != "SUCCEEDED" {
			possible++
		}
	}
	mode := strings.ToUpper(node.ActivationMode())
	switch mode {
	case "ANY":
		if satisfied > 0 {
			return "READY"
		}
		if possible == 0 {
			return "BLOCKED"
		}
		return "WAIT"
	case "QUORUM":
		threshold := number(node.Activation["quorum"])
		if threshold <= 0 {
			threshold = 1
		}
		if satisfied >= threshold {
			return "READY"
		}
		if satisfied+possible < threshold {
			return "BLOCKED"
		}
		return "WAIT"
	default:
		if satisfied == len(incoming) {
			return "READY"
		}
		if satisfied+possible < len(incoming) {
			return "BLOCKED"
		}
		return "WAIT"
	}
}

func (m Manifest) RequiredInputs(nodeID string, nodeStates map[string]string) []string {
	result := map[string]bool{}
	node, _ := m.Node(nodeID)
	for _, c := range node.InputContracts {
		if c.Required {
			result[c.ContractID] = true
		}
	}
	for _, e := range m.Incoming(nodeID) {
		if ConditionSatisfied(e, nodeStates) {
			for _, id := range e.ArtifactContractIDs {
				result[id] = true
			}
		}
	}
	out := []string{}
	for id := range result {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
func (m Manifest) RequiredOutputs(nodeID string) []string {
	node, _ := m.Node(nodeID)
	out := []string{}
	for _, c := range node.OutputContracts {
		if c.Required {
			out = append(out, c.ContractID)
		}
	}
	sort.Strings(out)
	return out
}
