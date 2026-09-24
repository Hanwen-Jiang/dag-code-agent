package xai

import (
	"fmt"
	"sort"
	"sync"
)

// DataClass is the sensitivity classification of a piece of shared data.
// Ordered from least to most sensitive.
type DataClass int

const (
	ClassPublic       DataClass = iota // shareable with anyone
	ClassInternal                      // shareable within an allowed contract
	ClassConfidential                  // shareable only under an explicit contract
	ClassRestricted                    // never shared across domains
)

func (d DataClass) String() string {
	switch d {
	case ClassPublic:
		return "public"
	case ClassInternal:
		return "internal"
	case ClassConfidential:
		return "confidential"
	case ClassRestricted:
		return "restricted"
	}
	return "unknown"
}

// ItemClass maps a logical data item key to its sensitivity.
type ItemClass map[string]DataClass

// SharingContract is a signed agreement permitting a specific kind of data
// to move from one domain to another.
type SharingContract struct {
	ID       string
	Source   string    // source domain
	Target   string    // target domain
	MaxClass DataClass // highest class this contract permits
	Items    []string  // specific item keys covered (empty = all at/below MaxClass)
	Valid    bool
}

// ShareDecision is one allow/deny result.
type ShareDecision struct {
	SourceDomain string
	TargetDomain string
	Item         string
	Class        DataClass
	Allowed      bool
	Reason       string
	ContractID   string
}

// AuditEntry is one line of the immutable audit log.
type AuditEntry struct {
	Sequence int
	Decision ShareDecision
}

// CrossDomainSharing enforces a policy for sharing embeddings/knowledge
// across task domains. Decisions are driven purely by data classification
// and sharing contracts, and every decision is written to an append-only
// audit log. Thread-safe.
type CrossDomainSharing struct {
	mu        sync.Mutex
	class     ItemClass
	contracts []*SharingContract
	audit     []AuditEntry
	seq       int
}

// NewCrossDomainSharing creates the policy engine with the given item
// classification table.
func NewCrossDomainSharing(class ItemClass) *CrossDomainSharing {
	if class == nil {
		class = ItemClass{}
	}
	return &CrossDomainSharing{
		class:     class,
		contracts: make([]*SharingContract, 0),
		audit:     make([]AuditEntry, 0),
	}
}

// SetClass records the classification for an item.
func (s *CrossDomainSharing) SetClass(item string, c DataClass) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.class[item] = c
}

// AddContract registers a sharing contract.
func (s *CrossDomainSharing) AddContract(c SharingContract) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.contracts = append(s.contracts, &c)
}

// ClassOf returns the classification for an item (defaults to Confidential for
// unknown items — the safe, conservative default).
func (s *CrossDomainSharing) ClassOf(item string) DataClass {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.class[item]; ok {
		return c
	}
	return ClassConfidential
}

// Audit returns a copy of the audit log in sequence order.
func (s *CrossDomainSharing) Audit() []AuditEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AuditEntry, len(s.audit))
	copy(out, s.audit)
	return out
}

// Evaluate decides whether a single item may be shared from source to target
// domain, and appends the decision to the audit log.
//
// Rules, in order (first matching rule wins):
//  1. Restricted data is never shared.
//  2. Public data is always shared.
//  3. A valid contract covering source->target that (a) covers the item (by
//     key or by wildcard) and (b) permits the item's class allows the share.
//  4. Otherwise the share is denied.
func (s *CrossDomainSharing) Evaluate(source, target, item string) ShareDecision {
	s.mu.Lock()
	defer s.mu.Unlock()

	class := ClassConfidential
	if c, ok := s.class[item]; ok {
		class = c
	}

	dec := ShareDecision{
		SourceDomain: source,
		TargetDomain: target,
		Item:         item,
		Class:        class,
	}

	switch {
	case class == ClassRestricted:
		dec.Allowed = false
		dec.Reason = "restricted data is never shared across domains"
	case class == ClassPublic:
		dec.Allowed = true
		dec.Reason = "public data may be shared freely"
	default:
		dec.Allowed, dec.ContractID, dec.Reason = s.matchContractLocked(source, target, item, class, len(s.contracts))
	}

	s.seq++
	s.audit = append(s.audit, AuditEntry{Sequence: s.seq, Decision: dec})
	return dec
}

// matchContractLocked finds a valid contract permitting the share.
// Caller holds s.mu.
func (s *CrossDomainSharing) matchContractLocked(source, target, item string, class DataClass, totalContracts int) (bool, string, string) {
	for _, c := range s.contracts {
		if !c.Valid {
			continue
		}
		if c.Source != source || c.Target != target {
			continue
		}
		if class > c.MaxClass {
			// Continue: a later contract with a higher cap might cover it.
			continue
		}
		if !contractCoversItem(c, item) {
			continue
		}
		return true, c.ID, fmt.Sprintf("contract %s permits %s data %s -> %s", c.ID, class, source, target)
	}
	return false, "", fmt.Sprintf("no valid contract permits %s data %s -> %s", class, source, target)
}

// contractCoversItem reports whether a contract covers a specific item key.
// A contract with an empty Items list is a wildcard covering all items at or
// below its MaxClass.
func contractCoversItem(c *SharingContract, item string) bool {
	if len(c.Items) == 0 {
		return true
	}
	for _, k := range c.Items {
		if k == item || k == "*" {
			return true
		}
	}
	return false
}

// BatchEvaluate evaluates a set of items from source to target, returning the
// decisions and a summary count.
func (s *CrossDomainSharing) BatchEvaluate(source, target string, items []string) (decisions []ShareDecision, allowed int) {
	decisions = make([]ShareDecision, 0, len(items))
	for _, item := range items {
		d := s.Evaluate(source, target, item)
		decisions = append(decisions, d)
		if d.Allowed {
			allowed++
		}
	}
	return decisions, allowed
}

// AuditSummary tallies the audit log by decision.
type AuditSummary struct {
	Total   int
	Allowed int
	Denied  int
	ByClass map[DataClass]int
}

// AuditSummary aggregates the audit log.
func (s *CrossDomainSharing) AuditSummary() AuditSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	sum := AuditSummary{ByClass: make(map[DataClass]int)}
	for _, e := range s.audit {
		sum.Total++
		sum.ByClass[e.Decision.Class]++
		if e.Decision.Allowed {
			sum.Allowed++
		} else {
			sum.Denied++
		}
	}
	return sum
}

// SortedAudit returns the audit log sorted by sequence (stable view for
// deterministic reporting).
func (s *CrossDomainSharing) SortedAudit() []AuditEntry {
	out := s.Audit()
	sort.Slice(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	return out
}
