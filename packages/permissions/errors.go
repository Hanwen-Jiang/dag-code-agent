package permissions

import "fmt"

// PermissionError represents a permission denial with the layer that produced
// it. Callers can type-assert on Code for machine handling.
type PermissionError struct {
	Code        string // HARD_BLOCKED, CONFIG_DENIED, COMPOSITE_DENIED, PATH_DENIED, RATE_LIMITED, APPROVAL_DENIED, DEFAULT_DENY, TOOL_NOT_ALLOWED
	Message     string
	Layer       int // 1..6
	Reason      string
	CanOverride bool // true only for approval-based denials
}

func (e *PermissionError) Error() string {
	return fmt.Sprintf("[%s] %s (layer %d): %s", e.Code, e.Message, e.Layer, e.Reason)
}

// Code returns the stable machine-readable denial code.
func (e *PermissionError) CodeString() string { return e.Code }

// PathError represents a path validation failure (layer 3).
type PathError struct {
	Code      string // PATH_TRAVERSAL, SENSITIVE_PATH, PATH_OUTSIDE_ALLOWED_REGION, PATH_CANONICALIZE_FAILED
	Message   string
	Path      string
	Operation string
}

func (e *PathError) Error() string {
	return fmt.Sprintf("[PathError %s] %s", e.Code, e.Message)
}
