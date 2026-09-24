package vector

import "fmt"

// VectorError is the standard error for vector operations across all
// backends. Code is a stable machine-readable identifier; Message is a
// human-readable summary; Details carries underlying driver errors.
type VectorError struct {
	Code    string
	Message string
	Details string
}

func (e *VectorError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("[%s] %s: %s", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Common error codes.
const (
	ErrDimensionMismatch   = "DIMENSION_MISMATCH"
	ErrEmptyRecord         = "EMPTY_RECORD"
	ErrNotFound            = "NOT_FOUND"
	ErrStoreClosed         = "STORE_CLOSED"
	ErrInvalidFilter       = "INVALID_FILTER"
	ErrEmbeddingFailed     = "EMBEDDING_FAILED"
	ErrConnectionFailed    = "CONNECTION_FAILED"
	ErrSchemaFailed        = "SCHEMA_FAILED"
	ErrQueryFailed         = "QUERY_FAILED"
	ErrUpsertFailed        = "UPSERT_FAILED"
	ErrDeleteFailed        = "DELETE_FAILED"
)
