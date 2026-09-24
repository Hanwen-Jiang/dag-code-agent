// Package pgvector provides a PostgreSQL + PGVector backend for the
// vector.VectorStore interface. It uses database/sql with the pgx stdlib
// driver, so no live database is required at compile time; Initialize()
// creates the schema at connect time.
package pgvector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver

	"agent-harness/packages/vector"
)

// VectorError aliases the shared vector error type for convenient use in
// this package.
type VectorError = vector.VectorError

var _ vector.VectorStore = (*Backend)(nil)

// Backend implements vector.VectorStore using PostgreSQL + PGVector.
type Backend struct {
	db       *sql.DB
	host     string
	port     int
	user     string
	password string
	database string
	poolSize int
}

// BackendConfig configures a PostgreSQL connection.
type BackendConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	PoolSize int
}

// NewBackend opens a (lazy) connection to PostgreSQL. sql.Open validates the
// DSN format but does not dial; call Initialize to create the schema.
func NewBackend(config BackendConfig) (*Backend, error) {
	if config.Port == 0 {
		config.Port = 5432
	}
	if config.PoolSize <= 0 {
		config.PoolSize = 10
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Password, config.Database)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, &VectorError{Code: vector.ErrConnectionFailed, Message: "failed to open connection", Details: err.Error()}
	}
	db.SetMaxOpenConns(config.PoolSize)

	return &Backend{
		db:       db,
		host:     config.Host,
		port:     config.Port,
		user:     config.User,
		password: config.Password,
		database: config.Database,
		poolSize: config.PoolSize,
	}, nil
}

func (b *Backend) Name() string { return "pgvector" }

// Initialize enables the pgvector extension and creates tables/indexes.
func (b *Backend) Initialize(ctx context.Context) error {
	if _, err := b.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS vector"); err != nil {
		return &VectorError{Code: vector.ErrSchemaFailed, Message: "failed to enable pgvector extension", Details: err.Error()}
	}

	statements := []string{
		`CREATE TABLE IF NOT EXISTS vectors (
			id TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL DEFAULT '',
			namespace TEXT NOT NULL DEFAULT 'default',
			vector vector(1536),
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			text TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_vectors_tenant_ns ON vectors (tenant_id, namespace)`,
		`CREATE INDEX IF NOT EXISTS idx_vectors_metadata ON vectors USING GIN (metadata)`,
		// Cosine-similarity ANN index (ivfflat). Built after data lands; the
		// exact search fallback works regardless.
		`CREATE INDEX IF NOT EXISTS idx_vectors_cosine ON vectors USING ivfflat (vector vector_cosine_ops)`,
	}
	for _, stmt := range statements {
		if _, err := b.db.ExecContext(ctx, stmt); err != nil {
			return &VectorError{Code: vector.ErrSchemaFailed, Message: "failed to create schema", Details: err.Error()}
		}
	}
	return nil
}

func (b *Backend) Upsert(ctx context.Context, records []vector.VectorRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return &VectorError{Code: vector.ErrUpsertFailed, Message: "failed to begin transaction", Details: err.Error()}
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO vectors (id, tenant_id, namespace, vector, metadata, text, created_at, updated_at)
		VALUES ($1, $2, $3, $4::vector, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			vector = EXCLUDED.vector,
			metadata = EXCLUDED.metadata,
			text = EXCLUDED.text,
			updated_at = NOW()
	`)
	if err != nil {
		return &VectorError{Code: vector.ErrUpsertFailed, Message: "failed to prepare upsert", Details: err.Error()}
	}
	defer func() { _ = stmt.Close() }()

	for _, rec := range records {
		metaJSON, err := json.Marshal(rec.Metadata)
		if err != nil {
			return &VectorError{Code: vector.ErrUpsertFailed, Message: "failed to encode metadata", Details: err.Error()}
		}
		createdAt := rec.CreatedAt
		if createdAt.IsZero() {
			createdAt = rec.UpdatedAt
		}
		updatedAt := rec.UpdatedAt
		if updatedAt.IsZero() {
			updatedAt = createdAt
		}
		if _, err := stmt.ExecContext(ctx, rec.ID, rec.TenantID, rec.Namespace,
			floatArrayToVector(rec.Vector), metaJSON, rec.Text, createdAt, updatedAt); err != nil {
			return &VectorError{Code: vector.ErrUpsertFailed, Message: fmt.Sprintf("failed to upsert vector %s", rec.ID), Details: err.Error()}
		}
	}

	if err := tx.Commit(); err != nil {
		return &VectorError{Code: vector.ErrUpsertFailed, Message: "failed to commit transaction", Details: err.Error()}
	}
	return nil
}

func (b *Backend) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf("DELETE FROM vectors WHERE id IN (%s)", strings.Join(placeholders, ", "))
	if _, err := b.db.ExecContext(ctx, query, args...); err != nil {
		return &VectorError{Code: vector.ErrDeleteFailed, Message: "failed to delete vectors", Details: err.Error()}
	}
	return nil
}

// Search performs exact cosine-similarity search ordered by the pgvector
// "<=>" (cosine distance) operator.
func (b *Backend) Search(ctx context.Context, query []float32, topK int, filters map[string]any) ([]vector.VectorMatch, error) {
	if topK <= 0 {
		topK = 10
	}
	vecStr := floatArrayToVector(query)

	var filterParts []string
	var filterArgs []any
	argIdx := 1
	if tenantID, ok := filters["tenant_id"].(string); ok && tenantID != "" {
		filterParts = append(filterParts, fmt.Sprintf("tenant_id = $%d", argIdx))
		filterArgs = append(filterArgs, tenantID)
		argIdx++
	}
	if namespace, ok := filters["namespace"].(string); ok && namespace != "" {
		filterParts = append(filterParts, fmt.Sprintf("namespace = $%d", argIdx))
		filterArgs = append(filterArgs, namespace)
		argIdx++
	}
	whereClause := "1=1"
	if len(filterParts) > 0 {
		whereClause = strings.Join(filterParts, " AND ")
	}

	querySQL := fmt.Sprintf(`
		SELECT id, tenant_id, namespace, vector::text, metadata, text,
		       1 - (vector <=> $%d::vector) AS score
		FROM vectors
		WHERE %s
		ORDER BY vector <=> $%d::vector ASC
		LIMIT %d
	`, argIdx, whereClause, argIdx, topK)

	args := make([]any, len(filterArgs)+1)
	copy(args, filterArgs)
	args[len(filterArgs)] = vecStr

	rows, err := b.db.QueryContext(ctx, querySQL, args...)
	if err != nil {
		return nil, &VectorError{Code: vector.ErrQueryFailed, Message: "failed to execute search query", Details: err.Error()}
	}
	defer rows.Close()

	var matches []vector.VectorMatch
	for rows.Next() {
		var (
			m            vector.VectorMatch
			tenantID     string
			namespace    string
			vectorText   string
			metadataJSON []byte
			score        float64
			createdAt    time.Time
			updatedAt    time.Time
		)
		if err := rows.Scan(&m.ID, &tenantID, &namespace, &vectorText, &metadataJSON, &m.Text, &score, &createdAt, &updatedAt); err != nil {
			return nil, &VectorError{Code: vector.ErrQueryFailed, Message: "failed to scan row", Details: err.Error()}
		}
		m.Vector = parseVectorText(vectorText)
		m.Metadata = map[string]any{}
		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &m.Metadata); err != nil {
				return nil, &VectorError{Code: vector.ErrQueryFailed, Message: "failed to parse metadata", Details: err.Error()}
			}
		}
		m.Metadata["tenant_id"] = tenantID
		m.Metadata["namespace"] = namespace
		m.Score = float32(score)
		m.DistanceType = "cosine"
		matches = append(matches, m)
	}
	if err := rows.Err(); err != nil {
		return nil, &VectorError{Code: vector.ErrQueryFailed, Message: "row iteration failed", Details: err.Error()}
	}
	return matches, nil
}

// SearchByQuery performs lexical search: candidate rows matching any query
// term (ILIKE) are ranked in Go by term-frequency scoring. This avoids a
// hard dependency on pg_trgm or an external BM25 service.
func (b *Backend) SearchByQuery(ctx context.Context, text string, topK int, filters map[string]any) ([]vector.VectorMatch, error) {
	if topK <= 0 {
		topK = 10
	}
	terms := tokenizeQuery(text)
	if len(terms) == 0 {
		return nil, nil
	}

	// Build WHERE with explicit, unambiguous parameter indexing:
	// tenant/namespace equality (AND) plus an any-term ILIKE clause (OR)
	// to bound the candidate set.
	var parts []string
	args := make([]any, 0, len(terms)+2)
	argN := 0
	if tenantID, ok := filters["tenant_id"].(string); ok && tenantID != "" {
		argN++
		parts = append(parts, fmt.Sprintf("tenant_id = $%d", argN))
		args = append(args, tenantID)
	}
	if namespace, ok := filters["namespace"].(string); ok && namespace != "" {
		argN++
		parts = append(parts, fmt.Sprintf("namespace = $%d", argN))
		args = append(args, namespace)
	}
	likeParts := make([]string, 0, len(terms))
	for _, term := range terms {
		argN++
		likeParts = append(likeParts, fmt.Sprintf("text ILIKE $%d", argN))
		args = append(args, "%"+term+"%")
	}
	whereClause := "1=1"
	if len(parts) > 0 {
		whereClause = strings.Join(parts, " AND ")
	}
	whereClause += " AND (" + strings.Join(likeParts, " OR ") + ")"

	querySQL := fmt.Sprintf(`
		SELECT id, tenant_id, namespace, vector::text, metadata, text
		FROM vectors
		WHERE %s
		LIMIT %d
	`, whereClause, topK*5)

	rows, err := b.db.QueryContext(ctx, querySQL, args...)
	if err != nil {
		return nil, &VectorError{Code: vector.ErrQueryFailed, Message: "failed to execute text search", Details: err.Error()}
	}
	defer rows.Close()

	type candidate struct {
		m     vector.VectorMatch
		lower string
	}
	var candidates []candidate
	for rows.Next() {
		var (
			m            vector.VectorMatch
			tenantID     string
			namespace    string
			vectorText   string
			metadataJSON []byte
		)
		if err := rows.Scan(&m.ID, &tenantID, &namespace, &vectorText, &metadataJSON, &m.Text); err != nil {
			return nil, &VectorError{Code: vector.ErrQueryFailed, Message: "failed to scan row", Details: err.Error()}
		}
		m.Vector = parseVectorText(vectorText)
		m.Metadata = map[string]any{}
		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &m.Metadata); err != nil {
				return nil, &VectorError{Code: vector.ErrQueryFailed, Message: "failed to parse metadata", Details: err.Error()}
			}
		}
		m.Metadata["tenant_id"] = tenantID
		m.Metadata["namespace"] = namespace
		m.DistanceType = "text"
		candidates = append(candidates, candidate{m: m, lower: strings.ToLower(m.Text)})
	}
	if err := rows.Err(); err != nil {
		return nil, &VectorError{Code: vector.ErrQueryFailed, Message: "row iteration failed", Details: err.Error()}
	}

	// Rank candidates by number of distinct query terms present, then by ID.
	for i := range candidates {
		score := 0
		for _, term := range terms {
			if strings.Contains(candidates[i].lower, term) {
				score++
			}
		}
		candidates[i].m.Score = float32(score)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].m.Score != candidates[j].m.Score {
			return candidates[i].m.Score > candidates[j].m.Score
		}
		return candidates[i].m.ID < candidates[j].m.ID
	})
	if len(candidates) > topK {
		candidates = candidates[:topK]
	}
	out := make([]vector.VectorMatch, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.m)
	}
	return out, nil
}

func (b *Backend) Close() error {
	if b.db == nil {
		return nil
	}
	return b.db.Close()
}

// Helper functions

func floatArrayToVector(vec []float32) string {
	if len(vec) == 0 {
		return "{}"
	}
	parts := make([]string, len(vec))
	for i, v := range vec {
		parts[i] = strconv.FormatFloat(float64(v), 'f', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func parseVectorText(text string) []float32 {
	text = strings.TrimSpace(text)
	if text == "" || text == "{}" {
		return nil
	}
	text = strings.Trim(text, "[]")
	parts := strings.Split(text, ",")
	vec := make([]float32, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		f, err := strconv.ParseFloat(p, 32)
		if err != nil {
			continue
		}
		vec = append(vec, float32(f))
	}
	return vec
}

func calculateCosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0.0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0.0
	}
	return float32(dot / (math.Sqrt(normA) * math.Sqrt(normB)))
}

// tokenizeQuery lowercases a query and extracts alphanumeric terms.
func tokenizeQuery(text string) []string {
	var terms []string
	var sb strings.Builder
	flush := func() {
		if sb.Len() > 0 {
			terms = append(terms, sb.String())
			sb.Reset()
		}
	}
	for _, r := range strings.ToLower(text) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return terms
}
