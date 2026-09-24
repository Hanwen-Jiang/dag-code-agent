package vectordb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgVectorDB implements VectorDB on top of PostgreSQL with the pgvector
// extension, using pgx/v5 and a connection pool. Vectors are stored in a
// vector(d) column and ranked by cosine distance (embedding <-> query).
//
// The constructor parses the DSN but does not dial; the pool and schema are
// created lazily in Initialize.
type PgVectorDB struct {
	dsn       string
	table     string
	dimension int
	timeout   time.Duration

	mu          sync.Mutex
	pool        *pgxpool.Pool
	initialized bool
	closed      bool
}

// pgVectorRow is one pending upsert row.
type pgVectorRow struct {
	id      string
	vector  []float32
	payload string // JSON-encoded payload
}

// NewPgVectorDB constructs a PgVectorDB without dialing. It returns an error
// only for a malformed DSN, an invalid table name, or a non-positive
// dimension; connection failures surface from Initialize/HealthCheck.
func NewPgVectorDB(dsn string, table string, dimension int, timeout time.Duration) (*PgVectorDB, error) {
	if dsn == "" {
		return nil, fmt.Errorf("vectordb: pgvector dsn must not be empty")
	}
	if !isSafeSQLIdentifier(table) {
		return nil, fmt.Errorf("vectordb: pgvector table %q is not a valid identifier", table)
	}
	if dimension <= 0 {
		return nil, fmt.Errorf("vectordb: pgvector dimension must be positive, got %d", dimension)
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	// Validate the DSN parses; do not connect yet.
	if _, err := pgxpool.ParseConfig(dsn); err != nil {
		return nil, fmt.Errorf("vectordb: pgvector dsn: %w", err)
	}

	return &PgVectorDB{
		dsn:       dsn,
		table:     table,
		dimension: dimension,
		timeout:   timeout,
	}, nil
}

// Name implements VectorDB.
func (p *PgVectorDB) Name() string { return "pgvector" }

// Initialize opens the pool and ensures the pgvector extension, table, and
// cosine index exist.
func (p *PgVectorDB) Initialize(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("vectordb: pgvector db is closed")
	}
	if p.initialized {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, p.dsn)
	if err != nil {
		return fmt.Errorf("vectordb: pgvector pool: %w", err)
	}
	p.pool = pool

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		p.pool = nil
		return fmt.Errorf("vectordb: pgvector ping: %w", err)
	}

	for _, stmt := range []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				id text PRIMARY KEY,
				embedding vector(%d) NOT NULL,
				payload jsonb NOT NULL DEFAULT '{}'::jsonb
			)`, p.quoteIdent(p.table), p.dimension),
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS %s ON %s USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)`,
			p.quoteIdent(p.table+"_emb_idx"), p.quoteIdent(p.table)),
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			pool.Close()
			p.pool = nil
			return fmt.Errorf("vectordb: pgvector init: %w", err)
		}
	}

	p.initialized = true
	return nil
}

// Upsert inserts or replaces points using ON CONFLICT, in bounded batches.
func (p *PgVectorDB) Upsert(ctx context.Context, points []VectorPoint) error {
	if len(points) == 0 {
		return nil
	}
	if err := p.requirePool(); err != nil {
		return err
	}
	pool := p.pool

	rows := make([]pgVectorRow, 0, len(points))
	for _, pt := range points {
		if pt.ID == "" {
			return fmt.Errorf("vectordb: pgvector point has empty ID")
		}
		if len(pt.Vector) == 0 {
			return fmt.Errorf("vectordb: pgvector point %q has an empty vector", pt.ID)
		}
		if len(pt.Vector) != p.dimension {
			return fmt.Errorf("vectordb: pgvector point %q has dimension %d, expected %d",
				pt.ID, len(pt.Vector), p.dimension)
		}
		payloadJSON, err := json.Marshal(pt.Payload)
		if err != nil {
			return fmt.Errorf("vectordb: pgvector payload for %q: %w", pt.ID, err)
		}
		rows = append(rows, pgVectorRow{id: pt.ID, vector: pt.Vector, payload: string(payloadJSON)})
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	const batchSize = 500
	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		if err := p.upsertBatch(ctx, pool, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// upsertBatch writes one batch with a single parameterized statement.
func (p *PgVectorDB) upsertBatch(ctx context.Context, pool *pgxpool.Pool, rows []pgVectorRow) error {
	// Parameterized bulk upsert: ($1, $2, $3), ($4, $5, $6), ...
	placeholders := make([]string, 0, len(rows))
	args := make([]any, 0, len(rows)*3)
	for i, r := range rows {
		base := i * 3
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d)", base+1, base+2, base+3))
		args = append(args, r.id, r.vector, r.payload)
	}

	stmt := fmt.Sprintf(
		`INSERT INTO %s (id, embedding, payload)
		 VALUES %s
		 ON CONFLICT (id) DO UPDATE
		   SET embedding = EXCLUDED.embedding,
		       payload   = EXCLUDED.payload`,
		p.quoteIdent(p.table), strings.Join(placeholders, ", "))

	if _, err := pool.Exec(ctx, stmt, args...); err != nil {
		return fmt.Errorf("vectordb: pgvector upsert: %w", err)
	}
	return nil
}

// Query returns the top-K nearest points by cosine distance, with optional
// exact payload filtering.
func (p *PgVectorDB) Query(ctx context.Context, req QueryRequest) ([]SearchResult, error) {
	if err := p.requirePool(); err != nil {
		return nil, err
	}
	if len(req.Vector) == 0 {
		return nil, fmt.Errorf("vectordb: pgvector query vector is empty")
	}
	if len(req.Vector) != p.dimension {
		return nil, fmt.Errorf("vectordb: pgvector query vector dimension %d, expected %d",
			len(req.Vector), p.dimension)
	}
	topK := req.TopK
	if topK <= 0 {
		topK = 10
	}

	where, filterArgs, err := p.buildPayloadFilter(req.FilterPayload)
	if err != nil {
		return nil, err
	}

	// Parameter layout: $1 = query vector, $2 = LIMIT, $3+ = filter args.
	query := fmt.Sprintf(
		`SELECT id, 1 - (embedding <-> $1) AS score, payload
		   FROM %s
		  %s
		  ORDER BY embedding <-> $1 ASC
		  LIMIT $2`,
		p.quoteIdent(p.table), where)

	args := append([]any{req.Vector, topK}, filterArgs...)

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("vectordb: pgvector query: %w", err)
	}
	defer rows.Close()

	results := make([]SearchResult, 0, topK)
	for rows.Next() {
		var (
			id      string
			score   float64
			payload []byte
		)
		if err := rows.Scan(&id, &score, &payload); err != nil {
			return nil, fmt.Errorf("vectordb: pgvector scan: %w", err)
		}
		pl := make(map[string]any)
		if len(payload) > 0 {
			_ = json.Unmarshal(payload, &pl)
		}
		results = append(results, SearchResult{ID: id, Score: score, Payload: pl})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("vectordb: pgvector query rows: %w", err)
	}
	return results, nil
}

// buildPayloadFilter renders an AND of exact jsonb-value matches and returns
// the SQL fragment plus the bound arguments (in binding order). Each
// condition compares payload -> $key (jsonb) against $val::jsonb, which is
// type-agnostic: the Go value is serialized to JSON and parsed as jsonb on the
// server, so numbers, strings, booleans, and nested structures compare exactly.
func (p *PgVectorDB) buildPayloadFilter(filter map[string]any) (string, []any, error) {
	if len(filter) == 0 {
		return "", nil, nil
	}
	keys := make([]string, 0, len(filter))
	for k := range filter {
		keys = append(keys, k)
	}
	sortStrings(keys)

	var b strings.Builder
	b.WriteString("WHERE ")
	args := make([]any, 0, len(keys)*2)
	for i, k := range keys {
		if i > 0 {
			b.WriteString(" AND ")
		}
		keyParam, valParam := 3+i*2, 4+i*2
		v, err := json.Marshal(filter[k])
		if err != nil {
			return "", nil, fmt.Errorf("vectordb: pgvector filter value %q: %w", k, err)
		}
		args = append(args, k, string(v))
		b.WriteString(fmt.Sprintf("payload -> $%d = $%d::jsonb", keyParam, valParam))
	}
	return b.String(), args, nil
}

// Delete removes the given point IDs.
func (p *PgVectorDB) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := p.requirePool(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	if _, err := p.pool.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE id = ANY($1)`, p.quoteIdent(p.table)), ids); err != nil {
		return fmt.Errorf("vectordb: pgvector delete: %w", err)
	}
	return nil
}

// Close drains the pool.
func (p *PgVectorDB) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.pool != nil {
		p.pool.Close()
		p.pool = nil
	}
	return nil
}

// HealthCheck verifies the database is reachable.
func (p *PgVectorDB) HealthCheck(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("vectordb: pgvector db is closed")
	}
	if p.pool == nil {
		return fmt.Errorf("vectordb: pgvector db not initialized; call Initialize first")
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if err := p.pool.Ping(ctx); err != nil {
		return fmt.Errorf("vectordb: pgvector ping: %w", err)
	}
	return nil
}

// requirePool returns an error if the db is closed or the pool is not open.
func (p *PgVectorDB) requirePool() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("vectordb: pgvector db is closed")
	}
	if p.pool == nil {
		return fmt.Errorf("vectordb: pgvector db not initialized; call Initialize first")
	}
	return nil
}

// quoteIdent double-quotes an identifier for safe interpolation.
func (p *PgVectorDB) quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// isSafeSQLIdentifier reports whether name is a conservative SQL identifier
// (letters, digits, underscore; must not start with a digit, ≤ 63 chars).
func isSafeSQLIdentifier(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// sortStrings sorts a small slice in place (insertion sort; the filter maps
// we sort are expected to be tiny).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
