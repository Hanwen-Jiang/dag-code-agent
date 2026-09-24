package vectordb

import (
	"context"
	"fmt"
	"sync"
	"time"

	qdrant "github.com/qdrant/go-client/qdrant"
)

// QdrantDB implements VectorDB on top of the official Qdrant gRPC client
// (github.com/qdrant/go-client). It stores a single named collection with
// cosine distance and maps the VectorDB surface onto Qdrant point operations.
//
// The constructor performs argument validation only; no network dial happens
// until Initialize or HealthCheck is called (gRPC connects lazily).
type QdrantDB struct {
	client     *qdrant.Client
	collection string
	dimension  int
	timeout    time.Duration

	mu          sync.Mutex
	initialized bool
	closed      bool
}

// NewQdrantClient constructs a QdrantDB without dialing. It returns an error
// only for malformed arguments (missing host, non-positive port, or a
// non-positive dimension); connection failures surface from Initialize/
// HealthCheck.
func NewQdrantClient(host string, port int, useTLS bool, collection string, dimension int, timeout time.Duration) (*QdrantDB, error) {
	if host == "" {
		return nil, fmt.Errorf("vectordb: qdrant host must not be empty")
	}
	if port <= 0 {
		return nil, fmt.Errorf("vectordb: qdrant port must be positive, got %d", port)
	}
	if collection == "" {
		return nil, fmt.Errorf("vectordb: qdrant collection must not be empty")
	}
	if dimension <= 0 {
		return nil, fmt.Errorf("vectordb: qdrant dimension must be positive, got %d", dimension)
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   host,
		Port:   port,
		UseTLS: useTLS,
		// Skip the construction-time version probe so the constructor never
		// blocks on the network; liveness is checked via HealthCheck.
		SkipCompatibilityCheck: true,
	})
	if err != nil {
		return nil, fmt.Errorf("vectordb: qdrant client init: %w", err)
	}

	return &QdrantDB{
		client:     client,
		collection: collection,
		dimension:  dimension,
		timeout:    timeout,
	}, nil
}

// Name implements VectorDB.
func (q *QdrantDB) Name() string { return "qdrant" }

// Initialize dials lazily and ensures the backing collection exists.
func (q *QdrantDB) Initialize(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return fmt.Errorf("vectordb: qdrant db is closed")
	}
	if err := q.ensureCollectionLocked(ctx); err != nil {
		return err
	}
	q.initialized = true
	return nil
}

// ensureCollectionLocked creates the collection if missing. Caller holds q.mu.
func (q *QdrantDB) ensureCollectionLocked(ctx context.Context) error {
	if q.initialized {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	exists, err := q.client.CollectionExists(ctx, q.collection)
	if err != nil {
		return fmt.Errorf("vectordb: check collection %q: %w", q.collection, err)
	}
	if exists {
		return nil
	}

	err = q.client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: q.collection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     uint64(q.dimension),
			Distance: qdrant.Distance_Cosine,
		}),
	})
	if err != nil {
		return fmt.Errorf("vectordb: create collection %q: %w", q.collection, err)
	}
	return nil
}

// Upsert inserts or replaces points, keyed by their string ID.
func (q *QdrantDB) Upsert(ctx context.Context, points []VectorPoint) error {
	if len(points) == 0 {
		return nil
	}
	if err := q.requireInit(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	qdrantPoints := make([]*qdrant.PointStruct, 0, len(points))
	for _, p := range points {
		if len(p.Vector) == 0 {
			return fmt.Errorf("vectordb: qdrant point %q has an empty vector", p.ID)
		}
		if len(p.Vector) != q.dimension {
			return fmt.Errorf("vectordb: qdrant point %q has dimension %d, expected %d",
				p.ID, len(p.Vector), q.dimension)
		}
		var payload map[string]*qdrant.Value
		if len(p.Payload) > 0 {
			vp, err := qdrant.TryValueMap(p.Payload)
			if err != nil {
				return fmt.Errorf("vectordb: qdrant payload for %q: %w", p.ID, err)
			}
			payload = vp
		}
		qdrantPoints = append(qdrantPoints, &qdrant.PointStruct{
			Id:      qdrant.NewID(p.ID),
			Vectors: qdrant.NewVectorsDense(p.Vector),
			Payload: payload,
		})
	}

	wait := true
	if _, err := q.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: q.collection,
		Points:         qdrantPoints,
		Wait:           &wait,
	}); err != nil {
		return fmt.Errorf("vectordb: qdrant upsert: %w", err)
	}
	return nil
}

// Query returns the top-K nearest points, optionally payload-filtered.
func (q *QdrantDB) Query(ctx context.Context, req QueryRequest) ([]SearchResult, error) {
	if err := q.requireInit(); err != nil {
		return nil, err
	}
	if len(req.Vector) == 0 {
		return nil, fmt.Errorf("vectordb: qdrant query vector is empty")
	}
	if len(req.Vector) != q.dimension {
		return nil, fmt.Errorf("vectordb: qdrant query vector dimension %d, expected %d",
			len(req.Vector), q.dimension)
	}
	topK := req.TopK
	if topK <= 0 {
		topK = 10
	}
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	qp := &qdrant.QueryPoints{
		CollectionName: q.collection,
		Query:          qdrant.NewQueryDense(req.Vector),
		Limit:          qdrant.PtrOf(uint64(topK)),
		WithPayload:    qdrant.NewWithPayload(true),
	}
	if f := buildQdrantFilter(req.FilterPayload); f != nil {
		qp.Filter = f
	}

	scored, err := q.client.Query(ctx, qp)
	if err != nil {
		return nil, fmt.Errorf("vectordb: qdrant query: %w", err)
	}

	results := make([]SearchResult, 0, len(scored))
	for _, sp := range scored {
		results = append(results, SearchResult{
			ID:      pointIDString(sp.Id),
			Score:   float64(sp.Score),
			Payload: fromQdrantPayload(sp.Payload),
		})
	}
	return results, nil
}

// Delete removes the given point IDs.
func (q *QdrantDB) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := q.requireInit(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	pointIDs := make([]*qdrant.PointId, 0, len(ids))
	for _, id := range ids {
		pointIDs = append(pointIDs, qdrant.NewID(id))
	}
	wait := true
	_, err := q.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: q.collection,
		Points: &qdrant.PointsSelector{
			PointsSelectorOneOf: &qdrant.PointsSelector_Points{
				Points: &qdrant.PointsIdsList{Ids: pointIDs},
			},
		},
		Wait: &wait,
	})
	if err != nil {
		return fmt.Errorf("vectordb: qdrant delete: %w", err)
	}
	return nil
}

// Close shuts the underlying gRPC client down.
func (q *QdrantDB) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	if err := q.client.Close(); err != nil {
		return fmt.Errorf("vectordb: qdrant close: %w", err)
	}
	return nil
}

// HealthCheck verifies the Qdrant service responds.
func (q *QdrantDB) HealthCheck(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return fmt.Errorf("vectordb: qdrant db is closed")
	}
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()
	if _, err := q.client.HealthCheck(ctx); err != nil {
		return fmt.Errorf("vectordb: qdrant health check: %w", err)
	}
	return nil
}

// requireInit returns an error if the db is closed or not yet initialized.
func (q *QdrantDB) requireInit() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return fmt.Errorf("vectordb: qdrant db is closed")
	}
	if !q.initialized {
		return fmt.Errorf("vectordb: qdrant db not initialized; call Initialize first")
	}
	return nil
}

// pointIDString renders a Qdrant PointId as its canonical string form.
func pointIDString(id *qdrant.PointId) string {
	if id == nil {
		return ""
	}
	if n := id.GetNum(); n != 0 {
		return fmt.Sprintf("%d", n)
	}
	if u := id.GetUuid(); u != "" {
		return u
	}
	return ""
}

// buildQdrantFilter converts a payload filter map into an AND (Must) filter of
// exact-match conditions.
func buildQdrantFilter(filter map[string]any) *qdrant.Filter {
	if len(filter) == 0 {
		return nil
	}
	must := make([]*qdrant.Condition, 0, len(filter))
	for k, v := range filter {
		cond := buildQdrantCondition(k, v)
		if cond != nil {
			must = append(must, cond)
		}
	}
	if len(must) == 0 {
		return nil
	}
	return &qdrant.Filter{Must: must}
}

func buildQdrantCondition(key string, value any) *qdrant.Condition {
	switch val := value.(type) {
	case string:
		return qdrant.NewMatch(key, val)
	case bool:
		return qdrant.NewMatchBool(key, val)
	case int:
		return qdrant.NewMatchInt(key, int64(val))
	case int32:
		return qdrant.NewMatchInt(key, int64(val))
	case int64:
		return qdrant.NewMatchInt(key, val)
	case uint:
		return qdrant.NewMatchInt(key, int64(val))
	case uint32:
		return qdrant.NewMatchInt(key, int64(val))
	case uint64:
		return qdrant.NewMatchInt(key, int64(val))
	case float32:
		return qdrant.NewRange(key, &qdrant.Range{Gte: qdrant.PtrOf(float64(val)), Lte: qdrant.PtrOf(float64(val))})
	case float64:
		return qdrant.NewRange(key, &qdrant.Range{Gte: qdrant.PtrOf(val), Lte: qdrant.PtrOf(val)})
	default:
		return qdrant.NewMatch(key, fmt.Sprintf("%v", value))
	}
}

// fromQdrantPayload converts a Qdrant payload map into a plain Go map.
func fromQdrantPayload(payload map[string]*qdrant.Value) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		out[k] = fromQdrantValue(v)
	}
	return out
}

func fromQdrantValue(v *qdrant.Value) any {
	if v == nil {
		return nil
	}
	switch kind := v.Kind.(type) {
	case *qdrant.Value_NullValue:
		return nil
	case *qdrant.Value_BoolValue:
		return kind.BoolValue
	case *qdrant.Value_IntegerValue:
		return kind.IntegerValue
	case *qdrant.Value_DoubleValue:
		return kind.DoubleValue
	case *qdrant.Value_StringValue:
		return kind.StringValue
	case *qdrant.Value_StructValue:
		if kind.StructValue == nil {
			return nil
		}
		return fromQdrantPayload(kind.StructValue.Fields)
	case *qdrant.Value_ListValue:
		if kind.ListValue == nil {
			return nil
		}
		items := make([]any, 0, len(kind.ListValue.Values))
		for _, item := range kind.ListValue.Values {
			items = append(items, fromQdrantValue(item))
		}
		return items
	default:
		return nil
	}
}
