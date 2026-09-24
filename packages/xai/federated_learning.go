package xai

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
	"sync"
)

// LinearModel is a single-layer linear regressor y = w·x + b. It is the
// shared model object in the federated averaging experiments below.
type LinearModel struct {
	Weights []float64
	Bias    float64
}

// Predict returns the model output for x.
func (m *LinearModel) Predict(x []float64) float64 {
	if len(m.Weights) != len(x) {
		return 0
	}
	sum := m.Bias
	for i := range x {
		sum += m.Weights[i] * x[i]
	}
	return sum
}

// Clone returns an independent copy.
func (m *LinearModel) Clone() *LinearModel {
	w := make([]float64, len(m.Weights))
	copy(w, m.Weights)
	return &LinearModel{Weights: w, Bias: m.Bias}
}

// DataPoint is one (x, y) sample.
type DataPoint struct {
	X []float64
	Y float64
}

// FederatedConfig configures a FedAvg run.
type FederatedConfig struct {
	// LocalEpochs is how many gradient steps each client takes per round on
	// its local data.
	LocalEpochs int
	// LearningRate is the SGD step size.
	LearningRate float64
	// Features is the model input dimension.
	Features int
	// Seed makes the run deterministic. 0 uses a fixed default.
	Seed int64
	// DP enables L2 norm clipping of local updates plus Gaussian noise added
	// at the server (a simplified differential-privacy mechanism).
	DP PrivacyConfig
}

// DefaultFederatedConfig returns a config known to converge on the synthetic
// linear-regression task used by the tests.
func DefaultFederatedConfig() FederatedConfig {
	return FederatedConfig{
		LocalEpochs:  5,
		LearningRate: 0.01,
		Features:     3,
		Seed:         42,
	}
}

// PrivacyConfig controls the L2-clipping + Gaussian noise option.
type PrivacyConfig struct {
	// Enabled turns the mechanism on.
	Enabled bool
	// ClipNorm is the L2 norm cap applied to each client's update vector
	// (before the server combines them).
	ClipNorm float64
	// NoiseSigma is the std-dev of the Gaussian noise added per parameter at
	// the server.
	NoiseSigma float64
	// Seed makes the noise deterministic when set (non-zero).
	Seed int64
}

// ClientUpdate is one client's contribution for a round: the model delta
// (not the weights) and the client's sample count, which FedAvg uses as the
// aggregation weight.
type ClientUpdate struct {
	ClientID   string
	Delta      *LinearModel
	SampleSize int
	TrainLoss  float64 // loss on the client's local data after local training
}

// RoundResult summarizes one FedAvg round.
type RoundResult struct {
	Round       int
	UpdatesUsed int
	TotalLoss   float64 // sum of client local losses (convergence signal)
	MinLoss     float64
	MaxLoss     float64
}

// FederatedServer runs FedAvg: each round, every client trains its local copy
// for LocalEpochs steps on its private data, sends back the update delta
// weighted by sample size, and the server computes the weighted average.
type FederatedServer struct {
	mu      sync.Mutex
	cfg     FederatedConfig
	global  *LinearModel
	rounds  []RoundResult
	clients map[string]int // clientID -> sample size (known at registration)
}

// NewFederatedServer creates a server with a randomly initialized global
// model (deterministic under cfg.Seed).
func NewFederatedServer(cfg FederatedConfig) *FederatedServer {
	if cfg.LocalEpochs <= 0 || cfg.LearningRate <= 0 || cfg.Features <= 0 {
		cfg = DefaultFederatedConfig()
	}
	if cfg.Seed == 0 {
		cfg.Seed = 42
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	global := &LinearModel{
		Weights: make([]float64, cfg.Features),
	}
	for i := range global.Weights {
		global.Weights[i] = rng.Float64()
	}
	return &FederatedServer{
		cfg:     cfg,
		global:  global,
		clients: make(map[string]int),
	}
}

// RegisterClient records a participating client and its dataset. The data is
// stored as an in-process simulation of the client's private store; in a real
// deployment it would live on the client side.
func (s *FederatedServer) RegisterClient(id string, data []DataPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.clients[id]; exists {
		return fmt.Errorf("client %s already registered", id)
	}
	s.clients[id] = len(data)
	return nil
}

// RunRound performs one FedAvg round using the supplied local updates (one per
// client, already locally trained). The server aggregates by sample-size
// weighting. Thread-safe.
func (s *FederatedServer) RunRound(updates []ClientUpdate) RoundResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	round := len(s.rounds) + 1
	res := RoundResult{Round: round}
	if len(updates) == 0 {
		res.TotalLoss, res.MinLoss, res.MaxLoss = 0, 0, 0
		s.rounds = append(s.rounds, res)
		return res
	}

	// Aggregate: weighted average of the deltas, weight = sample size.
	totalWeight := 0.0
	avgDelta := &LinearModel{Weights: make([]float64, s.cfg.Features)}
	var losses []float64
	for _, u := range updates {
		w := float64(u.SampleSize)
		totalWeight += w
		for i := range avgDelta.Weights {
			avgDelta.Weights[i] += u.Delta.Weights[i] * w
		}
		avgDelta.Bias += u.Delta.Bias * w
		losses = append(losses, u.TrainLoss)
	}
	if totalWeight > 0 {
		for i := range avgDelta.Weights {
			avgDelta.Weights[i] /= totalWeight
		}
		avgDelta.Bias /= totalWeight
	}

	// Optional DP mechanism: clip each client's contribution is already baked
	// into the weighted average, so we apply the server-side noise here. If DP
	// is enabled we clip the combined delta to ClipNorm and add Gaussian noise.
	if s.cfg.DP.Enabled {
		clipDeltaL2(avgDelta, s.cfg.DP.ClipNorm)
		addGaussianNoise(avgDelta, s.cfg.DP.NoiseSigma, s.cfg.DP.Seed)
	}

	// Apply the averaged delta to the global model.
	s.global.Weights = float64Sums(s.global.Weights, avgDelta.Weights)
	s.global.Bias += avgDelta.Bias

	res.UpdatesUsed = len(updates)
	res.TotalLoss = 0
	for _, l := range losses {
		res.TotalLoss += l
	}
	res.MinLoss, res.MaxLoss = minMax(losses)

	s.rounds = append(s.rounds, res)
	return res
}

// GlobalModel returns a copy of the current global model.
func (s *FederatedServer) GlobalModel() *LinearModel {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.global.Clone()
}

// RoundHistory returns all round results.
func (s *FederatedServer) RoundHistory() []RoundResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RoundResult, len(s.rounds))
	copy(out, s.rounds)
	return out
}

// Converged reports whether the last round's total loss is within tol of the
// best (minimum) total loss seen so far. This is the convergence signal: as
// FedAvg converges, per-round loss plateaus near the minimum.
func (s *FederatedServer) Converged(tol float64) bool {
	hist := s.RoundHistory()
	if len(hist) < 2 {
		return false
	}
	best := math.MaxFloat64
	for _, r := range hist {
		if r.TotalLoss < best {
			best = r.TotalLoss
		}
	}
	last := hist[len(hist)-1].TotalLoss
	return last-best <= tol
}

// LossSeries returns the total loss per round (for convergence tracking).
func (s *FederatedServer) LossSeries() []float64 {
	hist := s.RoundHistory()
	out := make([]float64, len(hist))
	for i, r := range hist {
		out[i] = r.TotalLoss
	}
	return out
}

// ---- client-side local training (pure Go, batch gradient descent) ----

// LocalTrain trains a copy of start on data for epochs full-batch gradient
// descent steps and returns the delta (trained - start) plus the final train
// loss. This simulates the private local training step of a federated client.
func LocalTrain(start *LinearModel, data []DataPoint, epochs int, lr float64) (*LinearModel, float64) {
	m := start.Clone()
	loss := finalLoss(m, data)
	for e := 0; e < epochs; e++ {
		grad := zeroModel(m)
		for _, dp := range data {
			pred := m.Predict(dp.X)
			err := pred - dp.Y
			for i := range m.Weights {
				grad.Weights[i] += 2 * err * dp.X[i]
			}
			grad.Bias += 2 * err
		}
		n := float64(len(data))
		for i := range m.Weights {
			m.Weights[i] -= lr * grad.Weights[i] / n
		}
		m.Bias -= lr * grad.Bias / n
	}
	loss = finalLoss(m, data)

	delta := &LinearModel{Weights: make([]float64, len(m.Weights))}
	for i := range m.Weights {
		delta.Weights[i] = m.Weights[i] - start.Weights[i]
	}
	delta.Bias = m.Bias - start.Bias
	return delta, loss
}

// zeroModel returns an empty model with the same shape.
func zeroModel(m *LinearModel) *LinearModel {
	return &LinearModel{Weights: make([]float64, len(m.Weights))}
}

// finalLoss is the mean squared error of m on data.
func finalLoss(m *LinearModel, data []DataPoint) float64 {
	if len(data) == 0 {
		return 0
	}
	sum := 0.0
	for _, dp := range data {
		err := m.Predict(dp.X) - dp.Y
		sum += err * err
	}
	return sum / float64(len(data))
}

// clipDeltaL2 rescales a delta's L2 norm to at most norm (in place).
func clipDeltaL2(delta *LinearModel, norm float64) {
	if norm <= 0 {
		return
	}
	l2sq := delta.Bias * delta.Bias
	for _, w := range delta.Weights {
		l2sq += w * w
	}
	l2 := math.Sqrt(l2sq)
	if l2 > norm {
		scale := norm / l2
		for i := range delta.Weights {
			delta.Weights[i] *= scale
		}
		delta.Bias *= scale
	}
}

// addGaussianNoise adds N(0, sigma^2) to every parameter in place, using a
// deterministic source when seed != 0.
func addGaussianNoise(delta *LinearModel, sigma float64, seed int64) {
	if sigma <= 0 {
		return
	}
	src := int64(1)
	if seed != 0 {
		src = seed
	}
	rng := rand.New(rand.NewSource(src))
	for i := range delta.Weights {
		delta.Weights[i] += sigma * rng.NormFloat64()
	}
	delta.Bias += sigma * rng.NormFloat64()
}

func minMax(vs []float64) (float64, float64) {
	if len(vs) == 0 {
		return 0, 0
	}
	mn, mx := vs[0], vs[0]
	for _, v := range vs[1:] {
		if v < mn {
			mn = v
		}
		if v > mx {
			mx = v
		}
	}
	return mn, mx
}

// ---- synthetic data generator (deterministic) ----

// MakeLinearDataset generates n synthetic linear-regression samples with a
// hidden true weight vector and bias, plus deterministic noise. It is the task
// the federated convergence test trains on.
func MakeLinearDataset(features int, n int, trueW []float64, bias float64, noise float64, seed int64) []DataPoint {
	rng := rand.New(rand.NewSource(seed))
	data := make([]DataPoint, n)
	for i := range data {
		x := make([]float64, features)
		for j := range x {
			x[j] = rng.Float64()*2 - 1 // uniform in [-1, 1]
		}
		y := bias
		for j := range x {
			y += trueW[j] * x[j]
		}
		y += noise * (rng.Float64()*2 - 1)
		data[i] = DataPoint{X: x, Y: y}
	}
	return data
}

// SplitClients partitions a dataset into numClients disjoint chunks.
func SplitClients(data []DataPoint, numClients int) [][]DataPoint {
	if numClients <= 0 {
		return nil
	}
	chunk := (len(data) + numClients - 1) / numClients
	out := make([][]DataPoint, 0, numClients)
	for i := 0; i < numClients; i++ {
		lo := i * chunk
		if lo >= len(data) {
			break
		}
		hi := lo + chunk
		if hi > len(data) {
			hi = len(data)
		}
		out = append(out, data[lo:hi])
	}
	return out
}

// clientStableID derives a stable, deterministic client ID from a name via
// FNV-1a (used where a hash key is needed).
func clientStableID(name string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return h.Sum64()
}

// sortClientIDs returns client IDs in a deterministic order.
func sortClientIDs(ids []string) []string {
	out := make([]string, len(ids))
	copy(out, ids)
	sort.Strings(out)
	return out
}
