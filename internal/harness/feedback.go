package harness

type FeedbackCollector struct{ Store FeedbackStore }

func (c FeedbackCollector) Record(auth AuthContext, signal FeedbackSignal) error {
	signal.TenantID = auth.TenantID
	signal.SubjectID = auth.SubjectID
	if signal.Source == "" {
		signal.Source = "product"
	}
	if signal.Stage == "" {
		signal.Stage = "unknown"
	}
	if signal.Outcome == "" {
		signal.Outcome = "unknown"
	}
	return c.Store.PutFeedback(signal)
}

type FunnelMetrics struct {
	Signals      int            `json:"signals"`
	Completed    int            `json:"completed"`
	DropOff      int            `json:"drop_off"`
	HumanHandoff int            `json:"human_handoff"`
	MeanRating   float64        `json:"mean_rating"`
	ByStage      map[string]int `json:"by_stage"`
	ByCategory   map[string]int `json:"by_category"`
}

func (c FeedbackCollector) Metrics(tenantID string) (FunnelMetrics, error) {
	items, err := c.Store.Feedback(tenantID)
	if err != nil {
		return FunnelMetrics{}, err
	}
	metrics := FunnelMetrics{Signals: len(items), ByStage: map[string]int{}, ByCategory: map[string]int{}}
	ratings, ratingSum := 0, 0
	for _, item := range items {
		metrics.ByStage[item.Stage]++
		if item.Category != "" {
			metrics.ByCategory[item.Category]++
		}
		switch item.Outcome {
		case "completed":
			metrics.Completed++
		case "drop_off":
			metrics.DropOff++
		case "human_handoff":
			metrics.HumanHandoff++
		}
		if item.Rating > 0 {
			ratings++
			ratingSum += item.Rating
		}
	}
	if ratings > 0 {
		metrics.MeanRating = float64(ratingSum) / float64(ratings)
	}
	return metrics, nil
}
