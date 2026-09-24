package xai

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Energy sources used in intensity profiles.
type EnergySource string

const (
	SourceSolar   EnergySource = "solar"
	SourceWind    EnergySource = "wind"
	SourceHydro   EnergySource = "hydro"
	SourceNuclear EnergySource = "nuclear"
	SourceCoal    EnergySource = "coal"
	SourceGas     EnergySource = "gas"
	SourceBiomass EnergySource = "biomass"
)

// regionalHourProfile models one region's average CO2e intensity per hour of
// the day (index 0 = midnight UTC). Values are in kg CO2e per kWh.
//
// Snapshot of public data: Ember's Global Electricity Review 2024 country-level
// average grid intensities, shaped with a simplified diurnal curve (gas peaking
// in the evening, solar midday offset where noted). This is a static,
// hardcoded approximation — replace with live carbon-intensity APIs (e.g.
// ElectricityMap) for production scheduling.
type regionalHourProfile [24]float64

var regionalProfiles = map[string]regionalHourProfile{
	// Coal-heavy grid: high baseline, modest diurnal swing.
	"IN": {0.94, 0.93, 0.93, 0.92, 0.92, 0.93, 0.95, 0.98, 1.02, 1.05, 1.06, 1.05, 1.04, 1.03, 1.04, 1.06, 1.09, 1.12, 1.13, 1.10, 1.06, 1.02, 0.99, 0.96},
	// Coal + gas mix, strong evening peak.
	"US": {0.40, 0.39, 0.38, 0.38, 0.39, 0.41, 0.44, 0.48, 0.53, 0.55, 0.55, 0.54, 0.53, 0.54, 0.56, 0.58, 0.61, 0.64, 0.66, 0.64, 0.58, 0.52, 0.47, 0.43},
	// Mix with large gas share; midday solar dip in some regions.
	"CN": {0.52, 0.51, 0.51, 0.50, 0.51, 0.53, 0.56, 0.60, 0.64, 0.65, 0.64, 0.62, 0.61, 0.62, 0.64, 0.67, 0.70, 0.73, 0.74, 0.71, 0.66, 0.61, 0.57, 0.54},
	// Hydro and nuclear dominated: flat, low intensity.
	"FR": {0.08, 0.08, 0.08, 0.07, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.12, 0.12, 0.12, 0.12, 0.12, 0.13, 0.14, 0.15, 0.16, 0.15, 0.13, 0.11, 0.09, 0.08},
	// Gas-dominated, evening peak.
	"DE": {0.30, 0.29, 0.29, 0.28, 0.29, 0.31, 0.34, 0.38, 0.42, 0.44, 0.44, 0.43, 0.42, 0.43, 0.45, 0.47, 0.50, 0.52, 0.53, 0.51, 0.46, 0.41, 0.36, 0.32},
	// Large hydro + nuclear share, flat and low.
	"CA": {0.05, 0.05, 0.05, 0.05, 0.05, 0.06, 0.06, 0.07, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.09, 0.10, 0.10, 0.10, 0.09, 0.08, 0.07, 0.06, 0.06},
	// Renewables-heavy, very low intensity.
	"BR": {0.06, 0.06, 0.06, 0.05, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.10, 0.10, 0.09, 0.09, 0.09, 0.10, 0.11, 0.12, 0.12, 0.11, 0.09, 0.08, 0.07, 0.06},
}

// ModelEnergyCoefficient maps model families to energy per 1K tokens (J).
// Order of magnitude from public estimates: an 8B-parameter model spends
// roughly 2–3 J per 1K tokens; frontier 100B+ models roughly 50–70 J per 1K.
type ModelEnergyCoefficient struct {
	ModelFamily       string
	JoulesPer1KTokens float64
}

var modelEnergyTable = []ModelEnergyCoefficient{
	{"small", 3.0},   // ~8B parameter class
	{"medium", 20.0}, // ~30B parameter class
	{"large", 60.0},  // ~100B+ parameter class
}

// RegionIntensity returns the average intensity for a region at a UTC hour.
// Unknown regions return -1.
func RegionIntensity(region string, hourUTC int) float64 {
	profile, ok := regionalProfiles[strings.ToUpper(region)]
	if !ok {
		return -1
	}
	h := hourUTC % 24
	if h < 0 {
		h += 24
	}
	return profile[h]
}

// RegionsKnown lists the regions present in the hardcoded table (sorted).
func RegionsKnown() []string {
	out := make([]string, 0, len(regionalProfiles))
	for r := range regionalProfiles {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// CarbonEstimate is the carbon and energy cost of a token workload.
type CarbonEstimate struct {
	Region         string
	Tokens         int
	Joules         float64
	KWH            float64
	KgCO2e         float64
	IntensityKgKWH float64
	ModelFamily    string
}

// EstimateCarbon computes the carbon footprint of processing `tokens` tokens
// with a model family in a region at a UTC hour.
//
// The calculation is: joules = tokens/1000 * coefficient; kWh = joules/3.6e6;
// kgCO2e = kWh * regional intensity at that hour. Unknown regions use the
// average of all known regional averages as a conservative default.
func EstimateCarbon(region string, modelFamily string, tokens int, at time.Time) CarbonEstimate {
	coeff := 0.0
	for _, mc := range modelEnergyTable {
		if strings.EqualFold(mc.ModelFamily, modelFamily) {
			coeff = mc.JoulesPer1KTokens
			break
		}
	}
	if coeff == 0 {
		coeff = modelEnergyTable[0].JoulesPer1KTokens
	}

	intensity := RegionIntensity(region, at.UTC().Hour())
	usedDefault := false
	if intensity < 0 {
		// Default: mean of the hourly table means across all known regions.
		var sum, n float64
		for _, profile := range regionalProfiles {
			for _, v := range profile {
				sum += v
				n++
			}
		}
		intensity = sum / n
		usedDefault = true
		_ = usedDefault
	}

	joules := float64(tokens) / 1000.0 * coeff
	kwh := joules / 3.6e6
	kg := kwh * intensity

	return CarbonEstimate{
		Region:         strings.ToUpper(region),
		Tokens:         tokens,
		Joules:         joules,
		KWH:            kwh,
		KgCO2e:         kg,
		IntensityKgKWH: intensity,
		ModelFamily:    modelFamily,
	}
}

// CarbonWindow is one candidate execution window.
type CarbonWindow struct {
	Region         string
	At             time.Time
	IntensityKgKWH float64
	KgCO2e         float64
}

// BestWindow searches the lowest-carbon (region, hour) slot for a workload
// within [from, to]. The hour granularity is 1h; the returned At is the start
// of the chosen hour. Deterministic: ties break by (intensity, region, time).
func BestWindow(region string, modelFamily string, tokens int, from, to time.Time) (CarbonWindow, bool) {
	est := EstimateCarbon(region, modelFamily, tokens, from)
	best := CarbonWindow{}
	found := false
	t := from.Truncate(time.Hour)
	for !t.After(to) {
		w := CarbonWindow{
			Region:         strings.ToUpper(region),
			At:             t,
			IntensityKgKWH: RegionIntensity(region, t.UTC().Hour()),
			KgCO2e:         est.KWH * RegionIntensity(region, t.UTC().Hour()),
		}
		if w.IntensityKgKWH < 0 {
			w.IntensityKgKWH = est.IntensityKgKWH
			w.KgCO2e = est.KgCO2e
		}
		if !found || betterWindow(w, best) {
			best, found = w, true
		}
		t = t.Add(time.Hour)
	}
	return best, found
}

// BestRegion picks, from candidate regions, the region whose best window in
// [from, to] has the lowest carbon footprint. Deterministic tie-break by
// region name.
func BestRegion(regions []string, modelFamily string, tokens int, from, to time.Time) (CarbonWindow, bool) {
	best := CarbonWindow{}
	found := false
	for _, r := range regions {
		w, ok := BestWindow(r, modelFamily, tokens, from, to)
		if !ok {
			continue
		}
		if !found || betterWindow(w, best) {
			best, found = w, true
		}
	}
	return best, found
}

func betterWindow(a, b CarbonWindow) bool {
	if a.KgCO2e != b.KgCO2e {
		return a.KgCO2e < b.KgCO2e
	}
	if a.Region != b.Region {
		return a.Region < b.Region
	}
	return a.At.Before(b.At)
}

// SchedulingRecommendation explains a window choice in human-readable form.
type SchedulingRecommendation struct {
	Window       CarbonWindow
	SavingsKgCO2 float64
	Reasoning    string
}

// RecommendScheduling compares the lowest-carbon window against immediate
// execution at `now` and returns the recommendation with a human-readable
// explanation of the savings.
func RecommendScheduling(region string, modelFamily string, tokens int, now, latest time.Time) (SchedulingRecommendation, error) {
	if now.After(latest) {
		return SchedulingRecommendation{}, fmt.Errorf("latest time %s is before now %s", latest, now)
	}
	window, ok := BestWindow(region, modelFamily, tokens, now, latest)
	if !ok {
		return SchedulingRecommendation{}, fmt.Errorf("no executable window in [%s, %s]", now, latest)
	}
	immediate := EstimateCarbon(region, modelFamily, tokens, now)
	savings := immediate.KgCO2e - window.KgCO2e
	if savings < 0 {
		savings = 0
	}

	reason := fmt.Sprintf(
		"Run in %s at %s (UTC): grid intensity %.3f kgCO2e/kWh vs %.3f now; saves %.4g kgCO2e on %d tokens (%s model).",
		window.Region, window.At.UTC().Format(time.RFC3339),
		window.IntensityKgKWH, immediate.IntensityKgKWH,
		savings, tokens, modelFamily,
	)
	if window.At.After(now) {
		reason += fmt.Sprintf(" Delay is %s.", window.At.Sub(now).Round(time.Minute))
	}
	return SchedulingRecommendation{Window: window, SavingsKgCO2: savings, Reasoning: reason}, nil
}
