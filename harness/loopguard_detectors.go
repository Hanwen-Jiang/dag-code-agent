package harness

import "github.com/Hanwen-Jiang/dag-code-agent/packages/loopsdetect"

// The only file in the core that names packages/loopsdetect.
//
// Keeping the dependency behind type aliases means the seam itself
// (loopguard.go) is written against a vocabulary rather than a package, so
// replacing the detector implementation, or vendoring it later, touches this
// file alone.
//
// loopsdetect has no third-party dependencies, which is why it can be imported
// by the core at all: the dependency budget guard (G7) counts the core's direct
// third-party imports and this package adds none.

type (
	detectorManager      = loopsdetect.LoopDetectorManager
	detectorToolCall     = loopsdetect.ToolCall
	detectorDecisionKind = loopsdetect.Decision
)

const (
	detectorContinue  = loopsdetect.Continue
	detectorNudge     = loopsdetect.Nudge
	detectorForceStop = loopsdetect.ForceStop
)

func newDetectorManager() *detectorManager { return loopsdetect.NewLoopDetectorManager() }
