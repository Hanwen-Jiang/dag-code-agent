package harness

import "runtime/debug"

// modulePath is this module's import path. It is what a consumer's build info
// records for the dependency it resolved.
const modulePath = "github.com/Hanwen-Jiang/dag-code-agent"

// Version reports the module version the running binary was built against.
//
// For an embedded control plane the useful answer is the consumer's resolved
// dependency version, not a constant compiled into the library: a library that
// hard-codes its own version reports the version of whatever source tree it was
// built from, which is exactly what an operator does not want to read. Falling
// back to "devel" means the module was built from a working tree rather than a
// tagged release.
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	if info.Main.Path == modulePath && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep == nil || dep.Path != modulePath {
			continue
		}
		if dep.Replace != nil && dep.Replace.Version != "" {
			return dep.Replace.Version
		}
		if dep.Version != "" {
			return dep.Version
		}
	}
	return "devel"
}
