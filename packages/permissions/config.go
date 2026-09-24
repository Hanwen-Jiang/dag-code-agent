package permissions

import (
	"os"
	"time"
)

// Config is the full permission engine configuration. All fields are YAML
// friendly; see LoadConfig.
type Config struct {
	// ToolAllowlist restricts which tools may be used at all. An empty list
	// means every tool name is accepted at layer 1 (default).
	ToolAllowlist []string `yaml:"tool_allowlist"`

	// HardBlockedPatterns is the immutable danger list. These regexes are
	// compiled at engine construction and can never be disabled by
	// configuration (they exist to survive bad config files).
	HardBlockedPatterns []string `yaml:"hard_blocked_patterns"`

	// DeniedCommandPatterns is the configurable deny list (regexes).
	DeniedCommandPatterns []string `yaml:"denied_command_patterns"`

	// DenyDescriptions holds optional human descriptions aligned by index
	// with DeniedCommandPatterns.
	DenyDescriptions []string `yaml:"deny_descriptions"`

	// AllowedCommandPatterns auto-allow safe read-only commands (layer 6).
	AllowedCommandPatterns []string `yaml:"allowed_command_patterns"`

	// SecretPatterns are regexes whose matches get replaced with
	// [REDACTED] in arguments before anything downstream sees them.
	SecretPatterns []string `yaml:"secret_patterns"`

	// PathPolicy bounds file access (layer 3).
	PathPolicy PathPolicy `yaml:"path_policy"`

	// RateLimit bounds call frequency (layer 5).
	RateLimit RateLimitConfig `yaml:"rate_limit"`

	// RequireApproval makes layer 6 default-deny when no ApprovalHandler is
	// installed and the command is not auto-allowed.
	RequireApproval bool `yaml:"require_approval"`
}

// PathPolicy defines file access rules (layer 3).
type PathPolicy struct {
	// SensitivePatterns are regexes that deny access when they match the
	// canonical path (ssh keys, private keys, credential files, ...).
	SensitivePatterns []string `yaml:"sensitive_patterns"`
	// AllowedBasePaths bounds which absolute paths may be accessed.
	// An empty list disables the boundary check.
	AllowedBasePaths []string `yaml:"allowed_base_paths"`
}

// RateLimitConfig configures the sliding-window rate limiter (layer 5).
type RateLimitConfig struct {
	// Enabled turns the limiter on/off.
	Enabled bool `yaml:"enabled"`
	// MaxCalls is the maximum number of Check calls per window.
	MaxCalls int `yaml:"max_calls"`
	// Window is a duration string ("60s", "5m"). Empty defaults to 1m.
	Window string `yaml:"window"`
}

// WindowDuration parses Window into a time.Duration, defaulting to 1m.
func (r RateLimitConfig) WindowDuration() time.Duration {
	if r.Window == "" {
		return time.Minute
	}
	d, err := time.ParseDuration(r.Window)
	if err != nil || d <= 0 {
		return time.Minute
	}
	return d
}

// DefaultConfig returns production defaults: a curated hard-block list,
// read-only auto-allow patterns, secret redaction, path policy bounded to
// the working directory and /tmp, and a 100 calls/minute rate limit.
func DefaultConfig() Config {
	cwd, _ := os.Getwd()
	return Config{
		ToolAllowlist: nil, // empty = allow all tools at layer 1

		HardBlockedPatterns: []string{
			`rm\s+-rf\s+(?:/|~)(\s|$)`,   // delete root or home
			`\bdd\s+.*of=/dev/`,           // wipe disks
			`^mkfs(\.|$)`,                 // format partitions
			`(^|&&\s*|\|\|\s*|;\s*)curl\b.*\|\s*(ba|z|da)?sh\b`, // download-and-pipe-to-shell
			`(^|&&\s*|\|\|\s*|;\s*)wget\b.*\|\s*(ba|z|da)?sh\b`, // download-and-pipe-to-shell
			`\beval\s+\$[A-Za-z_]`,        // eval of shell variables
			`\bsudo\b`,                    // privilege escalation
			`\bchmod\s+[0-7]{3,4}\s+`,     // world-permissive chmod
			`\bchown\s+[^:\s]+:[^:\s]+`,   // chown to explicit user:group
			`^export\s+(PASSWORD|SECRET|TOKEN|API_KEY|ACCESS_KEY)\s*=`, // leaking secrets into env
			`\bnc\s+-l\s+\d+`,             // netcat listener
			`\biptables\b.*--flush\b`,     // firewall reset
		},

		DeniedCommandPatterns: []string{
			`\bgit\s+push\s+.*--force\b`,
			`\bdocker\s+system\s+prune\b`,
			`\bdrop\s+(table|database)\b`,
		},
		DenyDescriptions: []string{
			"force pushes rewrite shared history",
			"docker system prune removes running containers and volumes",
			"destructive SQL DDL",
		},

		AllowedCommandPatterns: []string{
			`^ls(\s|$)`,
			`^pwd$`,
			`^cat\s+`,
			`^head\s+`,
			`^tail\s+`,
			`^git\s+(status|diff|log|show|branch)\b`,
			`^go\s+(build|test|vet)\b`,
			`^npm\s+(run|test)\b`,
			`^cargo\s+(test|check)\b`,
			`^echo\s+`,
			`^find\s+`,
			`^grep\s+`,
		},

		SecretPatterns: []string{
			`(sk-[A-Za-z0-9_-]{16,})`,                                    // OpenAI-style keys
			`(\b[A-Za-z0-9+/]{40,}={0,2}\b)`,                            // long base64 blobs (tokens, PEM b64)
			`((?i)(api[_-]?key|secret|token|password|passwd)\s*[:=]\s*["']?[^\s"']{6,})`, // key=value secrets
		},

		PathPolicy: PathPolicy{
			SensitivePatterns: []string{
				`(^|/)\.ssh/`,        // ssh keys
				`\.pem$`,             // private keys
				`\.(env|secret|private)$`,
				`/etc/shadow$`,
				`id_rsa$`,
				`credentials\.ya?ml$`,
			},
			// Bounded to the working directory and /tmp by default so a
			// misconfigured engine still cannot roam the filesystem.
			AllowedBasePaths: []string{cwd, "/tmp"},
		},

		RateLimit: RateLimitConfig{
			Enabled:  true,
			MaxCalls: 100,
			Window:   "1m",
		},

		RequireApproval: true,
	}
}
