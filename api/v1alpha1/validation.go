package v1alpha1

import (
	"fmt"
	"strings"
)

func (spec CacheClassSpec) Validate() error {
	if spec.Backend != "" && spec.Backend != "directory" && spec.Backend != "xfs-project" {
		return fmt.Errorf("unsupported backend %q", spec.Backend)
	}
	if spec.CrashRecovery != "" && spec.CrashRecovery != "discard" && spec.CrashRecovery != "reuse" {
		return fmt.Errorf("unsupported crashRecovery policy %q", spec.CrashRecovery)
	}
	if spec.SchemaVersion != "" && strings.ContainsAny(spec.SchemaVersion, "\x00/\\") {
		return fmt.Errorf("schemaVersion contains an invalid character")
	}
	if spec.MaxBytes.Sign() < 0 || spec.Quota.DefaultMaxBytes.Sign() < 0 {
		return fmt.Errorf("cache size limits must not be negative")
	}
	if spec.Quota.Enabled && spec.Backend != "xfs-project" {
		return fmt.Errorf("quota requires the xfs-project backend")
	}
	if spec.Backend == "xfs-project" && !spec.Quota.Enabled {
		return fmt.Errorf("xfs-project backend requires quota to be enabled")
	}
	if !spec.Quota.Enabled && (!spec.MaxBytes.IsZero() || !spec.Quota.DefaultMaxBytes.IsZero()) {
		return fmt.Errorf("maxBytes and quota.defaultMaxBytes require quota to be enabled")
	}
	if spec.Quota.Enabled && spec.MaxBytes.IsZero() && spec.Quota.DefaultMaxBytes.IsZero() {
		return fmt.Errorf("quota requires maxBytes or quota.defaultMaxBytes")
	}
	if spec.Quota.Enabled && !spec.MaxBytes.IsZero() && spec.Quota.DefaultMaxBytes.Cmp(spec.MaxBytes) > 0 {
		return fmt.Errorf("quota.defaultMaxBytes must not exceed the maxBytes ceiling")
	}
	p := spec.Pressure
	for name, value := range map[string]int32{"highFreePercent": p.HighFreePercent, "lowFreePercent": p.LowFreePercent, "highInodeFreePercent": p.HighInodeFreePercent, "lowInodeFreePercent": p.LowInodeFreePercent} {
		if value < 0 || value > 100 {
			return fmt.Errorf("%s must be between 0 and 100", name)
		}
	}
	if p.LowFreePercent > 0 && p.HighFreePercent > 0 && p.HighFreePercent <= p.LowFreePercent {
		return fmt.Errorf("highFreePercent must be greater than lowFreePercent")
	}
	if (p.LowFreePercent == 0) != (p.HighFreePercent == 0) {
		return fmt.Errorf("lowFreePercent and highFreePercent must be configured together")
	}
	if p.LowInodeFreePercent > 0 && p.HighInodeFreePercent > 0 && p.HighInodeFreePercent <= p.LowInodeFreePercent {
		return fmt.Errorf("highInodeFreePercent must be greater than lowInodeFreePercent")
	}
	if (p.LowInodeFreePercent == 0) != (p.HighInodeFreePercent == 0) {
		return fmt.Errorf("lowInodeFreePercent and highInodeFreePercent must be configured together")
	}
	return nil
}
