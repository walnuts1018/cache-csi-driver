package v1alpha1

import (
	"fmt"
	"strings"
)

func (spec CacheClassSpec) Validate() error {
	if err := spec.validateBackend(); err != nil {
		return err
	}
	if err := spec.validateQuota(); err != nil {
		return err
	}
	if err := spec.validatePressure(); err != nil {
		return err
	}
	if spec.Retention.Duration < 0 {
		return fmt.Errorf("retention must not be negative")
	}
	if spec.SchemaVersion != "" && strings.ContainsAny(spec.SchemaVersion, "\x00/\\") {
		return fmt.Errorf("schemaVersion contains an invalid character")
	}
	return nil
}

func (spec CacheClassSpec) validateBackend() error {
	switch spec.Backend {
	case "", BackendDirectory, BackendXFSProject:
	default:
		return fmt.Errorf("unsupported backend %q", spec.Backend)
	}
	switch spec.CrashRecovery {
	case "", CrashRecoveryDiscard, CrashRecoveryReuse:
		return nil
	default:
		return fmt.Errorf("unsupported crashRecovery policy %q", spec.CrashRecovery)
	}
}

func (spec CacheClassSpec) validateQuota() error {
	if spec.MaxBytes.Sign() < 0 || spec.Quota.DefaultMaxBytes.Sign() < 0 {
		return fmt.Errorf("cache size limits must not be negative")
	}
	if spec.Quota.Enabled && spec.Backend != BackendXFSProject {
		return fmt.Errorf("quota requires the xfs-project backend")
	}
	if spec.Backend == BackendXFSProject && !spec.Quota.Enabled {
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
	return nil
}

func (spec CacheClassSpec) validatePressure() error {
	pressure := spec.Pressure
	for _, threshold := range []struct {
		name  string
		value int32
	}{
		{name: "highFreePercent", value: pressure.HighFreePercent},
		{name: "lowFreePercent", value: pressure.LowFreePercent},
		{name: "highInodeFreePercent", value: pressure.HighInodeFreePercent},
		{name: "lowInodeFreePercent", value: pressure.LowInodeFreePercent},
	} {
		if threshold.value < 0 || threshold.value > 100 {
			return fmt.Errorf("%s must be between 0 and 100", threshold.name)
		}
	}
	if err := validateWatermarks("FreeBytes", pressure.HighFreePercent, pressure.LowFreePercent); err != nil {
		return err
	}
	return validateWatermarks("FreeInodes", pressure.HighInodeFreePercent, pressure.LowInodeFreePercent)
}

func validateWatermarks(name string, high, low int32) error {
	if (high == 0) != (low == 0) {
		return fmt.Errorf("high and low %s percentages must be configured together", name)
	}
	if high > 0 && high <= low {
		return fmt.Errorf("high %s percentage must be greater than the low percentage", name)
	}
	return nil
}
