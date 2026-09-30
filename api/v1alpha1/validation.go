package v1alpha1

import (
	"fmt"
	"strings"
)

func (spec CacheClassSpec) Validate() error {
	if err := spec.Storage.validate(); err != nil {
		return err
	}
	if err := spec.validateCrashRecovery(); err != nil {
		return err
	}
	if err := spec.validateSharingPolicy(); err != nil {
		return err
	}
	if err := spec.validateScope(); err != nil {
		return err
	}
	if err := spec.validatePressurePolicy(); err != nil {
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

func (spec CacheClassSpec) validatePressurePolicy() error {
	switch spec.PressurePolicy {
	case "", PressurePolicyUnused, PressurePolicyEvict, PressurePolicyForce:
		return nil
	default:
		return fmt.Errorf("unsupported pressurePolicy %q", spec.PressurePolicy)
	}
}

func (spec CacheClassSpec) validateScope() error {
	switch spec.Scope {
	case "", ScopeServiceAccount, ScopeNamespace:
		return nil
	default:
		return fmt.Errorf("unsupported scope %q", spec.Scope)
	}
}

func (policy StoragePolicy) validate() error {
	if policy.MaxBytes.Sign() < 0 || policy.DefaultMaxBytes.Sign() < 0 {
		return fmt.Errorf("cache size limits must not be negative")
	}
	switch policy.Backend {
	case "", BackendDirectory, BackendXFSProject:
	default:
		return fmt.Errorf("unsupported backend %q", policy.Backend)
	}
	switch policy.Backend {
	case "", BackendDirectory:
		if !policy.MaxBytes.IsZero() || !policy.DefaultMaxBytes.IsZero() {
			return fmt.Errorf("directory backend does not support quota limits")
		}
	case BackendXFSProject:
		if policy.MaxBytes.IsZero() && policy.DefaultMaxBytes.IsZero() {
			return fmt.Errorf("xfs-project backend requires maxBytes or defaultMaxBytes")
		}
		if !policy.MaxBytes.IsZero() && !policy.DefaultMaxBytes.IsZero() && policy.DefaultMaxBytes.Cmp(policy.MaxBytes) > 0 {
			return fmt.Errorf("defaultMaxBytes must not exceed maxBytes")
		}
	}
	return nil
}

func (spec CacheClassSpec) validateCrashRecovery() error {
	switch spec.CrashRecovery {
	case "", CrashRecoveryDiscard, CrashRecoveryReuse:
		return nil
	default:
		return fmt.Errorf("unsupported crashRecovery policy %q", spec.CrashRecovery)
	}
}

func (spec CacheClassSpec) validateSharingPolicy() error {
	switch spec.SharingPolicy {
	case "", SharingPolicyShared, SharingPolicyExclusive:
		return nil
	default:
		return fmt.Errorf("unsupported sharingPolicy %q", spec.SharingPolicy)
	}
}
