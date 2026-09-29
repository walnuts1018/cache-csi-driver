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
	if err := spec.validateSharingPolicy(); err != nil {
		return err
	}
	if err := spec.validateScope(); err != nil {
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

func (spec CacheClassSpec) validateScope() error {
	switch spec.Scope {
	case "", ScopeServiceAccount, ScopeNamespace:
		return nil
	default:
		return fmt.Errorf("unsupported scope %q", spec.Scope)
	}
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

func (spec CacheClassSpec) validateSharingPolicy() error {
	switch spec.SharingPolicy {
	case "", SharingPolicyShared, SharingPolicyExclusive:
		return nil
	default:
		return fmt.Errorf("unsupported sharingPolicy %q", spec.SharingPolicy)
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
