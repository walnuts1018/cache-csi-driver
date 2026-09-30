package v1alpha1

import (
	"fmt"
	"strings"
)

func (spec CacheClassSpec) Validate() error {
	if err := spec.Storage.validate(); err != nil {
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

func (policy StoragePolicy) validate() error {
	if policy.MaxBytes.Sign() < 0 || policy.DefaultMaxBytes.Sign() < 0 {
		return fmt.Errorf("cache size limits must not be negative")
	}
	if !policy.MaxBytes.IsZero() && !policy.DefaultMaxBytes.IsZero() && policy.DefaultMaxBytes.Cmp(policy.MaxBytes) > 0 {
		return fmt.Errorf("defaultMaxBytes must not exceed maxBytes")
	}
	return nil
}

func (spec CacheClassSpec) validateSharingPolicy() error {
	switch spec.SharingPolicy {
	case "", SharingPolicyShared, SharingPolicyExclusive:
		return nil
	default:
		return fmt.Errorf("unsupported sharingPolicy %q", spec.SharingPolicy)
	}
}
