package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type CacheBackend string
type CrashRecoveryPolicy string
type SharingPolicy string
type CacheScope string
type PressurePolicy string

const (
	BackendDirectory       CacheBackend        = "directory"
	BackendXFSProject      CacheBackend        = "xfs-project"
	CrashRecoveryDiscard   CrashRecoveryPolicy = "discard"
	CrashRecoveryReuse     CrashRecoveryPolicy = "reuse"
	SharingPolicyShared    SharingPolicy       = "Shared"
	SharingPolicyExclusive SharingPolicy       = "Exclusive"
	ScopeServiceAccount    CacheScope          = "ServiceAccount"
	ScopeNamespace         CacheScope          = "Namespace"
	PressurePolicyUnused   PressurePolicy      = "UnusedOnly"
	PressurePolicyEvict    PressurePolicy      = "Evict"
	PressurePolicyForce    PressurePolicy      = "ForceDelete"
)

// CacheClassSpec defines storage and reuse behavior for node-local cache generations.
// +kubebuilder:validation:XValidation:rule="!has(self.quota) || !has(self.quota.enabled) || !self.quota.enabled || (has(self.backend) && self.backend == 'xfs-project')",message="quota requires the xfs-project backend"
// +kubebuilder:validation:XValidation:rule="!has(self.backend) || self.backend != 'xfs-project' || (has(self.quota) && has(self.quota.enabled) && self.quota.enabled)",message="xfs-project backend requires quota to be enabled"
// +kubebuilder:validation:XValidation:rule="!has(self.maxBytes) || !quantity(string(self.maxBytes)).isLessThan(quantity('0'))",message="maxBytes must not be negative"
// +kubebuilder:validation:XValidation:rule="!has(self.quota) || !has(self.quota.defaultMaxBytes) || !quantity(string(self.quota.defaultMaxBytes)).isLessThan(quantity('0'))",message="quota.defaultMaxBytes must not be negative"
// +kubebuilder:validation:XValidation:rule="!has(self.maxBytes) || !quantity(string(self.maxBytes)).isGreaterThan(quantity('0')) || (has(self.quota) && has(self.quota.enabled) && self.quota.enabled && has(self.backend) && self.backend == 'xfs-project')",message="maxBytes requires quota to be enabled with the xfs-project backend"
// +kubebuilder:validation:XValidation:rule="!has(self.quota) || !has(self.quota.defaultMaxBytes) || !quantity(string(self.quota.defaultMaxBytes)).isGreaterThan(quantity('0')) || (has(self.quota.enabled) && self.quota.enabled && has(self.backend) && self.backend == 'xfs-project')",message="quota.defaultMaxBytes requires quota to be enabled with the xfs-project backend"
// +kubebuilder:validation:XValidation:rule="!has(self.quota) || !has(self.quota.enabled) || !self.quota.enabled || ((has(self.maxBytes) && quantity(string(self.maxBytes)).isGreaterThan(quantity('0'))) || (has(self.quota.defaultMaxBytes) && quantity(string(self.quota.defaultMaxBytes)).isGreaterThan(quantity('0'))))",message="quota requires maxBytes or quota.defaultMaxBytes"
// +kubebuilder:validation:XValidation:rule="!has(self.maxBytes) || !quantity(string(self.maxBytes)).isGreaterThan(quantity('0')) || !has(self.quota) || !has(self.quota.defaultMaxBytes) || !quantity(string(self.quota.defaultMaxBytes)).isGreaterThan(quantity(string(self.maxBytes)))",message="quota.defaultMaxBytes must not exceed maxBytes"
type CacheClassSpec struct {
	// Storage backend used for cache generations.
	// +kubebuilder:default=directory
	// +kubebuilder:validation:Enum=directory;xfs-project
	Backend CacheBackend `json:"backend,omitempty"`
	// Maximum per-cache quota ceiling for volume-level maxBytes requests in this class. Requires quota.enabled and the xfs-project backend; requests above this limit are rejected.
	// +kubebuilder:validation:Type=string
	MaxBytes resource.Quantity `json:"maxBytes,omitzero"`
	// Maximum age of an unused cache before it becomes eligible for garbage collection.
	// +kubebuilder:default="72h"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="retention must not be negative"
	Retention metav1.Duration `json:"retention,omitzero"`
	// Application cache format version included in cache identity.
	// +kubebuilder:default="v1"
	// +kubebuilder:validation:XValidation:rule="!self.contains('/') && !self.contains('\\\\') && !self.contains('\\u0000')",message="schemaVersion contains an invalid character"
	SchemaVersion string `json:"schemaVersion,omitempty"`
	// Policy for generations left active after an unclean node shutdown.
	// +kubebuilder:default=discard
	// +kubebuilder:validation:Enum=discard;reuse
	CrashRecovery CrashRecoveryPolicy `json:"crashRecovery,omitempty"`
	// SharingPolicy describes concurrent access to a cache generation. Shared is safe only when the cache implementation supports concurrent multi-process access and all Pods using the same cacheKey coordinate writes. Pods must use compatible effective UID and GID values because existing file modes can prevent a Pod with different credentials from writing reused data.
	// +kubebuilder:default=Exclusive
	// +kubebuilder:validation:Enum=Shared;Exclusive
	SharingPolicy SharingPolicy `json:"sharingPolicy,omitempty"`
	// Scope defines the workload trust boundary included in cache identity. Namespace allows all workloads in a namespace to share identity; ServiceAccount isolates workloads by ServiceAccount UID.
	// +kubebuilder:default=ServiceAccount
	// +kubebuilder:validation:Enum=ServiceAccount;Namespace
	Scope CacheScope `json:"scope,omitempty"`
	// Quota configuration for cache generations.
	Quota QuotaPolicy `json:"quota,omitzero"`
	// Policy for active cache generations during filesystem pressure. ForceDelete requires the node's critical pressure watermark and explicit Pod delete RBAC.
	// +kubebuilder:default=UnusedOnly
	// +kubebuilder:validation:Enum=UnusedOnly;Evict;ForceDelete
	PressurePolicy PressurePolicy `json:"pressurePolicy,omitempty"`
	// Mounts cache generations with execution disabled.
	NoExec bool `json:"noExec,omitempty"`
}

type QuotaPolicy struct {
	// Enables XFS project quota enforcement for each cache identity. Requires the xfs-project backend and a per-cache limit in maxBytes or defaultMaxBytes.
	Enabled bool `json:"enabled,omitempty"`
	// Per-cache hard limit used when the CSI volume context omits maxBytes. If maxBytes is set, this default must not exceed the class ceiling.
	// +kubebuilder:validation:Type=string
	DefaultMaxBytes resource.Quantity `json:"defaultMaxBytes,omitzero"`
}

// CacheClass is the schema for cacheclasses.
// +kubebuilder:resource:scope=Cluster,shortName=cc
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=".spec.backend"
// +kubebuilder:printcolumn:name="MaxBytes",type=string,JSONPath=".spec.maxBytes"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
// +kubebuilder:object:root=true
type CacheClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitzero"`
	Spec              CacheClassSpec `json:"spec"`
}

// CacheClassList contains a list of CacheClass.
// +kubebuilder:object:root=true
type CacheClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []CacheClass `json:"items"`
}
