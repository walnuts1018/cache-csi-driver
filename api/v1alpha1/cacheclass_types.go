package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type CacheBackend string
type CrashRecoveryPolicy string

const (
	BackendDirectory  CacheBackend = "directory"
	BackendXFSProject CacheBackend = "xfs-project"
	CrashRecoveryDiscard CrashRecoveryPolicy = "discard"
	CrashRecoveryReuse   CrashRecoveryPolicy = "reuse"
)

type CacheClassSpec struct {
	Backend       CacheBackend        `json:"backend,omitempty"`
	MaxBytes      resource.Quantity   `json:"maxBytes,omitzero"`
	Retention     metav1.Duration     `json:"retention,omitzero"`
	SchemaVersion string              `json:"schemaVersion,omitempty"`
	CrashRecovery CrashRecoveryPolicy `json:"crashRecovery,omitempty"`
	Pressure      PressurePolicy      `json:"pressure,omitzero"`
	Quota         QuotaPolicy         `json:"quota,omitzero"`
	EvictRunning  bool                `json:"evictRunning,omitempty"`
	NoExec        bool                `json:"noExec,omitempty"`
}

type PressurePolicy struct {
	HighFreePercent      int32 `json:"highFreePercent,omitempty"`
	LowFreePercent       int32 `json:"lowFreePercent,omitempty"`
	HighInodeFreePercent int32 `json:"highInodeFreePercent,omitempty"`
	LowInodeFreePercent  int32 `json:"lowInodeFreePercent,omitempty"`
}

type QuotaPolicy struct {
	Enabled         bool              `json:"enabled,omitempty"`
	DefaultMaxBytes resource.Quantity `json:"defaultMaxBytes,omitzero"`
}

type CacheClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitzero"`
	Spec              CacheClassSpec `json:"spec,omitzero"`
}

type CacheClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []CacheClass `json:"items"`
}
