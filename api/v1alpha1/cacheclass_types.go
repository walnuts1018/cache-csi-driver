package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type CacheBackend string
type CrashRecoveryPolicy string

type CacheClassSpec struct {
	Backend       CacheBackend        `json:"backend,omitempty"`
	MaxBytes      resource.Quantity   `json:"maxBytes,omitempty"`
	Retention     metav1.Duration     `json:"retention,omitempty"`
	SchemaVersion string              `json:"schemaVersion,omitempty"`
	CrashRecovery CrashRecoveryPolicy `json:"crashRecovery,omitempty"`
	Pressure      PressurePolicy      `json:"pressure,omitempty"`
	Quota         QuotaPolicy         `json:"quota,omitempty"`
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
	DefaultMaxBytes resource.Quantity `json:"defaultMaxBytes,omitempty"`
}

type CacheClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              CacheClassSpec `json:"spec,omitempty"`
}

type CacheClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CacheClass `json:"items"`
}
