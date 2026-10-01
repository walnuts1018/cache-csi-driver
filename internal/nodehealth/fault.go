package nodehealth

type FailureScope uint8

const (
	FailureScopeRequest FailureScope = iota + 1
	FailureScopeObject
	FailureScopeNode
)

type NodeImpact uint8

const (
	NodeImpactNone NodeImpact = iota
	NodeImpactNoSchedule
	NodeImpactEvict
)

type Retryability uint8

const (
	Retryable Retryability = iota + 1
	PermanentUntilProbe
)

type FailureReason string

const (
	ReasonFilesystemUnavailable FailureReason = "CacheFilesystemUnavailable"
	ReasonFilesystemPressure    FailureReason = "CacheFilesystemPressure"
	ReasonMountUnavailable      FailureReason = "CacheMountUnavailable"
	ReasonQuotaUnavailable      FailureReason = "CacheQuotaUnavailable"
	ReasonProjectRegistry       FailureReason = "CacheProjectRegistryUnavailable"
	ReasonStoreInitialization   FailureReason = "CacheStoreInitializationFailed"
	ReasonStoreOperation        FailureReason = "CacheStoreOperationFailed"
	ReasonRequestMount          FailureReason = "CacheMountOperationFailed"
	ReasonMetadataRead          FailureReason = "CacheMetadataReadFailed"
	ReasonLeaseUpdate           FailureReason = "CacheLeaseUpdateFailed"
	ReasonLeaseRelease          FailureReason = "CacheLeaseReleaseFailed"
	ReasonQuarantine            FailureReason = "CacheQuarantineFailed"
	ReasonMountVerification     FailureReason = "CacheMountVerificationFailed"
	ReasonMountStateUnknown     FailureReason = "CacheMountStateUnknown"
	ReasonGenerationExpose      FailureReason = "CacheGenerationExposeFailed"
	ReasonLeaseRollback         FailureReason = "CacheLeaseRollbackFailed"
	ReasonLeaseCommit           FailureReason = "CacheLeaseCommitFailed"
)

type Failure struct {
	Scope        FailureScope
	Subsystem    Subsystem
	Reason       FailureReason
	Impact       NodeImpact
	Retryability Retryability
}

func (tracker *Tracker) RecordFailure(failure Failure) {
	if failure.Scope != FailureScopeNode || failure.Subsystem == "" || failure.Reason == "" {
		return
	}
	tracker.SetCondition(failure.Subsystem, PhaseUnavailable, string(failure.Reason), failure.Impact == NodeImpactEvict)
}
