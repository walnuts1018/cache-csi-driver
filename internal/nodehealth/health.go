package nodehealth

import (
	"cmp"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"
)

type Phase string

const (
	PhaseStarting    Phase = "Starting"
	PhaseRecovering  Phase = "Recovering"
	PhaseReady       Phase = "Ready"
	PhaseDegraded    Phase = "Degraded"
	PhaseUnavailable Phase = "Unavailable"
)

type Subsystem string

const (
	SubsystemStartup          Subsystem = "startup"
	SubsystemRecovery         Subsystem = "recovery"
	SubsystemStore            Subsystem = "store"
	SubsystemStoreOperations  Subsystem = "store-operations"
	SubsystemMount            Subsystem = "mount"
	SubsystemFilesystem       Subsystem = "filesystem"
	SubsystemQuota            Subsystem = "quota"
	SubsystemProjectRegistry  Subsystem = "project-registry"
	SubsystemPressure         Subsystem = "pressure"
	SubsystemGarbageCollector Subsystem = "garbage-collector"
	SubsystemDegradedRecovery Subsystem = "degraded-recovery"
	SubsystemLegacyPrefix               = "legacy/"
)

type Condition struct {
	Subsystem Subsystem
	Phase     Phase
	Reason    string
	Evict     bool
	ChangedAt time.Time
}

type Snapshot struct {
	Subsystem Subsystem
	Phase     Phase
	Reason    string
	Evict     bool
	ChangedAt time.Time
}

func (snapshot Snapshot) Schedulable() bool {
	return snapshot.Phase == PhaseReady || snapshot.Phase == PhaseDegraded
}

type Tracker struct {
	mu         sync.RWMutex
	conditions map[Subsystem]Condition
	snapshot   Snapshot
}

func NewTracker() *Tracker {
	now := time.Now()
	startup := Condition{Subsystem: SubsystemStartup, Phase: PhaseStarting, Reason: "DriverStarting", ChangedAt: now}
	conditions := make(map[Subsystem]Condition)
	conditions[SubsystemStartup] = startup
	return &Tracker{
		conditions: conditions,
		snapshot: Snapshot{
			Subsystem: startup.Subsystem,
			Phase:     startup.Phase,
			Reason:    startup.Reason,
			ChangedAt: now,
		},
	}
}

func (tracker *Tracker) Set(phase Phase, reason string, evict bool) {
	if reason == "" {
		reason = string(phase)
	}
	subsystem := Subsystem(SubsystemLegacyPrefix + reason)
	if phase == PhaseReady {
		tracker.ClearCondition(subsystem)
		tracker.ClearCondition(SubsystemStartup)
		return
	}
	if phase != PhaseStarting {
		tracker.ClearCondition(SubsystemStartup)
	}
	tracker.SetCondition(subsystem, phase, reason, evict)
}

func (tracker *Tracker) SetCondition(subsystem Subsystem, phase Phase, reason string, evict bool) {
	if phase == PhaseReady {
		tracker.ClearCondition(subsystem)
		return
	}
	if subsystem == "" {
		subsystem = "unspecified"
	}
	if reason == "" {
		reason = string(phase)
	}
	condition := Condition{Subsystem: subsystem, Phase: phase, Reason: reason, Evict: evict, ChangedAt: time.Now()}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if current, exists := tracker.conditions[subsystem]; exists && current.Phase == PhaseUnavailable && phase == PhaseUnavailable && current.Evict {
		// Eviction要求はsubsystemがUnavailableから回復するまで維持します。
		evict = true
	}
	if current, exists := tracker.conditions[subsystem]; exists && current.Phase == phase && current.Reason == reason && current.Evict == evict {
		return
	}
	tracker.conditions[subsystem] = condition
	tracker.updateSnapshotLocked()
}

func (tracker *Tracker) ClearCondition(subsystem Subsystem) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if _, exists := tracker.conditions[subsystem]; !exists {
		return
	}
	delete(tracker.conditions, subsystem)
	tracker.updateSnapshotLocked()
}

func (tracker *Tracker) Conditions() []Condition {
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	conditions := make([]Condition, 0, len(tracker.conditions))
	for _, condition := range tracker.conditions {
		conditions = append(conditions, condition)
	}
	slices.SortFunc(conditions, func(left, right Condition) int {
		return cmp.Compare(string(left.Subsystem), string(right.Subsystem))
	})
	return conditions
}

func (tracker *Tracker) Current() Snapshot {
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	return tracker.snapshot
}

func (tracker *Tracker) updateSnapshotLocked() {
	next := Snapshot{Phase: PhaseReady, Reason: "CacheReady"}
	for _, condition := range tracker.conditions {
		if !preferredCondition(condition, next) {
			continue
		}
		next = Snapshot{
			Subsystem: condition.Subsystem,
			Phase:     condition.Phase,
			Reason:    condition.Reason,
			Evict:     condition.Evict,
		}
	}
	if tracker.snapshot.Subsystem == next.Subsystem && tracker.snapshot.Phase == next.Phase && tracker.snapshot.Reason == next.Reason && tracker.snapshot.Evict == next.Evict {
		next.ChangedAt = tracker.snapshot.ChangedAt
	} else {
		next.ChangedAt = time.Now()
	}
	tracker.snapshot = next
}

func preferredCondition(candidate Condition, current Snapshot) bool {
	candidatePriority := phasePriority(candidate.Phase)
	currentPriority := phasePriority(current.Phase)
	if candidatePriority != currentPriority {
		return candidatePriority > currentPriority
	}
	if candidate.Evict != current.Evict {
		return candidate.Evict
	}
	return string(candidate.Subsystem) < string(current.Subsystem)
}

func phasePriority(phase Phase) int {
	switch phase {
	case PhaseUnavailable:
		return 5
	case PhaseStarting:
		return 4
	case PhaseRecovering:
		return 3
	case PhaseDegraded:
		return 2
	case PhaseReady:
		return 1
	default:
		return 0
	}
}

func (tracker *Tracker) ReadinessHandler(w http.ResponseWriter, _ *http.Request) {
	snapshot := tracker.Current()
	if snapshot.Schedulable() {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, "cache node is %s: %s\n", snapshot.Phase, snapshot.Reason)
}
