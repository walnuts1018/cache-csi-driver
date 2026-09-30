package nodehealth

import (
	"fmt"
	"net/http"
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

type Snapshot struct {
	Phase     Phase
	Reason    string
	Evict     bool
	ChangedAt time.Time
}

func (snapshot Snapshot) Schedulable() bool {
	return snapshot.Phase == PhaseReady || snapshot.Phase == PhaseDegraded
}

type Tracker struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

func NewTracker() *Tracker {
	return &Tracker{snapshot: Snapshot{Phase: PhaseStarting, Reason: "DriverStarting", ChangedAt: time.Now()}}
}

func (tracker *Tracker) Set(phase Phase, reason string, evict bool) {
	if reason == "" {
		reason = string(phase)
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.snapshot.Phase == phase && tracker.snapshot.Reason == reason && tracker.snapshot.Evict == evict {
		return
	}
	tracker.snapshot = Snapshot{Phase: phase, Reason: reason, Evict: evict, ChangedAt: time.Now()}
}

func (tracker *Tracker) Current() Snapshot {
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	return tracker.snapshot
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
