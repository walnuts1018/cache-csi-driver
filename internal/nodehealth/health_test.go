package nodehealth

import "testing"

func TestTrackerConditionPriority(t *testing.T) {
	t.Parallel()
	type testCondition struct {
		subsystem Subsystem
		phase     Phase
	}
	tests := []struct {
		name     string
		phases   []testCondition
		want     Phase
		wantFrom Subsystem
	}{
		{
			name:     "unavailable precedes starting",
			phases:   []testCondition{{"z-starting", PhaseStarting}, {"a-unavailable", PhaseUnavailable}},
			want:     PhaseUnavailable,
			wantFrom: "a-unavailable",
		},
		{
			name:     "starting precedes recovering",
			phases:   []testCondition{{"z-recovering", PhaseRecovering}, {"a-starting", PhaseStarting}},
			want:     PhaseStarting,
			wantFrom: "a-starting",
		},
		{
			name:     "recovering precedes degraded",
			phases:   []testCondition{{"z-degraded", PhaseDegraded}, {"a-recovering", PhaseRecovering}},
			want:     PhaseRecovering,
			wantFrom: "a-recovering",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tracker := NewTracker()
			tracker.ClearCondition(SubsystemStartup)
			for _, condition := range test.phases {
				tracker.SetCondition(condition.subsystem, condition.phase, string(condition.phase), false)
			}

			snapshot := tracker.Current()
			if snapshot.Phase != test.want || snapshot.Subsystem != test.wantFrom {
				t.Fatalf("Current() = (%q, %q), want (%q, %q)", snapshot.Phase, snapshot.Subsystem, test.want, test.wantFrom)
			}
		})
	}
}

func TestTrackerEvictSticksUntilConditionCleared(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.ClearCondition(SubsystemStartup)
	tracker.SetCondition(SubsystemFilesystem, PhaseUnavailable, "FilesystemFailed", true)
	tracker.SetCondition(SubsystemFilesystem, PhaseDegraded, "FilesystemRecovering", false)
	tracker.SetCondition(SubsystemFilesystem, PhaseUnavailable, "FilesystemStillFailed", false)

	if got := tracker.Current(); !got.Evict {
		t.Fatalf("Current().Evict = false, want true while subsystem remains unavailable")
	}

	tracker.ClearCondition(SubsystemFilesystem)
	tracker.SetCondition(SubsystemFilesystem, PhaseUnavailable, "FilesystemFailedAgain", false)
	if got := tracker.Current(); got.Evict {
		t.Fatalf("Current().Evict = true after condition was cleared, want false")
	}
}

func TestTrackerClearConditionLeavesOtherSubsystems(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.ClearCondition(SubsystemStartup)
	tracker.SetCondition(SubsystemFilesystem, PhaseUnavailable, "FilesystemFailed", false)
	tracker.SetCondition(SubsystemQuota, PhaseDegraded, "QuotaDegraded", false)

	tracker.ClearCondition(SubsystemFilesystem)

	snapshot := tracker.Current()
	if snapshot.Phase != PhaseDegraded || snapshot.Subsystem != SubsystemQuota {
		t.Fatalf("Current() = (%q, %q), want (%q, %q)", snapshot.Phase, snapshot.Subsystem, PhaseDegraded, SubsystemQuota)
	}
	if got := len(tracker.Conditions()); got != 1 {
		t.Fatalf("len(Conditions()) = %d, want 1", got)
	}
}

func TestTrackerAggregatesUnavailableEvictRequests(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.ClearCondition(SubsystemStartup)
	tracker.SetCondition("a-filesystem", PhaseUnavailable, "FilesystemUnavailable", false)
	tracker.SetCondition("z-quota", PhaseUnavailable, "QuotaUnavailable", true)

	snapshot := tracker.Current()
	if snapshot.Subsystem != "a-filesystem" {
		t.Fatalf("Current().Subsystem = %q, want a-filesystem", snapshot.Subsystem)
	}
	if !snapshot.Evict {
		t.Fatalf("Current().Evict = false, want true when any unavailable subsystem requests eviction")
	}
}
