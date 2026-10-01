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

func TestSnapshotSchedulability(t *testing.T) {
	t.Parallel()
	tests := []struct {
		phase Phase
		want  bool
	}{
		{phase: PhaseStarting},
		{phase: PhaseRecovering},
		{phase: PhaseReady, want: true},
		{phase: PhaseDegraded, want: true},
		{phase: PhaseUnavailable},
	}
	for _, test := range tests {
		t.Run(string(test.phase), func(t *testing.T) {
			t.Parallel()
			snapshot := Snapshot{Phase: test.phase}
			if got := snapshot.Schedulable(); got != test.want {
				t.Fatalf("Schedulable() = %t, want %t for phase %q", got, test.want, test.phase)
			}
		})
	}
}

func TestTrackerConditionUpdatePreservesChangedAt(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.SetCondition(SubsystemFilesystem, PhaseDegraded, "FilesystemRecovering", false)
	before := tracker.Conditions()
	snapshotBefore := tracker.Current()
	tracker.SetCondition(SubsystemFilesystem, PhaseDegraded, "FilesystemRecovering", false)
	after := tracker.Conditions()
	snapshotAfter := tracker.Current()
	if len(before) != len(after) {
		t.Fatalf("condition count changed from %d to %d", len(before), len(after))
	}
	for index := range before {
		if before[index] != after[index] {
			t.Fatalf("condition changed after identical update: before=%+v after=%+v", before[index], after[index])
		}
	}
	if snapshotBefore.ChangedAt != snapshotAfter.ChangedAt {
		t.Fatalf("snapshot ChangedAt changed from %s to %s after identical update", snapshotBefore.ChangedAt, snapshotAfter.ChangedAt)
	}
}

func TestTrackerClearingLastConditionRestoresReady(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.ClearCondition(SubsystemStartup)
	tracker.SetCondition(SubsystemFilesystem, PhaseUnavailable, "FilesystemUnavailable", false)
	tracker.ClearCondition(SubsystemFilesystem)

	snapshot := tracker.Current()
	if snapshot.Phase != PhaseReady || !snapshot.Schedulable() {
		t.Fatalf("Current() = %+v, want schedulable Ready after condition clear", snapshot)
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

func TestTrackerEvictIsIndependentOfPhase(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.ClearCondition(SubsystemStartup)
	tracker.SetCondition(SubsystemFilesystem, PhaseUnavailable, "FilesystemUnavailable", true)
	tracker.SetCondition(SubsystemFilesystem, PhaseDegraded, "FilesystemRecovering", false)

	snapshot := tracker.Current()
	if snapshot.Phase != PhaseDegraded || !snapshot.Schedulable() || !snapshot.Evict {
		t.Fatalf("Current() = %+v, want schedulable Degraded with Evict=true", snapshot)
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
