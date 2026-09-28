package cache

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const firstLeaseID = "first"

func TestAcquireDiscardsDirtyGenerationBeforeExposure(t *testing.T) {
	t.Parallel()
	store, err := NewStore(t.TempDir(), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	identity := stableIdentity("test-cache")
	policy := Policy{CrashRecoveryReuse: false, Retention: time.Hour}
	firstPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: firstLeaseID, Target: filepath.Join(t.TempDir(), firstLeaseID)},
		Policy:   policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(firstPath, "stale"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	meta, err := store.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Policy.Retention != policy.Retention {
		t.Fatalf("persisted retention = %s, want %s", meta.Policy.Retention, policy.Retention)
	}
	meta.Leases = nil
	meta.Dirty = true
	if err := store.writeMetadata(filepath.Join(store.Root(), identity), meta); err != nil {
		t.Fatal(err)
	}

	nextPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "second", Target: filepath.Join(t.TempDir(), "second")},
		Policy:   policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if nextPath == firstPath {
		t.Fatal("dirty generation was reused")
	}
	if _, err := os.Stat(filepath.Join(firstPath, "stale")); !os.IsNotExist(err) {
		t.Fatalf("old generation path remains available: %v", err)
	}
	if info, err := os.Stat(nextPath); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o700 {
		t.Fatalf("generation mode before exposure = %o, want 700", info.Mode().Perm())
	}
	if _, err := store.Expose(identity); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(nextPath); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o777 {
		t.Fatalf("generation mode after exposure = %o, want 777", info.Mode().Perm())
	}
}

func TestAcquireRejectsQuotaChangeWhileGenerationIsActive(t *testing.T) {
	t.Parallel()
	store, err := NewStore(t.TempDir(), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	identity := stableIdentity("quota-cache")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: firstLeaseID, Target: filepath.Join(t.TempDir(), firstLeaseID)},
		Policy:   Policy{QuotaEnabled: true, MaxBytes: 1024},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "second", Target: filepath.Join(t.TempDir(), "second")},
		Policy:   Policy{QuotaEnabled: true, MaxBytes: 2048},
	}); !errors.Is(err, ErrQuotaPolicyConflict) {
		t.Fatal("active generation accepted a different quota limit")
	}
}

func TestQuotaModeChangeCreatesFreshGenerationAndReservesOldProjectID(t *testing.T) {
	t.Parallel()
	store, err := NewStore(t.TempDir(), StoreOptions{ProjectIDStart: 32000, ProjectIDCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identity := stableIdentity("toggle-quota")
	quotaPolicy := Policy{QuotaEnabled: true, MaxBytes: 1024}
	firstTarget := filepath.Join(t.TempDir(), "quota")
	oldPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "quota", Target: firstTarget},
		Policy:   quotaPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(identity, quotaPolicy.MaxBytes); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkProjectAssigned(identity); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkQuotaApplied(identity, quotaPolicy.MaxBytes); err != nil {
		t.Fatal(err)
	}
	if err := store.Release("quota", firstTarget); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "release")
	newPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "directory", Target: target},
		Policy:   Policy{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if newPath == oldPath {
		t.Fatal("quota mode change reused the old generation")
	}
	meta, err := store.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.ProjectID != 0 || meta.ProjectAssigned || meta.QuotaBytes != 0 {
		t.Fatalf("quota state was not reset: %+v", meta)
	}
	otherIdentity := stableIdentity("other-quota")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: otherIdentity,
		Lease:    Lease{ID: "other", Target: filepath.Join(t.TempDir(), "other")},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(otherIdentity, 1024); err == nil {
		t.Fatal("project ID was reused before its old generation was removed")
	}
	if err := store.CleanupTrash(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(otherIdentity, 1024); err != nil {
		t.Fatal(err)
	}
}

func TestValidatePressureWatermarks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pressure PressureConfig
		wantErr  bool
	}{
		{
			name: "configured watermarks",
			pressure: PressureConfig{
				HighFreePercent:      25,
				LowFreePercent:       20,
				HighInodeFreePercent: 15,
				LowInodeFreePercent:  10,
			},
		},
		{name: "disabled watermarks"},
		{
			name: "unpaired watermarks",
			pressure: PressureConfig{
				HighFreePercent: 25,
			},
			wantErr: true,
		},
		{
			name: "reversed watermarks",
			pressure: PressureConfig{
				HighFreePercent: 20,
				LowFreePercent:  20,
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validatePressure(test.pressure)
			if (err != nil) != test.wantErr {
				t.Fatalf("validatePressure() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestProjectIDAllocationStaysInsideConfiguredRange(t *testing.T) {
	t.Parallel()
	store, err := NewStore(t.TempDir(), StoreOptions{ProjectIDStart: 32000, ProjectIDCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identities := []string{stableIdentity("quota-one"), stableIdentity("quota-two")}
	leaseIDs := []string{"one", "two"}
	for index, identity := range identities {
		_, _, err := store.Acquire(AcquireOptions{
			Identity: identity,
			Lease:    Lease{ID: leaseIDs[index], Target: filepath.Join(t.TempDir(), "target")},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	projectID, _, _, err := store.QuotaState(identities[0], 1024)
	if err != nil {
		t.Fatal(err)
	}
	if projectID != 32000 {
		t.Fatalf("project ID = %d, want 32000", projectID)
	}
	if _, _, _, err := store.QuotaState(identities[1], 1024); err == nil {
		t.Fatal("allocation exceeded the configured project ID range")
	}
}

func TestMetadataReadFailuresStopCollectionEvictionAndQuotaAllocation(t *testing.T) {
	t.Parallel()
	store, err := NewStore(t.TempDir(), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identity := stableIdentity("healthy-cache")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "healthy", Target: filepath.Join(t.TempDir(), "healthy")},
	}); err != nil {
		t.Fatal(err)
	}

	brokenIdentity := stableIdentity("broken-metadata")
	brokenEntry := filepath.Join(store.Root(), brokenIdentity)
	if err := os.MkdirAll(brokenEntry, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brokenEntry, metadataName), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Collect(time.Now()); err == nil {
		t.Fatal("collection ignored unreadable cache metadata")
	}
	if _, err := store.PressureVictims(); err == nil {
		t.Fatal("pressure eviction ignored unreadable cache metadata")
	}
	if _, _, _, _, _, err := store.LeaseDetails("unknown"); err == nil {
		t.Fatal("lease lookup ignored unreadable cache metadata")
	}

	if _, _, _, err := store.QuotaState(identity, 1024); err == nil {
		t.Fatal("project ID allocation ignored unreadable cache metadata")
	}
}

func TestPressureRetiresLeasedGenerationUntilLastRelease(t *testing.T) {
	t.Parallel()
	store, identity, policy, target, oldPath := pressureLease(t, "pressure-cache", Policy{EvictRunning: true}, 0)
	victims, err := store.PressureVictims()
	if err != nil {
		t.Fatal(err)
	}
	if len(victims) != 1 || victims[0].ID != firstLeaseID {
		t.Fatalf("pressure victims = %+v, want the active lease", victims)
	}
	_, _, retiredPath, _, found, err := store.LeaseDetails(firstLeaseID)
	if err != nil || !found || retiredPath != oldPath {
		t.Fatalf("retired lease resolves to %q, found %v, error %v; want %q", retiredPath, found, err, oldPath)
	}

	meta, err := store.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	activeGeneration := meta.Generation
	retries, err := store.PressureVictims()
	if err != nil {
		t.Fatal(err)
	}
	if len(retries) != 1 || retries[0].ID != firstLeaseID {
		t.Fatalf("pressure retry victims = %+v, want the retired lease", retries)
	}
	meta, err = store.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Generation != activeGeneration {
		t.Fatal("pressure rotated an empty active generation repeatedly")
	}
	newPath, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: Lease{ID: "replacement", Target: filepath.Join(t.TempDir(), "replacement")}, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if newPath == oldPath {
		t.Fatal("replacement lease reused the retired generation")
	}
	if _, _, source, _, found, err := store.LeaseDetails(firstLeaseID); err != nil || !found || source != oldPath {
		t.Fatalf("old lease stopped resolving before unpublish: source %q, found %v, error %v", source, found, err)
	}

	if err := store.Release(firstLeaseID, target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released retired generation still exists at its original path: %v", err)
	}
	if _, _, source, _, found, err := store.LeaseDetails("replacement"); err != nil || !found || source != newPath {
		t.Fatalf("active replacement lease changed path: source %q, found %v, error %v", source, found, err)
	}
}

func TestRetiredGenerationReservesProjectIDUntilDeleted(t *testing.T) {
	t.Parallel()
	store, identity, policy, target, _ := pressureLease(t, "pressure-quota-cache", Policy{EvictRunning: true, QuotaEnabled: true, MaxBytes: 1024}, 2)
	oldProjectID, _, _, err := store.QuotaState(identity, policy.MaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PressureVictims(); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(firstLeaseID, target); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(identity, policy.MaxBytes); err != nil {
		t.Fatal(err)
	}
	meta, err := store.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.ProjectID == oldProjectID {
		t.Fatal("replacement generation reused the retired generation's project ID")
	}
	otherIdentity := stableIdentity("pressure-other-cache")
	if _, _, err := store.Acquire(AcquireOptions{Identity: otherIdentity, Lease: Lease{ID: "other", Target: filepath.Join(t.TempDir(), "other")}}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(otherIdentity, 1024); err == nil {
		t.Fatal("detached generation's project ID was reused before physical deletion")
	}
	if err := store.CleanupTrash(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(otherIdentity, 1024); err != nil {
		t.Fatalf("project ID was not released after retired generation deletion: %v", err)
	}
}

func pressureLease(t *testing.T, name string, policy Policy, projectIDCount uint32) (*Store, string, Policy, string, string) {
	t.Helper()
	if projectIDCount == 0 {
		projectIDCount = 1
	}
	store, err := NewStore(t.TempDir(), StoreOptions{
		Pressure:       PressureConfig{HighFreePercent: 100, LowFreePercent: 99},
		ProjectIDStart: 32000,
		ProjectIDCount: projectIDCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identity := stableIdentity(name)
	target := filepath.Join(t.TempDir(), firstLeaseID)
	oldPath, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: Lease{ID: firstLeaseID, Target: target}, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	store.pressureActive = true
	return store, identity, policy, target, oldPath
}
