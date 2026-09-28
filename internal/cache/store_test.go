package cache

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
		Lease:    Lease{ID: "first", Target: filepath.Join(t.TempDir(), "first")},
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
		Lease:    Lease{ID: "first", Target: filepath.Join(t.TempDir(), "first")},
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
