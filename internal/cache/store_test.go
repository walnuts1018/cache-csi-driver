package cache

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

const firstLeaseID = "first"
const otherLeaseID = "other"

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
		Lease:    Lease{ID: otherLeaseID, Target: filepath.Join(t.TempDir(), otherLeaseID)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(otherIdentity, 1024); err == nil {
		t.Fatal("project ID was reused before its old generation was removed")
	}
	if err := store.CleanupTrash(t.Context()); err != nil {
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

func TestMetadataDamageIsIsolatedAndProjectReservationSurvivesRestart(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{ProjectIDStart: 32000, ProjectIDCount: 3})
	if err != nil {
		t.Fatal(err)
	}
	healthyIdentity := stableIdentity("healthy-cache")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: healthyIdentity,
		Lease:    Lease{ID: "healthy", Target: filepath.Join(t.TempDir(), "healthy")},
	}); err != nil {
		t.Fatal(err)
	}

	brokenIdentity := stableIdentity("broken-metadata")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: brokenIdentity,
		Lease:    Lease{ID: "broken", Target: filepath.Join(t.TempDir(), "broken")},
	}); err != nil {
		t.Fatal(err)
	}
	brokenProjectID, _, _, err := store.QuotaState(brokenIdentity, 1024)
	if err != nil {
		t.Fatal(err)
	}
	brokenEntry := filepath.Join(store.Root(), brokenIdentity)
	if err := os.WriteFile(filepath.Join(brokenEntry, metadataName), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, err := store.LeaseDetails("broken"); err == nil {
		t.Fatal("lease lookup accepted unreadable cache metadata")
	}
	if err := store.Collect(time.Now()); err != nil {
		t.Fatalf("collection stopped on an unrelated broken object: %v", err)
	}
	if _, err := store.PressureVictims(); err != nil {
		t.Fatalf("pressure scan stopped on an unrelated broken object: %v", err)
	}
	if _, _, _, _, found, err := store.LeaseDetails("healthy"); err != nil || !found {
		t.Fatalf("healthy lease lookup was affected by broken metadata: found=%v error=%v", found, err)
	}
	otherIdentity := stableIdentity("other-quota-after-corruption")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: otherIdentity,
		Lease:    Lease{ID: otherLeaseID, Target: filepath.Join(t.TempDir(), otherLeaseID)},
	}); err != nil {
		t.Fatal(err)
	}
	otherProjectID, _, _, err := store.QuotaState(otherIdentity, 1024)
	if err != nil {
		t.Fatalf("quota allocation stopped on broken metadata: %v", err)
	}
	if otherProjectID == brokenProjectID {
		t.Fatal("project ID from the damaged object was reused")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStore(root, StoreOptions{ProjectIDStart: 32000, ProjectIDCount: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, _, _, _, found, err := store.LeaseDetails("healthy"); err != nil || !found {
		t.Fatalf("index rebuild failed for healthy lease: found=%v error=%v", found, err)
	}
	thirdIdentity := stableIdentity("third-quota-after-restart")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: thirdIdentity,
		Lease:    Lease{ID: "third", Target: filepath.Join(t.TempDir(), "third")},
	}); err != nil {
		t.Fatal(err)
	}
	thirdProjectID, _, _, err := store.QuotaState(thirdIdentity, 1024)
	if err != nil {
		t.Fatalf("quota allocation failed after index rebuild: %v", err)
	}
	if thirdProjectID == brokenProjectID || thirdProjectID == otherProjectID {
		t.Fatalf("project ID %d collides with an existing reservation", thirdProjectID)
	}
}

func TestRecoverLeasesQuarantinesDamagedObjectOnlyWhenUnmounted(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name           string
		mounted        bool
		verifyErr      bool
		wantQuarantine bool
	}{
		{name: "unmounted", wantQuarantine: true},
		{name: "mounted", mounted: true},
		{name: "unknown mount state", verifyErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			store, err := NewStore(root, StoreOptions{ProjectIDStart: 32000, ProjectIDCount: 1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			identity := stableIdentity("damaged-" + test.name)
			generationPath, _, err := store.Acquire(AcquireOptions{
				Identity: identity,
				Lease:    Lease{ID: "damaged", Target: filepath.Join(t.TempDir(), "target")},
			})
			if err != nil {
				t.Fatal(err)
			}
			projectID, _, _, err := store.QuotaState(identity, 1024)
			if err != nil {
				t.Fatal(err)
			}
			entry := filepath.Join(root, identity)
			if err := os.WriteFile(filepath.Join(entry, metadataName), []byte("{broken"), 0o600); err != nil {
				t.Fatal(err)
			}
			calls := 0
			err = store.RecoverLeases(func(source string, lease Lease, policy Policy) (bool, error) {
				calls++
				if source != generationPath || lease.ID != "" || policy != (Policy{}) {
					t.Fatalf("damaged object verifier input = (%q, %+v, %+v)", source, lease, policy)
				}
				if test.verifyErr {
					return false, errors.New("mount inspection failed")
				}
				return test.mounted, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("mount verifier calls = %d, want 1", calls)
			}
			if test.wantQuarantine {
				if _, err := os.Stat(entry); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unmounted damaged object was not detached: %v", err)
				}
				otherIdentity := stableIdentity("quota-during-quarantine")
				if _, _, err := store.Acquire(AcquireOptions{Identity: otherIdentity, Lease: Lease{ID: otherLeaseID, Target: filepath.Join(t.TempDir(), otherLeaseID)}}); err != nil {
					t.Fatal(err)
				}
				if _, _, _, err := store.QuotaState(otherIdentity, 1024); err == nil {
					t.Fatal("project ID was reused before trash deletion")
				}
				if err := store.CleanupTrash(t.Context()); err != nil {
					t.Fatal(err)
				}
				newProjectID, _, _, err := store.QuotaState(otherIdentity, 1024)
				if err != nil {
					t.Fatalf("project ID was not released after physical deletion: %v", err)
				}
				if newProjectID != projectID {
					t.Fatalf("reallocated project ID = %d, want released ID %d", newProjectID, projectID)
				}
			} else {
				if _, err := os.Stat(entry); err != nil {
					t.Fatalf("uncertain or active damaged object was removed: %v", err)
				}
				if err := store.MetadataError(); !errors.Is(err, ErrDegradedMetadata) {
					t.Fatalf("metadata health error = %v, want degraded metadata", err)
				}
				otherIdentity := stableIdentity("quota-while-degraded")
				if _, _, err := store.Acquire(AcquireOptions{Identity: otherIdentity, Lease: Lease{ID: otherLeaseID, Target: filepath.Join(t.TempDir(), otherLeaseID)}}); err != nil {
					t.Fatal(err)
				}
				if _, _, _, err := store.QuotaState(otherIdentity, 1024); err == nil {
					t.Fatal("project ID was reused while damaged object may still be mounted")
				}
			}
		})
	}
}

func TestExclusivePolicyAppliesAcrossRetiredGeneration(t *testing.T) {
	t.Parallel()
	policy := Policy{SharingPolicy: SharingPolicyExclusive, EvictRunning: true}
	store, identity, _, target, oldPath := pressureLease(t, "exclusive-cache", policy, 1)
	victims, err := store.PressureVictims()
	if err != nil {
		t.Fatal(err)
	}
	if len(victims) != 1 || victims[0].ID != firstLeaseID {
		t.Fatalf("pressure victims = %+v, want the active exclusive lease", victims)
	}
	retryPath, created, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: firstLeaseID, Target: target},
		Policy:   policy,
	})
	if err != nil || created || retryPath != oldPath {
		t.Fatalf("same lease retry = (%q, %v, %v), want original retired generation", retryPath, created, err)
	}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "second-exclusive", Target: filepath.Join(t.TempDir(), "second")},
		Policy:   policy,
	}); !errors.Is(err, ErrExclusivePolicyConflict) {
		t.Fatalf("second active lease error = %v, want exclusive conflict", err)
	}
	if err := store.Release(firstLeaseID, target); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "second-exclusive", Target: filepath.Join(t.TempDir(), "second")},
		Policy:   policy,
	}); err != nil {
		t.Fatalf("new lease after old generation release: %v", err)
	}
}

func TestRecoveryVerifierReceivesRetiredGenerationSourceAndPolicy(t *testing.T) {
	t.Parallel()
	policy := Policy{NoExec: true, SharingPolicy: SharingPolicyExclusive, EvictRunning: true}
	store, identity, _, target, oldPath := pressureLease(t, "recovery-retired-cache", policy, 1)
	if _, err := store.PressureVictims(); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverLeases(func(source string, lease Lease, recoveredPolicy Policy) (bool, error) {
		if source != oldPath {
			t.Errorf("recovered source = %q, want %q", source, oldPath)
		}
		if lease.ID != firstLeaseID || lease.Target != target || lease.Generation == "" {
			t.Errorf("recovered lease = %+v, want retired lease %q at %q", lease, firstLeaseID, target)
		}
		if recoveredPolicy != policy {
			t.Errorf("recovered policy = %+v, want %+v", recoveredPolicy, policy)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, source, recoveredPolicy, found, err := store.LeaseDetails(firstLeaseID); err != nil || !found || source != oldPath || recoveredPolicy != policy {
		t.Fatalf("retired lease after recovery = (%q, %+v, %v, %v), want preserved lease", source, recoveredPolicy, found, err)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), identity)); err != nil {
		t.Fatal(err)
	}
}

func TestPressureReclaimPhysicallyDeletesMoreThanOneBatch(t *testing.T) {
	t.Parallel()
	store, err := NewStore(t.TempDir(), StoreOptions{
		Pressure: PressureConfig{HighFreePercent: 100, LowFreePercent: 99},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identities := make([]string, 20)
	for index := range identities {
		identity := stableIdentity("pressure-unused-" + strconv.Itoa(index))
		identities[index] = identity
		target := filepath.Join(t.TempDir(), "target-"+strconv.Itoa(index))
		if _, _, err := store.Acquire(AcquireOptions{
			Identity: identity,
			Lease:    Lease{ID: "lease-" + strconv.Itoa(index), Target: target},
			Policy:   Policy{Retention: time.Hour},
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.Release("lease-"+strconv.Itoa(index), target); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().Add(2 * time.Hour)
	if err := store.Collect(now); err != nil {
		t.Fatal(err)
	}
	if err := store.Collect(now); err != nil {
		t.Fatal(err)
	}
	trash, err := store.readDir(filepath.Join(store.Root(), trashDirectoryName))
	if err != nil {
		t.Fatal(err)
	}
	if len(trash) != len(identities) {
		t.Fatalf("trash entries before pressure reclaim = %d, want %d", len(trash), len(identities))
	}
	store.pressureActive = true
	if err := store.ReclaimPressure(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, identity := range identities {
		if _, err := os.Stat(filepath.Join(store.Root(), identity)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unused object %q remains after pressure reclaim: %v", identity, err)
		}
	}
	trash, err = store.readDir(filepath.Join(store.Root(), trashDirectoryName))
	if err != nil {
		t.Fatal(err)
	}
	if len(trash) != 0 {
		t.Fatalf("trash entries = %d, want 0 after pressure reclaim", len(trash))
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
	if _, _, err := store.Acquire(AcquireOptions{Identity: otherIdentity, Lease: Lease{ID: otherLeaseID, Target: filepath.Join(t.TempDir(), otherLeaseID)}}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(otherIdentity, 1024); err == nil {
		t.Fatal("detached generation's project ID was reused before physical deletion")
	}
	if err := store.CleanupTrash(t.Context()); err != nil {
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
