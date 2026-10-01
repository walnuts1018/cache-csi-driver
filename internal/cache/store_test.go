package cache

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const firstLeaseID = "first"
const otherLeaseID = "other"
const secondName = "second"

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
	policy := Policy{Retention: time.Hour}
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
	meta, err := store.metadataRepository.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.FormatVersion != storeFormatVersion {
		t.Fatalf("metadata format version = %d, want %d", meta.FormatVersion, storeFormatVersion)
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
		Lease:    Lease{ID: secondName, Target: filepath.Join(t.TempDir(), secondName)},
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

func TestRecoveryCommitsPreparingLeaseWhenMountIsVerified(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identity := stableIdentity("recovered-preparing-lease")
	target := filepath.Join(t.TempDir(), "mount")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "recover-preparing", Target: target},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := NewStore(root, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Error(err)
		}
	})
	verified := false
	if err := recovered.RecoverLeases(func(_ string, lease Lease, _ Policy) (bool, error) {
		if lease.ID != "recover-preparing" || !lease.Preparing {
			t.Errorf("recovery verifier lease = %+v, want preparing lease", lease)
		}
		verified = true
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !verified {
		t.Fatal("recovery mount verifier was not called")
	}
	_, lease, _, _, found, err := recovered.LeaseDetails("recover-preparing")
	if err != nil || !found || lease.Preparing {
		t.Fatalf("recovered lease = (%+v, %t, %v), want published lease", lease, found, err)
	}
	meta, err := recovered.metadataRepository.readMetadata(filepath.Join(root, identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.FormatVersion != storeFormatVersion || len(meta.Leases) != 1 || meta.Leases[0].Preparing {
		t.Fatalf("persisted recovered metadata = %+v, want format 1 and published lease", meta)
	}
}

func TestZeroRetentionDisablesTTLCollectionButKeepsPressureReclaimEligibility(t *testing.T) {
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
	identity := stableIdentity("zero-retention")
	target := filepath.Join(t.TempDir(), "mount")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "zero-retention", Target: target},
		Policy:   Policy{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Release("zero-retention", target); err != nil {
		t.Fatal(err)
	}
	if err := store.Collect(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), identity)); err != nil {
		t.Fatalf("zero-retention cache was removed by TTL collection: %v", err)
	}
	store.mu.Lock()
	candidates := store.unusedPressureCandidates()
	store.mu.Unlock()
	if !slices.ContainsFunc(candidates, func(candidate pressureCandidate) bool { return candidate.identity == identity }) {
		t.Fatal("zero-retention cache was excluded from pressure reclaim")
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
		Lease:    Lease{ID: secondName, Target: filepath.Join(t.TempDir(), secondName)},
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
	meta, err := store.metadataRepository.readMetadata(filepath.Join(store.Root(), identity))
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
	registry, err := os.ReadFile(filepath.Join(store.Root(), projectRegistryName))
	if err != nil {
		t.Fatal(err)
	}
	var registryDocument projectReservationDocument
	if err := json.Unmarshal(registry, &registryDocument); err != nil {
		t.Fatal(err)
	}
	if registryDocument.FormatVersion != storeFormatVersion {
		t.Fatalf("project registry format version = %d, want %d", registryDocument.FormatVersion, storeFormatVersion)
	}
	if _, _, _, err := store.QuotaState(identities[1], 1024); err == nil {
		t.Fatal("allocation exceeded the configured project ID range")
	}
}

func TestMetadataDamageIsIsolatedAndProjectReservationSurvivesRestart(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{ProjectIDStart: 32000, ProjectIDCount: 3, ProjectQuotaEnabled: true})
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

	store, err = NewStore(root, StoreOptions{ProjectIDStart: 32000, ProjectIDCount: 3, ProjectQuotaEnabled: true})
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

func TestRecoverLeasesQuarantinesDamagedObjectAndPreservesMountedData(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		mounted       bool
		verifyErr     bool
		wantPreserved bool
	}{
		{name: "unmounted"},
		{name: "mounted", mounted: true, wantPreserved: true},
		{name: "unknown mount state", verifyErr: true, wantPreserved: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			trashMounted := test.mounted || test.verifyErr
			unmountCalls := 0
			store, err := NewStore(root, StoreOptions{
				ProjectIDStart:      32000,
				ProjectIDCount:      1,
				UnmountGeneration:   func(string) error { unmountCalls++; return nil },
				IsGenerationMounted: func(string) (bool, error) { return trashMounted, nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			trashGate := gateTrashRemoval(t, store)
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
			meta, err := store.metadataRepository.readMetadata(entry)
			if err != nil || validateMetadata(identity, meta) != nil || len(meta.Leases) != 0 {
				t.Fatalf("canonical cache after quarantine = (%+v, %v), want fresh metadata without old leases", meta, err)
			}
			if meta.Generation == filepath.Base(generationPath) {
				t.Fatal("canonical identity reused the damaged generation")
			}
			if _, err := os.Stat(filepath.Join(entry, generationsDirectoryName, meta.Generation)); err != nil {
				t.Fatalf("fresh cache generation is missing: %v", err)
			}
			trashEntries, err := store.metadataRepository.readDir(filepath.Join(root, trashDirectoryName))
			if err != nil || len(trashEntries) != 1 {
				t.Fatalf("quarantined object count = (%d, %v), want one object", len(trashEntries), err)
			}
			trashPath := filepath.Join(root, trashDirectoryName, trashEntries[0].Name())
			if _, err := os.Stat(filepath.Join(trashPath, generationsDirectoryName, filepath.Base(generationPath))); err != nil {
				t.Fatalf("damaged generation was not preserved in trash: %v", err)
			}
			markerErr := store.metadataRepository.stat(filepath.Join(trashPath, preserveMountedTrashMarker))
			if gotPreserved := markerErr == nil; gotPreserved != test.wantPreserved {
				t.Fatalf("mount-preserving quarantine = %t, want %t (error %v)", gotPreserved, test.wantPreserved, markerErr)
			}
			if test.wantPreserved && unmountCalls != 0 {
				t.Fatalf("unmount callback calls = %d, want 0 for mounted or uncertain data", unmountCalls)
			}
			if err := store.MetadataError(); err != nil {
				t.Fatalf("fresh canonical cache remains degraded: %v", err)
			}
			trashMounted = false
			assertProjectIDReservedUntilTrashRemoval(t, store, "quota-during-quarantine", projectID, trashGate)
		})
	}
}

func TestRecoverMissingMetadataQuarantinesAndPreservesMountedData(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		mounted    bool
		inspectErr bool
	}{
		{name: "unmounted"},
		{name: "mounted", mounted: true},
		{name: "inspection uncertain", inspectErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testMissingMetadataRecovery(t, test.name, test.mounted, test.inspectErr)
		})
	}
}

func testMissingMetadataRecovery(t *testing.T, name string, mounted, inspectErr bool) {
	t.Helper()
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
	trashGate := gateTrashRemoval(t, store)
	trashMounted := mounted || inspectErr
	store.isGenerationMounted = func(string) (bool, error) { return trashMounted, nil }
	identity := stableIdentity("missing-metadata-" + name)
	target := filepath.Join(t.TempDir(), "target")
	policy := Policy{QuotaEnabled: true, MaxBytes: 1024}
	source, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: Lease{ID: firstLeaseID, Target: target}, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	projectID, _, _, err := store.QuotaState(identity, policy.MaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, identity, metadataName)); err != nil {
		t.Fatal(err)
	}
	verifier, calls := missingMetadataVerifier(t, source, mounted, inspectErr)
	if err := store.RecoverLeases(verifier); err != nil {
		t.Fatal(err)
	}
	if *calls != 1 {
		t.Fatalf("mount verifier calls = %d, want 1", *calls)
	}
	entry := filepath.Join(root, identity)
	meta, err := store.metadataRepository.readMetadata(entry)
	if err != nil || validateMetadata(identity, meta) != nil || len(meta.Leases) != 0 {
		t.Fatalf("canonical cache after missing metadata recovery = (%+v, %v), want fresh metadata without leases", meta, err)
	}
	if _, err := os.Stat(filepath.Join(entry, generationsDirectoryName, meta.Generation)); err != nil {
		t.Fatalf("fresh generation after missing metadata recovery: %v", err)
	}
	trashEntries, err := store.metadataRepository.readDir(filepath.Join(root, trashDirectoryName))
	if err != nil || len(trashEntries) != 1 {
		t.Fatalf("quarantined objects = (%d, %v), want one", len(trashEntries), err)
	}
	trashPath := filepath.Join(root, trashDirectoryName, trashEntries[0].Name())
	if _, err := os.Stat(filepath.Join(trashPath, generationsDirectoryName, filepath.Base(source))); err != nil {
		t.Fatalf("old generation was not preserved in trash: %v", err)
	}
	markerErr := store.metadataRepository.stat(filepath.Join(trashPath, preserveMountedTrashMarker))
	if gotPreserved := markerErr == nil; gotPreserved != (mounted || inspectErr) {
		t.Fatalf("mount-preserving quarantine = %t, want %t (error %v)", gotPreserved, mounted || inspectErr, markerErr)
	}
	if err := store.MetadataError(); err != nil {
		t.Fatalf("fresh cache remains degraded after missing metadata recovery: %v", err)
	}
	trashMounted = false
	assertProjectIDReservedUntilTrashRemoval(t, store, "missing-metadata-reservation-"+name, projectID, trashGate)
}

func missingMetadataVerifier(t *testing.T, source string, mounted, inspectErr bool) (func(string, Lease, Policy) (bool, error), *int) {
	t.Helper()
	calls := new(int)
	verifier := func(checkedSource string, lease Lease, checkedPolicy Policy) (bool, error) {
		*calls = *calls + 1
		if checkedSource != source || lease != (Lease{}) || checkedPolicy != (Policy{}) {
			t.Fatalf("missing metadata verifier input = (%q, %+v, %+v)", checkedSource, lease, checkedPolicy)
		}
		if inspectErr {
			return false, errors.New("mount inspection failed")
		}
		return mounted, nil
	}
	return verifier, calls
}

type trashRemovalGate struct {
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func (gate *trashRemovalGate) unblock() {
	gate.releaseOnce.Do(func() { close(gate.release) })
}

func gateTrashRemoval(t *testing.T, store *Store) *trashRemovalGate {
	t.Helper()
	if err := store.CleanupTrash(t.Context()); err != nil {
		t.Fatalf("wait for initial trash cleanup: %v", err)
	}
	gate := &trashRemovalGate{started: make(chan struct{}, 1), release: make(chan struct{})}
	t.Cleanup(gate.unblock)
	store.trashCollector.removeTrashEntry = func(path string) error {
		select {
		case gate.started <- struct{}{}:
		default:
		}
		<-gate.release
		return store.metadataRepository.removeAll(path)
	}
	return gate
}

func assertProjectIDReservedUntilTrashRemoval(t *testing.T, store *Store, otherName string, projectID uint32, gate *trashRemovalGate) {
	t.Helper()
	otherIdentity := stableIdentity(otherName)
	if _, _, err := store.Acquire(AcquireOptions{Identity: otherIdentity, Lease: Lease{ID: otherLeaseID, Target: filepath.Join(t.TempDir(), otherLeaseID)}}); err != nil {
		t.Fatal(err)
	}
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- store.CleanupTrash(t.Context()) }()
	select {
	case <-gate.started:
	case <-t.Context().Done():
		t.Fatal("trash cleanup did not reach physical deletion")
	}
	if _, _, _, err := store.QuotaState(otherIdentity, 1024); err == nil {
		t.Fatal("project ID was reused before physical deletion completed")
	}
	gate.unblock()
	if err := <-cleanupDone; err != nil {
		t.Fatal(err)
	}
	allocated, _, _, err := store.QuotaState(otherIdentity, 1024)
	if err != nil || allocated != projectID {
		t.Fatalf("project ID after trash removal = (%d, %v), want %d", allocated, err, projectID)
	}
}

func TestRecoverKeepsEmptyObjectWithoutMetadata(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identity := stableIdentity("empty-object")
	entry := filepath.Join(root, identity)
	if err := store.metadataRepository.ensureDirectory(filepath.Join(entry, generationsDirectoryName)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := store.RecoverLeases(func(string, Lease, Policy) (bool, error) {
		calls++
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("mount verifier calls = %d, want 0", calls)
	}
	if _, err := os.Stat(entry); err != nil {
		t.Fatalf("empty initial object was removed: %v", err)
	}
	if err := store.MetadataError(); err != nil {
		t.Fatalf("empty initial object was marked degraded: %v", err)
	}
}

func TestDamagedProjectRegistryIsRebuiltFromCanonicalMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
	}{
		{name: "malformed JSON", data: []byte("{broken")},
		{name: "invalid reservation", data: []byte(`{"unknownReservations":[{"identity":""}]}`)},
		{name: "unknown format version", data: []byte(`{"formatVersion":2,"reservations":[]}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			registryPath := filepath.Join(root, projectRegistryName)
			if err := os.WriteFile(registryPath, test.data, 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := NewStore(root, StoreOptions{ProjectQuotaEnabled: true})
			if err != nil {
				t.Fatalf("open store with damaged project registry: %v", err)
			}
			if err := store.ProjectRegistryError(); err != nil {
				t.Fatalf("project registry could not be reconstructed from canonical metadata: %v", err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			identity := stableIdentity("registry-damage-" + test.name)
			if _, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: Lease{ID: firstLeaseID, Target: filepath.Join(t.TempDir(), firstLeaseID)}}); err != nil {
				t.Fatalf("non-quota cache operation failed: %v", err)
			}
			if _, _, _, err := store.QuotaState(identity, 1024); err != nil {
				t.Fatalf("quota allocation failed after project registry reconstruction: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewStore(root, StoreOptions{ProjectQuotaEnabled: true})
			if err != nil {
				t.Fatalf("reopen store with damaged project registry: %v", err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			otherIdentity := stableIdentity("registry-damage-reopen-" + test.name)
			if _, _, err := reopened.Acquire(AcquireOptions{Identity: otherIdentity, Lease: Lease{ID: otherLeaseID, Target: filepath.Join(t.TempDir(), otherLeaseID)}}); err != nil {
				t.Fatalf("non-quota cache operation failed after restart: %v", err)
			}
			if _, _, _, err := reopened.QuotaState(otherIdentity, 1024); err != nil {
				t.Fatalf("quota allocation failed after reopening reconstructed registry: %v", err)
			}
			data, err := os.ReadFile(registryPath)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) == string(test.data) {
				t.Fatal("damaged project registry was not replaced by the canonical reconstruction")
			}
		})
	}
}

func TestUnknownMetadataFormatVersionIsIsolatedAndQuarantined(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identities := []string{stableIdentity("format-good"), stableIdentity("format-unknown")}
	leaseIDs := []string{"format-good", "format-unknown"}
	for index, identity := range identities {
		if _, _, err := store.Acquire(AcquireOptions{
			Identity: identity,
			Lease:    Lease{ID: leaseIDs[index], Target: filepath.Join(t.TempDir(), leaseIDs[index])},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	unknownVersion := storeFormatVersion + 1
	unknownMetadataPath := filepath.Join(root, identities[1], metadataName)
	setDocumentFormatVersion(t, unknownMetadataPath, &unknownVersion)

	recovered, err := NewStore(root, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, _, _, _, _, err := recovered.LeaseDetails(leaseIDs[1]); !errors.Is(err, ErrDegradedMetadata) {
		t.Fatalf("unknown metadata version lookup error = %v, want degraded metadata", err)
	}
	if err := recovered.RecoverLeases(func(_ string, lease Lease, _ Policy) (bool, error) {
		return lease.ID == leaseIDs[0], nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, found, err := recovered.LeaseDetails(leaseIDs[0]); err != nil || !found {
		t.Fatalf("healthy object after unknown-version quarantine = found %t, error %v", found, err)
	}
	unknownEntry := filepath.Join(root, identities[1])
	unknownMeta, err := recovered.metadataRepository.readMetadata(unknownEntry)
	if err != nil || validateMetadata(identities[1], unknownMeta) != nil || len(unknownMeta.Leases) != 0 {
		t.Fatalf("unknown-version object after quarantine = (%+v, %v), want fresh metadata without old leases", unknownMeta, err)
	}
}

func TestOldStoreMetadataFormatsAreQuarantinedAndProjectRegistryIsRebuilt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		metadataVersion *int
		registryVersion *int
	}{
		{name: "missing versions"},
		{name: "zero versions", metadataVersion: new(0), registryVersion: new(0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			legacyStoreDocuments(t, test.metadataVersion, test.registryVersion)
		})
	}
}

func legacyStoreDocuments(t *testing.T, metadataVersion, registryVersion *int) {
	t.Helper()
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{ProjectIDStart: 32000, ProjectIDCount: 10})
	if err != nil {
		t.Fatal(err)
	}
	identity := stableIdentity("legacy-format")
	target := filepath.Join(t.TempDir(), "legacy-target")
	policy := Policy{QuotaEnabled: true, MaxBytes: 1024}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "legacy-format", Target: target},
		Policy:   policy,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(identity, policy.MaxBytes); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	setDocumentFormatVersion(t, filepath.Join(root, identity, metadataName), metadataVersion)
	setDocumentFormatVersion(t, filepath.Join(root, projectRegistryName), registryVersion)

	reopened, err := NewStore(root, StoreOptions{ProjectIDStart: 32000, ProjectIDCount: 10})
	if err != nil {
		t.Fatalf("open legacy store documents: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, _, _, _, found, err := reopened.LeaseDetails("legacy-format"); !errors.Is(err, ErrDegradedMetadata) || found {
		t.Fatalf("old format lease lookup = found %t, error %v, want quarantined metadata", found, err)
	}
	if err := reopened.RecoverLeases(func(string, Lease, Policy) (bool, error) { return false, nil }); err != nil {
		t.Fatalf("quarantine old format cache metadata: %v", err)
	}
	meta, err := reopened.metadataRepository.readMetadata(filepath.Join(root, identity))
	if err != nil || validateMetadata(identity, meta) != nil || len(meta.Leases) != 0 {
		t.Fatalf("cache after old format quarantine = (%+v, %v), want fresh current-format metadata", meta, err)
	}
	if err := reopened.CleanupTrash(t.Context()); err != nil {
		t.Fatalf("remove quarantined old format cache: %v", err)
	}
	otherIdentity := stableIdentity("legacy-format-other")
	if _, _, err := reopened.Acquire(AcquireOptions{
		Identity: otherIdentity,
		Lease:    Lease{ID: "legacy-format-other", Target: filepath.Join(t.TempDir(), "other")},
		Policy:   policy,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := reopened.QuotaState(otherIdentity, policy.MaxBytes); err != nil {
		t.Fatalf("upgrade legacy project registry on write: %v", err)
	}
	assertDocumentFormatVersion(t, filepath.Join(root, projectRegistryName), storeFormatVersion)
}

func setDocumentFormatVersion(t *testing.T, path string, version *int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if version == nil {
		delete(document, "formatVersion")
	} else {
		document["formatVersion"] = *version
	}
	data, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertDocumentFormatVersion(t *testing.T, path string, want int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		FormatVersion int `json:"formatVersion"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if document.FormatVersion != want {
		t.Fatalf("%s format version = %d, want %d", path, document.FormatVersion, want)
	}
}

func TestAcquireDoesNotDiscardGenerationsWhenMetadataIsMissing(t *testing.T) {
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
	identity := stableIdentity("missing-metadata-acquire")
	path, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: Lease{ID: firstLeaseID, Target: filepath.Join(t.TempDir(), firstLeaseID)}})
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(path, "still-present")
	if err := os.WriteFile(marker, []byte("cache"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(store.Root(), identity, metadataName)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: Lease{ID: "second-lease", Target: filepath.Join(t.TempDir(), "second-lease")}}); !errors.Is(err, ErrDegradedMetadata) {
		t.Fatalf("acquire with missing metadata error = %v, want degraded metadata", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("generation data was discarded without mount verification: %v", err)
	}
}

func TestAcquireUpdatesRuntimePolicyForSharedGeneration(t *testing.T) {
	t.Parallel()

	initialPolicy := Policy{
		SharingPolicy: SharingPolicyShared,
		Retention:     time.Hour,
		NoExec:        true,
	}
	store, err := newStore(t.TempDir(), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identity := stableIdentity("shared-policy-snapshot")
	firstTarget := filepath.Join(t.TempDir(), "first-shared")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: firstLeaseID, Target: firstTarget, NoExec: initialPolicy.NoExec},
		Policy:   initialPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitPublish(firstLeaseID, firstTarget); err != nil {
		t.Fatal(err)
	}
	updatedPolicy := initialPolicy
	updatedPolicy.Retention = 2 * time.Hour
	updatedPolicy.NoExec = false
	secondTarget := filepath.Join(t.TempDir(), "second-shared")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "second-shared-lease", Target: secondTarget, NoExec: updatedPolicy.NoExec},
		Policy:   updatedPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitPublish("second-shared-lease", secondTarget); err != nil {
		t.Fatal(err)
	}
	meta, err := store.metadataRepository.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Policy != updatedPolicy {
		t.Fatalf("shared generation runtime policy = %+v, want updated policy %+v", meta.Policy, updatedPolicy)
	}
	_, secondLease, _, secondPolicy, found, err := store.LeaseDetails("second-shared-lease")
	if err != nil || !found {
		t.Fatalf("second shared lease found = %t, error = %v", found, err)
	}
	if secondLease.NoExec != updatedPolicy.NoExec {
		t.Fatalf("second shared lease noexec = %t, want requested policy %t", secondLease.NoExec, updatedPolicy.NoExec)
	}
	if secondPolicy != updatedPolicy {
		t.Fatalf("second shared lease policy = %+v, want updated runtime policy %+v", secondPolicy, updatedPolicy)
	}
	_, firstLease, _, _, found, err := store.LeaseDetails(firstLeaseID)
	if err != nil || !found || !firstLease.NoExec {
		t.Fatalf("existing shared lease = (%+v, %t, %v), want its original noexec mount option", firstLease, found, err)
	}
	if len(meta.Leases) != 2 || meta.Leases[0].Generation != meta.Leases[1].Generation {
		t.Fatalf("shared leases = %+v, want both leases on the same generation", meta.Leases)
	}
	if err := store.Release(firstLeaseID, firstTarget); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireUpdatesIdleGenerationPolicyWithoutDiscardingData(t *testing.T) {
	t.Parallel()
	initialPolicy := Policy{
		SharingPolicy: SharingPolicyShared,
		SchemaVersion: "v1",
		QuotaEnabled:  true,
		MaxBytes:      1 << 20,
		Retention:     time.Hour,
		NoExec:        true,
	}
	store, err := newStore(t.TempDir(), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identity := stableIdentity("idle-policy-snapshot")
	firstTarget := filepath.Join(t.TempDir(), "first-idle")
	firstPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "first-idle-lease", Target: firstTarget, NoExec: initialPolicy.NoExec},
		Policy:   initialPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(firstPath, "warm-cache-entry")
	if err := os.WriteFile(marker, []byte("cache"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Release("first-idle-lease", firstTarget); err != nil {
		t.Fatal(err)
	}

	updatedPolicy := initialPolicy
	updatedPolicy.MaxBytes = 2 << 20
	updatedPolicy.Retention = 2 * time.Hour
	updatedPolicy.NoExec = false
	updatedPolicy.SharingPolicy = SharingPolicyExclusive
	secondTarget := filepath.Join(t.TempDir(), "second-idle")
	secondPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "second-idle-lease", Target: secondTarget, NoExec: updatedPolicy.NoExec},
		Policy:   updatedPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if secondPath != firstPath {
		t.Fatalf("updated generation path = %q, want existing generation %q", secondPath, firstPath)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "cache" {
		t.Fatalf("warm cache entry = (%q, %v), want preserved data", data, err)
	}
	_, lease, _, storedPolicy, found, err := store.LeaseDetails("second-idle-lease")
	if err != nil || !found {
		t.Fatalf("second idle lease found = %t, error = %v", found, err)
	}
	if lease.NoExec != updatedPolicy.NoExec {
		t.Fatalf("second idle lease noexec = %t, want updated policy %t", lease.NoExec, updatedPolicy.NoExec)
	}
	if storedPolicy != updatedPolicy {
		t.Fatalf("second idle lease policy = %+v, want updated policy %+v", storedPolicy, updatedPolicy)
	}
}

func TestGenerationPolicyHashTracksOnlySchemaVersion(t *testing.T) {
	t.Parallel()
	base := Policy{
		ClassName:     "default",
		ClassUID:      "class-uid",
		SharingPolicy: SharingPolicyShared,
		NoExec:        true,
		SchemaVersion: "v1",
		QuotaEnabled:  true,
		MaxBytes:      1 << 20,
		Retention:     time.Hour,
	}
	baseHash, err := generationPolicyHash(base)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		update func(*Policy)
		want   bool
	}{
		{name: "retention", update: func(policy *Policy) { policy.Retention *= 2 }},
		{name: "noexec", update: func(policy *Policy) { policy.NoExec = false }},
		{name: "sharing", update: func(policy *Policy) { policy.SharingPolicy = SharingPolicyExclusive }},
		{name: "quota", update: func(policy *Policy) { policy.MaxBytes *= 2 }},
		{name: "schema", update: func(policy *Policy) { policy.SchemaVersion = "v2" }, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			policy := base
			test.update(&policy)
			got, err := generationPolicyHash(policy)
			if err != nil {
				t.Fatal(err)
			}
			if changed := got != baseHash; changed != test.want {
				t.Fatalf("generation policy hash changed = %t, want %t", changed, test.want)
			}
		})
	}
}

func TestRuntimePolicyChangesPreserveActiveGeneration(t *testing.T) {
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

	identity := stableIdentity("active-runtime-policy-update")
	oldPolicy := Policy{SchemaVersion: "v1", SharingPolicy: SharingPolicyShared, NoExec: true, Retention: time.Hour}
	oldPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "runtime-old", Target: filepath.Join(t.TempDir(), "runtime-old"), NoExec: true},
		Policy:   oldPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	updatedPolicy := oldPolicy
	updatedPolicy.NoExec = false
	updatedPolicy.Retention = 24 * time.Hour
	newPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "runtime-new", Target: filepath.Join(t.TempDir(), "runtime-new"), NoExec: false},
		Policy:   updatedPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if newPath != oldPath {
		t.Fatalf("runtime-only policy update changed generation path from %q to %q", oldPath, newPath)
	}
	meta, err := store.metadataRepository.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Policy != updatedPolicy {
		t.Fatalf("stored runtime policy = %+v, want %+v", meta.Policy, updatedPolicy)
	}
	if len(meta.Leases) != 2 || !meta.Leases[0].NoExec || meta.Leases[1].NoExec {
		t.Fatalf("lease mount policies = %+v, want existing noexec retained and new noexec disabled", meta.Leases)
	}
}

func TestSchemaChangeRetiresActiveGenerationAndCreatesFreshCache(t *testing.T) {
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

	identity := stableIdentity("active-schema-transition")
	oldPolicy := Policy{SchemaVersion: "v1", SharingPolicy: SharingPolicyShared}
	oldTarget := filepath.Join(t.TempDir(), "schema-old")
	oldPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "schema-old", Target: oldTarget},
		Policy:   oldPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldPath, "warm"), []byte("warm"), 0o600); err != nil {
		t.Fatal(err)
	}
	newPolicy := oldPolicy
	newPolicy.SchemaVersion = "v2"
	newPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "schema-new", Target: filepath.Join(t.TempDir(), "schema-new")},
		Policy:   newPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if newPath == oldPath {
		t.Fatal("schema change reused the incompatible active generation")
	}
	if contents, err := os.ReadFile(filepath.Join(oldPath, "warm")); err != nil || string(contents) != "warm" {
		t.Fatalf("old active generation data = (%q, %v), want retained until its lease is released", contents, err)
	}
	meta, err := store.metadataRepository.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Retired) != 1 || meta.Retired[0].Generation == meta.Generation || meta.Retired[0].State != GenerationStateRetiring {
		t.Fatalf("retired generation metadata = %+v, want the old active generation in Retiring state", meta.Retired)
	}
	if len(meta.Leases) != 2 || meta.Leases[0].Generation != meta.Retired[0].Generation || meta.Leases[1].Generation != meta.Generation {
		t.Fatalf("generation lease assignment = %+v, want old and new leases on distinct generations", meta.Leases)
	}
	if err := store.Release("schema-old", oldTarget); err != nil {
		t.Fatal(err)
	}
	meta, err = store.metadataRepository.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil || len(meta.Retired) != 0 || meta.Generation == "" {
		t.Fatalf("metadata after old lease release = (%+v, %v), want only the fresh active generation", meta, err)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old generation remained in the canonical identity after lease release: %v", err)
	}
}

func TestValidateMetadataRejectsNonmatchingPolicyHash(t *testing.T) {
	t.Parallel()
	policy := Policy{SchemaVersion: "v1", Retention: time.Hour}
	meta := Metadata{
		FormatVersion:   storeFormatVersion,
		Identity:        stableIdentity("legacy-policy-hash"),
		Generation:      "generation",
		GenerationState: GenerationStateActive,
		PolicyHash:      "previous-policy-hash",
		Policy:          policy,
	}
	if err := validateMetadata(meta.Identity, meta); err == nil {
		t.Fatal("metadata with a policy hash from the previous policy definition was accepted")
	}
}

func TestAcquireMarksMissingGenerationWithActiveLeaseAsDegraded(t *testing.T) {
	t.Parallel()
	store, err := newStore(t.TempDir(), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	identity := stableIdentity("active-lease-missing-generation")
	target := filepath.Join(t.TempDir(), "mount")
	generationPath, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "active-missing-generation", Target: target},
	})
	if err != nil {
		t.Fatal(err)
	}
	metaPath := filepath.Join(store.Root(), identity)
	meta, err := store.metadataRepository.readMetadata(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(generationPath); err != nil {
		t.Fatal(err)
	}

	_, _, err = store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "another-lease", Target: filepath.Join(t.TempDir(), "other")},
	})
	if !errors.Is(err, ErrDegradedMetadata) {
		t.Fatalf("acquire with active lease and missing generation error = %v, want degraded metadata", err)
	}
	stored, err := store.metadataRepository.readMetadata(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Generation != meta.Generation || len(stored.Leases) != 1 || stored.Leases[0].ID != "active-missing-generation" {
		t.Fatalf("metadata after missing generation = %+v, want original generation and lease preserved", stored)
	}
}

func TestQuarantineMissingMountedGenerationPreservesTrashUntilMountEnds(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	var mounted atomic.Bool
	var oldSource string
	store, err := NewStore(root, StoreOptions{IsGenerationMounted: func(source string) (bool, error) {
		return source == oldSource && mounted.Load(), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	identity := stableIdentity("quarantine-missing-mounted-generation")
	target := filepath.Join(t.TempDir(), "mount")
	source, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "missing-mounted-generation", Target: target},
		Policy:   Policy{SharingPolicy: SharingPolicyShared},
	})
	if err != nil {
		t.Fatal(err)
	}
	oldSource = source
	mounted.Store(true)
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	store.markDegraded(identity, errors.New("generation directory disappeared"))
	if err := store.QuarantineDegradedObjectChecked(identity, func(_ string, lease Lease, _ Policy) (bool, error) {
		return lease.ID == "missing-mounted-generation", nil
	}); err != nil {
		t.Fatal(err)
	}
	meta, err := store.metadataRepository.readMetadata(filepath.Join(root, identity))
	if err != nil || meta.Generation == "" || len(meta.Leases) != 0 {
		t.Fatalf("fresh identity metadata = (%+v, %v), want an empty fresh generation", meta, err)
	}
	trashEntries, err := store.metadataRepository.readDir(filepath.Join(root, trashDirectoryName))
	if err != nil || len(trashEntries) != 1 {
		t.Fatalf("trash entries after quarantine = (%d, %v), want the old generation retained", len(trashEntries), err)
	}
	trashPath := filepath.Join(root, trashDirectoryName, trashEntries[0].Name())
	if err := store.cleanupTrashBatch(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(trashPath); err != nil {
		t.Fatalf("mounted generation trash was removed: %v", err)
	}
	mounted.Store(false)
	if err := store.cleanupTrashBatch(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(trashPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unmounted generation trash remains: %v", err)
	}
}

func TestRecoveryQuarantinesMissingCurrentGenerationWithoutDroppingLease(t *testing.T) {
	t.Parallel()
	const leaseID = "missing-current-generation"
	root := t.TempDir()
	store, err := newStore(root, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identity := stableIdentity("recovery-active-lease-missing-generation")
	target := filepath.Join(t.TempDir(), "mount")
	source, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: leaseID, Target: target},
	})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.metadataRepository.readMetadata(filepath.Join(root, identity))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	verifierCalls := 0
	if err := store.RecoverLeases(func(candidate string, lease Lease, _ Policy) (bool, error) {
		verifierCalls++
		if candidate == filepath.Join(root, identity) {
			return false, nil
		}
		if candidate != source || lease.ID != leaseID || lease.Target != target {
			t.Fatalf("missing generation verifier input = (%q, %+v)", candidate, lease)
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if verifierCalls != 2 {
		t.Fatalf("mount verifier calls = %d, want object and lease checks", verifierCalls)
	}
	newMeta, err := store.metadataRepository.readMetadata(filepath.Join(root, identity))
	if err != nil || validateMetadata(identity, newMeta) != nil || len(newMeta.Leases) != 0 {
		t.Fatalf("canonical metadata after recovery = (%+v, %v), want fresh generation without old lease", newMeta, err)
	}
	if newMeta.Generation == meta.Generation {
		t.Fatal("canonical identity reused the missing generation")
	}
	trashEntries, err := store.metadataRepository.readDir(filepath.Join(root, trashDirectoryName))
	if err != nil || len(trashEntries) != 1 {
		t.Fatalf("quarantined object entries = (%d, %v), want one", len(trashEntries), err)
	}
	oldMeta, err := store.metadataRepository.readMetadata(filepath.Join(root, trashDirectoryName, trashEntries[0].Name()))
	if err != nil || len(oldMeta.Leases) != 1 || oldMeta.Leases[0].ID != leaseID {
		t.Fatalf("quarantined lease metadata = (%+v, %v), want original lease retained", oldMeta.Leases, err)
	}
}

func TestDamagedProjectRegistryIsRebuiltFromMetadata(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	options := StoreOptions{ProjectQuotaEnabled: true, ProjectIDStart: 32000, ProjectIDCount: 4}
	store, err := NewStore(root, options)
	if err != nil {
		t.Fatal(err)
	}
	identity := stableIdentity("registry-rebuild")
	policy := Policy{QuotaEnabled: true, MaxBytes: 1024}
	if _, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: Lease{ID: "registry-rebuild", Target: filepath.Join(t.TempDir(), "mount")}, Policy: policy}); err != nil {
		t.Fatal(err)
	}
	projectID, _, _, err := store.QuotaState(identity, policy.MaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, projectRegistryName), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}

	rebuilt, err := NewStore(root, options)
	if err != nil {
		t.Fatalf("rebuild registry from canonical metadata: %v", err)
	}
	t.Cleanup(func() {
		if err := rebuilt.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := rebuilt.ProjectRegistryError(); err != nil {
		t.Fatalf("rebuilt project registry health: %v", err)
	}
	gotProjectID, _, _, err := rebuilt.QuotaState(identity, policy.MaxBytes)
	if err != nil || gotProjectID != projectID {
		t.Fatalf("restored project ID = (%d, %v), want %d", gotProjectID, err, projectID)
	}
	assertDocumentFormatVersion(t, filepath.Join(root, projectRegistryName), storeFormatVersion)
}

func TestDamagedTrashMetadataDisablesProjectIDReuse(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	options := StoreOptions{ProjectQuotaEnabled: true, ProjectIDStart: 32000, ProjectIDCount: 4}
	store, err := NewStore(root, options)
	if err != nil {
		t.Fatal(err)
	}
	identity := stableIdentity("damaged-trash-project")
	target := filepath.Join(t.TempDir(), "old")
	policy := Policy{QuotaEnabled: true, MaxBytes: 1024}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "damaged-trash-project", Target: target},
		Policy:   policy,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.QuotaState(identity, policy.MaxBytes); err != nil {
		t.Fatal(err)
	}
	if err := store.detachToTrashPreservingMounts(filepath.Join(root, identity)); err != nil {
		t.Fatal(err)
	}
	trashEntries, err := store.metadataRepository.readDir(filepath.Join(root, trashDirectoryName))
	if err != nil || len(trashEntries) != 1 {
		t.Fatalf("trash entries = (%d, %v), want one project-owning object", len(trashEntries), err)
	}
	trashMetadataPath := filepath.Join(root, trashDirectoryName, trashEntries[0].Name(), metadataName)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(trashMetadataPath, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, projectRegistryName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	rebuilt, err := NewStore(root, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rebuilt.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := rebuilt.WaitForIndexes(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := rebuilt.ProjectRegistryError(); err == nil {
		t.Fatal("damaged trash metadata allowed the project registry to be considered healthy")
	}
	newIdentity := stableIdentity("project-after-unknown-trash")
	if _, _, err := rebuilt.Acquire(AcquireOptions{
		Identity: newIdentity,
		Lease:    Lease{ID: "project-after-unknown-trash", Target: filepath.Join(t.TempDir(), "new")},
		Policy:   policy,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := rebuilt.QuotaState(newIdentity, policy.MaxBytes); err == nil {
		t.Fatal("project ID allocation succeeded while an unknown trash project remained")
	}
}

func TestAcquireWaitsForCollectionOfSameIdentity(t *testing.T) {
	t.Parallel()
	enteredDetach := make(chan struct{})
	continueDetach := make(chan struct{})
	var enteredOnce sync.Once
	var continueOnce sync.Once
	releaseDetach := func() { continueOnce.Do(func() { close(continueDetach) }) }
	store, err := newStore(t.TempDir(), StoreOptions{UnmountGeneration: func(string) error {
		enteredOnce.Do(func() { close(enteredDetach) })
		<-continueDetach
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseDetach()
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	identity := stableIdentity("acquire-collect-serialization")
	firstTarget := filepath.Join(t.TempDir(), "first")
	policy := Policy{Retention: time.Hour}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "collect-source", Target: firstTarget},
		Policy:   policy,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Release("collect-source", firstTarget); err != nil {
		t.Fatal(err)
	}

	collectDone := make(chan error, 1)
	go func() { collectDone <- store.Collect(time.Now().Add(2 * time.Hour)) }()
	select {
	case <-enteredDetach:
	case <-time.After(5 * time.Second):
		t.Fatal("collection did not reach generation detach")
	}

	type acquireResult struct {
		path string
		err  error
	}
	acquireDone := make(chan acquireResult, 1)
	newTarget := filepath.Join(t.TempDir(), "new")
	go func() {
		path, _, err := store.Acquire(AcquireOptions{
			Identity: identity,
			Lease:    Lease{ID: "concurrent-acquire", Target: newTarget},
			Policy:   policy,
		})
		acquireDone <- acquireResult{path: path, err: err}
	}()
	waitForKeyedLockReferences(t, &store.leaseManager.identityLocks, identity, 2)
	select {
	case result := <-acquireDone:
		t.Fatalf("Acquire completed while collection held the identity lock: %+v", result)
	default:
	}

	releaseDetach()
	if err := <-collectDone; err != nil {
		t.Fatal(err)
	}
	result := <-acquireDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	if _, err := os.Stat(result.path); err != nil {
		t.Fatalf("new generation path after collection = %q: %v", result.path, err)
	}
	_, lease, source, _, found, err := store.LeaseDetails("concurrent-acquire")
	if err != nil || !found || lease.Target != newTarget || source != result.path {
		t.Fatalf("acquired lease after collection = (%+v, %q, %t, %v)", lease, source, found, err)
	}
}

func TestAcquireClaimsLeaseIDAcrossIdentities(t *testing.T) {
	t.Parallel()
	store, err := newStore(t.TempDir(), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	type acquireResult struct {
		identity string
		target   string
		err      error
	}
	start := make(chan struct{})
	results := make(chan acquireResult, 2)
	for _, name := range []string{"first", secondName} {
		identity := stableIdentity("lease-claim-" + name)
		target := filepath.Join(t.TempDir(), name)
		go func() {
			<-start
			_, _, err := store.Acquire(AcquireOptions{
				Identity: identity,
				Lease:    Lease{ID: "globally-claimed-volume", Target: target},
			})
			results <- acquireResult{identity: identity, target: target, err: err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if (first.err == nil) == (second.err == nil) {
		t.Fatalf("Acquire results = (%v, %v), want exactly one global lease ID claim", first.err, second.err)
	}
	winner := first
	if winner.err != nil {
		winner = second
	}
	identity, lease, _, _, found, err := store.LeaseDetails("globally-claimed-volume")
	if err != nil || !found || identity != winner.identity || lease.Target != winner.target {
		t.Fatalf("globally claimed lease = (%q, %+v, %t, %v), winner = %+v", identity, lease, found, err, winner)
	}
}

func waitForKeyedLockReferences(t *testing.T, locks *keyedMutexes, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		locks.mu.Lock()
		lock := locks.locks[key]
		refs := 0
		if lock != nil {
			refs = lock.refs
		}
		locks.mu.Unlock()
		if refs >= want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("keyed lock references for %q did not reach %d", key, want)
}

func TestTrashCleanupPhysicallyDeletesMoreThanOneBatch(t *testing.T) {
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
	trash, err := store.metadataRepository.readDir(filepath.Join(store.Root(), trashDirectoryName))
	if err != nil {
		t.Fatal(err)
	}
	if len(trash) != len(identities) {
		t.Fatalf("trash entries before cleanup = %d, want %d", len(trash), len(identities))
	}
	if err := store.cleanupTrashUntilAttempted(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	for _, identity := range identities {
		if _, err := os.Stat(filepath.Join(store.Root(), identity)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unused object %q remains after trash cleanup: %v", identity, err)
		}
	}
	trash, err = store.metadataRepository.readDir(filepath.Join(store.Root(), trashDirectoryName))
	if err != nil {
		t.Fatal(err)
	}
	if len(trash) != 0 {
		t.Fatalf("trash entries = %d, want 0 after cleanup", len(trash))
	}
}

func TestTrashCleanupSkipsFailedEntriesAcrossBatches(t *testing.T) {
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
	if err := store.CleanupTrash(t.Context()); err != nil {
		t.Fatalf("wait for initial trash cleanup: %v", err)
	}
	blockedErr := errors.New("trash entry is temporarily unavailable")
	store.trashCollector.removeTrashEntry = func(path string) error {
		if filepath.Base(path) == "000" {
			return blockedErr
		}
		return store.metadataRepository.removeAll(path)
	}
	for index := range trashBatchSize + 1 {
		trashID := fmt.Sprintf("%03d", index)
		entry := filepath.Join(store.Root(), trashDirectoryName, trashID)
		if err := os.Mkdir(entry, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(entry, "payload"), []byte(trashID), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.CleanupTrash(t.Context()); !errors.Is(err, blockedErr) {
		t.Fatalf("first trash cleanup error = %v, want blocked entry error", err)
	}
	if err := store.CleanupTrash(t.Context()); !errors.Is(err, blockedErr) {
		t.Fatalf("second trash cleanup error = %v, want blocked entry error", err)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), trashDirectoryName, "016")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("entry after failed batch was starved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), trashDirectoryName, "000")); err != nil {
		t.Fatalf("temporarily unavailable entry was unexpectedly removed: %v", err)
	}
}
