package cache

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

const firstLeaseID = "first"
const otherLeaseID = "other"

func TestAcquireFallbackReservesPerVolumeAndAggregateLimits(t *testing.T) {
	t.Parallel()
	var cleaned []string
	store, err := NewStore(t.TempDir(), StoreOptions{UnmountGeneration: func(path string) error {
		cleaned = append(cleaned, path)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	pageSize := int64(os.Getpagesize())
	acquire := func(id string, requested int64) (string, int64, error) {
		identity, err := FallbackIdentity(id)
		if err != nil {
			return "", 0, err
		}
		target := filepath.Join(t.TempDir(), id)
		allocation, err := store.AcquireFallback(AcquireOptions{
			Identity: identity,
			Lease:    Lease{ID: id, Target: target},
			Policy:   Policy{ClassName: "fallback", SharingPolicy: SharingPolicyExclusive, DiscardOnLastRelease: true},
		}, requested, 8*pageSize, 10*pageSize)
		return allocation.Source, allocation.MaxBytes, err
	}

	firstPath, firstBytes, err := acquire("fallback-first", 20*pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if firstBytes != 8*pageSize {
		t.Fatalf("first fallback allocation = %d, want per-volume maximum %d", firstBytes, 8*pageSize)
	}
	_, secondBytes, err := acquire("fallback-second", 8*pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if secondBytes != 2*pageSize {
		t.Fatalf("second fallback allocation = %d, want remaining aggregate capacity %d", secondBytes, 2*pageSize)
	}
	if _, _, err := acquire("fallback-third", pageSize); !errors.Is(err, ErrFallbackCapacity) {
		t.Fatalf("acquire beyond aggregate fallback limit error = %v, want ErrFallbackCapacity", err)
	}

	firstIdentity, _ := FallbackIdentity("fallback-first")
	_, firstLease, _, _, found, err := store.LeaseDetails("fallback-first")
	if err != nil || !found {
		t.Fatalf("first fallback lease found = %t, error = %v", found, err)
	}
	if err := store.Release("fallback-first", firstLease.Target); err != nil {
		t.Fatal(err)
	}
	if len(cleaned) != 1 || cleaned[0] != firstPath {
		t.Fatalf("cleaned generations = %v, want [%q]", cleaned, firstPath)
	}
	_, thirdBytes, err := acquire("fallback-third", 8*pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if thirdBytes != 8*pageSize {
		t.Fatalf("third fallback allocation after release = %d, want %d", thirdBytes, 8*pageSize)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), firstIdentity)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released fallback object remains in the object namespace: %v", err)
	}
}

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

func TestFallbackObjectIsExclusiveAndDiscardedAfterLastRelease(t *testing.T) {
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

	identity, err := FallbackIdentity("fallback-volume")
	if err != nil {
		t.Fatal(err)
	}
	otherIdentity, err := FallbackIdentity("other-fallback-volume")
	if err != nil {
		t.Fatal(err)
	}
	if identity == otherIdentity {
		t.Fatal("different fallback volume IDs share an identity")
	}
	policy := Policy{SharingPolicy: SharingPolicyExclusive, DiscardOnLastRelease: true, NoExec: true}
	target := filepath.Join(t.TempDir(), "mount")
	lease := Lease{ID: "fallback-volume", Target: target, NoExec: true}
	source, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: lease, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "private"), []byte("fallback data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "second-fallback-lease", Target: filepath.Join(t.TempDir(), "second")},
		Policy:   policy,
	}); !errors.Is(err, ErrExclusivePolicyConflict) {
		t.Fatalf("second lease error = %v, want exclusive policy conflict", err)
	}
	if err := store.Release(lease.ID, lease.Target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), identity)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released fallback object remains in the object namespace: %v", err)
	}
	if _, found, err := store.ReleaseTarget(lease.ID); err != nil || found {
		t.Fatalf("released fallback lease found = %t, error = %v", found, err)
	}
	newSource, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: lease, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if newSource == source {
		t.Fatal("fallback object reused the generation from the previous volume lifecycle")
	}
	if _, err := os.Stat(filepath.Join(newSource, "private")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("previous fallback data is visible after a new lifecycle: %v", err)
	}
}

func TestFallbackRecoveryDiscardsUnmountedOrphan(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store, err := NewStore(root, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := FallbackIdentity("fallback-orphan")
	if err != nil {
		t.Fatal(err)
	}
	policy := Policy{SharingPolicy: SharingPolicyExclusive, DiscardOnLastRelease: true, NoExec: true}
	lease := Lease{ID: "fallback-orphan", Target: filepath.Join(t.TempDir(), "mount"), NoExec: true}
	if _, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: lease, Policy: policy}); err != nil {
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
	if err := recovered.RecoverLeases(func(source string, recoveredLease Lease, recoveredPolicy Policy) (bool, error) {
		if source != filepath.Join(root, identity, "generations", recoveredLease.Generation) || recoveredLease.ID != lease.ID || recoveredPolicy != policy {
			t.Errorf("recovery verifier arguments = (%q, %+v, %+v)", source, recoveredLease, recoveredPolicy)
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, identity)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unmounted fallback orphan remains after recovery: %v", err)
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
	meta, err := recovered.readMetadata(filepath.Join(root, identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.FormatVersion != storeFormatVersion || len(meta.Leases) != 1 || meta.Leases[0].Preparing {
		t.Fatalf("persisted recovered metadata = %+v, want format 1 and published lease", meta)
	}
}

func TestZeroRetentionDisablesTTLCollectionButKeepsPressureEligibility(t *testing.T) {
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
	candidates := store.unusedPressureCandidates(nil)
	store.mu.Unlock()
	if !slices.ContainsFunc(candidates, func(candidate pressureCandidate) bool { return candidate.identity == identity }) {
		t.Fatal("zero-retention cache was excluded from pressure reclaim")
	}
}

func TestPressureVictimsSkipsMetadataScanWhenPressureIsOff(t *testing.T) {
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
	identity := stableIdentity("pressure-off-metadata-scan")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "pressure-off", Target: filepath.Join(t.TempDir(), "target")},
		Policy:   Policy{EvictRunning: true},
	}); err != nil {
		t.Fatal(err)
	}
	metadataPath := filepath.Join(store.Root(), identity, metadataName)
	if err := os.WriteFile(metadataPath, []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if victims, err := store.PressureVictims(); err != nil || len(victims) != 0 {
		t.Fatalf("pressure-off victims = %+v, error = %v", victims, err)
	}
	if _, degraded := store.degraded[identity]; degraded {
		t.Fatal("pressure-off victim lookup read and degraded cache metadata")
	}
}

func TestPressureDoesNotRetirePreparingLeaseBeforePublishCommit(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "pressure-used-block"), make([]byte, 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(root, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	identity := stableIdentity("preparing-pressure-lease")
	publishedTarget := filepath.Join(t.TempDir(), "published-mount")
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "published", Target: publishedTarget},
		Policy:   Policy{EvictRunning: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitPublish("published", publishedTarget); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "mount")
	options := AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "preparing", Target: target},
		Policy:   Policy{EvictRunning: true},
	}
	var source string
	for attempt := range 2 {
		barrier := startAcquireBarrier(store, options)
		t.Cleanup(barrier.release)
		result := <-barrier.result
		if result.err != nil {
			t.Fatal(result.err)
		}
		if attempt == 0 && !result.created {
			t.Fatal("initial publish did not create a new lease")
		}
		if attempt == 1 && (result.created || result.source != source) {
			t.Fatalf("same-volume retry = (%q, %t), want existing source %q", result.source, result.created, source)
		}
		if attempt == 0 {
			source = result.source
			store.pressure = PressureConfig{
				HighFreePercent:      100,
				LowFreePercent:       99,
				HighInodeFreePercent: 100,
				LowInodeFreePercent:  99,
			}
			store.pressureActive = true
		}
		assertPreparingGenerationProtected(t, store, identity, result.source, options.Lease.ID, target)
		barrier.release()
		if err := store.CommitPublish(options.Lease.ID, target); err != nil {
			t.Fatal(err)
		}
	}
	victims, err := store.PressureVictims()
	if err != nil {
		t.Fatal(err)
	}
	if len(victims) != 2 {
		t.Fatalf("pressure victims after all leases are published = %+v, want both shared-generation leases", victims)
	}
}

type acquireBarrierResult struct {
	source  string
	created bool
	err     error
}

type acquireBarrier struct {
	result chan acquireBarrierResult
	resume chan struct{}
	done   chan struct{}
	once   sync.Once
}

func startAcquireBarrier(store *Store, options AcquireOptions) *acquireBarrier {
	barrier := &acquireBarrier{
		result: make(chan acquireBarrierResult, 1),
		resume: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go func() {
		defer close(barrier.done)
		source, created, err := store.Acquire(options)
		barrier.result <- acquireBarrierResult{source: source, created: created, err: err}
		<-barrier.resume
	}()
	return barrier
}

func (barrier *acquireBarrier) release() {
	barrier.once.Do(func() { close(barrier.resume) })
	<-barrier.done
}

func assertPreparingGenerationProtected(t *testing.T, store *Store, identity, source, leaseID, target string) {
	t.Helper()
	victims, err := store.PressureVictims()
	if err != nil {
		t.Fatal(err)
	}
	if len(victims) != 0 {
		t.Fatalf("preparing lease pressure victims = %+v, want none", victims)
	}
	if !store.pressureActive {
		t.Fatal("barrier test did not keep pressure active")
	}
	_, lease, currentSource, _, found, err := store.LeaseDetails(leaseID)
	if err != nil || !found || !lease.Preparing || currentSource != source || lease.Target != target {
		t.Fatalf("preparing lease details = (%+v, %q, %t, %v), want preparing source %q", lease, currentSource, found, err, source)
	}
	meta, err := store.readMetadata(filepath.Join(store.Root(), identity))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Generation != filepath.Base(source) || len(meta.Retired) != 0 {
		t.Fatalf("preparing generation state = (%q, %d retired), want unchanged generation %q", meta.Generation, len(meta.Retired), filepath.Base(source))
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
			if test.wantQuarantine {
				if _, err := os.Stat(entry); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unmounted damaged object was not detached: %v", err)
				}
				assertProjectIDReservedUntilTrashRemoval(t, store, "quota-during-quarantine", projectID, trashGate)
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

func TestRecoverMissingMetadataQuarantinesOnlyWhenUnmounted(t *testing.T) {
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
	if mounted || inspectErr {
		assertDegradedObjectRetained(t, store, identity, source, target, mounted, inspectErr)
	}
	assertMissingMetadataQuarantined(t, store, root, identity, name, projectID, trashGate)
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

func assertDegradedObjectRetained(t *testing.T, store *Store, identity, source, target string, mounted, inspectErr bool) {
	t.Helper()
	entry := filepath.Join(store.Root(), identity)
	if _, err := os.Stat(entry); err != nil {
		t.Fatalf("active or uncertain object was removed: %v", err)
	}
	if !errors.Is(store.MetadataError(), ErrDegradedMetadata) {
		t.Fatalf("metadata health error = %v, want degraded metadata", store.MetadataError())
	}
	foundIdentity, foundSource, found, err := store.FindDegradedGenerationForTarget(target, func(candidateSource, candidateTarget string) (bool, error) {
		return candidateSource == source && candidateTarget == target, nil
	})
	if err != nil || !found || foundIdentity != identity || foundSource != source {
		t.Fatalf("degraded source lookup = (%q, %q, %v, %v)", foundIdentity, foundSource, found, err)
	}
	err = store.CleanupDegradedObject(identity, func(string) (bool, error) {
		if inspectErr {
			return false, errors.New("mount inspection failed")
		}
		return mounted, nil
	})
	if inspectErr && err == nil {
		t.Fatal("degraded cleanup succeeded when mount state was unknown")
	}
	if !inspectErr && err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(entry); err != nil {
		t.Fatalf("degraded object was removed while a generation remained mounted: %v", err)
	}
	if err := store.CleanupDegradedObject(identity, func(string) (bool, error) { return false, nil }); err != nil {
		t.Fatal(err)
	}
}

func assertMissingMetadataQuarantined(t *testing.T, store *Store, root, identity, name string, projectID uint32, trashGate *trashRemovalGate) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(root, identity)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fully unmounted object was not quarantined: %v", err)
	}
	assertProjectIDReservedUntilTrashRemoval(t, store, "missing-metadata-reservation-"+name, projectID, trashGate)
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
	store.removeTrashEntry = func(path string) error {
		select {
		case gate.started <- struct{}{}:
		default:
		}
		<-gate.release
		return store.removeAll(path)
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
	if err := store.ensureDirectory(filepath.Join(entry, "generations")); err != nil {
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

func TestDamagedProjectRegistryDisablesOnlyQuotaAllocation(t *testing.T) {
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
			store, err := NewStore(root, StoreOptions{})
			if err != nil {
				t.Fatalf("open store with damaged project registry: %v", err)
			}
			if !errors.Is(store.MetadataError(), ErrDegradedMetadata) {
				t.Fatalf("storage health error = %v, want degraded metadata", store.MetadataError())
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
			if _, _, _, err := store.QuotaState(identity, 1024); err == nil {
				t.Fatal("quota allocation succeeded with a damaged project registry")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewStore(root, StoreOptions{})
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
			if _, _, _, err := reopened.QuotaState(otherIdentity, 1024); err == nil {
				t.Fatal("quota allocation resumed after reopening damaged registry")
			}
			data, err := os.ReadFile(registryPath)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != string(test.data) {
				t.Fatalf("damaged project registry was overwritten: %q", data)
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
	if _, err := os.Stat(filepath.Join(root, identities[1])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unknown-version object was not quarantined: %v", err)
	}
}

func TestLegacyStoreDocumentVersionsAreAcceptedAndUpgradedOnWrite(t *testing.T) {
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
	if _, _, _, _, found, err := reopened.LeaseDetails("legacy-format"); err != nil || !found {
		t.Fatalf("legacy metadata lease = found %t, error %v", found, err)
	}
	if err := reopened.CommitPublish("legacy-format", target); err != nil {
		t.Fatalf("upgrade legacy metadata on write: %v", err)
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
	assertDocumentFormatVersion(t, filepath.Join(root, identity, metadataName), storeFormatVersion)
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
	_, _, err = store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: firstLeaseID, Target: target},
		Policy:   policy,
	})
	if !errors.Is(err, ErrLeaseGenerationRetired) {
		t.Fatalf("same lease retry = %v, want ErrLeaseGenerationRetired", err)
	}
	if _, _, source, _, found, err := store.LeaseDetails(firstLeaseID); err != nil || !found || source != oldPath {
		t.Fatalf("retired lease details = source %q, found %v, error %v; want %q", source, found, err, oldPath)
	}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "second-exclusive", Target: filepath.Join(t.TempDir(), "second")},
		Policy:   policy,
	}); !errors.Is(err, ErrPressureActive) {
		t.Fatalf("second active lease error = %v, want ErrPressureActive", err)
	}
	if err := store.Release(firstLeaseID, target); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: "second-exclusive", Target: filepath.Join(t.TempDir(), "second")},
		Policy:   policy,
	}); !errors.Is(err, ErrPressureActive) {
		t.Fatalf("new lease after old generation release error = %v, want ErrPressureActive", err)
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
	trash, err := store.readDir(filepath.Join(store.Root(), trashDirectoryName))
	if err != nil {
		t.Fatal(err)
	}
	if len(trash) != len(identities) {
		t.Fatalf("trash entries before pressure reclaim = %d, want %d", len(trash), len(identities))
	}
	store.pressure = PressureConfig{HighFreePercent: 100, LowFreePercent: 99}
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
	store.removeTrashEntry = func(path string) error {
		if filepath.Base(path) == "000" {
			return blockedErr
		}
		return store.removeAll(path)
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

func TestPressureVictimsRemainAvailableWhenTrashDeletionFails(t *testing.T) {
	t.Parallel()
	store, _, _, _, _ := pressureLease(t, "blocked-trash-victim", Policy{EvictRunning: true}, 1)
	if err := store.CleanupTrash(t.Context()); err != nil {
		t.Fatalf("wait for initial trash cleanup: %v", err)
	}
	blockedErr := errors.New("trash entry is temporarily unavailable")
	store.removeTrashEntry = func(path string) error {
		if filepath.Base(path) == "blocked" {
			return blockedErr
		}
		return store.removeAll(path)
	}
	blockedEntry := filepath.Join(store.Root(), trashDirectoryName, "blocked")
	if err := os.Mkdir(blockedEntry, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := store.ReclaimPressure(t.Context()); !errors.Is(err, ErrPressureReclaimIncomplete) {
		t.Fatalf("pressure reclaim error = %v, want incomplete reclaim", err)
	}
	victims, err := store.PressureVictims()
	if err != nil {
		t.Fatal(err)
	}
	if len(victims) != 1 || victims[0].ID != firstLeaseID {
		t.Fatalf("pressure victims with undeleted trash = %+v, want active lease", victims)
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
	if err := store.BeginPublish(firstLeaseID, target); !errors.Is(err, ErrLeaseGenerationRetired) {
		t.Fatalf("begin publish for retired lease = %v, want ErrLeaseGenerationRetired", err)
	}
	if _, _, err := store.Acquire(AcquireOptions{
		Identity: identity,
		Lease:    Lease{ID: firstLeaseID, Target: target},
		Policy:   policy,
	}); !errors.Is(err, ErrLeaseGenerationRetired) {
		t.Fatalf("retry acquire for retired lease = %v, want ErrLeaseGenerationRetired", err)
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
	if _, _, err := store.Acquire(AcquireOptions{Identity: identity, Lease: Lease{ID: "replacement", Target: filepath.Join(t.TempDir(), "replacement")}, Policy: policy}); !errors.Is(err, ErrPressureActive) {
		t.Fatalf("replacement lease while pressure remains active error = %v, want ErrPressureActive", err)
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
	store.pressure = PressureConfig{}
	store.pressureActive = false
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
	if err := store.CommitPublish(firstLeaseID, target); err != nil {
		t.Fatal(err)
	}
	store.pressure = PressureConfig{HighFreePercent: 100, LowFreePercent: 99}
	store.pressureActive = true
	return store, identity, policy, target, oldPath
}
