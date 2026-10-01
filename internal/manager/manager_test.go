package manager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/nodehealth"
)

func TestManagerRecoversRuntimeDegradedCacheWithoutKubernetesClient(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mounted := true
	store, err := cache.NewStore(root, cache.StoreOptions{
		IsGenerationMounted: func(string) (bool, error) { return mounted, nil },
	})
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
	identity, err := cache.Identity("namespace-uid", "class", "class-uid", "key", "v1")
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	leaseID := "runtime-corruption-lease"
	source, _, err := store.Acquire(cache.AcquireOptions{Identity: identity, Lease: cache.Lease{ID: leaseID, Target: target}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, identity, ".cache-csi.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, err := store.LeaseDetails(leaseID); !errors.Is(err, cache.ErrDegradedMetadata) {
		t.Fatalf("runtime metadata lookup error = %v, want degraded metadata", err)
	}

	var inspected int
	manager := New(store, Options{InspectMount: func(checkedSource string, lease cache.Lease, policy cache.Policy) (bool, error) {
		inspected++
		if checkedSource != source || lease != (cache.Lease{}) || policy != (cache.Policy{}) {
			t.Errorf("degraded mount inspection input = (%q, %+v, %+v), want (%q, empty, empty)", checkedSource, lease, policy, source)
		}
		return mounted, nil
	}})
	manager.recoverDegraded(t.Context())
	if _, err := os.Stat(filepath.Join(root, identity)); err != nil {
		t.Fatalf("fresh canonical object was not recreated: %v", err)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old mounted generation remained at its canonical path: %v", err)
	}
	newSource, _, err := store.Acquire(cache.AcquireOptions{Identity: identity, Lease: cache.Lease{ID: "fresh-after-corruption", Target: filepath.Join(t.TempDir(), "fresh")}})
	if err != nil {
		t.Fatalf("acquire fresh generation while old mount remains: %v", err)
	}
	if newSource == source {
		t.Fatal("new Pod reused the quarantined generation")
	}
	if inspected != 1 {
		t.Fatalf("degraded mount inspections = %d, want one quarantine inspection", inspected)
	}
	trash, err := os.ReadDir(filepath.Join(root, ".trash"))
	if err != nil {
		t.Fatal(err)
	}
	if len(trash) != 1 {
		t.Fatalf("trash entries while old generation is mounted = %d, want 1", len(trash))
	}
	mounted = false
	if err := store.CleanupTrash(t.Context()); err != nil {
		t.Fatal(err)
	}
	trash, err = os.ReadDir(filepath.Join(root, ".trash"))
	if err != nil {
		t.Fatal(err)
	}
	if len(trash) != 0 {
		t.Fatalf("trash entries after the old mount ended = %d, want 0", len(trash))
	}
}

func TestBackendHealthUsesSubsystemProbesAndRecoversAutomatically(t *testing.T) {
	t.Parallel()

	store, err := cache.NewStore(t.TempDir(), cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	readOnly := false
	var mountError error
	var quotaError error
	health := nodehealth.NewTracker()
	health.ClearCondition(nodehealth.SubsystemStartup)
	manager := New(store, Options{
		Health: health,
		FilesystemReadOnly: func(string) (bool, error) {
			return readOnly, nil
		},
		MountProbe: func(context.Context) error {
			return mountError
		},
		QuotaRequired: true,
		QuotaProbe: func(context.Context, string) error {
			return quotaError
		},
	})

	if err := manager.backendHealth(t.Context(), true); err != nil {
		t.Fatalf("initial backend health probe: %v", err)
	}
	if snapshot := health.Current(); snapshot.Phase != nodehealth.PhaseReady {
		t.Fatalf("initial health = %+v, want Ready", snapshot)
	}

	mountError = errors.New("mount API unavailable")
	if err := manager.backendHealth(t.Context(), true); err == nil {
		t.Fatal("health refresh with mount failure succeeded")
	}
	if snapshot := health.Current(); snapshot.Phase != nodehealth.PhaseUnavailable || snapshot.Evict {
		t.Fatalf("mount failure health = %+v, want unavailable without eviction", snapshot)
	}
	mountError = nil
	manager.refreshHealth(t.Context())
	if snapshot := health.Current(); snapshot.Phase != nodehealth.PhaseReady {
		t.Fatalf("health after mount recovery = %+v, want Ready", snapshot)
	}

	quotaError = errors.New("quota allocation unavailable")
	if err := manager.backendHealth(t.Context(), true); err == nil {
		t.Fatal("backend health with quota failure succeeded")
	}
	if snapshot := health.Current(); snapshot.Phase != nodehealth.PhaseUnavailable || snapshot.Evict {
		t.Fatalf("quota failure health = %+v, want unavailable without eviction", snapshot)
	}
	quotaError = nil
	manager.refreshHealth(t.Context())
	if snapshot := health.Current(); snapshot.Phase != nodehealth.PhaseReady {
		t.Fatalf("health after quota recovery = %+v, want Ready", snapshot)
	}

	readOnly = true
	manager.refreshHealth(t.Context())
	if snapshot := health.Current(); snapshot.Phase != nodehealth.PhaseUnavailable || !snapshot.Evict {
		t.Fatalf("read-only filesystem health = %+v, want unavailable with eviction", snapshot)
	}
	readOnly = false
	manager.refreshHealth(t.Context())
	if snapshot := health.Current(); snapshot.Phase != nodehealth.PhaseReady {
		t.Fatalf("health after filesystem recovery = %+v, want Ready", snapshot)
	}
}
