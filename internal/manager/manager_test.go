package manager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/walnuts1018/cache-csi-driver/internal/cache"
)

func TestManagerRecoversRuntimeDegradedCacheWithoutKubernetesClient(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store, err := cache.NewStore(root, cache.StoreOptions{})
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
	mounted := true
	inspectionError := false
	manager := New(store, Options{InspectMount: func(checkedSource string, lease cache.Lease, policy cache.Policy) (bool, error) {
		inspected++
		if checkedSource != source || lease != (cache.Lease{}) || policy != (cache.Policy{}) {
			t.Errorf("degraded mount inspection input = (%q, %+v, %+v), want (%q, empty, empty)", checkedSource, lease, policy, source)
		}
		if inspectionError {
			return false, errors.New("mount inspection failed")
		}
		return mounted, nil
	}})
	manager.recoverDegraded(t.Context())
	if _, err := os.Stat(filepath.Join(root, identity)); err != nil {
		t.Fatalf("mounted degraded object was removed: %v", err)
	}
	mounted = false
	inspectionError = true
	manager.recoverDegraded(t.Context())
	if _, err := os.Stat(filepath.Join(root, identity)); err != nil {
		t.Fatalf("object with unknown mount state was removed: %v", err)
	}
	inspectionError = false
	manager.recoverDegraded(t.Context())
	if inspected != 3 {
		t.Fatalf("degraded mount inspections = %d, want 3", inspected)
	}
	if _, err := os.Stat(filepath.Join(root, identity)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime-degraded object remains after pressure tick: %v", err)
	}
	trash, err := os.ReadDir(filepath.Join(root, ".trash"))
	if err != nil {
		t.Fatal(err)
	}
	if len(trash) != 0 {
		t.Fatalf("trash entries after pressure tick = %d, want 0", len(trash))
	}
}
