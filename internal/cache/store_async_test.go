package cache

import (
	"testing"
)

func TestAsyncStoreBecomesReadyAfterLeaseRecovery(t *testing.T) {
	t.Parallel()

	store, err := NewStoreAsync(t.TempDir(), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close cache store: %v", err)
		}
	})
	if store.Ready() {
		t.Fatal("store is ready before lease recovery")
	}
	if err := store.WaitForIndexes(t.Context()); err != nil {
		t.Fatalf("wait for index initialization: %v", err)
	}
	if store.Ready() {
		t.Fatal("store is ready before lease recovery")
	}
	if err := store.RecoverLeases(func(string, Lease, Policy) (bool, error) { return false, nil }); err != nil {
		t.Fatalf("recover leases: %v", err)
	}
	if !store.Ready() {
		t.Fatal("store is not ready after lease recovery")
	}
}
