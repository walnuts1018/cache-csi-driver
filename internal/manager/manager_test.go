package manager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

const podResourceName = "pods"

func TestEvictionRetryDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		attempts int
		want     time.Duration
	}{
		{name: "server retry after", err: apierrors.NewTooManyRequests("rate limited", 17), attempts: 4, want: 17 * time.Second},
		{name: "first failure", err: errors.New("temporary API failure"), attempts: 1, want: 5 * time.Second},
		{name: "exponential delay", err: errors.New("temporary API failure"), attempts: 3, want: 20 * time.Second},
		{name: "zero retry after uses backoff", err: apierrors.NewTooManyRequests("rate limited", 0), attempts: 2, want: 10 * time.Second},
		{name: "maximum delay", err: errors.New("temporary API failure"), attempts: 20, want: 5 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := evictionRetryDelay(tt.err, tt.attempts); got != tt.want {
				t.Fatalf("evictionRetryDelay() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestAcceptedEvictionWaitsBeforeRetry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.September, 29, 0, 0, 0, 0, time.UTC)
	state := nextEvictionState(now, evictionState{attempts: 3}, nil)
	if state.gone || state.attempts != 0 {
		t.Fatalf("accepted eviction state = %+v, want pending cooldown with reset attempts", state)
	}
	if want := now.Add(acceptedEvictionDelay); !state.nextAttempt.Equal(want) {
		t.Fatalf("accepted eviction retry time = %s, want %s", state.nextAttempt, want)
	}
	if !state.shouldSkip(now) || state.shouldSkip(state.nextAttempt) {
		t.Fatalf("accepted eviction cooldown did not suppress retries until its deadline: %+v", state)
	}
}

func TestNotFoundEvictionStateWaitsForVictimPruning(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.September, 29, 0, 0, 0, 0, time.UTC)
	state := nextEvictionState(now, evictionState{}, apierrors.NewNotFound(schema.GroupResource{Resource: podResourceName}, "gone"))
	if !state.gone || !state.shouldSkip(now) {
		t.Fatalf("not-found eviction state = %+v, want terminal state until victim pruning", state)
	}
}

func TestCriticalForceDeleteUsesUIDPreconditionAndCooldown(t *testing.T) {
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
	client := kubefake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cache-user", Namespace: "workloads", UID: types.UID("pod-uid")}})
	manager := New(store, Options{Client: client, AllowForceDelete: true})
	lease := cache.Lease{Namespace: "workloads", PodName: "cache-user", PodUID: "pod-uid"}
	manager.evictWithBackoff(t.Context(), cache.PressureVictim{Lease: lease})
	manager.evictWithBackoff(t.Context(), cache.PressureVictim{Lease: lease, ForceDelete: true})
	manager.evictWithBackoff(t.Context(), cache.PressureVictim{Lease: lease, ForceDelete: true})
	actions := client.Actions()
	if len(actions) != 2 {
		t.Fatalf("eviction escalation actions = %d, want Eviction plus one Delete within cooldown", len(actions))
	}
	if actions[0].GetVerb() != "create" || actions[0].GetResource().Resource != podResourceName || actions[0].GetSubresource() != "eviction" {
		t.Fatalf("initial pressure action = %s %s/%s, want PDB-respecting Pod Eviction", actions[0].GetVerb(), actions[0].GetResource().Resource, actions[0].GetSubresource())
	}
	deleteAction, ok := actions[1].(kubetesting.DeleteAction)
	if !ok {
		t.Fatalf("critical escalation action = %T, want Pod delete", actions[1])
	}
	if actions[1].GetResource().Resource != podResourceName || actions[1].GetVerb() != "delete" {
		t.Fatalf("critical action = %s %s, want delete pods", actions[1].GetVerb(), actions[1].GetResource().Resource)
	}
	options := deleteAction.GetDeleteOptions()
	if options.GracePeriodSeconds == nil || *options.GracePeriodSeconds != 0 {
		t.Fatalf("grace period = %v, want zero", options.GracePeriodSeconds)
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "pod-uid" {
		t.Fatalf("delete UID precondition = %+v, want pod-uid", options.Preconditions)
	}
}

func TestPressureReclaimsRuntimeDegradedCacheWithoutKubernetesClient(t *testing.T) {
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
	manager.pressure(t.Context())
	if _, err := os.Stat(filepath.Join(root, identity)); err != nil {
		t.Fatalf("mounted degraded object was removed: %v", err)
	}
	mounted = false
	inspectionError = true
	manager.pressure(t.Context())
	if _, err := os.Stat(filepath.Join(root, identity)); err != nil {
		t.Fatalf("object with unknown mount state was removed: %v", err)
	}
	inspectionError = false
	manager.pressure(t.Context())
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

func TestRecoverIncludesFallbackStore(t *testing.T) {
	t.Parallel()
	mainStore, err := cache.NewStore(filepath.Join(t.TempDir(), "cache"), cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mainStore.Close(); err != nil {
			t.Error(err)
		}
	})

	fallbackRoot := filepath.Join(t.TempDir(), "fallback")
	fallbackStore, err := cache.NewStore(fallbackRoot, cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := cache.FallbackIdentity("fallback-recovery")
	if err != nil {
		t.Fatal(err)
	}
	policy := cache.Policy{SharingPolicy: cache.SharingPolicyExclusive, DiscardOnLastRelease: true, NoExec: true}
	lease := cache.Lease{ID: "fallback-recovery", Target: filepath.Join(t.TempDir(), "mount"), NoExec: true}
	if _, _, err := fallbackStore.Acquire(cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy}); err != nil {
		t.Fatal(err)
	}
	if err := fallbackStore.Close(); err != nil {
		t.Fatal(err)
	}

	fallbackStore, err = cache.NewStore(fallbackRoot, cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fallbackStore.Close(); err != nil {
			t.Error(err)
		}
	})
	manager := New(mainStore, Options{
		FallbackStore: fallbackStore,
		InspectMount: func(string, cache.Lease, cache.Policy) (bool, error) {
			return false, nil
		},
	})
	if err := manager.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(fallbackRoot, identity)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unmounted fallback object remains after manager recovery: %v", err)
	}
}
