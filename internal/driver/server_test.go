package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
	"github.com/walnuts1018/cache-csi-driver/internal/nodehealth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	testDefault            = "default"
	testPodName            = "pod"
	testPodUID             = "pod-uid"
	testServiceAccountName = "builder"
)

func TestNodeGetCapabilitiesAdvertisesImplementedRPCs(t *testing.T) {
	t.Parallel()

	response, err := (*Server)(nil).NodeGetCapabilities(t.Context(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}

	want := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_GET_VOLUME_HEALTH,
		csi.NodeServiceCapability_RPC_GET_STORAGE_HEALTH,
	}
	if len(response.GetCapabilities()) != len(want) {
		t.Fatalf("capability count = %d, want %d", len(response.GetCapabilities()), len(want))
	}
	for index, capability := range response.GetCapabilities() {
		if got := capability.GetRpc().GetType(); got != want[index] {
			t.Errorf("capability[%d] = %s, want %s", index, got, want[index])
		}
	}
}

func TestNodePublishRejectsUnsupportedFilesystemAndMountFlags(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		configure func(*csi.VolumeCapability_MountVolume)
	}{
		{
			name: "filesystem type",
			configure: func(mount *csi.VolumeCapability_MountVolume) {
				mount.FsType = "xfs"
			},
		},
		{
			name: "mount flags",
			configure: func(mount *csi.VolumeCapability_MountVolume) {
				mount.MountFlags = []string{"noexec"}
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server, _, _ := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
			request := newPublishRequest(t, server.options.KubeletRoot, test.name)
			test.configure(request.GetVolumeCapability().GetMount())

			if _, err := server.NodePublishVolume(t.Context(), request); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("publish with unsupported %s error = %v, want InvalidArgument", test.name, err)
			}
		})
	}
}

func TestNodePublishValidatesTargetPodIdentityAndShape(t *testing.T) {
	t.Parallel()

	server, _, _ := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	validTarget := filepath.Join(server.options.KubeletRoot, "pods", testPodUID, "volumes", "kubernetes.io~csi", "volume-name", "mount")
	cases := []struct {
		name       string
		target     string
		podUID     string
		wantReject bool
	}{
		{
			name:   "valid target without requiring kubelet volume ID reconstruction",
			target: validTarget,
			podUID: testPodUID,
		},
		{
			name:       "target belongs to another pod",
			target:     filepath.Join(server.options.KubeletRoot, "pods", "other-pod", "volumes", "kubernetes.io~csi", "volume-name", "mount"),
			podUID:     testPodUID,
			wantReject: true,
		},
		{
			name:       "target uses another volume plugin layout",
			target:     filepath.Join(server.options.KubeletRoot, "pods", testPodUID, "volumes", "other-driver", "volume-name", "mount"),
			podUID:     testPodUID,
			wantReject: true,
		},
		{
			name:       "target is not the kubelet CSI mount directory",
			target:     filepath.Join(server.options.KubeletRoot, "pods", testPodUID, "mount"),
			podUID:     testPodUID,
			wantReject: true,
		},
		{
			name:       "target has a noncanonical path",
			target:     validTarget + "/../mount",
			podUID:     testPodUID,
			wantReject: true,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := newPublishRequest(t, server.options.KubeletRoot, "volume-id")
			request.TargetPath = test.target
			request.VolumeContext["csi.storage.k8s.io/pod.uid"] = test.podUID

			_, err := server.validatePublishRequest(request)
			if test.wantReject {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("publish validation error = %v, want InvalidArgument", err)
				}
			} else if err != nil {
				t.Fatalf("publish validation: %v", err)
			}
		})
	}
}

func TestNodePublishIsIdempotentForAnExistingMatchingMount(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "volume-id")

	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatalf("idempotent publish: %v", err)
	}
	if mounts.mountCalls != 1 {
		t.Fatalf("mount calls = %d, want 1", mounts.mountCalls)
	}
	if _, _, _, _, found, err := store.LeaseDetails(request.GetVolumeId()); err != nil || !found {
		t.Fatalf("cache lease found = %t, error = %v; want an active lease", found, err)
	}
	state := mounts.mounts[request.GetTargetPath()]
	state.readOnly = true
	mounts.mounts[request.GetTargetPath()] = state
	request.Readonly = true
	if _, err := server.NodePublishVolume(t.Context(), request); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("publish with a changed readonly flag error = %v, want AlreadyExists", err)
	}
}

func TestNodePublishResolvesServiceAccountFromKubeletVolumeContext(t *testing.T) {
	t.Parallel()

	server, _, _ := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "service-account-context")
	request.VolumeContext["csi.storage.k8s.io/serviceAccount.name"] = "custom-builder"

	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if got := server.resolver.(*testResolver).serviceAccountName; got != "custom-builder" {
		t.Fatalf("resolver ServiceAccount name = %q, want custom-builder from podInfoOnMount", got)
	}
}

func TestNodePublishRejectsReaderOnlyAccessMode(t *testing.T) {
	t.Parallel()

	server, _, _ := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "reader-only-readonly")
	request.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
	request.Readonly = true
	if _, err := server.NodePublishVolume(t.Context(), request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reader-only publish error = %v, want InvalidArgument", err)
	}
}

func TestNodePublishWaitsForCacheStoreRecovery(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "store-not-ready")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	unreadyStore, err := cache.NewStoreAsync(store.Root(), cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server.store = unreadyStore
	t.Cleanup(func() {
		if err := unreadyStore.Close(); err != nil {
			t.Errorf("close asynchronously initialized cache store: %v", err)
		}
	})
	if err := unreadyStore.WaitForIndexes(t.Context()); err != nil {
		t.Fatalf("wait for cache store indexing: %v", err)
	}
	if unreadyStore.Ready() {
		t.Fatal("cache store became ready before lease recovery")
	}

	if _, err := server.NodePublishVolume(t.Context(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("publish before store recovery error = %v, want Unavailable", err)
	}
	if mounts.mountCalls != 0 {
		t.Fatalf("mount calls = %d, want no mount before store recovery", mounts.mountCalls)
	}
	if _, err := os.Lstat(request.GetTargetPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target path stat error = %v, want not-exist", err)
	}
}

func TestNodePublishPassesReadonlyAndNoExecMountOptions(t *testing.T) {
	t.Parallel()

	server, mounts, _ := newTestServer(t, cachev1alpha1.CacheClassSpec{NoExec: true}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "restricted-volume")
	request.Readonly = true

	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	state, mounted := mounts.mounts[request.GetTargetPath()]
	if !mounted || !state.readOnly || !state.noExec {
		t.Fatalf("published mount = %+v, present=%t; want readonly and noexec", state, mounted)
	}
}

func TestNodePublishRetryUsesPersistedQuotaPolicy(t *testing.T) {
	t.Parallel()

	spec := cachev1alpha1.CacheClassSpec{
		Storage: cachev1alpha1.StoragePolicy{
			DefaultMaxBytes: resource.MustParse("1Mi"),
		},
	}
	server, mounts, _ := newTestServer(t, spec, nil)
	quota := &recordingQuota{}
	server.quota = quota
	request := newPublishRequest(t, server.options.KubeletRoot, "quota-policy-retry")
	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	resolver := server.resolver.(*testResolver)
	resolver.spec.Storage.DefaultMaxBytes = resource.MustParse("2Mi")
	delete(mounts.mounts, request.GetTargetPath())
	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatalf("retry after CacheClass quota change: %v", err)
	}

	wantLimit := resource.MustParse("1Mi")
	if len(quota.limits) != 1 || quota.limits[0] != wantLimit.Value() {
		t.Fatalf("configured quota limits = %v, want only the persisted 1Mi policy", quota.limits)
	}
}

func TestNodePublishRollsBackLeaseWhenQuotaConfigurationFails(t *testing.T) {
	t.Parallel()

	quotaFailure := errors.New("quota configuration failed")
	spec := cachev1alpha1.CacheClassSpec{
		NoExec: true,
		Storage: cachev1alpha1.StoragePolicy{
			DefaultMaxBytes: resource.MustParse("1Mi"),
		},
	}
	server, mounts, store := newTestServer(t, spec, quotaFailure)
	request := newPublishRequest(t, server.options.KubeletRoot, "quota-volume")
	request.VolumeContext["maxBytes"] = "1Mi"

	_, err := server.NodePublishVolume(t.Context(), request)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("publish after quota configuration failure error = %v, want Unavailable", err)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(request.GetVolumeId()); leaseErr != nil || found {
		t.Fatalf("cache lease found = %t, error = %v; want the failed lease rolled back", found, leaseErr)
	}
	if mounts.mountCalls != 0 {
		t.Fatalf("mount calls = %d, want no mount after quota configuration failure", mounts.mountCalls)
	}
	if snapshot := server.options.Health.Current(); snapshot.Phase != "Unavailable" || snapshot.Reason != "CacheQuotaUnavailable" {
		t.Fatalf("cache node health = %+v, want CacheQuotaUnavailable", snapshot)
	}
}

func TestNodePublishRollsBackLeaseWhenDetachedMountSetupFails(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "mount-cleanup-volume")
	mounts.mountErr = errors.New("mount attributes failed")

	_, err := server.NodePublishVolume(t.Context(), request)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("publish error = %v, want Unavailable", err)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(request.GetVolumeId()); leaseErr != nil || found {
		t.Fatalf("cache lease found = %t, error = %v; want lease rollback before mount attachment", found, leaseErr)
	}
	if _, mounted := mounts.mounts[request.GetTargetPath()]; mounted {
		t.Fatal("mount should not be attached when detached mount setup fails")
	}
}

func TestNodePublishRejectsExclusiveSharingConflict(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{SharingPolicy: cachev1alpha1.SharingPolicyExclusive, NoExec: true}, nil)
	first := newPublishRequest(t, server.options.KubeletRoot, "exclusive-first")
	if _, err := server.NodePublishVolume(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	second := newPublishRequest(t, server.options.KubeletRoot, "exclusive-second")

	if _, err := server.NodePublishVolume(t.Context(), second); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("publish with conflicting exclusive identity error = %v, want FailedPrecondition", err)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(first.GetVolumeId()); leaseErr != nil || !found {
		t.Fatalf("first cache lease found = %t, error = %v; want the active lease preserved", found, leaseErr)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(second.GetVolumeId()); leaseErr != nil || found {
		t.Fatalf("second cache lease found = %t, error = %v; want the conflicting lease absent from the shared cache", found, leaseErr)
	}
	if mounts.mountCalls != 1 {
		t.Fatalf("mount calls = %d, want only the first cache mount", mounts.mountCalls)
	}
	if _, mounted := mounts.mounts[second.GetTargetPath()]; mounted {
		t.Fatal("conflicting volume received a different cache mount")
	}
}

func TestNodePublishRecreatesCacheAfterDegradedMetadata(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{NoExec: true}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "degraded-unmounted-volume")
	identity, err := cache.IdentityWithServiceAccount("namespace-uid", "service-account-uid", testDefault, "class-uid", "cache-key", "v1")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Acquire(cache.AcquireOptions{
		Identity: identity,
		Lease: cache.Lease{
			ID:        request.GetVolumeId(),
			Target:    request.GetTargetPath(),
			Namespace: testDefault,
			PodName:   testPodName,
			PodUID:    testPodUID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.Root(), identity, ".cache-csi.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatalf("publish after local metadata corruption: %v", err)
	}
	_, lease, source, policy, found, err := store.LeaseDetails(request.GetVolumeId())
	if err != nil || !found {
		t.Fatalf("cache lease found = %t, error = %v; want a new primary cache lease", found, err)
	}
	if !policy.NoExec || lease.NoExec != policy.NoExec || mounts.mounts[request.GetTargetPath()].source != source {
		t.Fatalf("cache mount source or policy = %q, %+v; want a new noexec generation", mounts.mounts[request.GetTargetPath()].source, policy)
	}
}

func TestNodePublishKeepsVerifiedDegradedMountAvailable(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	volumeName := "degraded-active-volume"
	unpublishRequest, _, _ := prepareDamagedMountedCache(t, server, mounts, store, volumeName)
	request := newPublishRequest(t, server.options.KubeletRoot, volumeName)
	request.VolumeId = unpublishRequest.GetVolumeId()

	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatalf("idempotent publish for verified degraded mount: %v", err)
	}
	if mounts.mountCalls != 0 {
		t.Fatalf("mount calls = %d, want no replacement mount", mounts.mountCalls)
	}
}

func TestNodePublishRejectsQuotaIdentityConflict(t *testing.T) {
	t.Parallel()

	spec := cachev1alpha1.CacheClassSpec{
		SharingPolicy: cachev1alpha1.SharingPolicyExclusive,
		Storage: cachev1alpha1.StoragePolicy{
			DefaultMaxBytes: resource.MustParse("1Mi"),
		},
	}
	server, _, _ := newTestServer(t, spec, nil)
	server.quota = &testQuota{}
	first := newPublishRequest(t, server.options.KubeletRoot, "quota-exclusive-first")
	if _, err := server.NodePublishVolume(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	second := newPublishRequest(t, server.options.KubeletRoot, "quota-exclusive-second")
	if _, err := server.NodePublishVolume(t.Context(), second); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second publish with an exclusive quota conflict error = %v, want FailedPrecondition", err)
	}
	if _, _, _, _, found, err := server.store.LeaseDetails(second.GetVolumeId()); err != nil || found {
		t.Fatalf("second cache lease found = %t, error = %v; want no lease after conflict", found, err)
	}
}

func TestNodePublishRejectsForeignMount(t *testing.T) {
	t.Parallel()

	server, mounts, _ := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "foreign-volume")
	if err := os.MkdirAll(filepath.Dir(request.GetTargetPath()), 0o755); err != nil {
		t.Fatal(err)
	}
	mounts.mounts[request.GetTargetPath()] = testMount{source: filepath.Join(t.TempDir(), "foreign")}

	_, err := server.NodePublishVolume(t.Context(), request)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("publish error = %v, want AlreadyExists", err)
	}
	if mounts.mountCalls != 0 {
		t.Fatalf("mount calls = %d, want 0", mounts.mountCalls)
	}
}

func TestNodeUnpublishUnmountsAndReleasesCacheLease(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "unpublish-volume")
	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	_, err := server.NodeUnpublishVolume(t.Context(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   request.GetVolumeId(),
		TargetPath: request.GetTargetPath(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, mounted := mounts.mounts[request.GetTargetPath()]; mounted {
		t.Fatal("mount remains after unpublish")
	}
	if _, _, _, _, found, err := store.LeaseDetails(request.GetVolumeId()); err != nil || found {
		t.Fatalf("cache lease found = %t, error = %v; want no lease after unpublish", found, err)
	}
	if _, err := os.Lstat(request.GetTargetPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mount target stat error = %v, want not-exist", err)
	}
}

func TestResolverUnavailableDoesNotPublishCache(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	server.resolver = nil
	request := newPublishRequest(t, server.options.KubeletRoot, "api-unavailable-volume")
	request.Readonly = true

	if _, err := server.NodePublishVolume(t.Context(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("publish without a CacheClass resolver error = %v, want Unavailable", err)
	}
	if _, mounted := mounts.mounts[request.GetTargetPath()]; mounted || mounts.mountCalls != 0 {
		t.Fatalf("mount state = %+v, calls = %d; want no mount after resolver failure", mounts.mounts[request.GetTargetPath()], mounts.mountCalls)
	}
	if _, _, _, _, found, err := store.LeaseDetails(request.GetVolumeId()); err != nil || found {
		t.Fatalf("cache lease found = %t, error = %v; want no lease after resolver failure", found, err)
	}
}

func TestResolutionErrorsDoNotPublishCache(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		resolver   ClassResolver
		maxBytes   string
		cancel     bool
		wantStatus codes.Code
	}{
		{
			name:       "CacheClass not found",
			resolver:   &testResolver{err: apierrors.NewNotFound(schema.GroupResource{Resource: "cacheclasses"}, testDefault)},
			wantStatus: codes.FailedPrecondition,
		},
		{
			name:       "CacheClass forbidden",
			resolver:   &testResolver{err: apierrors.NewForbidden(schema.GroupResource{Resource: "cacheclasses"}, testDefault, errors.New("denied"))},
			wantStatus: codes.FailedPrecondition,
		},
		{
			name:       "informer cache has not synced",
			resolver:   &testResolver{err: kube.ErrResolverNotSynced},
			wantStatus: codes.Unavailable,
		},
		{
			name:       "ServiceAccount does not exist",
			resolver:   &testResolver{err: kube.ErrServiceAccountNotFound},
			wantStatus: codes.FailedPrecondition,
		},
		{
			name:       "resolver is unavailable",
			resolver:   nil,
			maxBytes:   "1Mi",
			wantStatus: codes.Unavailable,
		},
		{
			name:       "cancelled request",
			resolver:   nil,
			cancel:     true,
			wantStatus: codes.Canceled,
		},
	}
	for index, test := range cases {
		server, _, _ := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
		server.resolver = test.resolver
		volumeID := fmt.Sprintf("resolver-unavailable-%d", index)
		request := newPublishRequest(t, server.options.KubeletRoot, volumeID)
		if test.maxBytes != "" {
			request.VolumeContext["maxBytes"] = test.maxBytes
		}
		ctx := t.Context()
		if test.cancel {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			ctx = cancelled
		}
		_, err := server.NodePublishVolume(ctx, request)
		if got := status.Code(err); got != test.wantStatus {
			t.Errorf("%s: publish status = %s, want %s (error: %v)", test.name, got, test.wantStatus, err)
		}
		if _, mounted := server.mounter.(*testMounter).mounts[request.GetTargetPath()]; mounted {
			t.Errorf("%s: mount was created after resolution failed", test.name)
		}
		if _, _, _, _, found, leaseErr := server.store.LeaseDetails(volumeID); leaseErr != nil || found {
			t.Errorf("%s: cache lease found = %t, error = %v; want no lease", test.name, found, leaseErr)
		}
	}
}

func TestNodeUnpublishCleansMountedDegradedGeneration(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request, identity, source := prepareDamagedMountedCache(t, server, mounts, store, "damaged-volume")

	if _, err := server.NodeUnpublishVolume(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if mounts.unmountCalls != 1 {
		t.Fatalf("unmount calls = %d, want one verified cache unmount", mounts.unmountCalls)
	}
	if _, mounted := mounts.mounts[request.GetTargetPath()]; mounted {
		t.Fatal("degraded mount remains after unpublish")
	}
	if _, err := os.Stat(filepath.Join(store.Root(), identity)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("degraded cache object remains after its last mount was removed: %v", err)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("degraded generation remains after cleanup: %v", err)
	}
}

func TestNodeUnpublishRejectsForeignMountBesideDegradedGeneration(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request, identity, source := prepareDamagedMountedCache(t, server, mounts, store, "foreign-damaged-volume")
	otherTarget := filepath.Join(server.options.KubeletRoot, "pods", "other-pod", "volumes", "kubernetes.io~csi", "other-volume", "mount")
	if err := os.MkdirAll(otherTarget, 0o755); err != nil {
		t.Fatal(err)
	}
	mounts.mounts[request.GetTargetPath()] = testMount{source: filepath.Join(t.TempDir(), "foreign")}
	mounts.mounts[otherTarget] = testMount{source: source}

	_, err := server.NodeUnpublishVolume(t.Context(), request)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unpublish error = %v, want FailedPrecondition", err)
	}
	if mounts.unmountCalls != 0 {
		t.Fatalf("unmount calls = %d, want 0 for a foreign mount", mounts.unmountCalls)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), identity)); err != nil {
		t.Fatalf("active degraded cache object was removed: %v", err)
	}
}

func TestNodeUnpublishCleansDegradedMountWithoutKubeletVolumeIDDerivation(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request, identity, _ := prepareDamagedMountedCache(t, server, mounts, store, "damaged-volume")
	request.VolumeId = "opaque-volume-id"

	if _, err := server.NodeUnpublishVolume(t.Context(), request); err != nil {
		t.Fatalf("unpublish degraded cache by its verified target and source: %v", err)
	}
	if mounts.unmountCalls != 1 {
		t.Fatalf("unmount calls = %d, want one verified cache unmount", mounts.unmountCalls)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), identity)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("degraded cache object remains after unpublish: %v", err)
	}
}

func TestNodeUnpublishPreservesLeaseForForeignMount(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "foreign-unpublish-volume")
	identity, err := cache.IdentityWithServiceAccount("namespace-uid", "service-account-uid", testDefault, "class-uid", "cache-key", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Acquire(cache.AcquireOptions{
		Identity: identity,
		Lease: cache.Lease{
			ID:        request.GetVolumeId(),
			Target:    request.GetTargetPath(),
			Namespace: testDefault,
			PodName:   testPodName,
			PodUID:    testPodUID,
		},
		Policy: cache.Policy{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(request.GetTargetPath()), 0o755); err != nil {
		t.Fatal(err)
	}
	mounts.mounts[request.GetTargetPath()] = testMount{source: filepath.Join(t.TempDir(), "foreign")}

	_, err = server.NodeUnpublishVolume(t.Context(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   request.GetVolumeId(),
		TargetPath: request.GetTargetPath(),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unpublish error = %v, want FailedPrecondition", err)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(request.GetVolumeId()); leaseErr != nil || !found {
		t.Fatalf("cache lease found = %t, error = %v; want the lease preserved", found, leaseErr)
	}
	if mounts.unmountCalls != 0 {
		t.Fatalf("unmount calls = %d, want 0 for a foreign mount", mounts.unmountCalls)
	}
}

func TestNodeHealthKeepsObjectMetadataFailureScopedToVolume(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "unreadable-metadata-volume")
	identity, err := cache.IdentityWithServiceAccount("namespace-uid", "service-account-uid", testDefault, "class-uid", "cache-key", "v1")
	if err != nil {
		t.Fatal(err)
	}
	source, _, err := store.Acquire(cache.AcquireOptions{
		Identity: identity,
		Lease: cache.Lease{
			ID:        request.GetVolumeId(),
			Target:    request.GetTargetPath(),
			Namespace: testDefault,
			PodName:   testPodName,
			PodUID:    testPodUID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mounts.mounts[request.GetTargetPath()] = testMount{source: source}
	if err := os.WriteFile(filepath.Join(store.Root(), identity, ".cache-csi.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}

	healthRequest := &csi.NodeGetVolumeHealthRequest{VolumeId: request.GetVolumeId(), VolumePublishPath: request.GetTargetPath()}
	for poll := range 2 {
		response, err := server.NodeGetVolumeHealth(t.Context(), healthRequest)
		if err != nil {
			t.Fatal(err)
		}
		statuses := response.GetVolumeHealth().GetHealthStatuses()
		if len(statuses) != 1 || statuses[0].GetStatus() != csi.VolumeHealthErrorType_INACCESSIBLE || statuses[0].GetReason() != "VolumeMetadataUnreadable" {
			t.Fatalf("poll %d volume health statuses = %+v, want inaccessible unreadable-metadata status", poll+1, statuses)
		}
	}

	storageResponse, err := server.NodeGetStorageHealth(t.Context(), &csi.NodeGetStorageHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if storageHealth := storageResponse.GetBackendHealth(); len(storageHealth) != 0 {
		t.Fatalf("storage health = %+v, want healthy backend status despite isolated metadata corruption", storageHealth)
	}
}

func TestNodeGetStorageHealthReportsProjectRegistryFailure(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cacheRoot := filepath.Join(root, "cache")
	if err := os.MkdirAll(cacheRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cacheRoot, ".project-ids.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := cache.NewStore(cacheRoot, cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	server := New(store, nil, nil, Options{})
	server.mounter = &testMounter{}

	response, err := server.NodeGetStorageHealth(t.Context(), &csi.NodeGetStorageHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	backendHealth := response.GetBackendHealth()
	if len(backendHealth) != 1 || backendHealth[0].GetStatus() != csi.StorageHealthErrorType_STORAGE_DEGRADED || backendHealth[0].GetReason() != "CacheProjectIDRegistryUnavailable" {
		t.Fatalf("storage health = %+v, want degraded project registry status", backendHealth)
	}
}

func TestNodeGetStorageHealthReportsRecoveryBeforeInspectingStore(t *testing.T) {
	t.Parallel()

	store, err := cache.NewStoreAsync(filepath.Join(t.TempDir(), "cache"), cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	mounts := &testMounter{filesystemReadOnlyErr: errors.New("must not inspect root before recovery")}
	server := New(store, nil, nil, Options{})
	server.mounter = mounts

	response, err := server.NodeGetStorageHealth(t.Context(), &csi.NodeGetStorageHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	backendHealth := response.GetBackendHealth()
	if len(backendHealth) != 1 || backendHealth[0].GetStatus() != csi.StorageHealthErrorType_STORAGE_DEGRADED || backendHealth[0].GetReason() != "CacheRecoveryInProgress" {
		t.Fatalf("storage health = %+v, want cache-recovery-in-progress status", backendHealth)
	}
}

func TestNodeGetStorageHealthReportsCacheRootFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  func(*testMounter)
		status csi.StorageHealthErrorType
		reason string
	}{
		{
			name: "read only root",
			setup: func(mounts *testMounter) {
				mounts.filesystemReadOnlyResult = true
			},
			status: csi.StorageHealthErrorType_STORAGE_DEGRADED,
			reason: "CacheRootReadOnly",
		},
		{
			name: "unavailable root",
			setup: func(mounts *testMounter) {
				mounts.filesystemReadOnlyErr = errors.New("filesystem inspection failed")
			},
			status: csi.StorageHealthErrorType_STORAGE_UNREACHABLE,
			reason: "CacheRootUnavailable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server, mounts, _ := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
			test.setup(mounts)
			response, err := server.NodeGetStorageHealth(t.Context(), &csi.NodeGetStorageHealthRequest{})
			if err != nil {
				t.Fatal(err)
			}
			backendHealth := response.GetBackendHealth()
			if len(backendHealth) != 1 || backendHealth[0].GetStatus() != test.status || backendHealth[0].GetReason() != test.reason {
				t.Fatalf("storage health = %+v, want status %s with reason %q", backendHealth, test.status, test.reason)
			}
		})
	}
}

func TestRecoverCacheLeasesVerifiesMountSourceAndOptions(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{NoExec: true}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "recovery-volume")
	request.Readonly = true
	identity, err := cache.IdentityWithServiceAccount("namespace-uid", "service-account-uid", testDefault, "class-uid", "cache-key", "v1")
	if err != nil {
		t.Fatal(err)
	}
	source, _, err := store.Acquire(cache.AcquireOptions{
		Identity: identity,
		Lease: cache.Lease{
			ID:        request.GetVolumeId(),
			Target:    request.GetTargetPath(),
			Namespace: testDefault,
			PodName:   testPodName,
			PodUID:    testPodUID,
			ReadOnly:  true,
			NoExec:    true,
		},
		Policy: cache.Policy{NoExec: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	mounts.mounts[request.GetTargetPath()] = testMount{source: source, readOnly: true, noExec: true}

	if err := recoverCacheLeases(store, mounts); err != nil {
		t.Fatal(err)
	}
	if mounts.lastVerification != (mountVerification{
		source:   source,
		target:   request.GetTargetPath(),
		readOnly: true,
		noExec:   true,
	}) {
		t.Fatalf("recovery mount verification = %+v, want the lease source and security options", mounts.lastVerification)
	}
	if _, _, _, _, found, err := store.LeaseDetails(request.GetVolumeId()); err != nil || !found {
		t.Fatalf("cache lease found = %t, error = %v; want verified lease retained", found, err)
	}

	mounts.mounts[request.GetTargetPath()] = testMount{source: source, readOnly: false, noExec: true}
	if err := recoverCacheLeases(store, mounts); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, found, err := store.LeaseDetails(request.GetVolumeId()); err != nil || found {
		t.Fatalf("cache lease found = %t, error = %v; want lease removed after readonly mismatch", found, err)
	}
}

func TestVerifyCacheMountWithUnknownTargetUsesSourceInspection(t *testing.T) {
	t.Parallel()

	mounts := &testMounter{sourceMountedResult: true, mounts: make(map[string]testMount)}
	const source = "/cache/identity/generations/generation"

	mounted, err := verifyCacheMount(mounts, source, cache.Lease{}, cache.Policy{})
	if err != nil || !mounted {
		t.Fatalf("source mount inspection = %t, error = %v; want mounted source", mounted, err)
	}
	if mounts.sourceMountedCalls != 1 || mounts.lastSource != source {
		t.Fatalf("source inspections = %d for %q, want one inspection of the generation", mounts.sourceMountedCalls, mounts.lastSource)
	}

	inspectionFailure := errors.New("mount table is unavailable")
	mounts.sourceMountedErr = inspectionFailure
	if _, err := verifyCacheMount(mounts, source, cache.Lease{}, cache.Policy{}); !errors.Is(err, inspectionFailure) {
		t.Fatalf("source mount inspection error = %v, want the inspection failure", err)
	}
}

func TestPolicyForMapsSharingPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  cachev1alpha1.SharingPolicy
		wanted string
	}{
		{name: "default", wanted: string(cachev1alpha1.SharingPolicyExclusive)},
		{name: "shared", input: cachev1alpha1.SharingPolicyShared, wanted: string(cachev1alpha1.SharingPolicyShared)},
		{name: "exclusive", input: cachev1alpha1.SharingPolicyExclusive, wanted: string(cachev1alpha1.SharingPolicyExclusive)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			policy, err := policyFor(cachev1alpha1.CacheClassSpec{SharingPolicy: test.input}, testDefault, "class-uid", "", false)
			if err != nil {
				t.Fatal(err)
			}
			if policy.SharingPolicy != test.wanted {
				t.Fatalf("sharing policy = %q, want %q", policy.SharingPolicy, test.wanted)
			}
		})
	}
}

func TestPolicyForPreservesZeroRetention(t *testing.T) {
	t.Parallel()
	policy, err := policyFor(cachev1alpha1.CacheClassSpec{}, testDefault, "class-uid", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Retention != 0 {
		t.Fatalf("zero retention was replaced with %s", policy.Retention)
	}
}

type testResolver struct {
	spec               cachev1alpha1.CacheClassSpec
	err                error
	serviceAccountName string
}

func (resolver *testResolver) Resolve(_ context.Context, _, _, serviceAccountName string) (string, string, kube.ResolvedClass, error) {
	resolver.serviceAccountName = serviceAccountName
	if resolver.err != nil {
		return "", "", kube.ResolvedClass{}, resolver.err
	}
	return "namespace-uid", "service-account-uid", kube.ResolvedClass{
		Object: cachev1alpha1.CacheClass{Spec: resolver.spec},
		UID:    "class-uid",
	}, nil
}

type testQuota struct {
	err error
}

func (quota *testQuota) Configure(context.Context, string, string, uint32, int64) error {
	return quota.err
}

type recordingQuota struct {
	limits []int64
}

func (quota *recordingQuota) Configure(_ context.Context, _, _ string, _ uint32, limit int64) error {
	quota.limits = append(quota.limits, limit)
	return nil
}

type testMount struct {
	source   string
	readOnly bool
	noExec   bool
}

type mountVerification struct {
	source   string
	target   string
	readOnly bool
	noExec   bool
}

type testMounter struct {
	mounts                   map[string]testMount
	mountCalls               int
	unmountCalls             int
	mountErr                 error
	unmountErr               error
	lastVerification         mountVerification
	sourceMountedCalls       int
	sourceMountedResult      bool
	sourceMountedErr         error
	lastSource               string
	sourceMountedFromTargets bool
	filesystemReadOnlyResult bool
	filesystemReadOnlyErr    error
}

func newTestServer(t *testing.T, spec cachev1alpha1.CacheClassSpec, quotaError error) (*Server, *testMounter, *cache.Store) {
	t.Helper()
	root := t.TempDir()
	projectQuotaEnabled := spec.Storage.MaxBytes.Sign() > 0 || spec.Storage.DefaultMaxBytes.Sign() > 0
	store, err := cache.NewStore(filepath.Join(root, "cache"), cache.StoreOptions{ProjectQuotaEnabled: projectQuotaEnabled})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	resolver := &testResolver{spec: spec}
	var quota ProjectQuota
	if quotaError != nil {
		quota = &testQuota{err: quotaError}
	}
	server := New(store, resolver, quota, Options{
		KubeletRoot:         filepath.Join(root, "kubelet"),
		ProjectQuotaEnabled: projectQuotaEnabled,
	})
	server.options.Health.Set(nodehealth.PhaseReady, "TestReady", false)
	mounts := &testMounter{mounts: make(map[string]testMount)}
	server.mounter = mounts
	return server, mounts, store
}

func newPublishRequest(t *testing.T, kubeletRoot, volumeID string) *csi.NodePublishVolumeRequest {
	t.Helper()
	target := filepath.Join(kubeletRoot, "pods", testPodUID, "volumes", "kubernetes.io~csi", volumeID, "mount")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	return &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: target,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		VolumeContext: map[string]string{
			"csi.storage.k8s.io/ephemeral":           "true",
			"csi.storage.k8s.io/pod.namespace":       testDefault,
			"csi.storage.k8s.io/pod.name":            testPodName,
			"csi.storage.k8s.io/pod.uid":             testPodUID,
			"csi.storage.k8s.io/serviceAccount.name": testServiceAccountName,
			"cacheClass":                             testDefault,
			"cacheKey":                               "cache-key",
		},
	}
}

func prepareDamagedMountedCache(t *testing.T, server *Server, mounts *testMounter, store *cache.Store, volumeID string) (*csi.NodeUnpublishVolumeRequest, string, string) {
	t.Helper()
	request := newPublishRequest(t, server.options.KubeletRoot, volumeID)
	identity, err := cache.IdentityWithServiceAccount("namespace-uid", "service-account-uid", testDefault, "class-uid", "cache-key", "v1")
	if err != nil {
		t.Fatal(err)
	}
	source, _, err := store.Acquire(cache.AcquireOptions{
		Identity: identity,
		Lease: cache.Lease{
			ID:        request.GetVolumeId(),
			Target:    request.GetTargetPath(),
			Namespace: testDefault,
			PodName:   testPodName,
			PodUID:    testPodUID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.Root(), identity, ".cache-csi.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(request.GetTargetPath(), 0o755); err != nil {
		t.Fatal(err)
	}
	mounts.sourceMountedFromTargets = true
	mounts.mounts[request.GetTargetPath()] = testMount{source: source}
	if err := recoverCacheLeases(store, mounts); err != nil {
		t.Fatal(err)
	}
	return &csi.NodeUnpublishVolumeRequest{VolumeId: request.GetVolumeId(), TargetPath: request.GetTargetPath()}, identity, source
}

func (mounts *testMounter) mount(source, target string, readOnly, noExec bool) error {
	mounts.mountCalls++
	if mounts.mountErr != nil {
		return mounts.mountErr
	}
	mounts.mounts[target] = testMount{source: source, readOnly: readOnly, noExec: noExec}
	return nil
}

func (mounts *testMounter) unmount(target string) error {
	mounts.unmountCalls++
	if mounts.unmountErr != nil {
		return mounts.unmountErr
	}
	if _, found := mounts.mounts[target]; !found {
		return errNotMounted
	}
	delete(mounts.mounts, target)
	return nil
}

func (mounts *testMounter) mountedAt(target string) (bool, error) {
	_, found := mounts.mounts[target]
	return found, nil
}

func (mounts *testMounter) sameCacheMount(source, target string, readOnly, noExec bool) (bool, error) {
	mounts.lastVerification = mountVerification{source: source, target: target, readOnly: readOnly, noExec: noExec}
	state, found := mounts.mounts[target]
	return found && state.source == source && state.readOnly == readOnly && state.noExec == noExec, nil
}

func (mounts *testMounter) sameCacheSource(source, target string) (bool, error) {
	state, found := mounts.mounts[target]
	return found && state.source == source, nil
}

func (mounts *testMounter) sourceWithinRoot(target, root string) (bool, error) {
	state, found := mounts.mounts[target]
	if !found {
		return false, nil
	}
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(state.source))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return false, err
	}
	return true, nil
}

func (mounts *testMounter) sourceMounted(source string) (bool, error) {
	mounts.sourceMountedCalls++
	mounts.lastSource = source
	if mounts.sourceMountedErr != nil {
		return false, mounts.sourceMountedErr
	}
	if mounts.sourceMountedFromTargets {
		for _, mount := range mounts.mounts {
			if mount.source == source {
				return true, nil
			}
		}
		return false, nil
	}
	return mounts.sourceMountedResult, nil
}

func (mounts *testMounter) filesystemReadOnly(string) (bool, error) {
	return mounts.filesystemReadOnlyResult, mounts.filesystemReadOnlyErr
}
