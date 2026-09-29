package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	testDefault = "default"
	testPodName = "pod"
	testPodUID  = "pod-uid"
)

func TestNodeGetCapabilitiesAdvertisesImplementedRPCs(t *testing.T) {
	t.Parallel()

	response, err := (*Server)(nil).NodeGetCapabilities(t.Context(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}

	want := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
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
		Backend: cachev1alpha1.BackendXFSProject,
		Quota: cachev1alpha1.QuotaPolicy{
			Enabled:         true,
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
	resolver.spec.Quota.DefaultMaxBytes = resource.MustParse("2Mi")
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
		Backend: cachev1alpha1.BackendXFSProject,
		Quota: cachev1alpha1.QuotaPolicy{
			Enabled:         true,
			DefaultMaxBytes: resource.MustParse("1Mi"),
		},
	}
	server, mounts, store := newTestServer(t, spec, quotaFailure)
	request := newPublishRequest(t, server.options.KubeletRoot, "quota-volume")

	_, err := server.NodePublishVolume(t.Context(), request)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("publish error = %v, want FailedPrecondition", err)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(request.GetVolumeId()); leaseErr != nil || found {
		t.Fatalf("cache lease found = %t, error = %v; want the failed lease rolled back", found, leaseErr)
	}
	if _, _, _, _, found, leaseErr := server.fallbackStore.LeaseDetails(request.GetVolumeId()); leaseErr != nil || found {
		t.Fatalf("fallback lease found = %t, error = %v; quota failure must not create fallback state", found, leaseErr)
	}
	if mounts.mountCalls != 0 {
		t.Fatalf("mount calls = %d, want 0 after quota failure", mounts.mountCalls)
	}
}

func TestNodePublishRollsBackLeaseWhenDetachedMountSetupFails(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "mount-cleanup-volume")
	mounts.mountErr = errors.New("mount attributes failed")

	_, err := server.NodePublishVolume(t.Context(), request)
	if status.Code(err) != codes.Internal {
		t.Fatalf("publish error = %v, want Internal", err)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(request.GetVolumeId()); leaseErr != nil || found {
		t.Fatalf("cache lease found = %t, error = %v; want lease rollback before mount attachment", found, leaseErr)
	}
	if _, mounted := mounts.mounts[request.GetTargetPath()]; mounted {
		t.Fatal("mount should not be attached when detached mount setup fails")
	}
}

func TestNodePublishUsesFallbackForExclusiveSharingConflict(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{SharingPolicy: cachev1alpha1.SharingPolicyExclusive, EvictRunning: true}, nil)
	first := newPublishRequest(t, server.options.KubeletRoot, "exclusive-first")
	if _, err := server.NodePublishVolume(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	second := newPublishRequest(t, server.options.KubeletRoot, "exclusive-second")

	if _, err := server.NodePublishVolume(t.Context(), second); err != nil {
		t.Fatalf("publish with conflicting shared identity: %v", err)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(first.GetVolumeId()); leaseErr != nil || !found {
		t.Fatalf("first cache lease found = %t, error = %v; want the active lease preserved", found, leaseErr)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(second.GetVolumeId()); leaseErr != nil || found {
		t.Fatalf("second cache lease found = %t, error = %v; want the conflicting lease absent from the shared cache", found, leaseErr)
	}
	if _, _, source, policy, found, err := server.fallbackStore.LeaseDetails(second.GetVolumeId()); err != nil || !found {
		t.Fatalf("fallback lease found = %t, error = %v; want an isolated fallback lease", found, err)
	} else if !policy.EvictRunning || !policy.NoExec {
		t.Fatalf("fallback policy = %+v, want evictRunning and noexec preserved", policy)
	} else if mounts.mounts[second.GetTargetPath()].source != source {
		t.Fatalf("second target source = %q, want fallback source %q", mounts.mounts[second.GetTargetPath()].source, source)
	}
}

func TestNodePublishUsesFallbackForDegradedMetadataWithoutQuota(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "degraded-unmounted-volume")
	identity, err := cache.Identity("namespace-uid", testDefault, "class-uid", "cache-key", "v1")
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
	if _, _, source, policy, found, err := server.fallbackStore.LeaseDetails(request.GetVolumeId()); err != nil || !found {
		t.Fatalf("fallback lease found = %t, error = %v; want an isolated fallback lease", found, err)
	} else if !policy.NoExec || mounts.mounts[request.GetTargetPath()].source != source {
		t.Fatalf("fallback mount source or policy = %q, %+v; want a noexec isolated cache", mounts.mounts[request.GetTargetPath()].source, policy)
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

func TestNodePublishKeepsQuotaIdentityConflictHard(t *testing.T) {
	t.Parallel()

	spec := cachev1alpha1.CacheClassSpec{
		Backend:       cachev1alpha1.BackendXFSProject,
		SharingPolicy: cachev1alpha1.SharingPolicyExclusive,
		Quota: cachev1alpha1.QuotaPolicy{
			Enabled:         true,
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
		t.Fatalf("second publish error = %v, want FailedPrecondition", err)
	}
	if _, _, _, _, found, err := server.fallbackStore.LeaseDetails(second.GetVolumeId()); err != nil || found {
		t.Fatalf("fallback lease found = %t, error = %v; quota policy must not be bypassed", found, err)
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

func TestResolverUnavailablePublishesRestrictedFallbackAndUnpublishes(t *testing.T) {
	t.Parallel()

	server, mounts, _ := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	server.resolver = nil
	request := newPublishRequest(t, server.options.KubeletRoot, "api-unavailable-volume")
	request.Readonly = true

	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	state, mounted := mounts.mounts[request.GetTargetPath()]
	if !mounted || !state.readOnly || !state.noExec {
		t.Fatalf("published mount = %+v, present=%t; want readonly and noexec", state, mounted)
	}
	identity, err := cache.FallbackIdentity(request.GetVolumeId())
	if err != nil {
		t.Fatal(err)
	}
	_, lease, source, policy, found, err := server.fallbackStore.LeaseDetails(request.GetVolumeId())
	if err != nil || !found {
		t.Fatalf("fallback lease found = %t, error = %v", found, err)
	}
	if lease.Target != request.GetTargetPath() || !lease.NoExec || !lease.ReadOnly || policy.SharingPolicy != cache.SharingPolicyExclusive || !policy.DiscardOnLastRelease {
		t.Fatalf("fallback lease policy = (%+v, %+v), identity = %s", lease, policy, identity)
	}
	if filepath.Dir(filepath.Dir(filepath.Dir(source))) != server.fallbackStore.Root() {
		t.Fatalf("fallback source %q is outside fallback Store root %q", source, server.fallbackStore.Root())
	}
	if err := os.WriteFile(filepath.Join(source, "private"), []byte("fallback data"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := server.NodeUnpublishVolume(t.Context(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   request.GetVolumeId(),
		TargetPath: request.GetTargetPath(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, mounted := mounts.mounts[request.GetTargetPath()]; mounted {
		t.Fatal("mount remains after cleanup")
	}
	if _, err := os.Stat(filepath.Join(server.fallbackStore.Root(), identity)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fallback object remains in the object namespace after unpublish: %v", err)
	}
	if _, _, _, _, found, err := server.fallbackStore.LeaseDetails(request.GetVolumeId()); err != nil || found {
		t.Fatalf("fallback lease after unpublish found = %t, error = %v", found, err)
	}
	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatalf("republish fallback volume: %v", err)
	}
	_, _, newSource, _, found, err := server.fallbackStore.LeaseDetails(request.GetVolumeId())
	if err != nil || !found {
		t.Fatalf("republished fallback lease found = %t, error = %v", found, err)
	}
	if newSource == source {
		t.Fatal("fallback republish reused the prior generation")
	}
	if _, err := os.Stat(filepath.Join(newSource, "private")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("data from the previous fallback lifecycle is visible after republish: %v", err)
	}
}

func TestFallbackRejectsHardResolutionErrorsLimitsAndCancellation(t *testing.T) {
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
			name:       "maxBytes has no resolved quota policy",
			resolver:   nil,
			maxBytes:   "1Mi",
			wantStatus: codes.FailedPrecondition,
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
		volumeID := fmt.Sprintf("fallback-rejected-%d", index)
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
		identity, err := cache.FallbackIdentity(volumeID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(server.fallbackStore.Root(), identity)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s: fallback object was created: %v", test.name, err)
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

func TestNodeUnpublishRejectsWrongVolumeIDForDegradedMount(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request, identity, _ := prepareDamagedMountedCache(t, server, mounts, store, "damaged-volume")
	wrongVolumeID := inlineVolumeID(testPodUID, "other-volume")

	_, err := server.NodeUnpublishVolume(t.Context(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   wrongVolumeID,
		TargetPath: request.GetTargetPath(),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unpublish error = %v, want FailedPrecondition", err)
	}
	if mounts.unmountCalls != 0 {
		t.Fatalf("unmount calls = %d, want 0 for a mismatched volume ID", mounts.unmountCalls)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), identity)); err != nil {
		t.Fatalf("degraded cache object was removed: %v", err)
	}
}

func TestNodeUnpublishPreservesLeaseForForeignMount(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "foreign-unpublish-volume")
	identity, err := cache.Identity("namespace-uid", testDefault, "class-uid", "cache-key", "v1")
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

func TestNodeGetVolumeHealthReportsUnreadableMetadataAsInaccessible(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "unreadable-metadata-volume")
	request.VolumeId = inlineVolumeID(testPodUID, "unreadable-metadata-volume")
	identity, err := cache.Identity("namespace-uid", testDefault, "class-uid", "cache-key", "v1")
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
	storageHealth := storageResponse.GetBackendHealth()
	if len(storageHealth) != 1 || storageHealth[0].GetStatus() != csi.StorageHealthErrorType_STORAGE_DEGRADED || storageHealth[0].GetReason() != "CacheMetadataUnreadable" {
		t.Fatalf("storage health = %+v, want degraded unreadable-metadata status", storageHealth)
	}
}

func TestRecoverCacheLeasesVerifiesMountSourceAndOptions(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{NoExec: true}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "recovery-volume")
	request.Readonly = true
	identity, err := cache.Identity("namespace-uid", testDefault, "class-uid", "cache-key", "v1")
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
		{name: "default", wanted: string(cachev1alpha1.SharingPolicyShared)},
		{name: "shared", input: cachev1alpha1.SharingPolicyShared, wanted: string(cachev1alpha1.SharingPolicyShared)},
		{name: "exclusive", input: cachev1alpha1.SharingPolicyExclusive, wanted: string(cachev1alpha1.SharingPolicyExclusive)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			policy, err := policyFor(cachev1alpha1.CacheClassSpec{SharingPolicy: test.input}, testDefault, "class-uid", "")
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
	policy, err := policyFor(cachev1alpha1.CacheClassSpec{}, testDefault, "class-uid", "")
	if err != nil {
		t.Fatal(err)
	}
	if policy.Retention != 0 {
		t.Fatalf("zero retention was replaced with %s", policy.Retention)
	}
}

type testResolver struct {
	spec cachev1alpha1.CacheClassSpec
	err  error
}

func (resolver *testResolver) Resolve(context.Context, string, string) (string, kube.ResolvedClass, error) {
	if resolver.err != nil {
		return "", kube.ResolvedClass{}, resolver.err
	}
	return "namespace-uid", kube.ResolvedClass{
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
}

func newTestServer(t *testing.T, spec cachev1alpha1.CacheClassSpec, quotaError error) (*Server, *testMounter, *cache.Store) {
	t.Helper()
	root := t.TempDir()
	store, err := cache.NewStore(filepath.Join(root, "cache"), cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	fallbackStore, err := cache.NewStore(filepath.Join(root, "fallback"), cache.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fallbackStore.Close(); err != nil {
			t.Error(err)
		}
	})
	resolver := &testResolver{spec: spec}
	var quota ProjectQuota
	if quotaError != nil {
		quota = &testQuota{err: quotaError}
	}
	server := New(store, resolver, quota, Options{
		KubeletRoot:   filepath.Join(root, "kubelet"),
		FallbackRoot:  filepath.Join(root, "fallback"),
		FallbackStore: fallbackStore,
	})
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
			"csi.storage.k8s.io/ephemeral":     "true",
			"csi.storage.k8s.io/pod.namespace": testDefault,
			"csi.storage.k8s.io/pod.name":      testPodName,
			"csi.storage.k8s.io/pod.uid":       testPodUID,
			"cacheClass":                       testDefault,
			"cacheKey":                         "cache-key",
		},
	}
}

func prepareDamagedMountedCache(t *testing.T, server *Server, mounts *testMounter, store *cache.Store, volumeID string) (*csi.NodeUnpublishVolumeRequest, string, string) {
	t.Helper()
	request := newPublishRequest(t, server.options.KubeletRoot, volumeID)
	request.VolumeId = inlineVolumeID(testPodUID, volumeID)
	identity, err := cache.Identity("namespace-uid", testDefault, "class-uid", "cache-key", "v1")
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

func (*testMounter) filesystemReadOnly(string) (bool, error) { return false, nil }
