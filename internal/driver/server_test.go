package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
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
	if mounts.bindCalls != 1 {
		t.Fatalf("bind mount calls = %d, want 1", mounts.bindCalls)
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
	if mounts.bindCalls != 0 {
		t.Fatalf("bind mount calls = %d, want 0 after quota failure", mounts.bindCalls)
	}
}

func TestNodePublishRetainsLeaseWhenFailedMountCannotBeUndone(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	request := newPublishRequest(t, server.options.KubeletRoot, "mount-cleanup-volume")
	mounts.remountErr = errors.New("mount options failed")
	mounts.unmountErr = errors.New("unmount failed")

	_, err := server.NodePublishVolume(t.Context(), request)
	if status.Code(err) != codes.Internal {
		t.Fatalf("publish error = %v, want Internal", err)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(request.GetVolumeId()); leaseErr != nil || !found {
		t.Fatalf("cache lease found = %t, error = %v; want lease retained for an active mount", found, leaseErr)
	}
	if _, mounted := mounts.mounts[request.GetTargetPath()]; !mounted {
		t.Fatal("fake mount should remain after unmount failure")
	}
}

func TestNodePublishMapsExclusiveSharingConflictToFailedPrecondition(t *testing.T) {
	t.Parallel()

	server, _, store := newTestServer(t, cachev1alpha1.CacheClassSpec{SharingPolicy: cachev1alpha1.SharingPolicyExclusive}, nil)
	first := newPublishRequest(t, server.options.KubeletRoot, "exclusive-first")
	if _, err := server.NodePublishVolume(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	second := newPublishRequest(t, server.options.KubeletRoot, "exclusive-second")

	_, err := server.NodePublishVolume(t.Context(), second)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second publish error = %v, want FailedPrecondition", err)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(first.GetVolumeId()); leaseErr != nil || !found {
		t.Fatalf("first cache lease found = %t, error = %v; want the active lease preserved", found, leaseErr)
	}
	if _, _, _, _, found, leaseErr := store.LeaseDetails(second.GetVolumeId()); leaseErr != nil || found {
		t.Fatalf("second cache lease found = %t, error = %v; want the conflicting lease absent", found, leaseErr)
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
	if mounts.bindCalls != 0 {
		t.Fatalf("bind mount calls = %d, want 0", mounts.bindCalls)
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

	if _, err := server.NodeUnpublishVolume(t.Context(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   request.GetVolumeId(),
		TargetPath: request.GetTargetPath(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, mounted := mounts.mounts[request.GetTargetPath()]; mounted {
		t.Fatal("mount remains after cleanup")
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

type testResolver struct {
	spec cachev1alpha1.CacheClassSpec
}

func (resolver *testResolver) Resolve(context.Context, string, string) (string, kube.ResolvedClass, error) {
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
	bindCalls                int
	unmountCalls             int
	remountErr               error
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
	resolver := &testResolver{spec: spec}
	var quota ProjectQuota
	if quotaError != nil {
		quota = &testQuota{err: quotaError}
	}
	server := New(store, resolver, quota, Options{
		KubeletRoot:  filepath.Join(root, "kubelet"),
		FallbackRoot: filepath.Join(root, "fallback"),
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

func (mounts *testMounter) bindMount(source, target string) error {
	mounts.bindCalls++
	mounts.mounts[target] = testMount{source: source}
	return nil
}

func (mounts *testMounter) remountOptions(target string, readOnly, noExec bool) error {
	if mounts.remountErr != nil {
		return mounts.remountErr
	}
	state, found := mounts.mounts[target]
	if !found {
		return errNotMounted
	}
	state.readOnly = readOnly
	state.noExec = noExec
	mounts.mounts[target] = state
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
