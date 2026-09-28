package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
)

const DriverName = "cache.csi.walnuts.dev"

const defaultRetention = 72 * time.Hour

type ClassResolver interface {
	Resolve(context.Context, string, string) (string, kube.ResolvedClass, error)
}

type ProjectQuota interface {
	Configure(context.Context, string, string, uint32, int64) error
}

type mounter interface {
	bindMount(string, string) error
	remountOptions(string, bool, bool) error
	unmount(string) error
	mountedAt(string) (bool, error)
	sameCacheMount(string, string, bool, bool) (bool, error)
	sameCacheSource(string, string) (bool, error)
	sourceMounted(string) (bool, error)
	filesystemReadOnly(string) (bool, error)
}

type Options struct {
	NodeID        string
	KubeletRoot   string
	FallbackRoot  string
	VendorVersion string
}

type Server struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer
	store         *cache.Store
	resolver      ClassResolver
	quota         ProjectQuota
	mounter       mounter
	options       Options
	locks         operationLocks
	identityLocks operationLocks
}

type podVolumeContext struct {
	namespace  string
	name       string
	uid        string
	cacheClass string
	cacheKey   string
	maxBytes   string
}

func New(store *cache.Store, resolver ClassResolver, quotaManager ProjectQuota, options Options) *Server {
	if options.VendorVersion == "" {
		options.VendorVersion = "dev"
	}
	return &Server{store: store, resolver: resolver, quota: quotaManager, mounter: newMounter(), options: options}
}

func (s *Server) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: DriverName, VendorVersion: s.options.VendorVersion}, nil
}

func (*Server) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (*Server) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

func (s *Server) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	if s.options.NodeID == "" {
		return nil, status.Error(codes.FailedPrecondition, "node ID is not configured")
	}
	return &csi.NodeGetInfoResponse{NodeId: s.options.NodeID}, nil
}

func (*Server) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	types := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.NodeServiceCapability_RPC_GET_VOLUME_HEALTH,
		csi.NodeServiceCapability_RPC_GET_STORAGE_HEALTH,
	}
	capabilities := make([]*csi.NodeServiceCapability, 0, len(types))
	for _, typ := range types {
		capabilities = append(capabilities, &csi.NodeServiceCapability{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: typ}}})
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: capabilities}, nil
}

func (s *Server) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeContext, err := s.validatePublishRequest(req)
	if err != nil {
		return nil, err
	}
	unlock := s.locks.Lock(req.GetTargetPath())
	defer unlock()

	handled, err := s.handleExistingPublish(req)
	if err != nil {
		return nil, err
	}
	if handled {
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if err := s.publish(ctx, req, volumeContext); err != nil {
		return nil, err
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *Server) validatePublishRequest(req *csi.NodePublishVolumeRequest) (podVolumeContext, error) {
	if req.GetVolumeId() == "" || req.GetVolumeCapability() == nil || !filepath.IsAbs(req.GetTargetPath()) {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "volume ID, volume capability, and absolute target path are required")
	}
	if req.GetVolumeCapability().GetMount() == nil {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "only mount volumes are supported")
	}
	if !isSingleNodeAccessMode(req.GetVolumeCapability().GetAccessMode()) {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "a single-node access mode is required")
	}
	if err := s.validateTarget(req.GetTargetPath()); err != nil {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, err.Error())
	}
	attributes := req.GetVolumeContext()
	volumeContext := podVolumeContext{
		namespace:  attributes["csi.storage.k8s.io/pod.namespace"],
		name:       attributes["csi.storage.k8s.io/pod.name"],
		uid:        attributes["csi.storage.k8s.io/pod.uid"],
		cacheClass: attributes["cacheClass"],
		cacheKey:   attributes["cacheKey"],
		maxBytes:   attributes["maxBytes"],
	}
	if attributes["csi.storage.k8s.io/ephemeral"] != "true" {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "only inline ephemeral CSI volumes are supported")
	}
	if volumeContext.namespace == "" || volumeContext.name == "" || volumeContext.uid == "" || volumeContext.cacheClass == "" || volumeContext.cacheKey == "" {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "pod information, cacheClass, and cacheKey are required")
	}
	if len(volumeContext.cacheKey) > 1024 || strings.ContainsRune(volumeContext.cacheKey, '\x00') {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "cacheKey is invalid or exceeds 1024 bytes")
	}
	if volumeContext.maxBytes != "" {
		maxBytes, err := resource.ParseQuantity(volumeContext.maxBytes)
		if err != nil || maxBytes.Sign() <= 0 {
			return podVolumeContext{}, status.Error(codes.InvalidArgument, "maxBytes must be a positive Kubernetes quantity")
		}
	}
	return volumeContext, nil
}

func (s *Server) handleExistingPublish(req *csi.NodePublishVolumeRequest) (bool, error) {
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return false, status.Errorf(codes.Internal, "inspect target mount: %v", err)
	}
	if !mounted {
		return false, nil
	}
	_, lease, source, _, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil {
		return false, status.Errorf(codes.Internal, "read cache lease: %v", err)
	}
	if found {
		if lease.Target != req.GetTargetPath() {
			return false, status.Error(codes.AlreadyExists, "volume ID is already published at a different target")
		}
		if lease.ReadOnly != req.GetReadonly() {
			return false, status.Error(codes.AlreadyExists, "volume ID is already published with a different readonly flag")
		}
		same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), lease.NoExec)
		if err != nil {
			return false, status.Errorf(codes.Internal, "verify existing cache mount: %v", err)
		}
		if same {
			return true, nil
		}
		return false, status.Error(codes.AlreadyExists, "target is mounted from a different source or with different options")
	}
	fallback := fallbackPath(s.options.FallbackRoot, req.GetVolumeId())
	same, err := s.mounter.sameCacheMount(fallback, req.GetTargetPath(), req.GetReadonly(), true)
	if err != nil {
		return false, status.Errorf(codes.Internal, "verify fallback mount: %v", err)
	}
	if same {
		return true, nil
	}
	return false, status.Error(codes.AlreadyExists, "target is already mounted by another volume")
}

func (s *Server) publish(ctx context.Context, req *csi.NodePublishVolumeRequest, volumeContext podVolumeContext) error {
	identity, policy, err := s.resolve(ctx, volumeContext)
	if err != nil {
		return s.publishAfterResolutionFailure(ctx, req, volumeContext, err)
	}
	unlockIdentity := s.identityLocks.Lock(identity)
	defer unlockIdentity()

	if err := s.prepareLease(req, identity, policy.NoExec); err != nil {
		return err
	}
	lease := cache.Lease{ID: req.GetVolumeId(), Target: req.GetTargetPath(), Namespace: volumeContext.namespace, PodName: volumeContext.name, PodUID: volumeContext.uid, ReadOnly: req.GetReadonly(), NoExec: policy.NoExec}
	return s.publishNewCache(ctx, req, identity, lease, policy)
}

func (s *Server) prepareLease(req *csi.NodePublishVolumeRequest, identity string, noExec bool) error {
	oldIdentity, oldLease, _, _, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil {
		return status.Errorf(codes.Internal, "read cache lease: %v", err)
	}
	if !found {
		return nil
	}
	if oldLease.Target != req.GetTargetPath() {
		return status.Error(codes.AlreadyExists, "volume ID is already published at a different target")
	}
	if oldLease.ReadOnly != req.GetReadonly() {
		return status.Error(codes.AlreadyExists, "volume ID is already published with a different readonly flag")
	}
	if oldLease.NoExec != noExec {
		return status.Error(codes.AlreadyExists, "volume ID is already published with different mount options")
	}
	if oldIdentity != identity {
		if err := s.store.Release(req.GetVolumeId(), oldLease.Target); err != nil {
			return status.Errorf(codes.Internal, "release cache lease after CacheClass change: %v", err)
		}
	}
	return nil
}

func (s *Server) publishAfterResolutionFailure(ctx context.Context, req *csi.NodePublishVolumeRequest, volumeContext podVolumeContext, resolveErr error) error {
	if !errors.Is(resolveErr, kube.ErrAPIResolverUnavailable) && !kube.IsTemporaryAPIError(resolveErr) {
		return status.Errorf(codes.FailedPrecondition, "resolve CacheClass %q: %v", volumeContext.cacheClass, resolveErr)
	}
	identity, lease, source, policy, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil {
		return status.Errorf(codes.Internal, "read cache lease: %v", err)
	}
	if !found {
		if volumeContext.maxBytes != "" {
			return status.Error(codes.FailedPrecondition, "cannot use fallback cache while maxBytes is requested and CacheClass cannot be resolved")
		}
		return s.publishFallback(req)
	}
	if lease.Target != req.GetTargetPath() {
		return status.Error(codes.AlreadyExists, "volume ID is already published at a different target")
	}
	if lease.ReadOnly != req.GetReadonly() {
		return status.Error(codes.AlreadyExists, "volume ID is already published with a different readonly flag")
	}
	unlockIdentity := s.identityLocks.Lock(identity)
	defer unlockIdentity()

	return s.publishCache(ctx, req, identity, source, policy)
}

func (s *Server) resolve(ctx context.Context, volumeContext podVolumeContext) (string, cache.Policy, error) {
	if s.resolver == nil {
		return "", cache.Policy{}, kube.ErrAPIResolverUnavailable
	}
	namespaceUID, class, err := s.resolver.Resolve(ctx, volumeContext.namespace, volumeContext.cacheClass)
	if err != nil {
		return "", cache.Policy{}, err
	}
	policy, err := policyFor(class.Object.Spec, volumeContext.cacheClass, class.UID, volumeContext.maxBytes)
	if err != nil {
		return "", cache.Policy{}, err
	}
	identity, err := cache.Identity(namespaceUID, volumeContext.cacheClass, class.UID, volumeContext.cacheKey, policy.SchemaVersion)
	return identity, policy, err
}

func policyFor(spec cachev1alpha1.CacheClassSpec, className, classUID, requestedMaxBytes string) (cache.Policy, error) {
	if err := spec.Validate(); err != nil {
		return cache.Policy{}, err
	}
	sharingPolicy := spec.SharingPolicy
	if sharingPolicy == "" {
		sharingPolicy = cachev1alpha1.SharingPolicyShared
	}
	policy := cache.Policy{
		ClassName:          className,
		ClassUID:           classUID,
		SharingPolicy:      string(sharingPolicy),
		NoExec:             spec.NoExec,
		SchemaVersion:      spec.SchemaVersion,
		CrashRecoveryReuse: spec.CrashRecovery == "reuse",
		EvictRunning:       spec.EvictRunning,
		QuotaEnabled:       spec.Quota.Enabled,
		Retention:          spec.Retention.Duration,
	}
	if policy.SchemaVersion == "" {
		policy.SchemaVersion = "v1"
	}
	if policy.Retention <= 0 {
		policy.Retention = defaultRetention
	}
	classMaxBytes := spec.MaxBytes.Value()
	if requestedMaxBytes != "" {
		if !spec.Quota.Enabled {
			return cache.Policy{}, errors.New("maxBytes requires an XFS project quota CacheClass")
		}
		requested, err := resource.ParseQuantity(requestedMaxBytes)
		if err != nil || requested.Sign() <= 0 {
			return cache.Policy{}, errors.New("maxBytes must be a positive Kubernetes quantity")
		}
		if classMaxBytes > 0 && requested.Value() > classMaxBytes {
			return cache.Policy{}, errors.New("maxBytes exceeds the CacheClass per-cache quota ceiling")
		}
		policy.MaxBytes = requested.Value()
	} else if spec.Quota.DefaultMaxBytes.Sign() > 0 {
		policy.MaxBytes = spec.Quota.DefaultMaxBytes.Value()
	} else {
		policy.MaxBytes = classMaxBytes
	}
	if spec.Quota.Enabled && policy.MaxBytes <= 0 {
		return cache.Policy{}, errors.New("CacheClass quota is enabled without an effective maxBytes limit")
	}
	return policy, nil
}

func (s *Server) publishNewCache(ctx context.Context, req *csi.NodePublishVolumeRequest, identity string, lease cache.Lease, policy cache.Policy) error {
	source, _, err := s.store.Acquire(cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy})
	if err != nil {
		code := codes.Internal
		if errors.Is(err, cache.ErrQuotaPolicyConflict) || errors.Is(err, cache.ErrExclusivePolicyConflict) {
			code = codes.FailedPrecondition
		}
		return status.Errorf(code, "acquire cache: %v", err)
	}
	if err := ApplyQuota(ctx, s.store, s.quota, policy, identity, source); err != nil {
		return s.rollbackPublish(req, status.Errorf(codes.FailedPrecondition, "apply cache quota: %v", err))
	}
	source, err = s.store.Expose(identity)
	if err != nil {
		return s.rollbackPublish(req, status.Errorf(codes.FailedPrecondition, "expose cache generation: %v", err))
	}
	if err := s.mount(req, source, policy.NoExec); err != nil {
		mounted, inspectErr := s.mounter.mountedAt(req.GetTargetPath())
		if inspectErr != nil {
			return status.Errorf(codes.Internal, "publish cache failed: %v; inspect mount before lease rollback: %v", err, inspectErr)
		}
		if mounted {
			return status.Errorf(codes.Internal, "publish cache failed: %v; cache mount remains and its lease was retained", err)
		}
		return s.rollbackPublish(req, err)
	}
	return nil
}

func (s *Server) rollbackPublish(req *csi.NodePublishVolumeRequest, publishErr error) error {
	if err := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "publish cache failed: %v; release lease failed: %v", publishErr, err)
	}
	return publishErr
}

func (s *Server) publishCache(ctx context.Context, req *csi.NodePublishVolumeRequest, identity, source string, policy cache.Policy) error {
	if err := ApplyQuota(ctx, s.store, s.quota, policy, identity, source); err != nil {
		return status.Errorf(codes.FailedPrecondition, "apply cache quota: %v", err)
	}
	source, err := s.store.Expose(identity)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "expose cache generation: %v", err)
	}
	return s.mount(req, source, policy.NoExec)
}

func (s *Server) mount(req *csi.NodePublishVolumeRequest, source string, noExec bool) error {
	target := req.GetTargetPath()
	if err := makeTargetDirectory(target); err != nil {
		return status.Errorf(codes.Internal, "prepare mount target: %v", err)
	}
	if err := s.mounter.bindMount(source, target); err != nil {
		return status.Errorf(codes.Internal, "bind cache: %v", err)
	}
	if err := s.mounter.remountOptions(target, req.GetReadonly(), noExec); err != nil {
		if unmountErr := s.mounter.unmount(target); unmountErr != nil && !errors.Is(unmountErr, errNotMounted) && !errors.Is(unmountErr, os.ErrNotExist) {
			return status.Errorf(codes.Internal, "apply cache mount options: %v; unmount failed: %v", err, unmountErr)
		}
		return status.Errorf(codes.Internal, "apply cache mount options: %v", err)
	}
	return nil
}

func (s *Server) publishFallback(req *csi.NodePublishVolumeRequest) error {
	if !filepath.IsAbs(s.options.FallbackRoot) {
		return status.Error(codes.FailedPrecondition, "fallback cache root is not configured")
	}
	source := fallbackPath(s.options.FallbackRoot, req.GetVolumeId())
	if err := os.MkdirAll(s.options.FallbackRoot, 0o700); err != nil {
		return status.Errorf(codes.Internal, "create fallback cache root: %v", err)
	}
	if err := ensureFallbackDirectory(source); err != nil {
		return status.Errorf(codes.Internal, "create fallback cache: %v", err)
	}
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return status.Errorf(codes.Internal, "inspect fallback target: %v", err)
	}
	if mounted {
		if same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), true); err != nil {
			return status.Errorf(codes.Internal, "verify fallback target: %v", err)
		} else if same {
			return nil
		}
		return status.Error(codes.AlreadyExists, "target is mounted from a different source")
	}
	if err := makeTargetDirectory(req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "prepare fallback target: %v", err)
	}
	if err := s.mounter.bindMount(source, req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "bind fallback cache: %v", err)
	}
	if err := s.mounter.remountOptions(req.GetTargetPath(), req.GetReadonly(), true); err != nil {
		if unmountErr := s.mounter.unmount(req.GetTargetPath()); unmountErr != nil {
			return status.Errorf(codes.Internal, "apply fallback mount options: %v; unmount failed: %v", err, unmountErr)
		}
		return status.Errorf(codes.Internal, "apply fallback mount options: %v", err)
	}
	return nil
}

func (s *Server) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || !filepath.IsAbs(req.GetTargetPath()) {
		return nil, status.Error(codes.InvalidArgument, "volume ID and absolute target path are required")
	}
	if err := s.validateTarget(req.GetTargetPath()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	unlock := s.locks.Lock(req.GetTargetPath())
	defer unlock()
	_, lease, source, _, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if errors.Is(err, cache.ErrDegradedMetadata) {
		found = false
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "read cache lease: %v", err)
	}
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect target mount: %v", err)
	}
	if found {
		if err := s.unpublishCacheLease(req, lease, source, mounted); err != nil {
			return nil, err
		}
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}
	if mounted {
		handled, err := s.unpublishDegradedMount(req.GetTargetPath())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "unpublish degraded cache: %v", err)
		}
		if handled {
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
	}
	if err := s.unpublishFallback(req, mounted); err != nil {
		return nil, err
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *Server) unpublishCacheLease(req *csi.NodeUnpublishVolumeRequest, lease cache.Lease, source string, mounted bool) error {
	if lease.Target != req.GetTargetPath() {
		return status.Error(codes.FailedPrecondition, "volume lease target does not match")
	}
	if mounted {
		same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), lease.ReadOnly, lease.NoExec)
		if err != nil {
			return status.Errorf(codes.Internal, "verify cache mount: %v", err)
		}
		if !same {
			return status.Error(codes.FailedPrecondition, "target mount does not belong to this cache volume")
		}
		if err := s.mounter.unmount(req.GetTargetPath()); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
			return status.Errorf(codes.Internal, "unmount cache: %v", err)
		}
	}
	if err := removeTargetDirectory(req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "remove cache mount target: %v", err)
	}
	if err := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "release cache lease: %v", err)
	}
	return nil
}

func (s *Server) unpublishDegradedMount(target string) (bool, error) {
	identity, _, found, err := s.store.FindDegradedGenerationForTarget(target, s.mounter.sameCacheSource)
	if err != nil {
		return false, fmt.Errorf("inspect degraded cache mount: %w", err)
	}
	if !found {
		return false, nil
	}
	if err := s.mounter.unmount(target); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("unmount degraded cache: %w", err)
	}
	if err := removeTargetDirectory(target); err != nil {
		return false, fmt.Errorf("remove degraded cache mount target: %w", err)
	}
	// CSIのunpublishはunmountで完了しているため、cacheの隔離失敗でkubeletへ不要な再試行を要求しない。
	_ = s.store.CleanupDegradedObject(identity, s.mounter.sourceMounted)
	return true, nil
}

func (s *Server) unpublishFallback(req *csi.NodeUnpublishVolumeRequest, mounted bool) error {
	fallback := fallbackPath(s.options.FallbackRoot, req.GetVolumeId())
	if mounted {
		same, err := s.mounter.sameCacheSource(fallback, req.GetTargetPath())
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return status.Errorf(codes.Internal, "verify fallback mount: %v", err)
		}
		if !same {
			return status.Error(codes.FailedPrecondition, "target mount has no matching cache lease")
		}
		if err := s.mounter.unmount(req.GetTargetPath()); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
			return status.Errorf(codes.Internal, "unmount fallback cache: %v", err)
		}
	}
	if err := removeTargetDirectory(req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "remove fallback target: %v", err)
	}
	if err := os.RemoveAll(fallback); err != nil {
		return status.Errorf(codes.Internal, "remove fallback cache: %v", err)
	}
	return nil
}

func (s *Server) NodeGetVolumeHealth(_ context.Context, req *csi.NodeGetVolumeHealthRequest) (*csi.NodeGetVolumeHealthResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	_, lease, source, _, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil {
		if errors.Is(err, cache.ErrDegradedMetadata) {
			return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeMetadataUnreadable", "cache volume metadata is unreadable")}, nil
		}
		return nil, status.Errorf(codes.Internal, "read cache lease: %v", err)
	}
	if !found {
		if !filepath.IsAbs(req.GetVolumePublishPath()) {
			return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeLeaseMissing", "cache volume lease is missing")}, nil
		}
		lease.Target = req.GetVolumePublishPath()
		source = fallbackPath(s.options.FallbackRoot, req.GetVolumeId())
	}
	mounted, err := s.mounter.mountedAt(lease.Target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect cache mount: %v", err)
	}
	if !mounted {
		return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeMountMissing", "cache volume mount is missing")}, nil
	}
	var same bool
	if found {
		same, err = s.mounter.sameCacheMount(source, lease.Target, lease.ReadOnly, lease.NoExec)
	} else {
		same, err = s.mounter.sameCacheSource(source, lease.Target)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "verify cache mount: %v", err)
	}
	if !same {
		return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeSourceMismatch", "cache volume source does not match its lease")}, nil
	}
	return &csi.NodeGetVolumeHealthResponse{VolumeHealth: &csi.VolumeHealth{VolumeId: req.GetVolumeId()}}, nil
}

func unhealthyVolume(volumeID, reason, message string) *csi.VolumeHealth {
	return &csi.VolumeHealth{VolumeId: volumeID, HealthStatuses: []*csi.VolumeHealth_VolumeHealthEntry{{Status: csi.VolumeHealthErrorType_INACCESSIBLE, Reason: reason, Message: message}}}
}

func (s *Server) NodeGetStorageHealth(context.Context, *csi.NodeGetStorageHealthRequest) (*csi.NodeGetStorageHealthResponse, error) {
	if err := s.store.MetadataError(); err != nil {
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, "CacheMetadataUnreadable", "one or more cache metadata records cannot be read")
	}
	readOnly, err := s.mounter.filesystemReadOnly(s.store.Root())
	if err != nil {
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_UNREACHABLE, "CacheRootUnavailable", "cache root filesystem cannot be inspected")
	}
	if readOnly {
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, "CacheRootReadOnly", "cache root filesystem is read-only")
	}
	return &csi.NodeGetStorageHealthResponse{}, nil
}

func storageHealthResponse(status csi.StorageHealthErrorType, reason, message string) (*csi.NodeGetStorageHealthResponse, error) {
	return &csi.NodeGetStorageHealthResponse{BackendHealth: []*csi.NodeGetStorageHealthResponse_StorageBackendHealth{{Status: status, Reason: reason, Message: message}}}, nil
}

func (s *Server) validateTarget(target string) error {
	if !filepath.IsAbs(s.options.KubeletRoot) {
		return errors.New("kubelet root is not configured")
	}
	podsRoot := filepath.Join(filepath.Clean(s.options.KubeletRoot), "pods")
	relative, err := filepath.Rel(podsRoot, target)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("target path must be inside the kubelet pods directory")
	}
	return nil
}

func isSingleNodeAccessMode(mode *csi.VolumeCapability_AccessMode) bool {
	if mode == nil {
		return false
	}
	switch mode.GetMode() {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
		return true
	case csi.VolumeCapability_AccessMode_UNKNOWN,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		return false
	default:
		return false
	}
}

func makeTargetDirectory(path string) error {
	if err := os.Mkdir(path, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("mount target is not a real directory")
	}
	return nil
}

func removeTargetDirectory(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("mount target is not a real directory")
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func fallbackPath(root, volumeID string) string {
	hash := sha256.Sum256([]byte(volumeID))
	return filepath.Join(root, hex.EncodeToString(hash[:]))
}

func ensureFallbackDirectory(path string) error {
	if err := os.MkdirAll(path, 0o777); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("fallback cache path is not a real directory")
	}
	return os.Chmod(path, 0o777)
}

func IsMountedAt(target string) (bool, error) { return mountedAt(target) }

// VerifyCacheMountはleaseの情報を使ってcache bind mountを検証する。targetが不明な場合はsourceがmountされているか確認する。
func VerifyCacheMount(source string, lease cache.Lease, policy cache.Policy) (bool, error) {
	return verifyCacheMount(newMounter(), source, lease, policy)
}

func verifyCacheMount(mount mounter, source string, lease cache.Lease, _ cache.Policy) (bool, error) {
	if lease.Target == "" {
		return mount.sourceMounted(source)
	}
	return mount.sameCacheMount(source, lease.Target, lease.ReadOnly, lease.NoExec)
}

func RecoverCacheLeases(store *cache.Store) error { return recoverCacheLeases(store, newMounter()) }

func recoverCacheLeases(store *cache.Store, mount mounter) error {
	return store.RecoverLeases(func(source string, lease cache.Lease, policy cache.Policy) (bool, error) {
		return verifyCacheMount(mount, source, lease, policy)
	})
}

func ApplyQuota(ctx context.Context, store *cache.Store, quotaManager ProjectQuota, policy cache.Policy, identity, source string) error {
	if !policy.QuotaEnabled {
		return nil
	}
	if quotaManager == nil {
		return errors.New("XFS project quota support is unavailable")
	}
	projectID, assignProject, setLimit, err := store.QuotaState(identity, policy.MaxBytes)
	if err != nil {
		return err
	}
	if assignProject || setLimit {
		if err := quotaManager.Configure(ctx, store.Root(), source, projectID, policy.MaxBytes); err != nil {
			return err
		}
	}
	if assignProject {
		if err := store.MarkProjectAssigned(identity); err != nil {
			return err
		}
	}
	if setLimit {
		if err := store.MarkQuotaApplied(identity, policy.MaxBytes); err != nil {
			return err
		}
	}
	return nil
}
