package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

const (
	defaultRetention        = 72 * time.Hour
	defaultHighFreePercent  = 25
	defaultLowFreePercent   = 20
	defaultHighInodePercent = 15
	defaultLowInodePercent  = 10
)

type ClassResolver interface {
	Resolve(context.Context, string, string) (string, kube.ResolvedClass, error)
}

type ProjectQuota interface {
	AssignProject(context.Context, string, string, uint32) error
	SetLimit(context.Context, string, uint32, int64) error
}

type Options struct {
	NodeID       string
	KubeletRoot  string
	FallbackRoot string
}

type Server struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer
	store    *cache.Store
	resolver ClassResolver
	quota    ProjectQuota
	options  Options
	locks    operationLocks
}

func New(store *cache.Store, resolver ClassResolver, quotaManager ProjectQuota, options Options) *Server {
	return &Server{store: store, resolver: resolver, quota: quotaManager, options: options}
}

func (*Server) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: DriverName, VendorVersion: "0.1.0"}, nil
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
	if req.GetVolumeId() == "" || req.GetVolumeCapability() == nil || !filepath.IsAbs(req.GetTargetPath()) {
		return nil, status.Error(codes.InvalidArgument, "volume ID, volume capability, and absolute target path are required")
	}
	if req.GetVolumeCapability().GetMount() == nil {
		return nil, status.Error(codes.InvalidArgument, "only mount volumes are supported")
	}
	if !isSingleNodeAccessMode(req.GetVolumeCapability().GetAccessMode()) {
		return nil, status.Error(codes.InvalidArgument, "a single-node access mode is required")
	}
	if err := s.validateTarget(req.GetTargetPath()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	unlock := s.locks.Lock(req.GetTargetPath())
	defer unlock()

	mounted, err := mountedAt(req.GetTargetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect target mount: %v", err)
	}
	if mounted {
		if _, lease, source, policy, found, err := s.store.LeaseDetails(req.GetVolumeId()); err != nil {
			return nil, status.Errorf(codes.Internal, "read cache lease: %v", err)
		} else if found {
			if lease.Target != req.GetTargetPath() {
				return nil, status.Error(codes.AlreadyExists, "volume ID is already published at a different target")
			}
			if same, err := sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), policy.NoExec); err != nil {
				return nil, status.Errorf(codes.Internal, "verify existing cache mount: %v", err)
			} else if same {
				return &csi.NodePublishVolumeResponse{}, nil
			}
			return nil, status.Error(codes.AlreadyExists, "target is mounted from a different source or with different options")
		}
		fallback := fallbackPath(s.options.FallbackRoot, req.GetVolumeId())
		if same, err := sameCacheMount(fallback, req.GetTargetPath(), req.GetReadonly(), false); err != nil {
			return nil, status.Errorf(codes.Internal, "verify fallback mount: %v", err)
		} else if same {
			return &csi.NodePublishVolumeResponse{}, nil
		}
		return nil, status.Error(codes.AlreadyExists, "target is already mounted by another volume")
	}

	attributes := req.GetVolumeContext()
	podNamespace := attributes["csi.storage.k8s.io/pod.namespace"]
	podName := attributes["csi.storage.k8s.io/pod.name"]
	podUID := attributes["csi.storage.k8s.io/pod.uid"]
	cacheClassName := attributes["cacheClass"]
	cacheKey := attributes["cacheKey"]
	if podNamespace == "" || podName == "" || podUID == "" || cacheClassName == "" || cacheKey == "" {
		return nil, status.Error(codes.InvalidArgument, "pod information, cacheClass, and cacheKey are required")
	}
	if attributes["csi.storage.k8s.io/ephemeral"] != "true" {
		return nil, status.Error(codes.InvalidArgument, "only inline ephemeral CSI volumes are supported")
	}
	if len(cacheKey) > 1024 || strings.ContainsRune(cacheKey, '\x00') {
		return nil, status.Error(codes.InvalidArgument, "cacheKey is invalid or exceeds 1024 bytes")
	}

	identity, policy, resolveErr := s.resolve(ctx, attributes, cacheClassName)
	if resolveErr != nil {
		if oldIdentity, oldLease, source, oldPolicy, found, err := s.store.LeaseDetails(req.GetVolumeId()); err == nil && found && oldLease.Target == req.GetTargetPath() {
			if err := s.publishCache(ctx, req, oldIdentity, source, oldPolicy); err == nil {
				return &csi.NodePublishVolumeResponse{}, nil
			}
			_ = s.store.Release(req.GetVolumeId(), req.GetTargetPath())
		}
		if err := s.publishFallback(req); err != nil {
			return nil, err
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if oldIdentity, oldLease, _, _, found, err := s.store.LeaseDetails(req.GetVolumeId()); err != nil {
		return nil, status.Errorf(codes.Internal, "read cache lease: %v", err)
	} else if found {
		if oldLease.Target != req.GetTargetPath() {
			return nil, status.Error(codes.AlreadyExists, "volume ID is already published at a different target")
		}
		if oldIdentity != identity {
			if err := s.store.Release(req.GetVolumeId(), oldLease.Target); err != nil {
				return nil, status.Errorf(codes.Internal, "release cache lease after CacheClass change: %v", err)
			}
		}
	}
	lease := cache.Lease{ID: req.GetVolumeId(), Target: req.GetTargetPath(), Namespace: podNamespace, PodName: podName, PodUID: podUID}
	if err := s.publishNewCache(ctx, req, identity, lease, policy); err != nil {
		_ = s.store.Release(req.GetVolumeId(), req.GetTargetPath())
		if fallbackErr := s.publishFallback(req); fallbackErr == nil {
			return &csi.NodePublishVolumeResponse{}, nil
		}
		return nil, err
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *Server) resolve(ctx context.Context, attributes map[string]string, className string) (string, cache.Policy, error) {
	if s.resolver == nil {
		return "", cache.Policy{}, errors.New("Kubernetes API resolver is unavailable")
	}
	namespaceUID, class, err := s.resolver.Resolve(ctx, attributes["csi.storage.k8s.io/pod.namespace"], className)
	if err != nil {
		return "", cache.Policy{}, err
	}
	policy, err := policyFor(class.Object.Spec, className, class.UID, attributes["maxBytes"])
	if err != nil {
		return "", cache.Policy{}, err
	}
	identity, err := cache.Identity(namespaceUID, className, class.UID, attributes["cacheKey"], policy.SchemaVersion)
	return identity, policy, err
}

func policyFor(spec cachev1alpha1.CacheClassSpec, className, classUID, requestedMaxBytes string) (cache.Policy, error) {
	if err := spec.Validate(); err != nil {
		return cache.Policy{}, err
	}
	policy := cache.Policy{
		ClassName:            className,
		ClassUID:             classUID,
		NoExec:               spec.NoExec,
		SchemaVersion:        spec.SchemaVersion,
		CrashRecoveryReuse:   spec.CrashRecovery == "reuse",
		EvictRunning:         spec.EvictRunning,
		QuotaEnabled:         spec.Quota.Enabled,
		Retention:            spec.Retention.Duration,
		HighFreePercent:      int(spec.Pressure.HighFreePercent),
		LowFreePercent:       int(spec.Pressure.LowFreePercent),
		HighInodeFreePercent: int(spec.Pressure.HighInodeFreePercent),
		LowInodeFreePercent:  int(spec.Pressure.LowInodeFreePercent),
	}
	if policy.SchemaVersion == "" {
		policy.SchemaVersion = "v1"
	}
	if policy.Retention <= 0 {
		policy.Retention = defaultRetention
	}
	if policy.HighFreePercent == 0 {
		policy.HighFreePercent = defaultHighFreePercent
		policy.LowFreePercent = defaultLowFreePercent
	}
	if policy.HighInodeFreePercent == 0 {
		policy.HighInodeFreePercent = defaultHighInodePercent
		policy.LowInodeFreePercent = defaultLowInodePercent
	}
	if spec.MaxBytes.Sign() > 0 {
		policy.MaxBytes = spec.MaxBytes.Value()
	}
	if spec.Quota.DefaultMaxBytes.Sign() > 0 && policy.MaxBytes == 0 {
		policy.MaxBytes = spec.Quota.DefaultMaxBytes.Value()
	}
	if requestedMaxBytes != "" {
		if !spec.Quota.Enabled {
			return cache.Policy{}, errors.New("maxBytes requires an XFS project quota CacheClass")
		}
		requested, err := resource.ParseQuantity(requestedMaxBytes)
		if err != nil || requested.Sign() <= 0 {
			return cache.Policy{}, errors.New("maxBytes must be a positive Kubernetes quantity")
		}
		limit := requested.Value()
		if policy.MaxBytes > 0 && limit > policy.MaxBytes {
			limit = policy.MaxBytes
		}
		policy.MaxBytes = limit
	}
	if spec.Quota.Enabled && policy.MaxBytes <= 0 {
		return cache.Policy{}, errors.New("CacheClass quota is enabled without an effective maxBytes limit")
	}
	return policy, nil
}

func (s *Server) publishNewCache(ctx context.Context, req *csi.NodePublishVolumeRequest, identity string, lease cache.Lease, policy cache.Policy) error {
	source, _, err := s.store.Acquire(cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy})
	if err != nil {
		return status.Errorf(codes.Internal, "acquire cache: %v", err)
	}
	if err := ApplyQuota(ctx, s.store, s.quota, policy, identity, source); err != nil {
		return status.Errorf(codes.FailedPrecondition, "apply cache quota: %v", err)
	}
	return s.mount(req, source, policy.NoExec)
}

func (s *Server) publishCache(ctx context.Context, req *csi.NodePublishVolumeRequest, identity, source string, policy cache.Policy) error {
	if err := ApplyQuota(ctx, s.store, s.quota, policy, identity, source); err != nil {
		return status.Errorf(codes.FailedPrecondition, "apply cache quota: %v", err)
	}
	return s.mount(req, source, policy.NoExec)
}

func (s *Server) mount(req *csi.NodePublishVolumeRequest, source string, noExec bool) error {
	target := req.GetTargetPath()
	if err := makeTargetDirectory(target); err != nil {
		return status.Errorf(codes.Internal, "prepare mount target: %v", err)
	}
	if err := bindMount(source, target); err != nil {
		return status.Errorf(codes.Internal, "bind cache: %v", err)
	}
	if err := remountOptions(target, req.GetReadonly(), noExec); err != nil {
		_ = unmount(target)
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
	mounted, err := mountedAt(req.GetTargetPath())
	if err != nil {
		return status.Errorf(codes.Internal, "inspect fallback target: %v", err)
	}
	if mounted {
		if same, err := sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), false); err != nil {
			return status.Errorf(codes.Internal, "verify fallback target: %v", err)
		} else if same {
			return nil
		}
		return status.Error(codes.AlreadyExists, "target is mounted from a different source")
	}
	if err := makeTargetDirectory(req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "prepare fallback target: %v", err)
	}
	if err := bindMount(source, req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "bind fallback cache: %v", err)
	}
	if err := remountOptions(req.GetTargetPath(), req.GetReadonly(), false); err != nil {
		if unmountErr := unmount(req.GetTargetPath()); unmountErr != nil {
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
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read cache lease: %v", err)
	}
	mounted, err := mountedAt(req.GetTargetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect target mount: %v", err)
	}
	if found {
		if lease.Target != req.GetTargetPath() {
			return nil, status.Error(codes.FailedPrecondition, "volume lease target does not match")
		}
		if mounted {
			same, err := sameCacheMount(source, req.GetTargetPath(), false, false)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "verify cache mount: %v", err)
			}
			if !same {
				return nil, status.Error(codes.FailedPrecondition, "target mount does not belong to this cache volume")
			}
			if err := unmount(req.GetTargetPath()); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
				return nil, status.Errorf(codes.Internal, "unmount cache: %v", err)
			}
		}
		if err := removeTargetDirectory(req.GetTargetPath()); err != nil {
			return nil, status.Errorf(codes.Internal, "remove cache mount target: %v", err)
		}
		if err := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); err != nil {
			return nil, status.Errorf(codes.Internal, "release cache lease: %v", err)
		}
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}
	fallback := fallbackPath(s.options.FallbackRoot, req.GetVolumeId())
	if mounted {
		if same, err := sameCacheMount(fallback, req.GetTargetPath(), false, false); err != nil {
			return nil, status.Errorf(codes.Internal, "verify fallback mount: %v", err)
		} else if !same {
			return nil, status.Error(codes.FailedPrecondition, "target mount has no matching cache lease")
		}
		if err := unmount(req.GetTargetPath()); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.Internal, "unmount fallback cache: %v", err)
		}
	}
	if err := removeTargetDirectory(req.GetTargetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "remove fallback target: %v", err)
	}
	if err := os.RemoveAll(fallback); err != nil {
		return nil, status.Errorf(codes.Internal, "remove fallback cache: %v", err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *Server) NodeGetVolumeHealth(_ context.Context, req *csi.NodeGetVolumeHealthRequest) (*csi.NodeGetVolumeHealthResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	_, lease, source, _, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read cache lease: %v", err)
	}
	if !found {
		if !filepath.IsAbs(req.GetVolumePublishPath()) {
			return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeLeaseMissing", "cache volume lease is missing")}, nil
		}
		lease.Target = req.GetVolumePublishPath()
		source = fallbackPath(s.options.FallbackRoot, req.GetVolumeId())
	}
	mounted, err := mountedAt(lease.Target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect cache mount: %v", err)
	}
	if !mounted {
		return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeMountMissing", "cache volume mount is missing")}, nil
	}
	if same, err := sameCacheMount(source, lease.Target, false, false); err != nil {
		return nil, status.Errorf(codes.Internal, "verify cache mount: %v", err)
	} else if !same {
		return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeSourceMismatch", "cache volume source does not match its lease")}, nil
	}
	return &csi.NodeGetVolumeHealthResponse{VolumeHealth: &csi.VolumeHealth{VolumeId: req.GetVolumeId()}}, nil
}

func unhealthyVolume(volumeID, reason, message string) *csi.VolumeHealth {
	return &csi.VolumeHealth{VolumeId: volumeID, HealthStatuses: []*csi.VolumeHealth_VolumeHealthEntry{{Status: csi.VolumeHealthErrorType_INACCESSIBLE, Reason: reason, Message: message}}}
}

func (s *Server) NodeGetStorageHealth(context.Context, *csi.NodeGetStorageHealthRequest) (*csi.NodeGetStorageHealthResponse, error) {
	if err := s.store.MetadataError(); err != nil {
		return &csi.NodeGetStorageHealthResponse{BackendHealth: []*csi.NodeGetStorageHealthResponse_StorageBackendHealth{{Status: csi.StorageHealthErrorType_STORAGE_DEGRADED, Reason: "CacheMetadataUnreadable", Message: "one or more cache metadata records cannot be read"}}}, nil
	}
	readOnly, err := filesystemReadOnly(s.store.Root())
	if err != nil {
		return &csi.NodeGetStorageHealthResponse{BackendHealth: []*csi.NodeGetStorageHealthResponse_StorageBackendHealth{{Status: csi.StorageHealthErrorType_STORAGE_UNREACHABLE, Reason: "CacheRootUnavailable", Message: "cache root filesystem cannot be inspected"}}}, nil
	}
	if readOnly {
		return &csi.NodeGetStorageHealthResponse{BackendHealth: []*csi.NodeGetStorageHealthResponse_StorageBackendHealth{{Status: csi.StorageHealthErrorType_STORAGE_DEGRADED, Reason: "CacheRootReadOnly", Message: "cache root filesystem is read-only"}}}, nil
	}
	return &csi.NodeGetStorageHealthResponse{}, nil
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

func RecoverCacheLeases(store *cache.Store) error { return store.RecoverLeases(mountedAt) }

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
	if assignProject {
		if err := quotaManager.AssignProject(ctx, store.Root(), source, projectID); err != nil {
			return err
		}
		if err := store.MarkProjectAssigned(identity); err != nil {
			return err
		}
	}
	if setLimit {
		if err := quotaManager.SetLimit(ctx, store.Root(), projectID, policy.MaxBytes); err != nil {
			return err
		}
		if err := store.MarkQuotaApplied(identity, policy.MaxBytes); err != nil {
			return err
		}
	}
	return nil
}
