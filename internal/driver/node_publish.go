package driver

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
	"github.com/walnuts1018/cache-csi-driver/internal/kubeletcompat"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (s *Server) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeContext, err := s.validatePublishRequest(req)
	if err != nil {
		return nil, err
	}
	unlock := s.locks.Lock(req.GetTargetPath())
	defer unlock()
	unlockVolume := s.identityLocks.Lock("\x00volume\x00" + req.GetVolumeId())
	defer unlockVolume()

	if outcome, err := s.handleExistingPublish(ctx, req); err != nil {
		return nil, err
	} else if outcome != "" {
		s.recordNormalPublish(outcome)
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if !s.store.Ready() {
		return nil, status.Error(codes.Unavailable, "cache store recovery is in progress")
	}
	if snapshot := s.options.Health.Current(); !snapshot.Schedulable() {
		return nil, status.Errorf(codes.Unavailable, "cache node is %s: %s", snapshot.Phase, snapshot.Reason)
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
	mountVolume := req.GetVolumeCapability().GetMount()
	if mountVolume == nil {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "only mount volumes are supported")
	}
	if mountVolume.GetFsType() != "" {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "filesystem type is not supported for directory bind mounts")
	}
	if len(mountVolume.GetMountFlags()) != 0 {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "mount flags are not supported")
	}
	if !isSingleNodeAccessMode(req.GetVolumeCapability().GetAccessMode()) {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "SINGLE_NODE_WRITER access mode is required")
	}
	attributes := req.GetVolumeContext()
	for key := range attributes {
		switch key {
		case "cacheClass", "cacheKey", "maxBytes",
			"csi.storage.k8s.io/ephemeral",
			"csi.storage.k8s.io/pod.namespace",
			"csi.storage.k8s.io/pod.name",
			"csi.storage.k8s.io/pod.uid",
			"csi.storage.k8s.io/serviceAccount.name",
			"csi.storage.k8s.io/serviceAccount.tokens":
		default:
			return podVolumeContext{}, status.Error(codes.InvalidArgument, "volume context contains an unsupported attribute")
		}
	}
	volumeContext := podVolumeContext{
		namespace:          attributes["csi.storage.k8s.io/pod.namespace"],
		name:               attributes["csi.storage.k8s.io/pod.name"],
		uid:                attributes["csi.storage.k8s.io/pod.uid"],
		serviceAccountName: attributes["csi.storage.k8s.io/serviceAccount.name"],
		cacheClass:         attributes["cacheClass"],
		cacheKey:           attributes["cacheKey"],
		maxBytes:           attributes["maxBytes"],
	}
	if attributes["csi.storage.k8s.io/ephemeral"] != "true" {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "only inline ephemeral CSI volumes are supported")
	}
	if volumeContext.namespace == "" || volumeContext.name == "" || volumeContext.uid == "" || volumeContext.cacheClass == "" || volumeContext.cacheKey == "" {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "pod information, cacheClass, and cacheKey are required")
	}
	if err := s.validatePublishTarget(req.GetTargetPath(), volumeContext.uid); err != nil {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, err.Error())
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

func (s *Server) handleExistingPublish(ctx context.Context, req *csi.NodePublishVolumeRequest) (string, error) {
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return "", s.nodeBackendError(ctx, "MountInspectionFailed", err)
	}
	if !mounted {
		return "", nil
	}
	_, lease, source, _, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if errors.Is(err, cache.ErrDegradedMetadata) {
		verified, verifyErr := s.verifyExistingDegradedMount(req)
		if verifyErr != nil {
			return "", s.nodeBackendError(ctx, "DegradedMountInspectionFailed", verifyErr)
		}
		if verified {
			return publishOutcomeHit, nil
		}
		return "", status.Error(codes.Unavailable, "cache metadata is degraded and the existing mount could not be verified")
	}
	if err != nil {
		return "", s.nodeBackendError(ctx, "CacheMetadataReadFailed", err)
	}
	if found {
		handled, verifyErr := s.verifyExistingLeaseMount(ctx, req, lease, source)
		if verifyErr != nil {
			return "", verifyErr
		}
		if handled {
			return publishOutcomeHit, nil
		}
	}
	return "", status.Error(codes.AlreadyExists, "target is already mounted by another volume or cache generation")
}

func (s *Server) verifyExistingDegradedMount(req *csi.NodePublishVolumeRequest) (bool, error) {
	if _, ok := kubeletcompat.ParsePodTarget(s.options.KubeletRoot, req.GetTargetPath()); !ok {
		return false, nil
	}
	_, source, found, err := s.store.FindDegradedGenerationForTarget(req.GetTargetPath(), s.mounter.sameCacheSource)
	if err != nil || !found {
		return false, err
	}
	for _, noExec := range []bool{false, true} {
		same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), noExec)
		if err != nil || same {
			return same, err
		}
	}
	return false, nil
}

func (s *Server) verifyExistingLeaseMount(ctx context.Context, req *csi.NodePublishVolumeRequest, lease cache.Lease, source string) (bool, error) {
	if lease.Target != req.GetTargetPath() {
		return false, status.Error(codes.AlreadyExists, "volume ID is already published at a different target")
	}
	if lease.ReadOnly != req.GetReadonly() {
		return false, status.Error(codes.AlreadyExists, "volume ID is already published with a different readonly flag")
	}
	same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), lease.NoExec)
	if err != nil {
		return false, s.nodeBackendError(ctx, "CacheMountVerificationFailed", err)
	}
	if !same {
		return false, status.Error(codes.AlreadyExists, "target is mounted from a different source or with different options")
	}
	if err := s.store.CommitPublish(lease.ID, lease.Target); err != nil {
		return false, s.recordMountedLeaseCommitFailure(ctx, lease.ID, err)
	}
	return true, nil
}

func (s *Server) publish(ctx context.Context, req *csi.NodePublishVolumeRequest, volumeContext podVolumeContext) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	identity, policy, err := s.resolve(ctx, volumeContext)
	if err != nil {
		if errors.Is(err, kube.ErrAPIResolverUnavailable) || errors.Is(err, kube.ErrResolverNotSynced) || kube.IsTemporaryAPIError(err) {
			return status.Errorf(codes.Unavailable, "resolve CacheClass %q while the Kubernetes API is unavailable: %v", volumeContext.cacheClass, err)
		}
		return status.Errorf(codes.FailedPrecondition, "resolve CacheClass %q: %v", volumeContext.cacheClass, err)
	}
	unlockIdentity := s.identityLocks.Lock(identity)
	defer unlockIdentity()

	storedPolicy, existingLease, err := s.prepareLease(ctx, req, identity)
	if err != nil {
		return err
	}
	outcome := publishOutcomeMiss
	if existingLease {
		policy = storedPolicy
		outcome = publishOutcomeHit
	}
	lease := cache.Lease{ID: req.GetVolumeId(), Target: req.GetTargetPath(), Namespace: volumeContext.namespace, PodName: volumeContext.name, PodUID: volumeContext.uid, ReadOnly: req.GetReadonly(), NoExec: policy.NoExec}
	return s.publishNewCache(ctx, req, identity, lease, policy, outcome)
}

func (s *Server) prepareLease(ctx context.Context, req *csi.NodePublishVolumeRequest, identity string) (cache.Policy, bool, error) {
	oldIdentity, oldLease, _, oldPolicy, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil {
		if errors.Is(err, cache.ErrDegradedMetadata) {
			recoverErr := s.store.RecoverDegraded(ctx, func(source string, _ cache.Lease, _ cache.Policy) (bool, error) {
				return s.mounter.sourceMounted(source)
			})
			if recoverErr != nil {
				if ctx.Err() != nil {
					return cache.Policy{}, false, status.FromContextError(ctx.Err()).Err()
				}
				return cache.Policy{}, false, s.nodeBackendError(ctx, "CacheQuarantineFailed", recoverErr)
			}
			_, _, _, _, found, err = s.store.LeaseDetails(req.GetVolumeId())
			if errors.Is(err, cache.ErrDegradedMetadata) {
				return cache.Policy{}, false, status.Error(codes.Unavailable, "cache metadata is degraded and an existing generation is still mounted")
			}
			if err != nil {
				return cache.Policy{}, false, s.nodeBackendError(ctx, "CacheMetadataReadFailed", err)
			}
			if found {
				return cache.Policy{}, false, status.Error(codes.AlreadyExists, "volume ID is still present after degraded cache quarantine")
			}
			return cache.Policy{}, false, nil
		}
		return cache.Policy{}, false, s.nodeBackendError(ctx, "CacheMetadataReadFailed", err)
	}
	if found {
		if oldLease.Target != req.GetTargetPath() {
			return cache.Policy{}, false, status.Error(codes.AlreadyExists, "volume ID is already published at a different target")
		}
		if oldLease.ReadOnly != req.GetReadonly() {
			return cache.Policy{}, false, status.Error(codes.AlreadyExists, "volume ID is already published with a different readonly flag")
		}
		if oldIdentity == identity {
			if err := s.store.BeginPublish(req.GetVolumeId(), oldLease.Target); err != nil {
				if errors.Is(err, cache.ErrLeaseGenerationRetired) {
					return cache.Policy{}, false, status.Errorf(codes.FailedPrecondition, "begin cache publish: %v", err)
				}
				return cache.Policy{}, false, s.nodeBackendError(ctx, "CacheLeaseUpdateFailed", err)
			}
			return oldPolicy, true, nil
		}
		if err := s.store.Release(req.GetVolumeId(), oldLease.Target); err != nil {
			return cache.Policy{}, false, s.nodeBackendError(ctx, "CacheLeaseReleaseFailed", err)
		}
	}
	return cache.Policy{}, false, nil
}

func (s *Server) resolve(ctx context.Context, volumeContext podVolumeContext) (string, cache.Policy, error) {
	if s.resolver == nil {
		return "", cache.Policy{}, kube.ErrAPIResolverUnavailable
	}
	namespaceUID, serviceAccountUID, class, err := s.resolver.Resolve(ctx, volumeContext.namespace, volumeContext.cacheClass, volumeContext.serviceAccountName)
	if err != nil {
		return "", cache.Policy{}, err
	}
	policy, err := policyFor(class.Object.Spec, volumeContext.cacheClass, class.UID, volumeContext.maxBytes, s.options.ProjectQuotaEnabled)
	if err != nil {
		return "", cache.Policy{}, err
	}
	var identity string
	if class.Object.Spec.Scope == cachev1alpha1.ScopeNamespace {
		identity, err = cache.Identity(namespaceUID, volumeContext.cacheClass, class.UID, volumeContext.cacheKey, policy.SchemaVersion)
	} else {
		identity, err = cache.IdentityWithServiceAccount(namespaceUID, serviceAccountUID, volumeContext.cacheClass, class.UID, volumeContext.cacheKey, policy.SchemaVersion)
	}
	return identity, policy, err
}

func policyFor(spec cachev1alpha1.CacheClassSpec, className, classUID, requestedMaxBytes string, projectQuotaEnabled bool) (cache.Policy, error) {
	if err := spec.Validate(); err != nil {
		return cache.Policy{}, err
	}
	sharingPolicy := spec.SharingPolicy
	if sharingPolicy == "" {
		sharingPolicy = cachev1alpha1.SharingPolicyExclusive
	}
	policy := cache.Policy{
		ClassName:     className,
		ClassUID:      classUID,
		SharingPolicy: string(sharingPolicy),
		NoExec:        spec.NoExec,
		SchemaVersion: spec.SchemaVersion,
		QuotaEnabled:  projectQuotaEnabled,
		Retention:     spec.Retention.Duration,
	}
	if policy.SchemaVersion == "" {
		policy.SchemaVersion = "v1"
	}
	classMaxBytes := spec.Storage.MaxBytes.Value()
	defaultMaxBytes := spec.Storage.DefaultMaxBytes.Value()
	if !policy.QuotaEnabled && (classMaxBytes > 0 || defaultMaxBytes > 0 || requestedMaxBytes != "") {
		return cache.Policy{}, errors.New("CacheClass size limits require the node's xfs-project storage backend")
	}
	if policy.QuotaEnabled && classMaxBytes <= 0 && defaultMaxBytes <= 0 {
		return cache.Policy{}, errors.New("xfs-project storage backend requires CacheClass maxBytes or defaultMaxBytes")
	}
	if requestedMaxBytes != "" {
		if !policy.QuotaEnabled {
			return cache.Policy{}, errors.New("maxBytes requires the node's xfs-project storage backend")
		}
		requested, err := resource.ParseQuantity(requestedMaxBytes)
		if err != nil || requested.Sign() <= 0 {
			return cache.Policy{}, errors.New("maxBytes must be a positive Kubernetes quantity")
		}
		if classMaxBytes > 0 && requested.Value() > classMaxBytes {
			return cache.Policy{}, errors.New("maxBytes exceeds the CacheClass per-cache quota ceiling")
		}
		policy.MaxBytes = requested.Value()
	} else if defaultMaxBytes > 0 {
		policy.MaxBytes = defaultMaxBytes
	} else {
		policy.MaxBytes = classMaxBytes
	}
	if policy.QuotaEnabled && policy.MaxBytes <= 0 {
		return cache.Policy{}, errors.New("xfs-project CacheClass has no effective maxBytes limit")
	}
	return policy, nil
}

func (s *Server) publishNewCache(ctx context.Context, req *csi.NodePublishVolumeRequest, identity string, lease cache.Lease, policy cache.Policy, outcome string) error {
	_, _, err := s.store.Acquire(cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy})
	if errors.Is(err, cache.ErrDegradedMetadata) {
		if quarantineErr := s.store.QuarantineDegradedObject(identity, s.mounter.sourceMounted); quarantineErr != nil {
			return s.nodeBackendError(ctx, "CacheQuarantineFailed", quarantineErr)
		}
		_, _, err = s.store.Acquire(cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy})
		if errors.Is(err, cache.ErrDegradedMetadata) {
			return status.Error(codes.Unavailable, "cache object is degraded and cannot be replaced while one of its generations remains mounted")
		}
	}
	if err != nil {
		return s.cacheAcquireError(ctx, err)
	}
	_, storedLease, source, storedPolicy, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil || !found {
		if err == nil {
			err = errors.New("cache lease disappeared after acquisition")
		}
		return s.rollbackPublish(ctx, req, status.Errorf(codes.Internal, "read acquired cache lease: %v", err))
	}
	policy = storedPolicy
	policy.NoExec = storedLease.NoExec
	if err := ApplyQuota(ctx, s.store, s.quota, policy, identity, source); err != nil {
		return s.rollbackPublish(ctx, req, s.nodeBackendError(ctx, "CacheQuotaUnavailable", err))
	}
	source, err = s.store.Expose(identity)
	if err != nil {
		if errors.Is(err, cache.ErrDegradedMetadata) {
			if quarantineErr := s.store.QuarantineDegradedObject(identity, s.mounter.sourceMounted); quarantineErr == nil {
				return s.rollbackPublish(ctx, req, status.Error(codes.Unavailable, "cache metadata became degraded before mount"))
			} else {
				err = errors.Join(err, quarantineErr)
			}
		}
		return s.rollbackPublish(ctx, req, s.nodeBackendError(ctx, "CacheGenerationExposeFailed", err))
	}
	return s.publishMountedCache(ctx, req, source, policy.NoExec, outcome)
}

func (s *Server) cacheAcquireError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, cache.ErrExclusivePolicyConflict), errors.Is(err, cache.ErrQuotaPolicyConflict), errors.Is(err, cache.ErrLeaseGenerationRetired):
		return status.Errorf(codes.FailedPrecondition, "acquire cache: %v", err)
	case errors.Is(err, cache.ErrPressureActive):
		return s.markNodeUnavailable(ctx, "CacheFilesystemPressure", err, false)
	default:
		return s.nodeBackendError(ctx, "CacheAcquireFailed", err)
	}
}

func (s *Server) rollbackPublish(ctx context.Context, req *csi.NodePublishVolumeRequest, publishErr error) error {
	if err := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return s.nodeBackendError(ctx, "CacheLeaseRollbackFailed", errors.Join(publishErr, err))
	}
	return publishErr
}

func (s *Server) publishMountedCache(ctx context.Context, req *csi.NodePublishVolumeRequest, source string, noExec bool, outcome string) error {
	if err := makeTargetDirectory(req.GetTargetPath()); err != nil {
		return s.rollbackPublish(ctx, req, status.Errorf(codes.Internal, "prepare cache mount target: %v", err))
	}
	if err := s.mounter.mount(source, req.GetTargetPath(), req.GetReadonly(), noExec); err != nil {
		mounted, inspectErr := s.mounter.mountedAt(req.GetTargetPath())
		if inspectErr != nil {
			return s.nodeBackendError(ctx, "CacheMountStateUnknown", errors.Join(err, inspectErr))
		}
		if mounted {
			same, verifyErr := s.mounter.sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), noExec)
			if verifyErr == nil && same {
				if commitErr := s.store.CommitPublish(req.GetVolumeId(), req.GetTargetPath()); commitErr != nil {
					return s.recordMountedLeaseCommitFailure(ctx, req.GetVolumeId(), commitErr)
				}
				s.recordNormalPublish(outcome)
				return nil
			}
			return s.nodeBackendError(ctx, "CacheMountStateUnknown", errors.Join(err, verifyErr))
		}
		return s.rollbackPublish(ctx, req, s.nodeBackendError(ctx, "CacheMountOperationFailed", err))
	}
	if err := s.store.CommitPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return s.recordMountedLeaseCommitFailure(ctx, req.GetVolumeId(), err)
	}
	s.recordNormalPublish(outcome)
	return nil
}

func (s *Server) recordMountedLeaseCommitFailure(ctx context.Context, volumeID string, err error) error {
	return s.nodeBackendError(ctx, "CacheLeaseCommitFailed", fmt.Errorf("volume %s: %w", volumeID, err))
}

func (s *Server) nodeBackendError(ctx context.Context, reason string, err error) error {
	return s.markNodeUnavailable(ctx, reason, err, true)
}
