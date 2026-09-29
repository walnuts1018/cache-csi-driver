package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
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

	if !s.store.Ready() {
		if err := s.publishWhileStoreNotReady(ctx, req); err != nil {
			return nil, err
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

	outcome, err := s.handleExistingPublish(req)
	if err != nil {
		return nil, err
	}
	if outcome != "" {
		s.recordPublishOutcome(outcome)
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if err := s.publish(ctx, req, volumeContext); err != nil {
		return nil, err
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *Server) publishWhileStoreNotReady(ctx context.Context, req *csi.NodePublishVolumeRequest) error {
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return status.Errorf(codes.Internal, "inspect target mount while cache store is recovering: %v", err)
	}
	if !mounted {
		return s.useFallback(ctx, req, requestedFallbackBytes(req), true, cache.PressurePolicyUnusedOnly, expectedFallbackCause("store_recovering"), nil)
	}
	if s.fallbackStore != nil {
		_, lease, source, _, found, err := s.fallbackStore.LeaseDetails(req.GetVolumeId())
		if err != nil {
			return status.Errorf(codes.Internal, "read fallback cache lease while cache store is recovering: %v", err)
		}
		if found {
			handled, err := s.verifyExistingLeaseMount(s.fallbackStore, req, lease, source)
			if err != nil {
				return err
			}
			if handled {
				s.recordFallbackReuse()
				return nil
			}
		}
	}
	fallback := fallbackPath(s.options.FallbackRoot, req.GetVolumeId())
	same, err := s.mounter.sameCacheMount(fallback, req.GetTargetPath(), req.GetReadonly(), true)
	if err != nil {
		return status.Errorf(codes.Internal, "verify fallback mount while cache store is recovering: %v", err)
	}
	if same {
		s.recordFallbackReuse()
		return nil
	}
	return status.Error(codes.Unavailable, "cache store recovery is in progress and the existing mount has not been verified")
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
	accessMode := req.GetVolumeCapability().GetAccessMode()
	if !isSingleNodeAccessMode(accessMode) {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "a single-node access mode is required")
	}
	if accessMode.GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY && !req.GetReadonly() {
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "single-node reader-only access requires a readonly volume publish")
	}
	attributes := req.GetVolumeContext()
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

func (s *Server) handleExistingPublish(req *csi.NodePublishVolumeRequest) (string, error) {
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return "", status.Errorf(codes.Internal, "inspect target mount: %v", err)
	}
	if !mounted {
		return "", nil
	}
	_, lease, source, _, found, err := s.store.LeaseDetails(req.GetVolumeId())
	metadataDegraded := errors.Is(err, cache.ErrDegradedMetadata)
	if metadataDegraded {
		err = nil
	}
	if err != nil {
		return "", status.Errorf(codes.Internal, "read cache lease: %v", err)
	}
	if found {
		handled, err := s.verifyExistingLeaseMount(s.store, req, lease, source)
		if handled {
			return publishOutcomeHit, err
		}
		return "", err
	}
	if s.fallbackStore != nil {
		_, fallbackLease, fallbackSource, _, fallbackFound, err := s.fallbackStore.LeaseDetails(req.GetVolumeId())
		fallbackDegraded := errors.Is(err, cache.ErrDegradedMetadata)
		metadataDegraded = metadataDegraded || fallbackDegraded
		if fallbackDegraded {
			err = nil
		}
		if err != nil {
			return "", status.Errorf(codes.Internal, "read fallback lease: %v", err)
		}
		if fallbackFound {
			handled, err := s.verifyExistingLeaseMount(s.fallbackStore, req, fallbackLease, fallbackSource)
			if handled {
				return publishOutcomeFallback, err
			}
			return "", err
		}
	}
	verifiedDegradedMount, verifyErr := s.verifyExistingDegradedMount(req)
	if verifyErr != nil {
		return "", status.Errorf(codes.Internal, "verify degraded cache mount: %v", verifyErr)
	}
	if verifiedDegradedMount {
		return publishOutcomeHit, nil
	}
	if metadataDegraded {
		return "", status.Error(codes.Internal, "cache volume metadata is unreadable")
	}
	fallback := fallbackPath(s.options.FallbackRoot, req.GetVolumeId())
	same, err := s.mounter.sameCacheMount(fallback, req.GetTargetPath(), req.GetReadonly(), true)
	if err != nil {
		return "", status.Errorf(codes.Internal, "verify fallback mount: %v", err)
	}
	if same {
		return publishOutcomeFallback, nil
	}
	return "", status.Error(codes.AlreadyExists, "target is already mounted by another volume")
}

func (s *Server) verifyExistingDegradedMount(req *csi.NodePublishVolumeRequest) (bool, error) {
	if !matchesInlineVolumeIDTarget(req.GetVolumeId(), s.options.KubeletRoot, req.GetTargetPath()) {
		return false, nil
	}
	for _, store := range []*cache.Store{s.store, s.fallbackStore} {
		if store == nil {
			continue
		}
		_, source, found, err := store.FindDegradedGenerationForTarget(req.GetTargetPath(), s.mounter.sameCacheSource)
		if err != nil {
			return false, err
		}
		if !found {
			continue
		}
		for _, noExec := range []bool{false, true} {
			same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), noExec)
			if err != nil {
				return false, err
			}
			if same {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *Server) verifyExistingLeaseMount(store *cache.Store, req *csi.NodePublishVolumeRequest, lease cache.Lease, source string) (bool, error) {
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
	if !same {
		return false, status.Error(codes.AlreadyExists, "target is mounted from a different source or with different options")
	}
	if err := store.CommitPublish(lease.ID, lease.Target); err != nil {
		return false, status.Errorf(codes.Internal, "commit published cache lease: %v", err)
	}
	return true, nil
}

func (s *Server) publish(ctx context.Context, req *csi.NodePublishVolumeRequest, volumeContext podVolumeContext) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	identity, policy, err := s.resolve(ctx, volumeContext)
	if err != nil {
		return s.publishAfterResolutionFailure(ctx, req, volumeContext, err)
	}
	unlockIdentity := s.identityLocks.Lock(identity)
	defer unlockIdentity()

	storedPolicy, existingLease, err := s.prepareLease(req, identity)
	if err != nil {
		if cause, ok := fallbackCauseForError(err, "cache_acquire_failed"); ok {
			return s.useFallback(ctx, req, policy.MaxBytes, policy.NoExec, policy.PressurePolicy, cause, err)
		}
		if errors.Is(err, cache.ErrDegradedMetadata) {
			return status.Errorf(codes.Internal, "read cache lease: %v", err)
		}
		return err
	}
	if existingLease {
		policy = storedPolicy
	}
	lease := cache.Lease{ID: req.GetVolumeId(), Target: req.GetTargetPath(), Namespace: volumeContext.namespace, PodName: volumeContext.name, PodUID: volumeContext.uid, ReadOnly: req.GetReadonly(), NoExec: policy.NoExec}
	return s.publishNewCache(ctx, req, identity, lease, policy)
}

func (s *Server) prepareLease(req *csi.NodePublishVolumeRequest, identity string) (cache.Policy, bool, error) {
	oldIdentity, oldLease, _, oldPolicy, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil {
		if errors.Is(err, cache.ErrDegradedMetadata) {
			return cache.Policy{}, false, err
		}
		return cache.Policy{}, false, status.Errorf(codes.Internal, "read cache lease: %v", err)
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
				code := codes.Internal
				if errors.Is(err, cache.ErrLeaseGenerationRetired) {
					code = codes.FailedPrecondition
				}
				return cache.Policy{}, false, status.Errorf(code, "begin cache publish: %v", err)
			}
			return oldPolicy, true, nil
		}
		if err := s.store.Release(req.GetVolumeId(), oldLease.Target); err != nil {
			return cache.Policy{}, false, status.Errorf(codes.Internal, "release cache lease after CacheClass change: %v", err)
		}
	}
	if s.fallbackStore != nil {
		_, fallbackLease, _, _, found, err := s.fallbackStore.LeaseDetails(req.GetVolumeId())
		if err != nil {
			return cache.Policy{}, false, status.Errorf(codes.Internal, "read fallback cache lease: %v", err)
		}
		if found {
			if fallbackLease.Target != req.GetTargetPath() || fallbackLease.ReadOnly != req.GetReadonly() {
				return cache.Policy{}, false, status.Error(codes.AlreadyExists, "fallback volume ID is already published with different target settings")
			}
			if err := s.fallbackStore.Release(req.GetVolumeId(), fallbackLease.Target); err != nil {
				return cache.Policy{}, false, status.Errorf(codes.Internal, "release fallback cache lease after CacheClass resolution: %v", err)
			}
		}
	}
	return cache.Policy{}, false, nil
}

func (s *Server) publishAfterResolutionFailure(ctx context.Context, req *csi.NodePublishVolumeRequest, volumeContext podVolumeContext, resolveErr error) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	canFallback := errors.Is(resolveErr, kube.ErrAPIResolverUnavailable) || errors.Is(resolveErr, kube.ErrResolverNotSynced) ||
		errors.Is(resolveErr, kube.ErrServiceAccountNotCached) || kube.IsTemporaryAPIError(resolveErr)
	if !canFallback {
		return status.Errorf(codes.FailedPrecondition, "resolve CacheClass %q: %v", volumeContext.cacheClass, resolveErr)
	}
	identity, lease, source, policy, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil {
		if errors.Is(err, cache.ErrDegradedMetadata) {
			return s.useFallback(ctx, req, requestedFallbackBytes(req), true, cache.PressurePolicyUnusedOnly, expectedFallbackCause("metadata_degraded"), err)
		}
		return s.useFallback(ctx, req, requestedFallbackBytes(req), true, cache.PressurePolicyUnusedOnly, unexpectedFallbackCause("cache_acquire_failed"), err)
	}
	if !found {
		if s.fallbackStore != nil {
			_, lease, _, _, found, err := s.fallbackStore.LeaseDetails(req.GetVolumeId())
			if err != nil {
				return status.Errorf(codes.Internal, "read fallback cache lease: %v", err)
			}
			if found {
				if lease.Target != req.GetTargetPath() {
					return status.Error(codes.AlreadyExists, "fallback volume ID is already published at a different target")
				}
				if lease.ReadOnly != req.GetReadonly() {
					return status.Error(codes.AlreadyExists, "fallback volume ID is already published with a different readonly flag")
				}
			}
		}
		return s.useFallback(ctx, req, requestedFallbackBytes(req), true, cache.PressurePolicyUnusedOnly, fallbackCauseForResolution(resolveErr), resolveErr)
	}
	if lease.Target != req.GetTargetPath() {
		return status.Error(codes.AlreadyExists, "volume ID is already published at a different target")
	}
	if lease.ReadOnly != req.GetReadonly() {
		return status.Error(codes.AlreadyExists, "volume ID is already published with a different readonly flag")
	}
	unlockIdentity := s.identityLocks.Lock(identity)
	defer unlockIdentity()
	if err := s.store.BeginPublish(req.GetVolumeId(), lease.Target); err != nil {
		code := codes.Internal
		if errors.Is(err, cache.ErrLeaseGenerationRetired) {
			code = codes.FailedPrecondition
		}
		return status.Errorf(code, "begin cache publish: %v", err)
	}
	return s.publishCache(ctx, req, identity, source, policy)
}

func (s *Server) resolve(ctx context.Context, volumeContext podVolumeContext) (string, cache.Policy, error) {
	if s.resolver == nil {
		return "", cache.Policy{}, kube.ErrAPIResolverUnavailable
	}
	namespaceUID, serviceAccountUID, class, err := s.resolver.Resolve(ctx, volumeContext.namespace, volumeContext.cacheClass, volumeContext.serviceAccountName)
	if err != nil {
		return "", cache.Policy{}, err
	}
	policy, err := policyFor(class.Object.Spec, volumeContext.cacheClass, class.UID, volumeContext.maxBytes)
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

func policyFor(spec cachev1alpha1.CacheClassSpec, className, classUID, requestedMaxBytes string) (cache.Policy, error) {
	if err := spec.Validate(); err != nil {
		return cache.Policy{}, err
	}
	sharingPolicy := spec.SharingPolicy
	if sharingPolicy == "" {
		sharingPolicy = cachev1alpha1.SharingPolicyExclusive
	}
	pressurePolicy := spec.PressurePolicy
	if pressurePolicy == "" {
		pressurePolicy = cachev1alpha1.PressurePolicyUnused
	}
	policy := cache.Policy{
		ClassName:          className,
		ClassUID:           classUID,
		SharingPolicy:      string(sharingPolicy),
		NoExec:             spec.NoExec,
		SchemaVersion:      spec.SchemaVersion,
		CrashRecoveryReuse: spec.CrashRecovery == "reuse",
		PressurePolicy:     string(pressurePolicy),
		QuotaEnabled:       spec.Quota.Enabled,
		Retention:          spec.Retention.Duration,
	}
	if policy.SchemaVersion == "" {
		policy.SchemaVersion = "v1"
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
		if cause, ok := fallbackCauseForError(err, "cache_acquire_failed"); ok {
			return s.useFallback(ctx, req, policy.MaxBytes, policy.NoExec, policy.PressurePolicy, cause, err)
		}
		code := codes.Internal
		if errors.Is(err, cache.ErrQuotaPolicyConflict) || errors.Is(err, cache.ErrExclusivePolicyConflict) || errors.Is(err, cache.ErrLeaseGenerationRetired) {
			code = codes.FailedPrecondition
		}
		return status.Errorf(code, "acquire cache: %v", err)
	}
	if err := ApplyQuota(ctx, s.store, s.quota, policy, identity, source); err != nil {
		if err := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); err != nil {
			s.markUnexpectedBackendFailure(ctx, unexpectedFallbackCause("quota_setup_failed"), err)
			return status.Errorf(codes.Internal, "apply cache quota failed; release normal cache lease failed: %v", err)
		}
		return s.useFallback(ctx, req, policy.MaxBytes, policy.NoExec, policy.PressurePolicy, unexpectedFallbackCause("quota_setup_failed"), err)
	}
	source, err = s.store.Expose(identity)
	if err != nil {
		if cause, ok := fallbackCauseForError(err, "generation_expose_failed"); ok {
			if releaseErr := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); releaseErr != nil {
				if cause.class == fallbackClassUnexpected {
					s.markUnexpectedBackendFailure(ctx, cause, err)
				}
				return status.Errorf(codes.Internal, "expose cache generation failed; release cache lease failed: %v", releaseErr)
			}
			return s.useFallback(ctx, req, policy.MaxBytes, policy.NoExec, policy.PressurePolicy, cause, err)
		}
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
	if err := s.store.CommitPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "commit published cache lease: %v", err)
	}
	s.recordNormalPublish(publishOutcomeMiss)
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
		if releaseErr := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); releaseErr != nil {
			s.markUnexpectedBackendFailure(ctx, unexpectedFallbackCause("quota_setup_failed"), err)
			return status.Errorf(codes.Internal, "apply cache quota failed; release cache lease failed: %v", releaseErr)
		}
		return s.useFallback(ctx, req, policy.MaxBytes, policy.NoExec, policy.PressurePolicy, unexpectedFallbackCause("quota_setup_failed"), err)
	}
	source, err := s.store.Expose(identity)
	if err != nil {
		if releaseErr := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); releaseErr != nil {
			s.markUnexpectedBackendFailure(ctx, unexpectedFallbackCause("generation_expose_failed"), err)
			return status.Errorf(codes.Internal, "expose cache generation failed; release cache lease failed: %v", releaseErr)
		}
		return s.useFallback(ctx, req, policy.MaxBytes, policy.NoExec, policy.PressurePolicy, unexpectedFallbackCause("generation_expose_failed"), err)
	}
	if err := s.mount(req, source, policy.NoExec); err != nil {
		return err
	}
	if err := s.store.CommitPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "commit published cache lease: %v", err)
	}
	s.recordNormalPublish(publishOutcomeHit)
	return nil
}

func (s *Server) mount(req *csi.NodePublishVolumeRequest, source string, noExec bool) error {
	target := req.GetTargetPath()
	if err := makeTargetDirectory(target); err != nil {
		return status.Errorf(codes.Internal, "prepare mount target: %v", err)
	}
	if err := s.mounter.mount(source, target, req.GetReadonly(), noExec); err != nil {
		mounted, inspectErr := s.mounter.mountedAt(target)
		if inspectErr != nil {
			return status.Errorf(codes.Internal, "mount cache: %v; inspect target mount: %v", err, inspectErr)
		}
		if mounted {
			return status.Errorf(codes.Internal, "mount cache: %v; cache mount remains and its lease was retained", err)
		}
		return status.Errorf(codes.Internal, "mount cache: %v", err)
	}
	return nil
}

func requestedFallbackBytes(req *csi.NodePublishVolumeRequest) int64 {
	quantity, err := resource.ParseQuantity(req.GetVolumeContext()["maxBytes"])
	if err != nil || quantity.Sign() <= 0 {
		return 0
	}
	return quantity.Value()
}

func (s *Server) publishFallback(ctx context.Context, req *csi.NodePublishVolumeRequest, requestedBytes int64, noExec bool, pressurePolicy string) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if !filepath.IsAbs(s.options.FallbackRoot) || s.fallbackStore == nil || s.fallbackStore.Root() != filepath.Clean(s.options.FallbackRoot) {
		return status.Error(codes.FailedPrecondition, "fallback cache root is not configured")
	}
	identity, err := cache.FallbackIdentity(req.GetVolumeId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "derive fallback cache identity: %v", err)
	}
	unlock := s.identityLocks.Lock(identity)
	defer unlock()
	volumeContext := req.GetVolumeContext()
	policy := cache.Policy{ClassName: "fallback", SharingPolicy: cache.SharingPolicyExclusive, DiscardOnLastRelease: true, NoExec: noExec, PressurePolicy: pressurePolicy}
	lease := cache.Lease{
		ID:        req.GetVolumeId(),
		Target:    req.GetTargetPath(),
		Namespace: volumeContext["csi.storage.k8s.io/pod.namespace"],
		PodName:   volumeContext["csi.storage.k8s.io/pod.name"],
		PodUID:    volumeContext["csi.storage.k8s.io/pod.uid"],
		ReadOnly:  req.GetReadonly(),
		NoExec:    noExec,
	}
	allocation, err := s.fallbackStore.AcquireFallback(cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy}, requestedBytes, s.options.FallbackVolumeMaxBytes, s.options.FallbackMaxBytes)
	if err != nil {
		code := codes.Internal
		if errors.Is(err, cache.ErrFallbackCapacity) {
			code = codes.ResourceExhausted
		} else if errors.Is(err, cache.ErrFallbackLeaseConflict) {
			code = codes.AlreadyExists
		} else if errors.Is(err, cache.ErrExclusivePolicyConflict) {
			code = codes.FailedPrecondition
		}
		return status.Errorf(code, "acquire fallback cache: %v", err)
	}
	noExec = allocation.NoExec
	if err := s.fallbackStore.BeginPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "begin fallback cache publish: %v", err)
	}
	if err := s.mounter.prepareFallback(allocation.Source, allocation.MaxBytes, noExec); err != nil {
		return s.rollbackFallbackPublish(req, status.Errorf(codes.FailedPrecondition, "prepare bounded fallback cache: %v", err))
	}
	if err := ctx.Err(); err != nil {
		return s.rollbackFallbackPublish(req, status.FromContextError(err).Err())
	}
	source, err := s.fallbackStore.Expose(identity)
	if err != nil {
		return s.rollbackFallbackPublish(req, status.Errorf(codes.FailedPrecondition, "expose fallback cache: %v", err))
	}
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return status.Errorf(codes.Internal, "inspect fallback target: %v", err)
	}
	if mounted {
		if same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), noExec); err != nil {
			return status.Errorf(codes.Internal, "verify fallback target: %v", err)
		} else if same {
			if err := s.fallbackStore.CommitPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
				return status.Errorf(codes.Internal, "commit published fallback lease: %v", err)
			}
			return nil
		}
		return s.rollbackFallbackPublish(req, status.Error(codes.AlreadyExists, "target is mounted from a different source"))
	}
	if err := makeTargetDirectory(req.GetTargetPath()); err != nil {
		return s.rollbackFallbackPublish(req, status.Errorf(codes.Internal, "prepare fallback target: %v", err))
	}
	if err := ctx.Err(); err != nil {
		return s.rollbackFallbackPublish(req, status.FromContextError(err).Err())
	}
	if err := s.mounter.mount(source, req.GetTargetPath(), req.GetReadonly(), noExec); err != nil {
		mounted, inspectErr := s.mounter.mountedAt(req.GetTargetPath())
		if inspectErr != nil {
			return status.Errorf(codes.Internal, "mount fallback cache: %v; inspect target mount before lease rollback: %v", err, inspectErr)
		}
		if mounted {
			return status.Errorf(codes.Internal, "mount fallback cache: %v; fallback mount remains and its lease was retained", err)
		}
		return s.rollbackFallbackPublish(req, status.Errorf(codes.Internal, "mount fallback cache: %v", err))
	}
	if err := ctx.Err(); err != nil {
		if unmountErr := s.mounter.unmount(req.GetTargetPath()); unmountErr != nil && !errors.Is(unmountErr, errNotMounted) && !errors.Is(unmountErr, os.ErrNotExist) {
			return status.Errorf(codes.Internal, "cancel fallback publish: %v; unmount fallback cache: %v", err, unmountErr)
		}
		if removeErr := removeTargetDirectory(req.GetTargetPath()); removeErr != nil {
			return status.Errorf(codes.Internal, "cancel fallback publish: %v; remove fallback target: %v", err, removeErr)
		}
		return s.rollbackFallbackPublish(req, status.FromContextError(err).Err())
	}
	if err := s.fallbackStore.CommitPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "commit published fallback lease: %v", err)
	}
	return nil
}

func (s *Server) rollbackFallbackPublish(req *csi.NodePublishVolumeRequest, publishErr error) error {
	if err := s.fallbackStore.Release(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "fallback publish failed: %v; release fallback lease failed: %v", publishErr, err)
	}
	return publishErr
}
