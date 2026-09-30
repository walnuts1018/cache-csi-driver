package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

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

	if !s.store.Ready() {
		if err := s.publishWhileStoreNotReady(ctx, req, volumeContext); err != nil {
			return nil, err
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

	outcome, err := s.handleExistingPublish(ctx, req)
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

func (s *Server) publishWhileStoreNotReady(ctx context.Context, req *csi.NodePublishVolumeRequest, volumeContext podVolumeContext) error {
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return status.Errorf(codes.Internal, "inspect target mount while cache store is recovering: %v", err)
	}
	if !mounted {
		_, policy, resolveErr := s.resolve(ctx, volumeContext)
		if resolveErr != nil {
			if !resolutionFailureAllowsFallback(resolveErr) {
				return status.Errorf(codes.FailedPrecondition, "resolve CacheClass %q: %v", volumeContext.cacheClass, resolveErr)
			}
			return s.useFallback(ctx, req, expectedFallbackCause("store_recovering"), resolveErr)
		}
		return s.useFallbackWithPolicy(ctx, req, expectedFallbackCause("store_recovering"), nil, &policy)
	}
	for _, store := range s.fallbackStores() {
		_, lease, source, _, found, err := store.LeaseDetails(req.GetVolumeId())
		if err != nil {
			if errors.Is(err, cache.ErrDegradedMetadata) {
				continue
			}
			return status.Errorf(codes.Internal, "read fallback cache lease while cache store is recovering: %v", err)
		}
		if found {
			handled, err := s.verifyExistingLeaseMount(ctx, store, req, lease, source)
			if err != nil {
				return err
			}
			if handled {
				s.recordFallbackReuse()
				return nil
			}
		}
	}
	verifiedDegradedMount, verifyErr := s.verifyExistingDegradedMount(req)
	if verifyErr != nil {
		return status.Errorf(codes.Internal, "verify degraded cache mount while cache store is recovering: %v", verifyErr)
	}
	if verifiedDegradedMount {
		s.recordFallbackReuse()
		return nil
	}
	terminalSame, err := s.verifyTerminalFallbackMount(req)
	if err != nil {
		return status.Errorf(codes.Internal, "verify terminal fallback mount while cache store is recovering: %v", err)
	}
	if terminalSame {
		s.recordFallbackReuse()
		return nil
	}
	for _, store := range s.fallbackStores() {
		fallback := fallbackPath(s.fallbackRootForStore(store), req.GetVolumeId())
		same, err := s.mounter.sameCacheMount(fallback, req.GetTargetPath(), req.GetReadonly(), true)
		if err != nil {
			return status.Errorf(codes.Internal, "verify fallback mount while cache store is recovering: %v", err)
		}
		if same {
			s.recordFallbackReuse()
			return nil
		}
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
		return podVolumeContext{}, status.Error(codes.InvalidArgument, "SINGLE_NODE_WRITER access mode is required")
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

func (s *Server) handleExistingPublish(ctx context.Context, req *csi.NodePublishVolumeRequest) (string, error) {
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return "", status.Errorf(codes.Internal, "inspect target mount: %v", err)
	}
	if !mounted {
		return "", nil
	}
	terminalSame, err := s.verifyTerminalFallbackMount(req)
	if err != nil {
		return "", status.Errorf(codes.Internal, "verify terminal fallback mount: %v", err)
	}
	if terminalSame {
		return publishOutcomeFallback, nil
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
		handled, err := s.verifyExistingLeaseMount(ctx, s.store, req, lease, source)
		if handled {
			return publishOutcomeHit, err
		}
		return "", err
	}
	for _, fallbackStore := range s.fallbackStores() {
		_, fallbackLease, fallbackSource, _, fallbackFound, err := fallbackStore.LeaseDetails(req.GetVolumeId())
		fallbackDegraded := errors.Is(err, cache.ErrDegradedMetadata)
		metadataDegraded = metadataDegraded || fallbackDegraded
		if fallbackDegraded {
			err = nil
		}
		if err != nil {
			return "", status.Errorf(codes.Internal, "read fallback lease: %v", err)
		}
		if fallbackFound {
			handled, err := s.verifyExistingLeaseMount(ctx, fallbackStore, req, fallbackLease, fallbackSource)
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
	for _, fallbackStore := range s.fallbackStores() {
		fallback := fallbackPath(s.fallbackRootForStore(fallbackStore), req.GetVolumeId())
		same, err := s.mounter.sameCacheMount(fallback, req.GetTargetPath(), req.GetReadonly(), true)
		if err != nil {
			return "", status.Errorf(codes.Internal, "verify fallback mount: %v", err)
		}
		if same {
			return publishOutcomeFallback, nil
		}
	}
	return "", status.Error(codes.AlreadyExists, "target is already mounted by another volume")
}

func (s *Server) verifyExistingDegradedMount(req *csi.NodePublishVolumeRequest) (bool, error) {
	if _, ok := kubeletcompat.ParsePodTarget(s.options.KubeletRoot, req.GetTargetPath()); !ok {
		return false, nil
	}
	stores := append([]*cache.Store{s.store}, s.fallbackStores()...)
	for _, store := range stores {
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

func (s *Server) verifyTerminalFallbackMount(req *csi.NodePublishVolumeRequest) (bool, error) {
	return s.terminalFallbackMount(req.GetVolumeId(), req.GetTargetPath())
}

func (s *Server) terminalFallbackMount(volumeID, target string) (bool, error) {
	source := fallbackPath(s.options.FallbackTerminalRoot, volumeID)
	for _, noExec := range []bool{false, true} {
		same, err := s.mounter.sameCacheMount(source, target, true, noExec)
		if err != nil || same {
			return same, err
		}
	}
	return false, nil
}

func (s *Server) verifyExistingLeaseMount(ctx context.Context, store *cache.Store, req *csi.NodePublishVolumeRequest, lease cache.Lease, source string) (bool, error) {
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
		s.recordMountedLeaseCommitFailure(ctx, lease.ID, err)
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
			return s.useFallbackWithPolicy(ctx, req, cause, err, &policy)
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
	for _, fallbackStore := range s.fallbackStores() {
		_, fallbackLease, _, _, found, err := fallbackStore.LeaseDetails(req.GetVolumeId())
		if err != nil {
			return cache.Policy{}, false, status.Errorf(codes.Internal, "read fallback cache lease: %v", err)
		}
		if found {
			if fallbackLease.Target != req.GetTargetPath() || fallbackLease.ReadOnly != req.GetReadonly() {
				return cache.Policy{}, false, status.Error(codes.AlreadyExists, "fallback volume ID is already published with different target settings")
			}
			if err := fallbackStore.Release(req.GetVolumeId(), fallbackLease.Target); err != nil {
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
	if !resolutionFailureAllowsFallback(resolveErr) {
		return status.Errorf(codes.FailedPrecondition, "resolve CacheClass %q: %v", volumeContext.cacheClass, resolveErr)
	}
	identity, lease, source, policy, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil {
		if errors.Is(err, cache.ErrDegradedMetadata) {
			return s.useFallback(ctx, req, expectedFallbackCause("metadata_degraded"), err)
		}
		return s.useFallback(ctx, req, unexpectedFallbackCause("cache_acquire_failed"), err)
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
		return s.useFallback(ctx, req, fallbackCauseForResolution(resolveErr), resolveErr)
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

func resolutionFailureAllowsFallback(err error) bool {
	return errors.Is(err, kube.ErrAPIResolverUnavailable) || errors.Is(err, kube.ErrResolverNotSynced) || kube.IsTemporaryAPIError(err)
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
		QuotaEnabled:       spec.Storage.Backend == cachev1alpha1.BackendXFSProject,
		Retention:          spec.Retention.Duration,
	}
	if policy.SchemaVersion == "" {
		policy.SchemaVersion = "v1"
	}
	classMaxBytes := spec.Storage.MaxBytes.Value()
	if requestedMaxBytes != "" {
		if !policy.QuotaEnabled {
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
	} else if spec.Storage.DefaultMaxBytes.Sign() > 0 {
		policy.MaxBytes = spec.Storage.DefaultMaxBytes.Value()
	} else {
		policy.MaxBytes = classMaxBytes
	}
	if policy.QuotaEnabled && policy.MaxBytes <= 0 {
		return cache.Policy{}, errors.New("xfs-project CacheClass has no effective maxBytes limit")
	}
	return policy, nil
}

func (s *Server) publishNewCache(ctx context.Context, req *csi.NodePublishVolumeRequest, identity string, lease cache.Lease, policy cache.Policy) error {
	_, _, err := s.store.Acquire(cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy})
	if err != nil {
		if cause, ok := fallbackCauseForError(err, "cache_acquire_failed"); ok {
			return s.useFallbackWithPolicy(ctx, req, cause, err, &policy)
		}
		code := codes.Internal
		if errors.Is(err, cache.ErrQuotaPolicyConflict) || errors.Is(err, cache.ErrExclusivePolicyConflict) || errors.Is(err, cache.ErrLeaseGenerationRetired) {
			code = codes.FailedPrecondition
		}
		return status.Errorf(code, "acquire cache: %v", err)
	}
	_, storedLease, storedSource, storedPolicy, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if err != nil || !found {
		if err == nil {
			err = errors.New("cache lease disappeared after acquisition")
		}
		if releaseErr := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("release cache lease after reading its generation policy: %w", releaseErr))
		}
		return s.useFallbackWithPolicy(ctx, req, unexpectedFallbackCause("cache_acquire_failed"), err, &policy)
	}
	source := storedSource
	policy = storedPolicy
	policy.NoExec = storedLease.NoExec
	if err := ApplyQuota(ctx, s.store, s.quota, policy, identity, source); err != nil {
		if err := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); err != nil {
			s.markUnexpectedBackendFailure(ctx, unexpectedFallbackCause("quota_setup_failed"), err)
			return status.Errorf(codes.Internal, "apply cache quota failed; release normal cache lease failed: %v", err)
		}
		return s.useFallbackWithPolicy(ctx, req, unexpectedFallbackCause("quota_setup_failed"), err, &policy)
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
			return s.useFallbackWithPolicy(ctx, req, cause, err, &policy)
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
		s.recordMountedLeaseCommitFailure(ctx, req.GetVolumeId(), err)
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
		return s.useFallbackWithPolicy(ctx, req, unexpectedFallbackCause("quota_setup_failed"), err, &policy)
	}
	source, err := s.store.Expose(identity)
	if err != nil {
		if releaseErr := s.store.Release(req.GetVolumeId(), req.GetTargetPath()); releaseErr != nil {
			s.markUnexpectedBackendFailure(ctx, unexpectedFallbackCause("generation_expose_failed"), err)
			return status.Errorf(codes.Internal, "expose cache generation failed; release cache lease failed: %v", releaseErr)
		}
		return s.useFallbackWithPolicy(ctx, req, unexpectedFallbackCause("generation_expose_failed"), err, &policy)
	}
	if err := s.mount(req, source, policy.NoExec); err != nil {
		return err
	}
	if err := s.store.CommitPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		s.recordMountedLeaseCommitFailure(ctx, req.GetVolumeId(), err)
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

func (s *Server) publishFallback(ctx context.Context, req *csi.NodePublishVolumeRequest, semantics fallbackSemantics) error {
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
	requestedBytes := requestedFallbackBytes(req)
	noExec := true
	if semantics.mode == fallbackModeResolved {
		noExec = semantics.noExec
	}
	pressurePolicy := cache.PressurePolicyUnusedOnly
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
	stores := s.fallbackStores()
	for _, store := range stores {
		_, _, _, storedPolicy, found, leaseErr := store.LeaseDetails(req.GetVolumeId())
		if leaseErr != nil || !found {
			continue
		}
		if store.Root() != filepath.Clean(s.fallbackRootForStore(store)) {
			return status.Error(codes.FailedPrecondition, "fallback cache store root is not configured")
		}
		perVolumeLimit, aggregateLimit := s.fallbackLimits(store)
		perVolumeLimit = max(perVolumeLimit, storedPolicy.MaxBytes)
		allocation, err := store.AcquireFallback(cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy}, requestedBytes, perVolumeLimit, aggregateLimit)
		if err != nil {
			if shouldRetryEmergencyFallback(err) {
				if releaseErr := store.Release(req.GetVolumeId(), req.GetTargetPath()); releaseErr != nil {
					return status.Errorf(codes.Internal, "fallback acquisition failed: %v; release fallback lease before retry failed: %v", err, releaseErr)
				}
				if store != s.emergencyStore && s.emergencyStore != nil {
					allocation, emergencyErr := s.emergencyStore.AcquireFallback(cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy}, requestedBytes, s.options.FallbackEmergencyVolumeMaxBytes, s.options.FallbackEmergencyMaxBytes)
					if emergencyErr == nil {
						return s.publishFallbackOnStoreOrTerminal(ctx, req, s.emergencyStore, identity, allocation, cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy}, requestedBytes, semantics)
					}
					if !shouldRetryEmergencyFallback(emergencyErr) {
						return fallbackAcquireError(emergencyErr)
					}
				}
				terminalSemantics := fallbackSemantics{mode: fallbackModeResolved, noExec: storedPolicy.NoExec}
				return s.publishTerminalFallback(ctx, req, terminalSemantics)
			}
			return fallbackAcquireError(err)
		}
		return s.publishFallbackOnStoreOrTerminal(ctx, req, store, identity, allocation, cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy}, requestedBytes, semantics)
	}
	acquireOptions := cache.AcquireOptions{Identity: identity, Lease: lease, Policy: policy}
	allocation, err := s.fallbackStore.AcquireFallback(acquireOptions, requestedBytes, s.options.FallbackVolumeMaxBytes, s.options.FallbackMaxBytes)
	store := s.fallbackStore
	if err != nil && shouldRetryEmergencyFallback(err) && s.emergencyStore != nil {
		if !filepath.IsAbs(s.options.FallbackEmergencyRoot) || s.emergencyStore.Root() != filepath.Clean(s.options.FallbackEmergencyRoot) {
			return status.Error(codes.FailedPrecondition, "emergency fallback cache store root is not configured")
		}
		// emergency generationはvolumeごとにサイズを制限し、集約予約枠の枯渇でPod起動を止めない。
		allocation, err = s.emergencyStore.AcquireFallback(acquireOptions, requestedBytes, s.options.FallbackEmergencyVolumeMaxBytes, s.options.FallbackEmergencyMaxBytes)
		store = s.emergencyStore
	}
	if err != nil {
		if shouldRetryEmergencyFallback(err) {
			return s.publishTerminalFallback(ctx, req, semantics)
		}
		return fallbackAcquireError(err)
	}
	return s.publishFallbackOnStoreOrTerminal(ctx, req, store, identity, allocation, acquireOptions, requestedBytes, semantics)
}

func (s *Server) fallbackLimits(store *cache.Store) (int64, int64) {
	if store == s.emergencyStore {
		return s.options.FallbackEmergencyVolumeMaxBytes, s.options.FallbackEmergencyMaxBytes
	}
	return s.options.FallbackVolumeMaxBytes, s.options.FallbackMaxBytes
}

func (s *Server) publishTerminalFallback(ctx context.Context, req *csi.NodePublishVolumeRequest, semantics fallbackSemantics) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if !filepath.IsAbs(s.options.FallbackTerminalRoot) {
		return status.Error(codes.FailedPrecondition, "terminal fallback root is not configured")
	}
	source, err := makeTerminalFallbackDirectory(s.options.FallbackTerminalRoot, req.GetVolumeId())
	if err != nil {
		return status.Errorf(codes.Internal, "prepare terminal fallback source: %v", err)
	}
	noExec := semantics.mode != fallbackModeResolved || semantics.noExec
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return status.Errorf(codes.Internal, "inspect terminal fallback target: %v", err)
	}
	if mounted {
		same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), true, noExec)
		if err != nil {
			return status.Errorf(codes.Internal, "verify terminal fallback target: %v", err)
		}
		if same {
			return nil
		}
		return status.Error(codes.AlreadyExists, "target is mounted from a different source")
	}
	if err := makeTargetDirectory(req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "prepare terminal fallback target: %v", err)
	}
	// 終端fallbackはデータ保持よりPodの起動を優先し、volumeをread-onlyで公開する。
	if err := s.mounter.mount(source, req.GetTargetPath(), true, noExec); err != nil {
		return status.Errorf(codes.Internal, "mount terminal fallback: %v", err)
	}
	return nil
}

func shouldRetryEmergencyFallback(err error) bool {
	return errors.Is(err, cache.ErrFallbackCapacity) || errors.Is(err, cache.ErrDegradedMetadata) || errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}

func shouldUseTerminalFallback(err error) bool {
	tierErr, ok := errors.AsType[*fallbackTierError](err)
	if !ok {
		return false
	}
	return errors.Is(tierErr.cause, cache.ErrFallbackCapacity) || errors.Is(tierErr.cause, cache.ErrDegradedMetadata) || errors.Is(tierErr.cause, syscall.ENOSPC) || errors.Is(tierErr.cause, syscall.EDQUOT)
}

type fallbackTierError struct {
	rpcErr error
	cause  error
}

func (e *fallbackTierError) Error() string { return e.rpcErr.Error() }

func (e *fallbackTierError) Unwrap() error { return e.cause }

func fallbackAcquireError(err error) error {
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

func (s *Server) publishFallbackOnStoreOrTerminal(ctx context.Context, req *csi.NodePublishVolumeRequest, store *cache.Store, identity string, allocation cache.FallbackAllocation, acquireOptions cache.AcquireOptions, requestedBytes int64, semantics fallbackSemantics) error {
	err := s.publishFallbackOnStore(ctx, req, store, identity, allocation)
	if err == nil {
		return err
	}
	tierErr, tierFailure := errors.AsType[*fallbackTierError](err)
	if !tierFailure || !shouldUseTerminalFallback(err) {
		if tierFailure {
			return tierErr.rpcErr
		}
		return err
	}
	mounted, inspectErr := s.mounter.mountedAt(req.GetTargetPath())
	if inspectErr != nil {
		return status.Errorf(codes.Internal, "inspect fallback target before terminal retry: %v", inspectErr)
	}
	if mounted {
		return tierErr.rpcErr
	}
	if releaseErr := store.Release(req.GetVolumeId(), req.GetTargetPath()); releaseErr != nil {
		s.markUnexpectedBackendFailure(ctx, unexpectedFallbackCause("fallback_lease_release_failed"), releaseErr)
		s.logger.WarnContext(ctx, "fallback lease could not be released before changing tiers", "volumeID", req.GetVolumeId(), "root", store.Root(), "error", releaseErr)
		if s.isFallbackStore(store) {
			if err := s.mounter.unmountGeneration(allocation.Source); err != nil {
				s.logger.WarnContext(ctx, "failed to unmount fallback generation before changing tiers", "source", allocation.Source, "error", err)
			}
		}
	}
	if store != s.emergencyStore && s.emergencyStore != nil {
		allocation, acquireErr := s.emergencyStore.AcquireFallback(acquireOptions, requestedBytes, s.options.FallbackEmergencyVolumeMaxBytes, s.options.FallbackEmergencyMaxBytes)
		if acquireErr == nil {
			return s.publishFallbackOnStoreOrTerminal(ctx, req, s.emergencyStore, identity, allocation, acquireOptions, requestedBytes, semantics)
		}
		if !shouldRetryEmergencyFallback(acquireErr) {
			return fallbackAcquireError(acquireErr)
		}
	}
	terminalSemantics := semantics
	terminalSemantics.mode = fallbackModeResolved
	terminalSemantics.noExec = allocation.NoExec
	return s.publishTerminalFallback(ctx, req, terminalSemantics)
}

func (s *Server) publishFallbackOnStore(ctx context.Context, req *csi.NodePublishVolumeRequest, store *cache.Store, identity string, allocation cache.FallbackAllocation) error {
	noExec := allocation.NoExec
	if err := store.BeginPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return &fallbackTierError{rpcErr: status.Errorf(codes.Internal, "begin fallback cache publish: %v", err), cause: err}
	}
	if err := s.mounter.prepareFallback(allocation.Source, allocation.MaxBytes, noExec); err != nil {
		return s.rollbackFallbackTierPublish(store, req, err, "prepare bounded fallback cache")
	}
	if err := ctx.Err(); err != nil {
		return s.rollbackFallbackPublish(store, req, status.FromContextError(err).Err())
	}
	source, err := store.Expose(identity)
	if err != nil {
		return s.rollbackFallbackTierPublish(store, req, err, "expose fallback cache")
	}
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return status.Errorf(codes.Internal, "inspect fallback target: %v", err)
	}
	if mounted {
		if same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), req.GetReadonly(), noExec); err != nil {
			return status.Errorf(codes.Internal, "verify fallback target: %v", err)
		} else if same {
			if err := store.CommitPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
				s.recordMountedLeaseCommitFailure(ctx, req.GetVolumeId(), err)
			}
			return nil
		}
		return s.rollbackFallbackPublish(store, req, status.Error(codes.AlreadyExists, "target is mounted from a different source"))
	}
	if err := makeTargetDirectory(req.GetTargetPath()); err != nil {
		return s.rollbackFallbackPublish(store, req, status.Errorf(codes.Internal, "prepare fallback target: %v", err))
	}
	if err := ctx.Err(); err != nil {
		return s.rollbackFallbackPublish(store, req, status.FromContextError(err).Err())
	}
	if err := s.mounter.mount(source, req.GetTargetPath(), req.GetReadonly(), noExec); err != nil {
		mounted, inspectErr := s.mounter.mountedAt(req.GetTargetPath())
		if inspectErr != nil {
			return status.Errorf(codes.Internal, "mount fallback cache: %v; inspect target mount before lease rollback: %v", err, inspectErr)
		}
		if mounted {
			return status.Errorf(codes.Internal, "mount fallback cache: %v; fallback mount remains and its lease was retained", err)
		}
		rpcErr := s.rollbackFallbackPublish(store, req, status.Errorf(codes.Internal, "mount fallback cache: %v", err))
		if status.Code(rpcErr) == codes.Internal {
			return &fallbackTierError{rpcErr: rpcErr, cause: err}
		}
		return rpcErr
	}
	if err := ctx.Err(); err != nil {
		if unmountErr := s.mounter.unmount(req.GetTargetPath()); unmountErr != nil && !errors.Is(unmountErr, errNotMounted) && !errors.Is(unmountErr, os.ErrNotExist) {
			return status.Errorf(codes.Internal, "cancel fallback publish: %v; unmount fallback cache: %v", err, unmountErr)
		}
		if removeErr := removeTargetDirectory(req.GetTargetPath()); removeErr != nil {
			return status.Errorf(codes.Internal, "cancel fallback publish: %v; remove fallback target: %v", err, removeErr)
		}
		return s.rollbackFallbackPublish(store, req, status.FromContextError(err).Err())
	}
	if err := store.CommitPublish(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		s.recordMountedLeaseCommitFailure(ctx, req.GetVolumeId(), err)
	}
	return nil
}

func (s *Server) recordMountedLeaseCommitFailure(ctx context.Context, volumeID string, err error) {
	s.backendDegraded.Store(true)
	if s.metrics != nil {
		s.metrics.RecordBackendFailure("lease_commit_failed")
	}
	s.logger.WarnContext(ctx, "cache mount is active but its lease commit failed", "volumeID", volumeID, "error", err)
}

func (s *Server) rollbackFallbackPublish(store *cache.Store, req *csi.NodePublishVolumeRequest, publishErr error) error {
	if err := store.Release(req.GetVolumeId(), req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "fallback publish failed: %v; release fallback lease failed: %v", publishErr, err)
	}
	return publishErr
}

func (s *Server) rollbackFallbackTierPublish(store *cache.Store, req *csi.NodePublishVolumeRequest, cause error, operation string) error {
	rpcErr := status.Errorf(codes.FailedPrecondition, "%s: %v", operation, cause)
	if releaseErr := store.Release(req.GetVolumeId(), req.GetTargetPath()); releaseErr != nil {
		rpcErr = status.Errorf(codes.Internal, "%s failed: %v; release fallback lease failed: %v", operation, cause, releaseErr)
		cause = errors.Join(cause, releaseErr)
	}
	return &fallbackTierError{rpcErr: rpcErr, cause: cause}
}
