package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kubeletcompat"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || !filepath.IsAbs(req.GetTargetPath()) {
		return nil, status.Error(codes.InvalidArgument, "volume ID and absolute target path are required")
	}
	if _, ok := kubeletcompat.ParsePodTarget(s.options.KubeletRoot, req.GetTargetPath()); !ok {
		return nil, status.Error(codes.InvalidArgument, "target path must be inside a kubelet Pod directory")
	}
	if err := ensureNoSymlinkTraversal(req.GetTargetPath()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "validate target path: %v", err)
	}
	unlock := s.locks.Lock(req.GetTargetPath())
	defer unlock()
	unlockVolume := s.identityLocks.Lock("\x00volume\x00" + req.GetVolumeId())
	defer unlockVolume()

	if err := s.unpublishTarget(ctx, req); err != nil {
		return nil, err
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *Server) unpublishTarget(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) error {
	target := req.GetTargetPath()
	identity, lease, _, _, found, leaseErr := s.store.LeaseDetails(req.GetVolumeId())
	if leaseErr != nil {
		s.logger.WarnContext(ctx, "cache lease metadata is unavailable before target teardown", "volumeID", req.GetVolumeId(), "error", leaseErr)
		if !errors.Is(leaseErr, cache.ErrDegradedMetadata) {
			_ = s.markNodeUnavailable(ctx, "CacheLeaseReadFailed", leaseErr, true)
		}
	}
	if leaseErr == nil && found && lease.Target != target {
		return status.Error(codes.FailedPrecondition, "volume lease target does not match")
	}
	mounted, err := s.mounter.mountedAt(target)
	if err != nil {
		return s.markNodeUnavailable(ctx, "MountInspectionFailed", err, true)
	}

	var degradedIdentity string
	if mounted {
		withinCacheRoot, err := s.mounter.sourceWithinRoot(target, s.store.Root())
		if err != nil {
			return s.markNodeUnavailable(ctx, "CacheMountOwnershipUnknown", err, true)
		}
		if !withinCacheRoot {
			return status.Error(codes.FailedPrecondition, "target mount is outside the cache root")
		}
		candidateIdentity, _, degradedFound, identifyErr := s.store.FindDegradedGenerationForTarget(target, s.mounter.sameCacheSource)
		if identifyErr != nil {
			s.logger.WarnContext(ctx, "failed to identify degraded cache before unpublish", "error", identifyErr)
			_ = s.markNodeUnavailable(ctx, "DegradedCacheInspectionFailed", identifyErr, true)
		} else if degradedFound {
			degradedIdentity = candidateIdentity
		}
		if err := s.mounter.unmount(target); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
			return s.markNodeUnavailable(ctx, "CacheUnmountFailed", err, true)
		}
	}
	if err := removeTargetDirectory(target); err != nil {
		return status.Errorf(codes.Internal, "remove cache mount target: %v", err)
	}

	if errors.Is(leaseErr, cache.ErrDegradedMetadata) {
		if degradedIdentity != "" {
			identity = degradedIdentity
		}
	} else if leaseErr == nil && found {
		if err := s.store.Release(req.GetVolumeId(), target); err != nil {
			s.logger.WarnContext(ctx, "failed to release cache lease after target teardown", "volumeID", req.GetVolumeId(), "root", s.store.Root(), "error", err)
			_ = s.markNodeUnavailable(ctx, "CacheLeaseReleaseFailed", err, true)
			if quarantineErr := s.store.QuarantineDegradedObject(identity, s.mounter.sourceMounted); quarantineErr != nil {
				s.logger.WarnContext(ctx, "failed to quarantine cache after lease release", "root", s.store.Root(), "error", quarantineErr)
				_ = s.markNodeUnavailable(ctx, "CacheQuarantineFailed", quarantineErr, true)
			}
		}
	}
	if degradedIdentity != "" {
		if err := s.store.QuarantineDegradedObject(degradedIdentity, s.mounter.sourceMounted); err != nil {
			s.logger.WarnContext(ctx, "failed to quarantine degraded cache after target teardown", "identity", degradedIdentity, "error", err)
			_ = s.markNodeUnavailable(ctx, "CacheQuarantineFailed", err, true)
		}
	}
	return nil
}
