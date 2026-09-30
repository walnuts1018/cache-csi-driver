package driver

import (
	"context"
	"errors"
	"fmt"
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
	if _, _, ok := kubeletcompat.ParseInlineCSITarget(s.options.KubeletRoot, req.GetTargetPath()); !ok {
		return nil, status.Error(codes.InvalidArgument, "target path must identify an inline CSI volume mount")
	}
	unlock := s.locks.Lock(req.GetTargetPath())
	defer unlock()
	unlockVolume := s.identityLocks.Lock("\x00volume\x00" + req.GetVolumeId())
	defer unlockVolume()
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
		handled, err := s.unpublishDegradedMount(ctx, req.GetTargetPath())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "unpublish degraded cache: %v", err)
		}
		if handled {
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
	}
	if err := s.unpublishFallback(ctx, req, mounted); err != nil {
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

func (s *Server) unpublishDegradedMount(ctx context.Context, target string) (bool, error) {
	if _, _, ok := kubeletcompat.ParseInlineCSITarget(s.options.KubeletRoot, target); !ok {
		return false, nil
	}
	for _, store := range []*cache.Store{s.store, s.fallbackStore} {
		if store == nil {
			continue
		}
		identity, _, found, err := store.FindDegradedGenerationForTarget(target, s.mounter.sameCacheSource)
		if err != nil {
			return false, fmt.Errorf("inspect degraded cache mount: %w", err)
		}
		if !found {
			continue
		}
		if err := s.mounter.unmount(target); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("unmount degraded cache: %w", err)
		}
		if err := removeTargetDirectory(target); err != nil {
			return false, fmt.Errorf("remove degraded cache mount target: %w", err)
		}
		if err := store.ScheduleQuarantine(identity, s.mounter.sourceMounted); err != nil {
			s.logger.WarnContext(ctx, "failed to schedule degraded cache quarantine", "error", err)
		}
		return true, nil
	}
	return false, nil
}

func (s *Server) unpublishFallback(ctx context.Context, req *csi.NodeUnpublishVolumeRequest, mounted bool) error {
	if s.fallbackStore == nil {
		return status.Error(codes.FailedPrecondition, "fallback cache root is not configured")
	}
	_, lease, source, _, found, err := s.fallbackStore.LeaseDetails(req.GetVolumeId())
	if err != nil {
		if !errors.Is(err, cache.ErrDegradedMetadata) {
			return status.Errorf(codes.Internal, "read fallback cache lease: %v", err)
		}
		identity, identityErr := cache.FallbackIdentity(req.GetVolumeId())
		if identityErr == nil {
			if err := s.fallbackStore.ScheduleQuarantine(identity, s.mounter.sourceMounted); err != nil {
				s.logger.WarnContext(ctx, "failed to schedule degraded fallback quarantine", "error", err)
			}
		}
		if mounted {
			return status.Error(codes.FailedPrecondition, "target mount has unreadable fallback metadata")
		}
		return removeTargetDirectory(req.GetTargetPath())
	}
	if found {
		if lease.Target != req.GetTargetPath() {
			return status.Error(codes.FailedPrecondition, "fallback lease target does not match")
		}
		if mounted {
			same, err := s.mounter.sameCacheMount(source, req.GetTargetPath(), lease.ReadOnly, lease.NoExec)
			if err != nil {
				return status.Errorf(codes.Internal, "verify fallback mount: %v", err)
			}
			if !same {
				return status.Error(codes.FailedPrecondition, "target mount does not belong to this fallback volume")
			}
			if err := s.mounter.unmount(req.GetTargetPath()); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
				return status.Errorf(codes.Internal, "unmount fallback cache: %v", err)
			}
		}
		if err := removeTargetDirectory(req.GetTargetPath()); err != nil {
			return status.Errorf(codes.Internal, "remove fallback target: %v", err)
		}
		if err := s.mounter.unmountGeneration(source); err != nil {
			return status.Errorf(codes.Internal, "unmount bounded fallback generation: %v", err)
		}
		if err := s.fallbackStore.Release(req.GetVolumeId(), lease.Target); err != nil {
			return status.Errorf(codes.Internal, "release fallback cache lease: %v", err)
		}
		return nil
	}
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
	legacyIdentity := filepath.Base(fallback)
	if err := s.fallbackStore.CleanupDegradedObject(legacyIdentity, s.mounter.sourceMounted); err != nil {
		return status.Errorf(codes.Internal, "quarantine legacy fallback cache: %v", err)
	}
	return nil
}
