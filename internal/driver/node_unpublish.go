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
	mounted, err := s.mounter.mountedAt(req.GetTargetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect target mount: %v", err)
	}
	if err := s.unpublishTarget(ctx, req, mounted); err != nil {
		return nil, err
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

type unpublishLease struct {
	store    *cache.Store
	identity string
	lease    cache.Lease
	source   string
}

func (s *Server) unpublishTarget(ctx context.Context, req *csi.NodeUnpublishVolumeRequest, mounted bool) error {
	stores := append([]*cache.Store{s.store}, s.fallbackStores()...)
	terminalSource := fallbackPath(s.options.FallbackTerminalRoot, req.GetVolumeId())
	if mounted {
		terminalMount, err := s.mounter.sameCacheSource(terminalSource, req.GetTargetPath())
		if err != nil {
			return status.Errorf(codes.Internal, "verify terminal fallback source: %v", err)
		}
		if terminalMount {
			return s.unpublishTerminalFallback(ctx, req, terminalSource, stores)
		}
	}
	active, degraded, err := s.inspectUnpublishLeases(ctx, req.GetTargetPath(), req.GetVolumeId(), mounted, stores)
	if err != nil {
		return err
	}
	if mounted {
		owned, err := s.ownsUnpublishMount(active, stores, req.GetTargetPath())
		if err != nil {
			return err
		}
		if !owned {
			return status.Error(codes.FailedPrecondition, "target mount is outside the cache roots")
		}
		if err := s.mounter.unmount(req.GetTargetPath()); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
			return status.Errorf(codes.Internal, "unmount cache: %v", err)
		}
	}
	if err := removeTargetDirectory(req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "remove cache mount target: %v", err)
	}
	s.removeTerminalFallbackSource(ctx, terminalSource)
	s.finishUnpublishCleanup(ctx, req.GetVolumeId(), active, degraded)
	return nil
}

func (s *Server) unpublishTerminalFallback(ctx context.Context, req *csi.NodeUnpublishVolumeRequest, source string, stores []*cache.Store) error {
	if err := s.mounter.unmount(req.GetTargetPath()); err != nil && !errors.Is(err, errNotMounted) && !errors.Is(err, os.ErrNotExist) {
		return status.Errorf(codes.Internal, "unmount terminal cache: %v", err)
	}
	if err := removeTargetDirectory(req.GetTargetPath()); err != nil {
		return status.Errorf(codes.Internal, "remove terminal cache mount target: %v", err)
	}
	s.removeTerminalFallbackSource(ctx, source)
	s.cleanupTerminalFallbackLeases(ctx, req.GetVolumeId(), req.GetTargetPath(), stores)
	return nil
}

func (s *Server) removeTerminalFallbackSource(ctx context.Context, source string) {
	mounted, err := s.mounter.sourceMounted(source)
	if err != nil {
		s.logger.WarnContext(ctx, "failed to inspect terminal fallback source after unpublish", "source", source, "error", err)
		return
	}
	if mounted {
		return
	}
	if err := os.Remove(source); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.logger.WarnContext(ctx, "failed to remove terminal fallback source after unpublish", "source", source, "error", err)
	}
}

func (s *Server) cleanupTerminalFallbackLeases(ctx context.Context, volumeID, target string, stores []*cache.Store) {
	for _, store := range stores {
		identity, lease, source, _, found, err := store.LeaseDetails(volumeID)
		if err != nil {
			s.logger.WarnContext(ctx, "cache lease metadata is unavailable after terminal fallback unpublish", "root", store.Root(), "error", err)
			if errors.Is(err, cache.ErrDegradedMetadata) && s.isFallbackStore(store) {
				fallbackIdentity, identityErr := cache.FallbackIdentity(volumeID)
				if identityErr == nil {
					if quarantineErr := store.ScheduleQuarantine(fallbackIdentity, s.mounter.sourceMounted); quarantineErr != nil {
						s.logger.WarnContext(ctx, "failed to schedule fallback quarantine after terminal unpublish", "root", store.Root(), "error", quarantineErr)
					}
				}
			}
			continue
		}
		if !found || lease.Target != target {
			continue
		}
		if s.isFallbackStore(store) {
			if err := s.mounter.unmountGeneration(source); err != nil {
				s.logger.WarnContext(ctx, "failed to unmount fallback generation after terminal teardown", "source", source, "error", err)
			}
		}
		if err := store.Release(volumeID, target); err != nil {
			s.logger.WarnContext(ctx, "failed to release cache lease after terminal teardown", "root", store.Root(), "error", err)
			if quarantineErr := store.ScheduleQuarantine(identity, s.mounter.sourceMounted); quarantineErr != nil {
				s.logger.WarnContext(ctx, "failed to schedule cache quarantine after terminal teardown", "root", store.Root(), "error", quarantineErr)
			}
		}
	}
}

func (s *Server) inspectUnpublishLeases(ctx context.Context, target, volumeID string, mounted bool, stores []*cache.Store) (*unpublishLease, map[*cache.Store]string, error) {
	var active *unpublishLease
	degraded := make(map[*cache.Store]string)
	for _, store := range stores {
		identity, lease, source, _, found, err := store.LeaseDetails(volumeID)
		if err != nil {
			s.logger.WarnContext(ctx, "cache lease metadata is unavailable during unpublish", "root", store.Root(), "error", err)
			if mounted {
				identity, _, found, findErr := store.FindDegradedGenerationForTarget(target, s.mounter.sameCacheSource)
				if findErr != nil {
					s.logger.WarnContext(ctx, "failed to identify degraded cache during unpublish", "root", store.Root(), "error", findErr)
				} else if found {
					degraded[store] = identity
				}
			}
			continue
		}
		if found {
			if lease.Target != target {
				return nil, nil, status.Error(codes.FailedPrecondition, "volume lease target does not match")
			}
			if active != nil {
				return nil, nil, status.Error(codes.FailedPrecondition, "volume lease exists in multiple cache stores")
			}
			active = &unpublishLease{store: store, identity: identity, lease: lease, source: source}
			continue
		}
		if mounted {
			identity, _, found, err := store.FindDegradedGenerationForTarget(target, s.mounter.sameCacheSource)
			if err != nil {
				s.logger.WarnContext(ctx, "failed to identify degraded cache during unpublish", "root", store.Root(), "error", err)
			} else if found {
				degraded[store] = identity
			}
		}
	}
	return active, degraded, nil
}

func (s *Server) ownsUnpublishMount(active *unpublishLease, stores []*cache.Store, target string) (bool, error) {
	if active != nil {
		matches, err := s.mounter.sameCacheSource(active.source, target)
		if err != nil {
			return false, status.Errorf(codes.Internal, "verify cache mount source: %v", err)
		}
		return matches, nil
	}
	var inspectErr error
	for _, store := range stores {
		matches, err := s.mounter.sourceWithinRoot(target, store.Root())
		if err != nil {
			inspectErr = errors.Join(inspectErr, err)
			continue
		}
		if matches {
			return true, nil
		}
	}
	terminalMatches, err := s.mounter.sourceWithinRoot(target, s.options.FallbackTerminalRoot)
	if err == nil && terminalMatches {
		return true, nil
	}
	if err != nil {
		inspectErr = errors.Join(inspectErr, err)
	}
	if inspectErr != nil {
		return false, status.Errorf(codes.Internal, "verify cache mount source: %v", inspectErr)
	}
	return false, nil
}

func (s *Server) finishUnpublishCleanup(ctx context.Context, volumeID string, active *unpublishLease, degraded map[*cache.Store]string) {
	if active != nil {
		if s.isFallbackStore(active.store) {
			if err := s.mounter.unmountGeneration(active.source); err != nil {
				s.logger.WarnContext(ctx, "failed to unmount fallback generation after target teardown", "source", active.source, "error", err)
			}
		}
		if err := active.store.Release(volumeID, active.lease.Target); err != nil {
			s.logger.WarnContext(ctx, "failed to release cache lease after target teardown", "root", active.store.Root(), "error", err)
			if quarantineErr := active.store.ScheduleQuarantine(active.identity, s.mounter.sourceMounted); quarantineErr != nil {
				s.logger.WarnContext(ctx, "failed to schedule cache quarantine after lease release", "root", active.store.Root(), "error", quarantineErr)
			}
		}
	} else {
		identity, err := cache.FallbackIdentity(volumeID)
		if err == nil {
			for _, store := range s.fallbackStores() {
				degraded[store] = identity
			}
		}
	}
	for store, identity := range degraded {
		if err := store.ScheduleQuarantine(identity, s.mounter.sourceMounted); err != nil {
			s.logger.WarnContext(ctx, "failed to schedule degraded cache quarantine", "root", store.Root(), "error", err)
		}
	}
}
