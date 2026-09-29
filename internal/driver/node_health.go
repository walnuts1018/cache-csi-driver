package driver

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	if !found && s.fallbackStore != nil {
		_, fallbackLease, fallbackSource, _, fallbackFound, fallbackErr := s.fallbackStore.LeaseDetails(req.GetVolumeId())
		if errors.Is(fallbackErr, cache.ErrDegradedMetadata) {
			return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeMetadataUnreadable", "cache volume metadata is unreadable")}, nil
		}
		if fallbackErr != nil {
			return nil, status.Errorf(codes.Internal, "read fallback cache lease: %v", fallbackErr)
		}
		if fallbackFound {
			lease = fallbackLease
			source = fallbackSource
			found = true
		}
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
		if matchesInlineVolumeIDTarget(req.GetVolumeId(), s.options.KubeletRoot, lease.Target) {
			for _, store := range []*cache.Store{s.store, s.fallbackStore} {
				if store == nil {
					continue
				}
				_, _, degraded, err := store.FindDegradedGenerationForTarget(lease.Target, s.mounter.sameCacheSource)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "inspect degraded cache mount: %v", err)
				}
				if degraded {
					return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeMetadataUnreadable", "cache volume metadata is unreadable")}, nil
				}
			}
		}
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
	if s.fallbackStore != nil {
		if err := s.fallbackStore.MetadataError(); err != nil {
			return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, "FallbackMetadataUnreadable", "fallback cache metadata cannot be read")
		}
	}
	readOnly, err := s.mounter.filesystemReadOnly(s.store.Root())
	if err != nil {
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_UNREACHABLE, "CacheRootUnavailable", "cache root filesystem cannot be inspected")
	}
	if readOnly {
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, "CacheRootReadOnly", "cache root filesystem is read-only")
	}
	if s.fallbackStore != nil {
		readOnly, err := s.mounter.filesystemReadOnly(s.fallbackStore.Root())
		if err != nil {
			return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_UNREACHABLE, "FallbackRootUnavailable", "fallback cache filesystem cannot be inspected")
		}
		if readOnly {
			return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, "FallbackRootReadOnly", "fallback cache filesystem is read-only")
		}
	}
	return &csi.NodeGetStorageHealthResponse{}, nil
}

func storageHealthResponse(status csi.StorageHealthErrorType, reason, message string) (*csi.NodeGetStorageHealthResponse, error) {
	return &csi.NodeGetStorageHealthResponse{BackendHealth: []*csi.NodeGetStorageHealthResponse_StorageBackendHealth{{Status: status, Reason: reason, Message: message}}}, nil
}
