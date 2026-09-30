package driver

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kubeletcompat"
	"github.com/walnuts1018/cache-csi-driver/internal/nodehealth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) NodeGetVolumeHealth(_ context.Context, req *csi.NodeGetVolumeHealthRequest) (*csi.NodeGetVolumeHealthResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	_, lease, source, _, found, err := s.store.LeaseDetails(req.GetVolumeId())
	if errors.Is(err, cache.ErrDegradedMetadata) {
		if filepath.IsAbs(req.GetVolumePublishPath()) {
			_, _, degraded, findErr := s.store.FindDegradedGenerationForTarget(req.GetVolumePublishPath(), s.mounter.sameCacheSource)
			if findErr != nil {
				return nil, status.Errorf(codes.Internal, "inspect degraded cache mount: %v", findErr)
			}
			if degraded {
				return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeMetadataUnreadable", "cache volume metadata is unreadable")}, nil
			}
		}
		return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeMetadataUnreadable", "cache volume metadata is unreadable")}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read cache lease: %v", err)
	}
	if !found {
		if !filepath.IsAbs(req.GetVolumePublishPath()) {
			return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeLeaseMissing", "cache volume lease is missing")}, nil
		}
		if _, ok := kubeletcompat.ParsePodTarget(s.options.KubeletRoot, req.GetVolumePublishPath()); !ok {
			return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeLeaseMissing", "cache volume publish path is invalid")}, nil
		}
		if _, _, degraded, findErr := s.store.FindDegradedGenerationForTarget(req.GetVolumePublishPath(), s.mounter.sameCacheSource); findErr != nil {
			return nil, status.Errorf(codes.Internal, "inspect degraded cache mount: %v", findErr)
		} else if degraded {
			return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeMetadataUnreadable", "cache volume metadata is unreadable")}, nil
		}
		return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeLeaseMissing", "cache volume lease is missing")}, nil
	}
	if filepath.IsAbs(req.GetVolumePublishPath()) && req.GetVolumePublishPath() != lease.Target {
		return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeTargetMismatch", "cache volume publish path does not match its lease")}, nil
	}
	mounted, err := s.mounter.mountedAt(lease.Target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "inspect cache mount: %v", err)
	}
	if !mounted {
		return &csi.NodeGetVolumeHealthResponse{VolumeHealth: unhealthyVolume(req.GetVolumeId(), "VolumeMountMissing", "cache volume mount is missing")}, nil
	}
	same, err := s.mounter.sameCacheMount(source, lease.Target, lease.ReadOnly, lease.NoExec)
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
	snapshot := s.options.Health.Current()
	switch snapshot.Phase {
	case nodehealth.PhaseStarting, nodehealth.PhaseRecovering:
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, string(snapshot.Phase), snapshot.Reason)
	case nodehealth.PhaseUnavailable:
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_UNREACHABLE, snapshot.Reason, "cache node is unavailable")
	case nodehealth.PhaseDegraded:
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, snapshot.Reason, "cache node is degraded")
	}
	if !s.store.Ready() {
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, "CacheRecoveryInProgress", "cache store recovery is still in progress")
	}
	readOnly, err := s.mounter.filesystemReadOnly(s.store.Root())
	if err != nil {
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_UNREACHABLE, "CacheRootUnavailable", "cache filesystem cannot be inspected")
	}
	if readOnly {
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_UNREACHABLE, "CacheRootReadOnly", "cache filesystem is read-only")
	}
	if err := s.store.ProjectRegistryError(); err != nil {
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, "CacheProjectIDRegistryUnavailable", "cache project ID registry is unavailable")
	}
	if err := s.store.MetadataError(); err != nil {
		if errors.Is(err, cache.ErrDegradedMetadata) {
			return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, "CacheObjectDegraded", "one or more cache objects are being recovered")
		}
		return storageHealthResponse(csi.StorageHealthErrorType_STORAGE_DEGRADED, "CacheMetadataUnavailable", "cache metadata is unavailable")
	}
	return &csi.NodeGetStorageHealthResponse{}, nil
}

func storageHealthResponse(statusCode csi.StorageHealthErrorType, reason, message string) (*csi.NodeGetStorageHealthResponse, error) {
	return &csi.NodeGetStorageHealthResponse{BackendHealth: []*csi.NodeGetStorageHealthResponse_StorageBackendHealth{{Status: statusCode, Reason: reason, Message: message}}}, nil
}
