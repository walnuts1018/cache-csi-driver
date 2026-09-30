package driver

import (
	"context"
	"log/slog"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
	"github.com/walnuts1018/cache-csi-driver/internal/metrics"
	"github.com/walnuts1018/cache-csi-driver/internal/nodehealth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const DriverName = "cache.csi.walnuts.dev"

func FilesystemReadOnly(path string) (bool, error) { return filesystemReadOnly(path) }

type ClassResolver interface {
	Resolve(context.Context, string, string, string) (string, string, kube.ResolvedClass, error)
}

type ProjectQuota interface {
	Configure(context.Context, string, string, uint32, int64) error
}

type mounter interface {
	mount(string, string, bool, bool) error
	unmount(string) error
	mountedAt(string) (bool, error)
	sameCacheMount(string, string, bool, bool) (bool, error)
	sameCacheSource(string, string) (bool, error)
	sourceWithinRoot(string, string) (bool, error)
	sourceMounted(string) (bool, error)
	filesystemReadOnly(string) (bool, error)
}

type Options struct {
	NodeID              string
	KubeletRoot         string
	VendorVersion       string
	ProjectQuotaEnabled bool
	Metrics             *metrics.Metrics
	Logger              *slog.Logger
	Health              *nodehealth.Tracker
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
	metrics       *metrics.Metrics
	logger        *slog.Logger
}

type podVolumeContext struct {
	namespace          string
	name               string
	uid                string
	serviceAccountName string
	cacheClass         string
	cacheKey           string
	maxBytes           string
}

func New(store *cache.Store, resolver ClassResolver, quotaManager ProjectQuota, options Options) *Server {
	if options.VendorVersion == "" {
		options.VendorVersion = "dev"
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Health == nil {
		options.Health = nodehealth.NewTracker()
	}
	return &Server{
		store:    store,
		resolver: resolver,
		quota:    quotaManager,
		mounter:  newMounter(),
		options:  options,
		metrics:  options.Metrics,
		logger:   options.Logger,
	}
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
		csi.NodeServiceCapability_RPC_GET_VOLUME_HEALTH,
		csi.NodeServiceCapability_RPC_GET_STORAGE_HEALTH,
	}
	capabilities := make([]*csi.NodeServiceCapability, 0, len(types))
	for _, typ := range types {
		capabilities = append(capabilities, &csi.NodeServiceCapability{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: typ}}})
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: capabilities}, nil
}
