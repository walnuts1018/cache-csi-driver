package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/driver"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
	"github.com/walnuts1018/cache-csi-driver/internal/manager"
	"github.com/walnuts1018/cache-csi-driver/internal/metrics"
	"github.com/walnuts1018/cache-csi-driver/internal/nodehealth"
	"github.com/walnuts1018/cache-csi-driver/internal/quota"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var (
	version  = "dev"
	revision = "unknown"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var err error
	if len(os.Args) > 1 && os.Args[1] == "health-controller" {
		err = runHealthController(logger, os.Args[2:])
	} else {
		err = run(logger)
	}
	if err != nil {
		logger.Error("cache CSI node driver stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	endpoint := flag.String("endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
	metricsAddress := flag.String("metrics-address", ":9807", "Prometheus metrics HTTP listen address")
	cacheRoot := flag.String("cache-root", "/var/lib/cache-csi", "cache storage directory")
	storageBackend := flag.String("storage-backend", "directory", "node-wide cache storage backend: directory or xfs-project")
	requireCacheRootMountpoint := flag.Bool("require-cache-root-mountpoint", true, "require cache-root to be a filesystem mountpoint")
	nodeID := flag.String("node-id", os.Getenv("NODE_NAME"), "Kubernetes node name")
	kubeletRoot := flag.String("kubelet-root", "/var/lib/kubelet", "kubelet root directory")
	gcInterval := flag.Duration("gc-interval", 30*time.Second, "cache garbage collection interval")
	highFreePercent := flag.Int("pressure-high-free-percent", 25, "free-byte percentage at which cache pressure collection stops")
	lowFreePercent := flag.Int("pressure-low-free-percent", 20, "free-byte percentage at which cache pressure collection starts")
	highInodeFreePercent := flag.Int("pressure-high-inode-free-percent", 15, "free-inode percentage at which cache pressure collection stops")
	lowInodeFreePercent := flag.Int("pressure-low-inode-free-percent", 10, "free-inode percentage at which cache pressure collection starts")
	projectIDStart := flag.Uint("project-id-start", 2_000_000_000, "first project ID reserved for cache identities")
	projectIDCount := flag.Uint("project-id-count", 1_000_000, "number of project IDs reserved for cache identities")
	flag.Parse()

	if err := validateRuntimeConfig(*gcInterval, *projectIDStart, *projectIDCount, *nodeID); err != nil {
		return err
	}
	projectQuotaEnabled, err := validateStorageBackend(*storageBackend, *requireCacheRootMountpoint)
	if err != nil {
		return err
	}
	metricSet := metrics.New()
	health := nodehealth.NewTracker()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	resolver := kubernetesResolver(logger)
	quotaBackend := quota.XFS{Binary: "xfs_quota"}
	if resolver != nil {
		resolver.Start(ctx)
	}

	pressure := cache.PressureConfig{
		HighFreePercent:      *highFreePercent,
		LowFreePercent:       *lowFreePercent,
		HighInodeFreePercent: *highInodeFreePercent,
		LowInodeFreePercent:  *lowInodeFreePercent,
	}
	var listenConfig net.ListenConfig
	metricsListener, err := listenConfig.Listen(ctx, "tcp", *metricsAddress)
	if err != nil {
		return fmt.Errorf("listen on metrics endpoint: %w", err)
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metricSet.Handler())
	metricsMux.HandleFunc("GET /readyz", health.ReadinessHandler)
	metricsMux.HandleFunc("GET /health", health.HealthHandler)
	metricsServer := &http.Server{Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}
	metricsErr := make(chan error, 1)
	go func() { metricsErr <- serveMetrics(ctx, metricsServer, metricsListener) }()

	storeOptions := cache.StoreOptions{
		Pressure:              pressure,
		ProjectIDStart:        uint32(*projectIDStart),
		ProjectIDCount:        uint32(*projectIDCount),
		ProjectQuotaEnabled:   projectQuotaEnabled,
		RequireRootMountpoint: *requireCacheRootMountpoint,
		IsGenerationMounted: func(generation string) (bool, error) {
			return driver.SourceMounted(*cacheRoot, generation)
		},
	}
	var store *cache.Store
	for {
		store, err = initializeNodeStore(*cacheRoot, storeOptions)
		if err == nil {
			break
		}
		health.RecordFailure(storeInitializationFailure(*cacheRoot, err))
		logger.ErrorContext(ctx, "initialize cache store failed; retrying while reporting node health", "root", *cacheRoot, "error", err)
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case metricsServeErr := <-metricsErr:
			timer.Stop()
			return fmt.Errorf("serve metrics endpoint while waiting for cache store: %w", metricsServeErr)
		case <-timer.C:
		}
	}
	defer func() { _ = store.Close() }()
	paths, err := resolveRuntimePaths(*cacheRoot, *kubeletRoot, *endpoint)
	if err != nil {
		return err
	}

	cacheManager := manager.New(store, manager.Options{
		Interval:           *gcInterval,
		InspectMount:       driver.VerifyCacheMount,
		ResolverReady:      resolverSynced(resolver),
		MountProbe:         func(context.Context) error { return driver.PreflightMountAPI() },
		FilesystemReadOnly: driver.FilesystemReadOnly,
		QuotaRequired:      projectQuotaEnabled,
		QuotaProbe: func(probeContext context.Context, root string) error {
			return quotaBackend.Check(probeContext, root)
		},
		Health:  health,
		Logger:  logger,
		Metrics: metricSet,
	})
	service := driver.New(store, resolver, quotaBackend, driver.Options{
		NodeID:              *nodeID,
		KubeletRoot:         *kubeletRoot,
		VendorVersion:       version,
		ProjectQuotaEnabled: projectQuotaEnabled,
		MountProbe:          func(context.Context) error { return driver.PreflightMountAPI() },
		Metrics:             metricSet,
		Logger:              logger,
		Health:              health,
	})
	grpcServer := grpc.NewServer()
	csi.RegisterIdentityServer(grpcServer, service)
	csi.RegisterNodeServer(grpcServer, service)

	listener, cleanupSocket, err := listenUnixSocket(paths.socketPath)
	if err != nil {
		return fmt.Errorf("listen on CSI endpoint: %w", err)
	}
	defer cleanupSocket()
	logger.Info("starting cache CSI node driver", "version", version, "revision", revision, "nodeID", *nodeID, "endpoint", *endpoint, "metricsAddress", *metricsAddress)
	managerContext, cancelManager := context.WithCancel(ctx)
	managerDone := make(chan struct{})
	go func() {
		defer close(managerDone)
		for {
			if err := cacheManager.Recover(managerContext); err == nil {
				cacheManager.Run(managerContext)
				return
			} else if managerContext.Err() != nil {
				return
			} else {
				logger.WarnContext(managerContext, "cache store recovery is incomplete", "error", err)
			}
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-managerContext.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()

	select {
	case err := <-serveErr:
		cancelManager()
		<-managerDone
		if metricsServeErr := <-metricsErr; metricsServeErr != nil {
			return errors.Join(fmt.Errorf("serve CSI gRPC endpoint: %w", err), fmt.Errorf("serve metrics endpoint: %w", metricsServeErr))
		}
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve CSI gRPC endpoint: %w", err)
		}
		return nil
	case err := <-metricsErr:
		cancelManager()
		grpcServer.Stop()
		<-managerDone
		if err != nil {
			return fmt.Errorf("serve metrics endpoint: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	cancelManager()
	logger.Info("shutting down cache CSI node driver")
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(20 * time.Second):
		logger.Warn("graceful CSI shutdown timed out; stopping active gRPC calls")
		grpcServer.Stop()
		<-stopped
	}
	<-managerDone
	if err := <-metricsErr; err != nil {
		return fmt.Errorf("shut down metrics endpoint: %w", err)
	}
	return nil
}

func validateStorageBackend(backend string, requireRootMountpoint bool) (bool, error) {
	switch backend {
	case "directory":
		if !requireRootMountpoint {
			return false, errors.New("directory storage backend requires cache-root to be a filesystem mountpoint")
		}
		return false, nil
	case "xfs-project":
		return true, nil
	default:
		return false, fmt.Errorf("unsupported storage backend %q", backend)
	}
}

func initializeNodeStore(root string, options cache.StoreOptions) (*cache.Store, error) {
	store, err := cache.NewStoreAsync(root, options)
	if err != nil {
		return nil, fmt.Errorf("initialize cache store: %w", err)
	}
	return store, nil
}

func storeInitializationFailure(root string, err error) nodehealth.Failure {
	readOnly, readOnlyErr := driver.FilesystemReadOnly(root)
	if readOnly || errors.Is(err, unix.EIO) || errors.Is(err, unix.EROFS) || errors.Is(readOnlyErr, unix.EIO) || errors.Is(readOnlyErr, unix.EROFS) {
		return nodehealth.Failure{
			Scope: nodehealth.FailureScopeNode, Subsystem: nodehealth.SubsystemFilesystem,
			Reason: nodehealth.ReasonFilesystemUnavailable, Impact: nodehealth.NodeImpactEvict,
			Retryability: nodehealth.Retryable,
		}
	}
	return nodehealth.Failure{
		Scope: nodehealth.FailureScopeNode, Subsystem: nodehealth.SubsystemStore,
		Reason: nodehealth.ReasonStoreInitialization, Impact: nodehealth.NodeImpactNoSchedule,
		Retryability: nodehealth.Retryable,
	}
}

func resolverSynced(resolver *kube.Resolver) func() bool {
	if resolver == nil {
		return func() bool { return false }
	}
	return resolver.HasSynced
}

func runHealthController(logger *slog.Logger, args []string) error {
	flags := flag.NewFlagSet("health-controller", flag.ContinueOnError)
	namespace := flags.String("namespace", os.Getenv("POD_NAMESPACE"), "namespace containing cache CSI node plugin Pods")
	pluginPodSelector := flags.String("plugin-pod-label-selector", "", "label selector for cache CSI node plugin Pods")
	pluginDaemonSet := flags.String("plugin-daemonset-name", "", "name of the cache CSI node plugin DaemonSet")
	pluginHealthPort := flags.Int("plugin-health-port", 9807, "node-plugin health HTTP port")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *namespace == "" || *pluginPodSelector == "" || *pluginDaemonSet == "" {
		return errors.New("health controller namespace, node-plugin DaemonSet, and Pod selector must be configured")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load in-cluster Kubernetes configuration: %w", err)
	}
	config = rest.CopyConfig(config)
	config.Timeout = 10 * time.Second
	config.QPS = 30
	config.Burst = 60
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	identity := os.Getenv("POD_NAME")
	if identity == "" {
		identity, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("resolve health controller identity: %w", err)
		}
	}
	apiCache, err := nodehealth.NewAPICache(client, *namespace, *pluginDaemonSet, *pluginPodSelector)
	if err != nil {
		return err
	}
	controller, err := nodehealth.NewController(client, apiCache, *namespace, *pluginHealthPort, logger)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := apiCache.Start(ctx); err != nil {
		return fmt.Errorf("synchronize health controller Kubernetes informer caches: %w", err)
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{Name: "cache-csi-health-controller-leader", Namespace: *namespace},
			Client:    client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: identity,
			},
		},
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.InfoContext(ctx, "health controller acquired leadership", "identity", identity)
				controller.Run(ctx)
			},
			OnStoppedLeading: func() {
				logger.Warn("health controller lost leadership", "identity", identity)
			},
			OnNewLeader: func(identity string) {
				logger.Info("health controller leader changed", "identity", identity)
			},
		},
		ReleaseOnCancel: true,
		Name:            "cache-csi-health-controller",
	})
	return nil
}

func validateRuntimeConfig(gcInterval time.Duration, projectIDStart, projectIDCount uint, nodeID string) error {
	if gcInterval <= 0 {
		return errors.New("GC interval must be greater than zero")
	}
	if uint64(projectIDStart) > uint64(^uint32(0)) || uint64(projectIDCount) > uint64(^uint32(0)) {
		return errors.New("project ID range values must fit within uint32")
	}
	if nodeID == "" {
		return errors.New("node ID must be configured")
	}
	return nil
}

func serveMetrics(ctx context.Context, server *http.Server, listener net.Listener) error {
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	select {
	case err := <-serveDone:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		shutdownErr := server.Shutdown(shutdownContext)
		if shutdownErr != nil {
			_ = server.Close()
		}
		serveErr := <-serveDone
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	}
}

type runtimePaths struct {
	socketPath string
}

func resolveRuntimePaths(cacheRoot, kubeletRoot, endpoint string) (runtimePaths, error) {
	for _, item := range []struct{ name, path string }{{name: "cache root", path: cacheRoot}, {name: "kubelet root", path: kubeletRoot}} {
		if !filepath.IsAbs(item.path) {
			return runtimePaths{}, fmt.Errorf("%s must be an absolute path", item.name)
		}
	}
	canonicalCacheRoot, err := canonicalPath(cacheRoot)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("resolve cache root: %w", err)
	}
	canonicalKubeletRoot, err := canonicalPath(kubeletRoot)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("resolve kubelet root: %w", err)
	}
	if pathsOverlap(canonicalCacheRoot, canonicalKubeletRoot) {
		return runtimePaths{}, errors.New("cache root must not overlap the kubelet root")
	}
	socketPath, err := parseEndpoint(endpoint)
	if err != nil {
		return runtimePaths{}, err
	}
	return runtimePaths{socketPath: socketPath}, nil
}

func pathsOverlap(left, right string) bool {
	return pathWithin(left, right) || pathWithin(right, left)
}

func pathWithin(base, candidate string) bool {
	relative, err := filepath.Rel(base, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func canonicalPath(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	path = filepath.Clean(path)
	missing := make([]string, 0)
	for current := path; ; current = filepath.Dir(current) {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for _, component := range slices.Backward(missing) {
				resolved = filepath.Join(resolved, component)
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		missing = append(missing, filepath.Base(current))
	}
}

func kubernetesResolver(logger *slog.Logger) *kube.Resolver {
	config, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("in-cluster Kubernetes configuration is unavailable; Cache CSI cannot resolve policies", "error", err)
		return nil
	}
	config = rest.CopyConfig(config)
	config.Timeout = 5 * time.Second
	config.QPS = 50
	config.Burst = 100
	resolver, resolverErr := kube.NewResolver(config)
	if resolverErr != nil {
		logger.Error("create CacheClass resolver failed", "error", resolverErr)
	}
	return resolver
}

func parseEndpoint(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse CSI endpoint: %w", err)
	}
	if parsed.Scheme != "unix" || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" || !filepath.IsAbs(parsed.Path) {
		return "", errors.New("CSI endpoint must be an absolute unix socket URL")
	}
	return filepath.Clean(parsed.Path), nil
}

func listenUnixSocket(path string) (net.Listener, func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create CSI socket directory: %w", err)
	}
	if existing, err := os.Lstat(path); err == nil {
		if existing.Mode()&os.ModeSocket == 0 {
			return nil, nil, errors.New("CSI socket path exists and is not a socket")
		}
		connection, dialErr := (&net.Dialer{Timeout: time.Second}).DialContext(context.Background(), "unix", path)
		if dialErr == nil {
			_ = connection.Close()
			return nil, nil, errors.New("CSI socket is already in use")
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, nil, fmt.Errorf("check existing CSI socket: %w", dialErr)
		}
		current, statErr := os.Lstat(path)
		if statErr == nil && os.SameFile(existing, current) {
			if err := os.Remove(path); err != nil {
				return nil, nil, fmt.Errorf("remove stale CSI socket: %w", err)
			}
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("recheck existing CSI socket: %w", statErr)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("inspect CSI socket path: %w", err)
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	if err != nil {
		return nil, nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		return nil, nil, fmt.Errorf("inspect created CSI socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		_ = listener.Close()
		return nil, nil, errors.New("created CSI endpoint is not a socket")
	}
	if err := os.Chmod(path, 0o660); err != nil {
		_ = listener.Close()
		if current, statErr := os.Lstat(path); statErr == nil && os.SameFile(info, current) {
			_ = os.Remove(path)
		}
		return nil, nil, fmt.Errorf("set CSI socket permissions: %w", err)
	}
	cleanup := func() {
		_ = listener.Close()
		if current, err := os.Lstat(path); err == nil && os.SameFile(info, current) {
			_ = os.Remove(path)
		}
	}
	return listener, cleanup, nil
}
