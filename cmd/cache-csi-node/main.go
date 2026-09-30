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
	"github.com/walnuts1018/cache-csi-driver/internal/quota"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	version  = "dev"
	revision = "unknown"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("cache CSI node driver stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	endpoint := flag.String("endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
	metricsAddress := flag.String("metrics-address", ":9807", "Prometheus metrics HTTP listen address")
	cacheRoot := flag.String("cache-root", "/var/lib/cache-csi", "cache storage directory")
	requireCacheRootMountpoint := flag.Bool("require-cache-root-mountpoint", false, "require cache-root to be a filesystem mountpoint before measuring filesystem-wide pressure")
	fallbackRoot := flag.String("fallback-root", "/run/cache-csi/fallback", "bounded temporary cache directory used when the primary cache cannot serve a volume")
	fallbackMaxBytes := flag.String("fallback-max-bytes", "1Gi", "maximum total size of the fallback tmpfs filesystem")
	fallbackVolumeMaxBytes := flag.String("fallback-volume-max-bytes", "128Mi", "maximum size reserved for one fallback cache volume")
	fallbackEmergencyRoot := flag.String("fallback-emergency-root", "/run/cache-csi/fallback-emergency", "isolated tmpfs root used when the regular fallback store cannot allocate a volume")
	fallbackEmergencyMaxBytes := flag.String("fallback-emergency-max-bytes", "64Mi", "maximum aggregate writable generation capacity in emergency fallback")
	fallbackEmergencyRootMaxBytes := flag.String("fallback-emergency-root-max-bytes", "16Mi", "maximum size of the emergency fallback Store metadata tmpfs")
	fallbackEmergencyVolumeMaxBytes := flag.String("fallback-emergency-volume-max-bytes", "4Mi", "maximum size reserved for one emergency cache volume")
	fallbackTerminalRoot := flag.String("fallback-terminal-root", "/run/cache-csi/fallback-terminal", "bounded tmpfs root for terminal fallback scratch mounts")
	fallbackTerminalRootMaxBytes := flag.String("fallback-terminal-root-max-bytes", "64Mi", "maximum size of the terminal fallback tmpfs shared by volumes")
	nodeID := flag.String("node-id", os.Getenv("NODE_NAME"), "Kubernetes node name")
	kubeletRoot := flag.String("kubelet-root", "/var/lib/kubelet", "kubelet root directory")
	gcInterval := flag.Duration("gc-interval", 30*time.Second, "cache garbage collection interval")
	highFreePercent := flag.Int("pressure-high-free-percent", 25, "free-byte percentage at which cache pressure collection stops")
	lowFreePercent := flag.Int("pressure-low-free-percent", 20, "free-byte percentage at which cache pressure collection starts")
	criticalFreePercent := flag.Int("pressure-critical-free-percent", 0, "free-byte percentage below which ForceDelete policies become eligible; --allow-pod-eviction and --allow-force-delete are required for Pod deletion; zero disables critical escalation")
	highInodeFreePercent := flag.Int("pressure-high-inode-free-percent", 15, "free-inode percentage at which cache pressure collection stops")
	lowInodeFreePercent := flag.Int("pressure-low-inode-free-percent", 10, "free-inode percentage at which cache pressure collection starts")
	criticalInodeFreePercent := flag.Int("pressure-critical-inode-free-percent", 0, "free-inode percentage below which ForceDelete policies become eligible; --allow-pod-eviction and --allow-force-delete are required for Pod deletion; zero disables critical escalation")
	allowPodEviction := flag.Bool("allow-pod-eviction", false, "allow cache pressure handling to request Pod eviction")
	allowForceDelete := flag.Bool("allow-force-delete", false, "allow ForceDelete pressure policies to bypass PodDisruptionBudgets using UID-preconditioned Pod deletion")
	projectIDStart := flag.Uint("project-id-start", 2_000_000_000, "first project ID reserved for cache identities")
	projectIDCount := flag.Uint("project-id-count", 1_000_000, "number of project IDs reserved for cache identities")
	flag.Parse()

	if err := validateRuntimeConfig(*gcInterval, *allowPodEviction, *allowForceDelete, *projectIDStart, *projectIDCount, *nodeID); err != nil {
		return err
	}
	paths, err := resolveRuntimePaths(*cacheRoot, *fallbackRoot, *fallbackEmergencyRoot, *fallbackTerminalRoot, *kubeletRoot, *fallbackMaxBytes, *fallbackVolumeMaxBytes, *fallbackEmergencyMaxBytes, *fallbackEmergencyRootMaxBytes, *fallbackEmergencyVolumeMaxBytes, *fallbackTerminalRootMaxBytes, *endpoint)
	if err != nil {
		return err
	}
	if err := prepareFallbackFilesystems(*fallbackRoot, paths.fallbackSize, *fallbackEmergencyRoot, paths.fallbackEmergencyRootSize, *fallbackTerminalRoot, paths.fallbackTerminalRootSize); err != nil {
		return err
	}

	pressure := cache.PressureConfig{
		HighFreePercent:          *highFreePercent,
		LowFreePercent:           *lowFreePercent,
		CriticalFreePercent:      *criticalFreePercent,
		HighInodeFreePercent:     *highInodeFreePercent,
		LowInodeFreePercent:      *lowInodeFreePercent,
		CriticalInodeFreePercent: *criticalInodeFreePercent,
	}
	store, err := cache.NewStoreAsync(*cacheRoot, cache.StoreOptions{
		Pressure:              pressure,
		ProjectIDStart:        uint32(*projectIDStart),
		ProjectIDCount:        uint32(*projectIDCount),
		RequireRootMountpoint: *requireCacheRootMountpoint,
	})
	if err != nil {
		return fmt.Errorf("initialize cache store: %w", err)
	}
	defer func() { _ = store.Close() }()
	fallbackStore, err := cache.NewStore(*fallbackRoot, cache.StoreOptions{UnmountGeneration: driver.UnmountFallbackGeneration})
	if err != nil {
		return fmt.Errorf("initialize fallback cache store: %w", errors.Join(err, store.Close()))
	}
	defer func() { _ = fallbackStore.Close() }()
	emergencyStore, err := cache.NewStore(*fallbackEmergencyRoot, cache.StoreOptions{UnmountGeneration: driver.UnmountFallbackGeneration})
	if err != nil {
		return fmt.Errorf("initialize emergency fallback cache store: %w", errors.Join(err, fallbackStore.Close(), store.Close()))
	}
	defer func() { _ = emergencyStore.Close() }()
	metricSet := metrics.New()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client, resolver := kubernetesClients(logger)
	if resolver != nil {
		resolver.Start(ctx)
	}
	managerClient := kubernetes.Interface(nil)
	if *allowPodEviction {
		managerClient = client
	}
	cacheManager := manager.New(store, manager.Options{
		Interval:         *gcInterval,
		Client:           managerClient,
		InspectMount:     driver.VerifyCacheMount,
		FallbackStore:    fallbackStore,
		EmergencyStore:   emergencyStore,
		Logger:           logger,
		Metrics:          metricSet,
		AllowPodEviction: *allowPodEviction,
		AllowForceDelete: *allowForceDelete,
	})
	if !*allowPodEviction {
		logger.Info("Pod pressure eviction is disabled")
	} else if client == nil {
		logger.Warn("Pod pressure eviction is unavailable because the in-cluster Kubernetes client could not be created")
	}

	service := driver.New(store, resolver, quota.XFS{Binary: "xfs_quota"}, driver.Options{
		NodeID:                          *nodeID,
		KubeletRoot:                     *kubeletRoot,
		FallbackRoot:                    *fallbackRoot,
		FallbackStore:                   fallbackStore,
		FallbackMaxBytes:                paths.fallbackMaxBytes,
		FallbackVolumeMaxBytes:          paths.fallbackVolumeSize,
		FallbackEmergencyRoot:           *fallbackEmergencyRoot,
		FallbackEmergencyStore:          emergencyStore,
		FallbackEmergencyMaxBytes:       paths.fallbackEmergencyMaxBytes,
		FallbackEmergencyVolumeMaxBytes: paths.fallbackEmergencyVolumeSize,
		FallbackTerminalRoot:            *fallbackTerminalRoot,
		VendorVersion:                   version,
		Metrics:                         metricSet,
		Logger:                          logger,
	})
	grpcServer := grpc.NewServer()
	csi.RegisterIdentityServer(grpcServer, service)
	csi.RegisterNodeServer(grpcServer, service)

	listener, cleanupSocket, err := listenUnixSocket(paths.socketPath)
	if err != nil {
		return fmt.Errorf("listen on CSI endpoint: %w", err)
	}
	defer cleanupSocket()
	var listenConfig net.ListenConfig
	metricsListener, err := listenConfig.Listen(ctx, "tcp", *metricsAddress)
	if err != nil {
		return fmt.Errorf("listen on metrics endpoint: %w", err)
	}
	metricsServer := &http.Server{Handler: metricSet.Handler(), ReadHeaderTimeout: 5 * time.Second}

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
				logger.WarnContext(managerContext, "cache recovery is incomplete; new volumes use bounded fallback", "error", err)
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
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()
	metricsErr := make(chan error, 1)
	go func() {
		metricsErr <- serveMetrics(managerContext, metricsServer, metricsListener)
	}()

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

func validateRuntimeConfig(gcInterval time.Duration, allowPodEviction, allowForceDelete bool, projectIDStart, projectIDCount uint, nodeID string) error {
	if gcInterval <= 0 {
		return fmt.Errorf("gc interval must be greater than zero")
	}
	if allowForceDelete && !allowPodEviction {
		return fmt.Errorf("allow-force-delete requires --allow-pod-eviction")
	}
	if uint64(projectIDStart) > uint64(^uint32(0)) || uint64(projectIDCount) > uint64(^uint32(0)) {
		return fmt.Errorf("project ID range values must fit within uint32")
	}
	if nodeID == "" {
		return fmt.Errorf("node ID must be configured")
	}
	return nil
}

func prepareFallbackFilesystems(fallbackRoot string, fallbackSize int64, emergencyRoot string, emergencySize int64, terminalRoot string, terminalSize int64) error {
	if err := mountFallbackTmpfs(fallbackRoot, fallbackSize, true); err != nil {
		return fmt.Errorf("prepare bounded fallback filesystem: %w", err)
	}
	if err := mountFallbackTmpfs(emergencyRoot, emergencySize, true); err != nil {
		return fmt.Errorf("prepare emergency fallback filesystem: %w", err)
	}
	if err := mountFallbackTmpfs(terminalRoot, terminalSize, false); err != nil {
		return fmt.Errorf("prepare terminal fallback filesystem: %w", err)
	}
	if err := driver.PreflightMountAPI(); err != nil {
		return fmt.Errorf("preflight Linux mount APIs: %w", err)
	}
	return nil
}

func serveMetrics(ctx context.Context, server *http.Server, listener net.Listener) error {
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
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
	socketPath                  string
	fallbackSize                int64
	fallbackMaxBytes            int64
	fallbackVolumeSize          int64
	fallbackEmergencyMaxBytes   int64
	fallbackEmergencyRootSize   int64
	fallbackEmergencyVolumeSize int64
	fallbackTerminalRootSize    int64
}

func resolveRuntimePaths(cacheRoot, fallbackRoot, fallbackEmergencyRoot, fallbackTerminalRoot, kubeletRoot, fallbackMaxBytes, fallbackVolumeMaxBytes, fallbackEmergencyMaxBytes, fallbackEmergencyRootMaxBytes, fallbackEmergencyVolumeMaxBytes, fallbackTerminalRootMaxBytes, endpoint string) (runtimePaths, error) {
	for _, item := range []struct{ name, path string }{
		{name: "cache root", path: cacheRoot},
		{name: "fallback root", path: fallbackRoot},
		{name: "emergency fallback root", path: fallbackEmergencyRoot},
		{name: "terminal fallback root", path: fallbackTerminalRoot},
		{name: "kubelet root", path: kubeletRoot},
	} {
		if !filepath.IsAbs(item.path) {
			return runtimePaths{}, fmt.Errorf("%s must be an absolute path", item.name)
		}
	}
	fallbackSize, err := fallbackFilesystemSize(fallbackMaxBytes)
	if err != nil {
		return runtimePaths{}, err
	}
	fallbackVolumeSize, err := fallbackFilesystemSize(fallbackVolumeMaxBytes)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("parse fallback per-volume maximum: %w", err)
	}
	fallbackEmergencyVolumeSize, err := fallbackFilesystemSize(fallbackEmergencyVolumeMaxBytes)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("parse fallback emergency per-volume maximum: %w", err)
	}
	fallbackEmergencySize, err := fallbackFilesystemSize(fallbackEmergencyMaxBytes)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("parse emergency fallback aggregate generation maximum: %w", err)
	}
	fallbackEmergencyRootSize, err := fallbackFilesystemSize(fallbackEmergencyRootMaxBytes)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("parse emergency fallback root maximum: %w", err)
	}
	fallbackTerminalRootSize, err := fallbackFilesystemSize(fallbackTerminalRootMaxBytes)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("parse terminal fallback root maximum: %w", err)
	}
	if fallbackEmergencyVolumeSize < int64(os.Getpagesize()) {
		return runtimePaths{}, fmt.Errorf("fallback emergency per-volume maximum must be at least one page (%d bytes)", os.Getpagesize())
	}
	if fallbackEmergencyVolumeSize > fallbackEmergencySize {
		return runtimePaths{}, errors.New("fallback emergency per-volume maximum must not exceed the emergency aggregate maximum")
	}
	if fallbackVolumeSize > fallbackSize {
		return runtimePaths{}, errors.New("fallback per-volume maximum must not exceed the aggregate fallback maximum")
	}
	canonicalCacheRoot, err := canonicalPath(cacheRoot)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("resolve cache root: %w", err)
	}
	canonicalFallbackRoot, err := canonicalPath(fallbackRoot)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("resolve fallback root: %w", err)
	}
	canonicalEmergencyFallbackRoot, err := canonicalPath(fallbackEmergencyRoot)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("resolve emergency fallback root: %w", err)
	}
	canonicalTerminalFallbackRoot, err := canonicalPath(fallbackTerminalRoot)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("resolve terminal fallback root: %w", err)
	}
	canonicalKubeletRoot, err := canonicalPath(kubeletRoot)
	if err != nil {
		return runtimePaths{}, fmt.Errorf("resolve kubelet root: %w", err)
	}
	if pathsOverlap(canonicalCacheRoot, canonicalFallbackRoot) || pathsOverlap(canonicalKubeletRoot, canonicalFallbackRoot) || pathsOverlap(canonicalEmergencyFallbackRoot, canonicalFallbackRoot) || pathsOverlap(canonicalTerminalFallbackRoot, canonicalFallbackRoot) {
		return runtimePaths{}, fmt.Errorf("fallback root must not overlap the cache root, emergency fallback root, terminal fallback root, or kubelet root")
	}
	if pathsOverlap(canonicalCacheRoot, canonicalEmergencyFallbackRoot) || pathsOverlap(canonicalKubeletRoot, canonicalEmergencyFallbackRoot) || pathsOverlap(canonicalTerminalFallbackRoot, canonicalEmergencyFallbackRoot) {
		return runtimePaths{}, fmt.Errorf("emergency fallback root must not overlap the cache root, terminal fallback root, or kubelet root")
	}
	if pathsOverlap(canonicalCacheRoot, canonicalTerminalFallbackRoot) || pathsOverlap(canonicalKubeletRoot, canonicalTerminalFallbackRoot) {
		return runtimePaths{}, fmt.Errorf("terminal fallback root must not overlap the cache root or kubelet root")
	}
	if pathsOverlap(canonicalCacheRoot, canonicalKubeletRoot) {
		return runtimePaths{}, fmt.Errorf("cache root must not overlap the kubelet root")
	}
	socketPath, err := parseEndpoint(endpoint)
	if err != nil {
		return runtimePaths{}, err
	}
	return runtimePaths{
		socketPath:                  socketPath,
		fallbackSize:                fallbackSize,
		fallbackMaxBytes:            fallbackSize,
		fallbackVolumeSize:          fallbackVolumeSize,
		fallbackEmergencyMaxBytes:   fallbackEmergencySize,
		fallbackEmergencyRootSize:   fallbackEmergencyRootSize,
		fallbackEmergencyVolumeSize: fallbackEmergencyVolumeSize,
		fallbackTerminalRootSize:    fallbackTerminalRootSize,
	}, nil
}

func fallbackFilesystemSize(value string) (int64, error) {
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, fmt.Errorf("parse fallback maximum size: %w", err)
	}
	size := quantity.Value()
	if size <= 0 {
		return 0, fmt.Errorf("fallback maximum size must be positive")
	}
	return size, nil
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

func kubernetesClients(logger *slog.Logger) (kubernetes.Interface, *kube.Resolver) {
	config, err := rest.InClusterConfig()
	if err != nil {
		logger.Warn("in-cluster Kubernetes configuration is unavailable; using isolated fallback caches", "error", err)
		return nil, nil
	}
	config = rest.CopyConfig(config)
	config.Timeout = 5 * time.Second
	config.QPS = 50
	config.Burst = 100
	client, clientErr := kubernetes.NewForConfig(config)
	if clientErr != nil {
		logger.Warn("create Kubernetes client for cache pressure eviction failed", "error", clientErr)
	}
	resolver, resolverErr := kube.NewResolver(config)
	if resolverErr != nil {
		logger.Warn("create CacheClass resolver failed; using isolated fallback caches", "error", resolverErr)
	}
	return client, resolver
}

func parseEndpoint(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse CSI endpoint: %w", err)
	}
	if parsed.Scheme != "unix" || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" || !filepath.IsAbs(parsed.Path) {
		return "", fmt.Errorf("CSI endpoint must be an absolute unix socket URL")
	}
	return filepath.Clean(parsed.Path), nil
}

func listenUnixSocket(path string) (net.Listener, func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create CSI socket directory: %w", err)
	}
	if existing, err := os.Lstat(path); err == nil {
		if existing.Mode()&os.ModeSocket == 0 {
			return nil, nil, fmt.Errorf("CSI socket path exists and is not a socket")
		}
		connection, dialErr := (&net.Dialer{Timeout: time.Second}).DialContext(context.Background(), "unix", path)
		if dialErr == nil {
			_ = connection.Close()
			return nil, nil, fmt.Errorf("CSI socket is already in use")
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

	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(context.Background(), "unix", path)
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
		return nil, nil, fmt.Errorf("created CSI endpoint is not a socket")
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
