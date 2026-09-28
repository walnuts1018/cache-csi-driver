package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/driver"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
	"github.com/walnuts1018/cache-csi-driver/internal/manager"
	"github.com/walnuts1018/cache-csi-driver/internal/quota"
	"google.golang.org/grpc"
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
	cacheRoot := flag.String("cache-root", "/var/lib/cache-csi", "cache storage directory")
	fallbackRoot := flag.String("fallback-root", "/run/cache-csi/fallback", "temporary cache directory used when Kubernetes API resolution fails")
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

	if *gcInterval <= 0 {
		return fmt.Errorf("gc interval must be greater than zero")
	}
	if uint64(*projectIDStart) > uint64(^uint32(0)) || uint64(*projectIDCount) > uint64(^uint32(0)) {
		return fmt.Errorf("project ID range values must fit within uint32")
	}
	if *nodeID == "" {
		return fmt.Errorf("node ID must be configured")
	}
	socketPath, err := parseEndpoint(*endpoint)
	if err != nil {
		return err
	}
	for _, item := range []struct{ name, path string }{
		{name: "cache root", path: *cacheRoot},
		{name: "fallback root", path: *fallbackRoot},
		{name: "kubelet root", path: *kubeletRoot},
	} {
		if !filepath.IsAbs(item.path) {
			return fmt.Errorf("%s must be an absolute path", item.name)
		}
	}

	store, err := cache.NewStore(*cacheRoot, cache.StoreOptions{
		Pressure: cache.PressureConfig{
			HighFreePercent:      *highFreePercent,
			LowFreePercent:       *lowFreePercent,
			HighInodeFreePercent: *highInodeFreePercent,
			LowInodeFreePercent:  *lowInodeFreePercent,
		},
		ProjectIDStart: uint32(*projectIDStart),
		ProjectIDCount: uint32(*projectIDCount),
	})
	if err != nil {
		return fmt.Errorf("initialize cache store: %w", err)
	}
	defer func() { _ = store.Close() }()
	client, resolver := kubernetesClients(logger)
	cacheManager := manager.New(store, manager.Options{
		Interval:     *gcInterval,
		Client:       client,
		InspectMount: driver.VerifyCacheMount,
		Logger:       logger,
	})
	if err := cacheManager.Recover(); err != nil {
		return err
	}
	if client == nil {
		logger.Warn("Pod pressure eviction is unavailable because the in-cluster Kubernetes client could not be created")
	}

	service := driver.New(store, resolver, quota.XFS{Binary: "xfs_quota"}, driver.Options{
		NodeID:        *nodeID,
		KubeletRoot:   *kubeletRoot,
		FallbackRoot:  *fallbackRoot,
		VendorVersion: version,
	})
	grpcServer := grpc.NewServer()
	csi.RegisterIdentityServer(grpcServer, service)
	csi.RegisterNodeServer(grpcServer, service)

	listener, cleanupSocket, err := listenUnixSocket(socketPath)
	if err != nil {
		return fmt.Errorf("listen on CSI endpoint: %w", err)
	}
	defer cleanupSocket()

	logger.Info("starting cache CSI node driver", "version", version, "revision", revision, "nodeID", *nodeID, "endpoint", *endpoint)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	managerContext, cancelManager := context.WithCancel(ctx)
	managerDone := make(chan struct{})
	go func() {
		defer close(managerDone)
		cacheManager.Run(managerContext)
	}()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		cancelManager()
		<-managerDone
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve CSI gRPC endpoint: %w", err)
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
	return nil
}

func kubernetesClients(logger *slog.Logger) (kubernetes.Interface, driver.ClassResolver) {
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
