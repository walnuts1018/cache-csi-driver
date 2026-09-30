package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/metrics"
	"github.com/walnuts1018/cache-csi-driver/internal/nodehealth"
)

type MountInspector func(source string, lease cache.Lease, policy cache.Policy) (bool, error)

type Options struct {
	Interval           time.Duration
	PressureInterval   time.Duration
	InspectMount       MountInspector
	ResolverReady      func() bool
	CapabilityProbe    func(context.Context, string) error
	FilesystemReadOnly func(string) (bool, error)
	Health             *nodehealth.Tracker
	Logger             *slog.Logger
	Metrics            *metrics.Metrics
}

type Manager struct {
	store              *cache.Store
	interval           time.Duration
	pressureInterval   time.Duration
	inspectMount       MountInspector
	resolverReady      func() bool
	capabilityProbe    func(context.Context, string) error
	filesystemReadOnly func(string) (bool, error)
	health             *nodehealth.Tracker
	logger             *slog.Logger
	metrics            *metrics.Metrics
	stateMu            sync.Mutex
	pressureActive     bool
	maintenanceError   bool
}

func New(store *cache.Store, options Options) *Manager {
	if options.Interval <= 0 {
		options.Interval = 30 * time.Second
	}
	if options.PressureInterval <= 0 {
		options.PressureInterval = 3 * time.Second
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Health == nil {
		options.Health = nodehealth.NewTracker()
	}
	if options.Metrics != nil {
		options.Metrics.SetRecoveryState("primary", "initializing")
	}
	return &Manager{
		store:              store,
		interval:           options.Interval,
		pressureInterval:   options.PressureInterval,
		inspectMount:       options.InspectMount,
		resolverReady:      options.ResolverReady,
		capabilityProbe:    options.CapabilityProbe,
		filesystemReadOnly: options.FilesystemReadOnly,
		health:             options.Health,
		logger:             options.Logger,
		metrics:            options.Metrics,
	}
}

func (manager *Manager) Recover(ctx context.Context) error {
	manager.health.Set(nodehealth.PhaseRecovering, "CacheRecovery", false)
	if manager.metrics != nil {
		manager.metrics.SetRecoveryState("primary", "recovering")
	}
	started := time.Now()
	if manager.resolverReady != nil && !manager.resolverReady() {
		return manager.recoveryFailure(started, errors.New("CacheClass resolver has not synchronized"))
	}
	if manager.inspectMount == nil {
		return manager.recoveryFailure(started, errors.New("mount inspector is not configured"))
	}
	if err := manager.store.WaitForIndexes(ctx); err != nil {
		manager.classifyRecoveryFailure()
		return manager.recoveryFailure(started, fmt.Errorf("wait for cache indexes under %s: %w", manager.store.Root(), err))
	}
	if err := manager.store.RecoverLeasesContext(ctx, manager.inspectMount); err != nil {
		manager.classifyRecoveryFailure()
		return manager.recoveryFailure(started, fmt.Errorf("recover cache leases under %s: %w", manager.store.Root(), err))
	}
	if err := manager.backendHealth(); err != nil {
		manager.health.Set(nodehealth.PhaseUnavailable, "CacheBackendUnavailable", true)
		return manager.recoveryFailure(started, err)
	}
	if err := manager.store.CheckFilesystem(); err != nil {
		manager.health.Set(nodehealth.PhaseUnavailable, "CacheBackendUnavailable", true)
		return manager.recoveryFailure(started, fmt.Errorf("probe cache filesystem under %s: %w", manager.store.Root(), err))
	}
	manager.refreshHealth(ctx)
	if manager.metrics != nil {
		manager.metrics.RecordRecoveryAttempt("primary", "success", time.Since(started).Seconds())
		manager.metrics.SetRecoveryState("primary", "ready")
	}
	manager.syncStoreMetrics()
	return nil
}

func (manager *Manager) recoveryFailure(started time.Time, err error) error {
	if manager.metrics != nil {
		manager.metrics.RecordRecoveryAttempt("primary", "failure", time.Since(started).Seconds())
		manager.metrics.SetRecoveryState("primary", "failed")
	}
	return err
}

func (manager *Manager) classifyRecoveryFailure() {
	if !manager.store.Ready() {
		if err := manager.store.CheckFilesystem(); err != nil {
			manager.health.Set(nodehealth.PhaseUnavailable, "CacheBackendUnavailable", true)
			return
		}
		manager.health.Set(nodehealth.PhaseRecovering, "CacheRecovery", false)
		return
	}
	if err := manager.backendHealth(); err != nil {
		manager.health.Set(nodehealth.PhaseUnavailable, "CacheBackendUnavailable", true)
		return
	}
	manager.health.Set(nodehealth.PhaseRecovering, "CacheRecovery", false)
}

func (manager *Manager) Run(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	pressureRequests := make(chan struct{}, 1)
	var workers sync.WaitGroup
	workers.Go(func() { manager.runCollection(ctx) })
	workers.Go(func() { manager.runDegradedRecovery(ctx) })
	workers.Go(func() { manager.runPressureWorker(ctx, pressureRequests) })
	manager.runPressureMonitor(ctx, pressureRequests)
	workers.Wait()
}

func (manager *Manager) runCollection(ctx context.Context) {
	manager.collect(ctx, time.Now())
	ticker := time.NewTicker(manager.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			manager.collect(ctx, now)
		}
	}
}

func (manager *Manager) runDegradedRecovery(ctx context.Context) {
	manager.recoverDegraded(ctx)
	ticker := time.NewTicker(manager.pressureInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			manager.recoverDegraded(ctx)
		}
	}
}

func (manager *Manager) runPressureMonitor(ctx context.Context, requests chan<- struct{}) {
	manager.observePressure(ctx, requests)
	ticker := time.NewTicker(manager.pressureInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			manager.observePressure(ctx, requests)
		}
	}
}

func (manager *Manager) observePressure(ctx context.Context, requests chan<- struct{}) {
	active, err := manager.store.ObservePressure()
	if err != nil {
		manager.logger.ErrorContext(ctx, "observe cache filesystem pressure failed", "root", manager.store.Root(), "error", err)
		manager.health.Set(nodehealth.PhaseUnavailable, "CacheFilesystemUnavailable", true)
		return
	}
	manager.setPressure(active)
	manager.syncStoreMetrics()
	if active {
		current := manager.health.Current()
		if current.Phase != nodehealth.PhaseUnavailable || current.Reason != "CacheFilesystemPressure" || !current.Evict {
			manager.health.Set(nodehealth.PhaseUnavailable, "CacheFilesystemPressure", false)
		}
		select {
		case requests <- struct{}{}:
		default:
		}
		return
	}
	manager.refreshHealth(ctx)
}

func (manager *Manager) runPressureWorker(ctx context.Context, requests <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-requests:
			manager.pressure(ctx)
		}
	}
}

func (manager *Manager) pressure(ctx context.Context) {
	if err := manager.store.ReclaimPressure(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		manager.logger.WarnContext(ctx, "cache pressure reclaim was incomplete", "root", manager.store.Root(), "error", err)
	}
	active, err := manager.store.ObservePressure()
	if err != nil {
		manager.setPressure(true)
		manager.health.Set(nodehealth.PhaseUnavailable, "CacheFilesystemUnavailable", true)
		manager.logger.ErrorContext(ctx, "inspect cache filesystem after pressure reclaim failed", "root", manager.store.Root(), "error", err)
		return
	}
	manager.setPressure(active)
	if active {
		manager.health.Set(nodehealth.PhaseUnavailable, "CacheFilesystemPressure", true)
		manager.logger.WarnContext(ctx, "cache filesystem remains under pressure after unused cache reclamation")
	} else {
		manager.refreshHealth(ctx)
	}
	manager.syncStoreMetrics()
}

func (manager *Manager) recoverDegraded(ctx context.Context) {
	if manager.inspectMount == nil || ctx.Err() != nil {
		return
	}
	if err := manager.store.RecoverDegraded(ctx, manager.inspectMount); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		manager.logger.WarnContext(ctx, "degraded cache recovery was incomplete", "root", manager.store.Root(), "error", err)
		manager.setMaintenanceError(true)
	} else {
		manager.setMaintenanceError(false)
	}
	manager.refreshHealth(ctx)
	manager.syncStoreMetrics()
}

func (manager *Manager) collect(ctx context.Context, now time.Time) {
	var collectionErr error
	if err := manager.store.Collect(now); err != nil {
		collectionErr = errors.Join(collectionErr, fmt.Errorf("collect cache objects: %w", err))
		manager.logger.ErrorContext(ctx, "cache collection failed", "root", manager.store.Root(), "error", err)
	}
	if err := manager.store.CleanupTrash(ctx); err != nil {
		collectionErr = errors.Join(collectionErr, fmt.Errorf("clean cache trash: %w", err))
		manager.logger.ErrorContext(ctx, "cache trash cleanup failed", "root", manager.store.Root(), "error", err)
	}
	manager.setMaintenanceError(collectionErr != nil)
	manager.refreshHealth(ctx)
	manager.syncStoreMetrics()
}

func (manager *Manager) refreshHealth(ctx context.Context) {
	if err := manager.backendHealth(); err != nil {
		manager.health.Set(nodehealth.PhaseUnavailable, "CacheBackendUnavailable", true)
		return
	}
	manager.stateMu.Lock()
	pressureActive := manager.pressureActive
	maintenanceError := manager.maintenanceError
	manager.stateMu.Unlock()
	current := manager.health.Current()
	if current.Phase == nodehealth.PhaseUnavailable {
		if current.Reason == "CacheFilesystemPressure" && pressureActive {
			return
		}
		if current.Reason != "CacheFilesystemPressure" && current.Reason != "CacheBackendUnavailable" && current.Reason != "CacheFilesystemUnavailable" {
			if manager.capabilityProbe == nil {
				return
			}
			probeContext, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := manager.capabilityProbe(probeContext, current.Reason)
			cancel()
			if err != nil {
				return
			}
		}
		if err := manager.store.CheckFilesystem(); err != nil {
			manager.health.Set(nodehealth.PhaseUnavailable, "CacheBackendUnavailable", true)
			return
		}
	}
	if pressureActive {
		return
	}
	if err := manager.store.MetadataError(); err != nil {
		manager.health.Set(nodehealth.PhaseDegraded, "CacheMetadataDegraded", false)
		return
	}
	if maintenanceError {
		manager.health.Set(nodehealth.PhaseDegraded, "CacheMaintenanceDegraded", false)
		return
	}
	manager.health.Set(nodehealth.PhaseReady, "CacheReady", false)
}

func (manager *Manager) backendHealth() error {
	if !manager.store.Ready() {
		return cache.ErrStoreNotReady
	}
	if manager.filesystemReadOnly != nil {
		readOnly, err := manager.filesystemReadOnly(manager.store.Root())
		if err != nil {
			return fmt.Errorf("inspect cache root filesystem: %w", err)
		}
		if readOnly {
			return errors.New("cache root filesystem is read-only")
		}
	}
	if err := manager.store.ProjectRegistryError(); err != nil {
		return fmt.Errorf("cache project quota registry is unavailable: %w", err)
	}
	return nil
}

func (manager *Manager) setPressure(active bool) {
	manager.stateMu.Lock()
	manager.pressureActive = active
	manager.stateMu.Unlock()
}

func (manager *Manager) setMaintenanceError(active bool) {
	manager.stateMu.Lock()
	manager.maintenanceError = active
	manager.stateMu.Unlock()
}

func (manager *Manager) syncStoreMetrics() {
	if manager.metrics == nil {
		return
	}
	stats := manager.store.RuntimeStats()
	manager.metrics.SetStoreSnapshot("primary", metrics.StoreSnapshot{
		Ready:               stats.Ready,
		PressureState:       stats.PressureState,
		DegradedObjects:     stats.DegradedObjects,
		CacheObjects:        stats.CacheObjects,
		RetiredGenerations:  stats.RetiredGenerations,
		TrashObjectsDeleted: stats.TrashObjectsDeleted,
	})
}
