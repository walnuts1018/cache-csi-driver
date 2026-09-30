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
	QuotaRequired      bool
	QuotaProbe         func(context.Context, string) error
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
	quotaRequired      bool
	quotaProbe         func(context.Context, string) error
	health             *nodehealth.Tracker
	logger             *slog.Logger
	metrics            *metrics.Metrics
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
		quotaRequired:      options.QuotaRequired,
		quotaProbe:         options.QuotaProbe,
		health:             options.Health,
		logger:             options.Logger,
		metrics:            options.Metrics,
	}
}

func (manager *Manager) Recover(ctx context.Context) error {
	manager.health.ClearCondition(nodehealth.SubsystemStartup)
	manager.health.SetCondition(nodehealth.SubsystemRecovery, nodehealth.PhaseRecovering, "CacheRecovery", false)
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
		manager.classifyRecoveryFailure(ctx)
		return manager.recoveryFailure(started, fmt.Errorf("wait for cache indexes under %s: %w", manager.store.Root(), err))
	}
	if err := manager.store.RecoverLeasesContext(ctx, manager.inspectMount); err != nil {
		manager.classifyRecoveryFailure(ctx)
		return manager.recoveryFailure(started, fmt.Errorf("recover cache leases under %s: %w", manager.store.Root(), err))
	}
	if err := manager.backendHealth(ctx, true); err != nil {
		return manager.recoveryFailure(started, err)
	}
	manager.health.ClearCondition(nodehealth.SubsystemRecovery)
	manager.health.ClearCondition(nodehealth.SubsystemStartup)
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

func (manager *Manager) classifyRecoveryFailure(ctx context.Context) {
	manager.health.SetCondition(nodehealth.SubsystemRecovery, nodehealth.PhaseRecovering, "CacheRecovery", false)
	_ = manager.backendHealth(ctx, false)
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
		manager.health.SetCondition(nodehealth.SubsystemPressure, nodehealth.PhaseUnavailable, "CacheFilesystemUnavailable", true)
		return
	}
	manager.syncStoreMetrics()
	if active {
		manager.health.SetCondition(nodehealth.SubsystemPressure, nodehealth.PhaseUnavailable, "CacheFilesystemPressure", false)
		select {
		case requests <- struct{}{}:
		default:
		}
		return
	}
	manager.health.ClearCondition(nodehealth.SubsystemPressure)
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
		manager.health.SetCondition(nodehealth.SubsystemPressure, nodehealth.PhaseUnavailable, "CacheFilesystemUnavailable", true)
		manager.logger.ErrorContext(ctx, "inspect cache filesystem after pressure reclaim failed", "root", manager.store.Root(), "error", err)
		return
	}
	if active {
		manager.health.SetCondition(nodehealth.SubsystemPressure, nodehealth.PhaseUnavailable, "CacheFilesystemPressure", true)
		manager.logger.WarnContext(ctx, "cache filesystem remains under pressure after unused cache reclamation")
	} else {
		manager.health.ClearCondition(nodehealth.SubsystemPressure)
	}
	manager.refreshHealth(ctx)
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
		manager.health.SetCondition(nodehealth.SubsystemDegradedRecovery, nodehealth.PhaseDegraded, "CacheDegradedRecoveryFailed", false)
	} else {
		manager.health.ClearCondition(nodehealth.SubsystemDegradedRecovery)
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
	if collectionErr != nil {
		manager.health.SetCondition(nodehealth.SubsystemGarbageCollector, nodehealth.PhaseDegraded, "CacheCollectionDegraded", false)
	} else {
		manager.health.ClearCondition(nodehealth.SubsystemGarbageCollector)
	}
	manager.refreshHealth(ctx)
	manager.syncStoreMetrics()
}

func (manager *Manager) refreshHealth(ctx context.Context) {
	_ = manager.backendHealth(ctx, false)
	manager.probeReportedCapabilities(ctx)
}

func (manager *Manager) backendHealth(ctx context.Context, probeCapabilities bool) error {
	var healthErrors []error
	initialization := manager.store.InitializationStatus()
	if initialization.State == cache.InitializationInitializing {
		manager.health.ClearCondition(nodehealth.SubsystemStore)
		manager.health.SetCondition(nodehealth.SubsystemRecovery, nodehealth.PhaseRecovering, "CacheRecovery", false)
		return cache.ErrStoreNotReady
	}
	if initialization.State == cache.InitializationRetryableFailed || initialization.State == cache.InitializationNodeWideFailed {
		reason := "CacheStoreInitializationRetrying"
		evict := false
		if initialization.State == cache.InitializationNodeWideFailed {
			reason = "CacheStoreInitializationFailed"
			evict = true
		}
		failure := fmt.Errorf("cache store initialization failed: %w", initialization.Err)
		manager.health.SetCondition(nodehealth.SubsystemStore, nodehealth.PhaseUnavailable, reason, evict)
		return failure
	}
	if !manager.store.Ready() {
		manager.health.ClearCondition(nodehealth.SubsystemStore)
		manager.health.SetCondition(nodehealth.SubsystemRecovery, nodehealth.PhaseRecovering, "CacheRecovery", false)
		return cache.ErrStoreNotReady
	}
	manager.health.ClearCondition(nodehealth.SubsystemStore)
	var filesystemError error
	if manager.filesystemReadOnly != nil {
		readOnly, err := manager.filesystemReadOnly(manager.store.Root())
		if err != nil {
			filesystemError = fmt.Errorf("inspect cache root filesystem: %w", err)
		} else if readOnly {
			filesystemError = errors.New("cache root filesystem is read-only")
		}
	}
	if filesystemError == nil && (probeCapabilities || hasCondition(manager.health, nodehealth.SubsystemFilesystem)) {
		if err := manager.store.CheckFilesystem(); err != nil {
			filesystemError = fmt.Errorf("probe cache filesystem under %s: %w", manager.store.Root(), err)
		}
	}
	if filesystemError != nil {
		manager.health.SetCondition(nodehealth.SubsystemFilesystem, nodehealth.PhaseUnavailable, "CacheFilesystemUnavailable", true)
		healthErrors = append(healthErrors, filesystemError)
	} else {
		manager.health.ClearCondition(nodehealth.SubsystemFilesystem)
	}
	if manager.quotaRequired {
		if manager.quotaProbe == nil {
			failure := errors.New("cache quota is required but no quota capability probe is configured")
			manager.health.SetCondition(nodehealth.SubsystemQuota, nodehealth.PhaseUnavailable, "CacheQuotaProbeUnavailable", true)
			healthErrors = append(healthErrors, failure)
		} else if probeCapabilities || hasCondition(manager.health, nodehealth.SubsystemQuota) {
			probeContext, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := manager.quotaProbe(probeContext, manager.store.Root())
			cancel()
			if err != nil {
				failure := fmt.Errorf("probe cache quota backend: %w", err)
				manager.health.SetCondition(nodehealth.SubsystemQuota, nodehealth.PhaseUnavailable, "CacheQuotaUnavailable", true)
				healthErrors = append(healthErrors, failure)
			} else {
				manager.health.ClearCondition(nodehealth.SubsystemQuota)
			}
		}
		if err := manager.store.ProjectRegistryError(); err != nil {
			failure := fmt.Errorf("cache project quota registry is unavailable: %w", err)
			manager.health.SetCondition(nodehealth.SubsystemProjectRegistry, nodehealth.PhaseUnavailable, "CacheProjectRegistryUnavailable", true)
			healthErrors = append(healthErrors, failure)
		} else {
			manager.health.ClearCondition(nodehealth.SubsystemProjectRegistry)
		}
	} else {
		manager.health.ClearCondition(nodehealth.SubsystemQuota)
		manager.health.ClearCondition(nodehealth.SubsystemProjectRegistry)
	}
	return errors.Join(healthErrors...)
}

func (manager *Manager) probeReportedCapabilities(ctx context.Context) {
	for _, condition := range manager.health.Conditions() {
		if condition.Phase != nodehealth.PhaseUnavailable {
			continue
		}
		switch condition.Subsystem {
		case nodehealth.SubsystemQuota:
			if manager.quotaRequired && manager.quotaProbe != nil {
				probeContext, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := manager.quotaProbe(probeContext, manager.store.Root())
				cancel()
				if err == nil {
					manager.health.ClearCondition(condition.Subsystem)
				}
			}
		case nodehealth.SubsystemMount:
			if manager.capabilityProbe != nil {
				probeContext, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := manager.capabilityProbe(probeContext, condition.Reason)
				cancel()
				if err == nil {
					manager.health.ClearCondition(condition.Subsystem)
				}
			}
		case nodehealth.SubsystemStartup,
			nodehealth.SubsystemRecovery,
			nodehealth.SubsystemStore,
			nodehealth.SubsystemStoreOperations,
			nodehealth.SubsystemFilesystem,
			nodehealth.SubsystemProjectRegistry,
			nodehealth.SubsystemPressure,
			nodehealth.SubsystemGarbageCollector,
			nodehealth.SubsystemDegradedRecovery:
			continue
		}
	}
}

func hasCondition(tracker *nodehealth.Tracker, subsystem nodehealth.Subsystem) bool {
	for _, condition := range tracker.Conditions() {
		if condition.Subsystem == subsystem {
			return true
		}
	}
	return false
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
