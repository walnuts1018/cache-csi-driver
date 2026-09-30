package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/metrics"
)

const (
	initialEvictionBackoff = 5 * time.Second
	maximumEvictionBackoff = 5 * time.Minute
	acceptedEvictionDelay  = 30 * time.Second
)

type evictionKey struct {
	namespace string
	podUID    string
}

type evictionState struct {
	nextAttempt          time.Time
	attempts             int
	gone                 bool
	forceDeleteAttempted bool
}

func (s evictionState) shouldSkip(now time.Time) bool {
	return s.gone || now.Before(s.nextAttempt)
}

func evictionRetryDelay(err error, attempts int) time.Duration {
	if retryAfter, ok := apierrors.SuggestsClientDelay(err); ok && retryAfter > 0 {
		return time.Duration(retryAfter) * time.Second
	}
	if attempts < 1 {
		attempts = 1
	}
	delay := initialEvictionBackoff
	for i := 1; i < attempts && delay < maximumEvictionBackoff; i++ {
		if delay > maximumEvictionBackoff/2 {
			return maximumEvictionBackoff
		}
		delay *= 2
	}
	return min(delay, maximumEvictionBackoff)
}

func nextEvictionState(now time.Time, previous evictionState, err error) evictionState {
	if apierrors.IsNotFound(err) {
		return evictionState{gone: true}
	}
	if err == nil {
		return evictionState{nextAttempt: now.Add(acceptedEvictionDelay)}
	}
	attempts := previous.attempts + 1
	return evictionState{
		nextAttempt: now.Add(evictionRetryDelay(err, attempts)),
		attempts:    attempts,
	}
}

type MountInspector func(source string, lease cache.Lease, policy cache.Policy) (bool, error)

type Options struct {
	Interval         time.Duration
	PressureInterval time.Duration
	Client           kubernetes.Interface
	InspectMount     MountInspector
	FallbackStore    *cache.Store
	EmergencyStore   *cache.Store
	AllowPodEviction bool
	AllowForceDelete bool
	Logger           *slog.Logger
	Metrics          *metrics.Metrics
}

type Manager struct {
	stores           []*cache.Store
	interval         time.Duration
	pressureInterval time.Duration
	client           kubernetes.Interface
	inspectMount     MountInspector
	allowForceDelete bool
	logger           *slog.Logger
	metrics          *metrics.Metrics
	storeNames       []string
	evictionMu       sync.Mutex
	evictions        map[evictionKey]evictionState
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
	stores := []*cache.Store{store}
	storeNames := []string{"primary"}
	if options.FallbackStore != nil && options.FallbackStore != store {
		stores = append(stores, options.FallbackStore)
		storeNames = append(storeNames, "fallback")
	}
	if options.EmergencyStore != nil && options.EmergencyStore != store && options.EmergencyStore != options.FallbackStore {
		stores = append(stores, options.EmergencyStore)
		storeNames = append(storeNames, "fallback_emergency")
	}
	client := options.Client
	if !options.AllowPodEviction {
		client = nil
	}
	manager := &Manager{
		stores:           stores,
		storeNames:       storeNames,
		interval:         options.Interval,
		pressureInterval: options.PressureInterval,
		client:           client,
		inspectMount:     options.InspectMount,
		allowForceDelete: options.AllowForceDelete,
		logger:           options.Logger,
		metrics:          options.Metrics,
		evictions:        make(map[evictionKey]evictionState),
	}
	for index := range stores {
		if manager.metrics != nil {
			manager.metrics.SetRecoveryState(storeNames[index], "initializing")
		}
	}
	return manager
}

func (m *Manager) Recover(ctx context.Context) error {
	if m.inspectMount == nil {
		return errors.New("mount inspector is not configured")
	}
	// primary cacheのindex構築を待つ前にfallback leaseを復旧し、長時間scan中もfallback publishを維持する。
	for index, store := range slices.Backward(m.stores) {
		started := time.Now()
		if m.metrics != nil {
			m.metrics.SetRecoveryState(m.storeNames[index], "recovering")
		}
		if err := store.WaitForIndexes(ctx); err != nil {
			m.recordRecovery(index, "failure", time.Since(started).Seconds())
			return fmt.Errorf("wait for cache indexes under %s: %w", store.Root(), err)
		}
		indexDuration := time.Since(started)
		started = time.Now()
		if err := store.RecoverLeasesContext(ctx, m.inspectMount); err != nil {
			m.recordRecovery(index, "failure", (indexDuration + time.Since(started)).Seconds())
			return fmt.Errorf("recover cache leases under %s: %w", store.Root(), err)
		}
		m.recordRecovery(index, "success", (indexDuration + time.Since(started)).Seconds())
	}
	m.syncStoreMetrics()
	return nil
}

func (m *Manager) Run(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	m.collect(ctx, time.Now())
	m.pressure(ctx)
	retentionTicker := time.NewTicker(m.interval)
	defer retentionTicker.Stop()
	pressureTicker := time.NewTicker(m.pressureInterval)
	defer pressureTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-retentionTicker.C:
			m.collect(ctx, now)
		case <-pressureTicker.C:
			m.pressure(ctx)
		}
	}
}

func (m *Manager) collect(ctx context.Context, now time.Time) {
	defer m.syncStoreMetrics()
	for _, store := range m.stores {
		if err := store.Collect(now); err != nil {
			m.logger.ErrorContext(ctx, "cache collection failed", "root", store.Root(), "error", err)
			continue
		}
		if err := store.CleanupTrash(ctx); err != nil {
			m.logger.ErrorContext(ctx, "cache trash cleanup failed", "root", store.Root(), "error", err)
		}
	}
}

func (m *Manager) pressure(ctx context.Context) {
	defer m.syncStoreMetrics()
	if err := ctx.Err(); err != nil {
		return
	}
	for _, store := range m.stores {
		if m.inspectMount != nil {
			if err := store.RecoverDegraded(ctx, m.inspectMount); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				m.logger.WarnContext(ctx, "degraded cache recovery was incomplete", "root", store.Root(), "error", err)
			}
		}
		if err := store.CleanupTrash(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			m.logger.WarnContext(ctx, "cache trash cleanup failed during pressure check", "root", store.Root(), "error", err)
		}
		if err := store.ReclaimPressure(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if errors.Is(err, cache.ErrPressureReclaimIncomplete) {
				m.logger.WarnContext(ctx, "cache pressure reclaim was incomplete", "root", store.Root(), "error", err)
			} else {
				m.logger.ErrorContext(ctx, "cache pressure reclaim failed", "root", store.Root(), "error", err)
			}
		}
	}
	if m.client == nil {
		return
	}
	var victims []cache.PressureVictim
	victimsComplete := true
	for _, store := range m.stores {
		storeVictims, err := store.PressureVictims()
		if err != nil {
			m.logger.ErrorContext(ctx, "inspect cache pressure victims failed", "root", store.Root(), "error", err)
			victimsComplete = false
			continue
		}
		victims = append(victims, storeVictims...)
	}
	seen := make(map[evictionKey]int, len(victims))
	active := make(map[evictionKey]struct{}, len(victims))
	uniqueVictims := make([]cache.PressureVictim, 0, len(victims))
	for _, victim := range victims {
		lease := victim.Lease
		if lease.Namespace == "" || lease.PodName == "" || lease.PodUID == "" {
			m.logger.WarnContext(ctx, "skipping Pod eviction because lease Pod identity is incomplete", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID)
			continue
		}
		key := evictionKey{namespace: lease.Namespace, podUID: lease.PodUID}
		active[key] = struct{}{}
		if index, ok := seen[key]; ok {
			uniqueVictims[index].ForceDelete = uniqueVictims[index].ForceDelete || victim.ForceDelete
			continue
		}
		seen[key] = len(uniqueVictims)
		uniqueVictims = append(uniqueVictims, victim)
	}
	m.pruneEvictions(active, victimsComplete)
	for _, victim := range uniqueVictims {
		m.evictWithBackoff(ctx, victim)
	}
}

func (m *Manager) evictWithBackoff(ctx context.Context, victim cache.PressureVictim) {
	if ctx.Err() != nil {
		return
	}
	lease := victim.Lease
	key := evictionKey{namespace: lease.Namespace, podUID: lease.PodUID}
	now := time.Now()
	forceDelete := victim.ForceDelete && m.allowForceDelete
	state, found := m.evictionState(key)
	if found && state.shouldSkip(now) && (!forceDelete || state.forceDeleteAttempted || state.gone) {
		return
	}

	var err error
	action := "eviction"
	if forceDelete {
		action = "force_delete"
		err = m.forceDelete(ctx, lease)
	} else {
		if victim.ForceDelete {
			m.logger.WarnContext(ctx, "critical cache pressure force deletion is disabled; falling back to Pod eviction that respects PodDisruptionBudgets", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID)
		}
		err = m.evict(ctx, lease)
	}
	result := "accepted"
	switch {
	case apierrors.IsNotFound(err):
		result = "not_found"
	case apierrors.IsTooManyRequests(err):
		result = "blocked"
	case err != nil:
		result = "failed"
	}
	if m.metrics != nil {
		m.metrics.RecordPressureAction(action, result)
	}
	if apierrors.IsNotFound(err) {
		m.setEvictionState(key, nextEvictionState(now, state, err))
		m.logger.DebugContext(ctx, "cache pressure victim Pod no longer exists", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID)
		return
	}
	state = nextEvictionState(now, state, err)
	if forceDelete {
		state.forceDeleteAttempted = true
	}
	m.setEvictionState(key, state)
	if err != nil {
		if apierrors.IsTooManyRequests(err) && !forceDelete {
			m.logger.WarnContext(ctx, "Pod eviction was blocked, possibly by a PodDisruptionBudget", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", state.nextAttempt.Sub(now), "error", err)
			return
		}
		if forceDelete {
			m.logger.WarnContext(ctx, "Pod force deletion at critical cache pressure failed", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", state.nextAttempt.Sub(now), "error", err)
		} else {
			m.logger.WarnContext(ctx, "Pod eviction for cache pressure failed", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", state.nextAttempt.Sub(now), "error", err)
		}
		return
	}
	if forceDelete {
		m.logger.WarnContext(ctx, "requested UID-preconditioned Pod force deletion at critical cache pressure", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", acceptedEvictionDelay)
		return
	}
	m.logger.InfoContext(ctx, "requested Pod eviction for cache pressure", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", acceptedEvictionDelay)
}

func (m *Manager) recordRecovery(index int, result string, durationSeconds float64) {
	if m.metrics != nil {
		m.metrics.RecordRecoveryAttempt(m.storeNames[index], result, durationSeconds)
		if result == "success" {
			m.metrics.SetRecoveryState(m.storeNames[index], "ready")
		} else {
			m.metrics.SetRecoveryState(m.storeNames[index], "failed")
		}
	}
}

func (m *Manager) syncStoreMetrics() {
	if m.metrics == nil {
		return
	}
	for index, store := range m.stores {
		stats := store.RuntimeStats()
		m.metrics.SetStoreSnapshot(m.storeNames[index], metrics.StoreSnapshot{
			Ready:                 stats.Ready,
			PressureState:         stats.PressureState,
			DegradedObjects:       stats.DegradedObjects,
			CacheObjects:          stats.CacheObjects,
			RetiredGenerations:    stats.RetiredGenerations,
			FallbackReservedBytes: stats.FallbackReservedBytes,
			TrashObjectsDeleted:   stats.TrashObjectsDeleted,
		})
	}
}

func (m *Manager) evictionState(key evictionKey) (evictionState, bool) {
	m.evictionMu.Lock()
	defer m.evictionMu.Unlock()
	state, ok := m.evictions[key]
	return state, ok
}

func (m *Manager) setEvictionState(key evictionKey, state evictionState) {
	m.evictionMu.Lock()
	defer m.evictionMu.Unlock()
	if m.evictions == nil {
		m.evictions = make(map[evictionKey]evictionState)
	}
	m.evictions[key] = state
}

func (m *Manager) pruneEvictions(active map[evictionKey]struct{}, complete bool) {
	if !complete {
		return
	}
	m.evictionMu.Lock()
	defer m.evictionMu.Unlock()
	for key := range m.evictions {
		if _, ok := active[key]; !ok {
			delete(m.evictions, key)
		}
	}
}

func (m *Manager) evict(ctx context.Context, lease cache.Lease) error {
	uid := types.UID(lease.PodUID)
	eviction := &policyv1.Eviction{
		TypeMeta: metav1.TypeMeta{APIVersion: "policy/v1", Kind: "Eviction"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      lease.PodName,
			Namespace: lease.Namespace,
		},
		DeleteOptions: &metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid},
		},
	}
	return m.client.PolicyV1().Evictions(lease.Namespace).Evict(ctx, eviction)
}

func (m *Manager) forceDelete(ctx context.Context, lease cache.Lease) error {
	uid := types.UID(lease.PodUID)
	gracePeriodSeconds := int64(0)
	return m.client.CoreV1().Pods(lease.Namespace).Delete(ctx, lease.PodName, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriodSeconds,
		Preconditions:      &metav1.Preconditions{UID: &uid},
	})
}
