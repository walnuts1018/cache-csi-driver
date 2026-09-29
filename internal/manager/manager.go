package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/walnuts1018/cache-csi-driver/internal/cache"
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
	Logger           *slog.Logger
}

type Manager struct {
	stores           []*cache.Store
	interval         time.Duration
	pressureInterval time.Duration
	client           kubernetes.Interface
	inspectMount     MountInspector
	logger           *slog.Logger
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
	if options.FallbackStore != nil && options.FallbackStore != store {
		stores = append(stores, options.FallbackStore)
	}
	return &Manager{
		stores:           stores,
		interval:         options.Interval,
		pressureInterval: options.PressureInterval,
		client:           options.Client,
		inspectMount:     options.InspectMount,
		logger:           options.Logger,
		evictions:        make(map[evictionKey]evictionState),
	}
}

func (m *Manager) Recover(ctx context.Context) error {
	if m.inspectMount == nil {
		return errors.New("mount inspector is not configured")
	}
	for _, store := range m.stores {
		if err := store.WaitForIndexes(ctx); err != nil {
			return fmt.Errorf("wait for cache indexes under %s: %w", store.Root(), err)
		}
	}
	// Recover fallback leases first so the primary store remains unavailable until both stores are ready.
	for index := len(m.stores) - 1; index >= 0; index-- {
		store := m.stores[index]
		if err := store.RecoverLeasesContext(ctx, m.inspectMount); err != nil {
			return fmt.Errorf("recover cache leases under %s: %w", store.Root(), err)
		}
	}
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
	state, found := m.evictionState(key)
	if found && state.shouldSkip(now) && (!victim.ForceDelete || state.forceDeleteAttempted || state.gone) {
		return
	}

	var err error
	if victim.ForceDelete {
		err = m.forceDelete(ctx, lease)
	} else {
		err = m.evict(ctx, lease)
	}
	if apierrors.IsNotFound(err) {
		m.setEvictionState(key, nextEvictionState(now, state, err))
		m.logger.DebugContext(ctx, "cache pressure victim Pod no longer exists", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID)
		return
	}
	state = nextEvictionState(now, state, err)
	if victim.ForceDelete {
		state.forceDeleteAttempted = true
	}
	m.setEvictionState(key, state)
	if err != nil {
		if apierrors.IsTooManyRequests(err) && !victim.ForceDelete {
			m.logger.WarnContext(ctx, "Pod eviction was blocked, possibly by a PodDisruptionBudget", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", state.nextAttempt.Sub(now), "error", err)
			return
		}
		if victim.ForceDelete {
			m.logger.WarnContext(ctx, "Pod force deletion at critical cache pressure failed", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", state.nextAttempt.Sub(now), "error", err)
		} else {
			m.logger.WarnContext(ctx, "Pod eviction for cache pressure failed", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", state.nextAttempt.Sub(now), "error", err)
		}
		return
	}
	if victim.ForceDelete {
		m.logger.WarnContext(ctx, "requested UID-preconditioned Pod force deletion at critical cache pressure", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", acceptedEvictionDelay)
		return
	}
	m.logger.InfoContext(ctx, "requested Pod eviction for cache pressure", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "retryAfter", acceptedEvictionDelay)
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
