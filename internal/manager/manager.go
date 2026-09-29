package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/walnuts1018/cache-csi-driver/internal/cache"
)

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
	}
}

func (m *Manager) Recover() error {
	if m.inspectMount == nil {
		return errors.New("mount inspector is not configured")
	}
	for _, store := range m.stores {
		if err := store.RecoverLeases(m.inspectMount); err != nil {
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
	var victims []cache.Lease
	for _, store := range m.stores {
		storeVictims, err := store.PressureVictims()
		if err != nil {
			m.logger.ErrorContext(ctx, "inspect cache pressure victims failed", "root", store.Root(), "error", err)
			continue
		}
		victims = append(victims, storeVictims...)
	}
	if len(victims) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(victims))
	for _, lease := range victims {
		if lease.Namespace == "" || lease.PodName == "" || lease.PodUID == "" {
			m.logger.WarnContext(ctx, "skipping Pod eviction because lease Pod identity is incomplete", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID)
			continue
		}
		key := lease.Namespace + "\x00" + lease.PodUID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if err := m.evict(ctx, lease); err != nil {
			if apierrors.IsNotFound(err) {
				m.logger.DebugContext(ctx, "cache pressure victim Pod no longer exists", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID)
				continue
			}
			if apierrors.IsTooManyRequests(err) {
				m.logger.WarnContext(ctx, "Pod eviction was blocked, possibly by a PodDisruptionBudget", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "error", err)
				continue
			}
			m.logger.WarnContext(ctx, "Pod eviction for cache pressure failed", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID, "error", err)
			continue
		}
		m.logger.InfoContext(ctx, "requested Pod eviction for cache pressure", "namespace", lease.Namespace, "pod", lease.PodName, "podUID", lease.PodUID)
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
