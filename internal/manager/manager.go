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
	Logger           *slog.Logger
}

type Manager struct {
	store            *cache.Store
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
	return &Manager{
		store:            store,
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
	if err := m.store.RecoverLeases(m.inspectMount); err != nil {
		return fmt.Errorf("recover cache leases: %w", err)
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
	if err := m.store.Collect(now); err != nil {
		m.logger.ErrorContext(ctx, "cache collection failed", "error", err)
		return
	}
	if err := m.store.CleanupTrash(ctx); err != nil {
		m.logger.ErrorContext(ctx, "cache trash cleanup failed", "error", err)
		return
	}
}

func (m *Manager) pressure(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}
	if err := m.store.ReclaimPressure(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		m.logger.ErrorContext(ctx, "cache pressure reclaim failed", "error", err)
		return
	}
	if m.client == nil {
		return
	}
	victims, err := m.store.PressureVictims()
	if err != nil {
		m.logger.ErrorContext(ctx, "inspect cache pressure victims failed", "error", err)
		return
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
