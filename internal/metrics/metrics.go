package metrics

import (
	"net/http"
	"slices"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type StoreSnapshot struct {
	Ready                 bool
	PressureState         string
	DegradedObjects       int
	CacheObjects          int
	RetiredGenerations    int
	FallbackReservedBytes int64
	TrashObjectsDeleted   uint64
}

const (
	labelResult = "result"
	labelStore  = "store"
)

type Metrics struct {
	registry                *prometheus.Registry
	publish                 *prometheus.CounterVec
	fallback                *prometheus.CounterVec
	backendFallbackFailures *prometheus.CounterVec
	backendDegraded         prometheus.Gauge
	pressureState           *prometheus.GaugeVec
	degradedObjects         *prometheus.GaugeVec
	cacheObjects            *prometheus.GaugeVec
	retiredGenerations      *prometheus.GaugeVec
	storeReady              *prometheus.GaugeVec
	recoveryState           *prometheus.GaugeVec
	recoveryAttempts        *prometheus.CounterVec
	recoveryDuration        *prometheus.HistogramVec
	trashObjectsDeleted     *prometheus.CounterVec
	pressureActions         *prometheus.CounterVec
	fallbackReservedBytes   *prometheus.GaugeVec
	mu                      sync.Mutex
	observedTrashObjects    map[string]uint64
}

func New() *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry,
		publish: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_csi_publish_total",
			Help: "Successful NodePublishVolume calls by outcome.",
		}, []string{labelResult}),
		fallback: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_csi_fallback_total",
			Help: "Successful fallback publishes by cause classification.",
		}, []string{"class", "reason"}),
		backendFallbackFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_csi_backend_fallback_failures_total",
			Help: "Unexpected primary cache backend failures that triggered fallback.",
		}, []string{"reason"}),
		backendDegraded: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cache_csi_backend_degraded",
			Help: "Whether an unexpected primary cache backend failure has been observed since this process started.",
		}),
		pressureState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_pressure_state",
			Help: "Current cache filesystem pressure state, represented as a one-hot gauge.",
		}, []string{labelStore, "state"}),
		degradedObjects: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_store_degraded_objects",
			Help: "Number of cache objects with degraded metadata in the store index.",
		}, []string{labelStore}),
		cacheObjects: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_store_objects",
			Help: "Number of cache objects in the in-memory store index.",
		}, []string{labelStore}),
		retiredGenerations: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_retired_generations",
			Help: "Number of retired cache generations in the in-memory store index.",
		}, []string{labelStore}),
		storeReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_store_ready",
			Help: "Whether cache store indexes and lease recovery are ready.",
		}, []string{labelStore}),
		recoveryState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_store_recovery_state",
			Help: "Current cache store recovery state, represented as a one-hot gauge.",
		}, []string{labelStore, "state"}),
		recoveryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_csi_store_recovery_attempts_total",
			Help: "Cache store recovery attempts by result.",
		}, []string{labelStore, labelResult}),
		recoveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cache_csi_store_recovery_duration_seconds",
			Help:    "Duration of cache store recovery attempts.",
			Buckets: prometheus.DefBuckets,
		}, []string{labelStore}),
		trashObjectsDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_csi_trash_objects_deleted_total",
			Help: "Cache objects physically removed from trash.",
		}, []string{labelStore}),
		pressureActions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_csi_pod_pressure_actions_total",
			Help: "Pod eviction and force-delete API outcomes caused by cache pressure.",
		}, []string{"action", "result"}),
		fallbackReservedBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_fallback_reserved_bytes",
			Help: "Bytes reserved by active fallback cache volumes.",
		}, []string{labelStore}),
		observedTrashObjects: make(map[string]uint64),
	}
	registry.MustRegister(
		m.publish,
		m.fallback,
		m.backendFallbackFailures,
		m.backendDegraded,
		m.pressureState,
		m.degradedObjects,
		m.cacheObjects,
		m.retiredGenerations,
		m.storeReady,
		m.recoveryState,
		m.recoveryAttempts,
		m.recoveryDuration,
		m.trashObjectsDeleted,
		m.pressureActions,
		m.fallbackReservedBytes,
	)
	for _, store := range []string{"primary", "fallback"} {
		m.SetRecoveryState(store, "initializing")
		m.SetStoreSnapshot(store, StoreSnapshot{PressureState: "normal"})
	}
	return m
}

func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	return mux
}

func (m *Metrics) RecordPublish(result string) {
	m.publish.WithLabelValues(oneOf(result, "hit", "miss", "fallback")).Inc()
}

func (m *Metrics) RecordFallback(class, reason string) {
	class = oneOf(class, "expected", "unexpected")
	reason = fallbackReason(reason)
	m.fallback.WithLabelValues(class, reason).Inc()
}

func (m *Metrics) RecordBackendFailure(reason string) {
	m.backendDegraded.Set(1)
	m.backendFallbackFailures.WithLabelValues(fallbackReason(reason)).Inc()
}

func (m *Metrics) SetStoreSnapshot(store string, snapshot StoreSnapshot) {
	store = storeName(store)
	m.storeReady.WithLabelValues(store).Set(boolValue(snapshot.Ready))
	m.degradedObjects.WithLabelValues(store).Set(float64(max(snapshot.DegradedObjects, 0)))
	m.cacheObjects.WithLabelValues(store).Set(float64(max(snapshot.CacheObjects, 0)))
	m.retiredGenerations.WithLabelValues(store).Set(float64(max(snapshot.RetiredGenerations, 0)))
	m.fallbackReservedBytes.WithLabelValues(store).Set(float64(max(snapshot.FallbackReservedBytes, 0)))
	state := oneOf(snapshot.PressureState, "normal", "reclaiming", "critical")
	for _, candidate := range []string{"normal", "reclaiming", "critical"} {
		value := 0.0
		if state == candidate {
			value = 1
		}
		m.pressureState.WithLabelValues(store, candidate).Set(value)
	}
	m.observeTrashDeleted(store, snapshot.TrashObjectsDeleted)
}

func (m *Metrics) SetRecoveryState(store, state string) {
	store = storeName(store)
	state = oneOf(state, "initializing", "recovering", "ready", "failed")
	for _, candidate := range []string{"initializing", "recovering", "ready", "failed"} {
		value := 0.0
		if state == candidate {
			value = 1
		}
		m.recoveryState.WithLabelValues(store, candidate).Set(value)
	}
}

func (m *Metrics) RecordRecoveryAttempt(store, result string, durationSeconds float64) {
	store = storeName(store)
	result = oneOf(result, "success", "failure")
	m.recoveryAttempts.WithLabelValues(store, result).Inc()
	m.recoveryDuration.WithLabelValues(store).Observe(max(durationSeconds, 0))
}

func (m *Metrics) RecordPressureAction(action, result string) {
	action = oneOf(action, "eviction", "force_delete")
	result = oneOf(result, "accepted", "blocked", "not_found", "failed")
	m.pressureActions.WithLabelValues(action, result).Inc()
}

func (m *Metrics) observeTrashDeleted(store string, current uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	previous := m.observedTrashObjects[store]
	if current > previous {
		m.trashObjectsDeleted.WithLabelValues(store).Add(float64(current - previous))
	}
	m.observedTrashObjects[store] = current
}

func storeName(value string) string {
	return oneOf(value, "primary", "fallback")
}

func fallbackReason(value string) string {
	return oneOf(value,
		"api_unavailable",
		"resolver_not_synced",
		"store_recovering",
		"metadata_degraded",
		"exclusive_conflict",
		"quota_conflict",
		"generation_retired",
		"pressure_active",
		"cache_acquire_failed",
		"quota_setup_failed",
		"generation_expose_failed",
		"other_backend_failure",
	)
}

func oneOf(value string, choices ...string) string {
	if slices.Contains(choices, value) {
		return value
	}
	return "other"
}

func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
