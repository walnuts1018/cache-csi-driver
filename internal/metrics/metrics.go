package metrics

import (
	"net/http"
	"slices"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const storeLabelName = "store"

type StoreSnapshot struct {
	Ready               bool
	PressureState       string
	DegradedObjects     int
	CacheObjects        int
	RetiredGenerations  int
	TrashObjectsDeleted uint64
}

type Metrics struct {
	registry            *prometheus.Registry
	publish             *prometheus.CounterVec
	pressureState       *prometheus.GaugeVec
	degradedObjects     *prometheus.GaugeVec
	cacheObjects        *prometheus.GaugeVec
	retiredGenerations  *prometheus.GaugeVec
	storeReady          *prometheus.GaugeVec
	recoveryState       *prometheus.GaugeVec
	recoveryAttempts    *prometheus.CounterVec
	recoveryDuration    *prometheus.HistogramVec
	trashObjectsDeleted *prometheus.CounterVec
	mu                  sync.Mutex
	observedTrash       map[string]uint64
}

func New() *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		publish: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_csi_publish_total",
			Help: "Successful NodePublishVolume calls by cache outcome.",
		}, []string{"result"}),
		pressureState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_pressure_state",
			Help: "Current cache filesystem pressure state, represented as a one-hot gauge.",
		}, []string{storeLabelName, "state"}),
		degradedObjects: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_store_degraded_objects",
			Help: "Number of cache objects with degraded metadata in the store index.",
		}, []string{storeLabelName}),
		cacheObjects: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_store_objects",
			Help: "Number of cache objects in the in-memory store index.",
		}, []string{storeLabelName}),
		retiredGenerations: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_retired_generations",
			Help: "Number of retired cache generations in the in-memory store index.",
		}, []string{storeLabelName}),
		storeReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_store_ready",
			Help: "Whether cache store indexes and lease recovery are ready.",
		}, []string{storeLabelName}),
		recoveryState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_csi_store_recovery_state",
			Help: "Current cache store recovery state, represented as a one-hot gauge.",
		}, []string{storeLabelName, "state"}),
		recoveryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_csi_store_recovery_attempts_total",
			Help: "Cache store recovery attempts by result.",
		}, []string{storeLabelName, "result"}),
		recoveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cache_csi_store_recovery_duration_seconds",
			Help:    "Duration of cache store recovery attempts.",
			Buckets: prometheus.DefBuckets,
		}, []string{storeLabelName}),
		trashObjectsDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_csi_trash_objects_deleted_total",
			Help: "Cache objects physically removed from trash.",
		}, []string{storeLabelName}),
		observedTrash: make(map[string]uint64),
	}
	registry.MustRegister(
		metrics.publish,
		metrics.pressureState,
		metrics.degradedObjects,
		metrics.cacheObjects,
		metrics.retiredGenerations,
		metrics.storeReady,
		metrics.recoveryState,
		metrics.recoveryAttempts,
		metrics.recoveryDuration,
		metrics.trashObjectsDeleted,
	)
	metrics.SetRecoveryState("primary", "initializing")
	metrics.SetStoreSnapshot("primary", StoreSnapshot{PressureState: "normal"})
	return metrics
}

func (metrics *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{}))
	return mux
}

func (metrics *Metrics) RecordPublish(result string) {
	metrics.publish.WithLabelValues(oneOf(result, "hit", "miss", "error")).Inc()
}

func (metrics *Metrics) SetStoreSnapshot(store string, snapshot StoreSnapshot) {
	store = storeName(store)
	metrics.storeReady.WithLabelValues(store).Set(boolValue(snapshot.Ready))
	metrics.degradedObjects.WithLabelValues(store).Set(float64(max(snapshot.DegradedObjects, 0)))
	metrics.cacheObjects.WithLabelValues(store).Set(float64(max(snapshot.CacheObjects, 0)))
	metrics.retiredGenerations.WithLabelValues(store).Set(float64(max(snapshot.RetiredGenerations, 0)))
	state := oneOf(snapshot.PressureState, "normal", "reclaiming")
	for _, candidate := range []string{"normal", "reclaiming"} {
		value := 0.0
		if state == candidate {
			value = 1
		}
		metrics.pressureState.WithLabelValues(store, candidate).Set(value)
	}
	metrics.observeTrashDeleted(store, snapshot.TrashObjectsDeleted)
}

func (metrics *Metrics) SetRecoveryState(store, state string) {
	store = storeName(store)
	state = oneOf(state, "initializing", "recovering", "ready", "failed")
	for _, candidate := range []string{"initializing", "recovering", "ready", "failed"} {
		value := 0.0
		if state == candidate {
			value = 1
		}
		metrics.recoveryState.WithLabelValues(store, candidate).Set(value)
	}
}

func (metrics *Metrics) RecordRecoveryAttempt(store, result string, durationSeconds float64) {
	store = storeName(store)
	result = oneOf(result, "success", "failure")
	metrics.recoveryAttempts.WithLabelValues(store, result).Inc()
	metrics.recoveryDuration.WithLabelValues(store).Observe(max(durationSeconds, 0))
}

func (metrics *Metrics) observeTrashDeleted(store string, current uint64) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	previous := metrics.observedTrash[store]
	if current > previous {
		metrics.trashObjectsDeleted.WithLabelValues(store).Add(float64(current - previous))
	}
	metrics.observedTrash[store] = current
}

func storeName(value string) string {
	return oneOf(value, "primary")
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
