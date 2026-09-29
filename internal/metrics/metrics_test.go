package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPublishAndFallbackMetricsUseBoundedLabels(t *testing.T) {
	t.Parallel()

	m := New()
	m.RecordPublish("hit")
	m.RecordPublish("secret-volume-id")
	m.RecordFallback("unexpected", "quota_setup_failed")
	m.RecordFallback("unexpected", "pod-uid")

	if got := testutil.ToFloat64(m.publish.WithLabelValues("hit")); got != 1 {
		t.Fatalf("hit count = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.publish.WithLabelValues("other")); got != 1 {
		t.Fatalf("unrecognized publish result count = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.fallback.WithLabelValues("unexpected", "quota_setup_failed")); got != 1 {
		t.Fatalf("quota fallback count = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.fallback.WithLabelValues("unexpected", "other")); got != 1 {
		t.Fatalf("unrecognized fallback reason count = %v, want 1", got)
	}
}

func TestStoreSnapshotExportsStateAndTrashDelta(t *testing.T) {
	t.Parallel()

	m := New()
	snapshot := StoreSnapshot{
		Ready:                 true,
		PressureState:         "critical",
		DegradedObjects:       2,
		CacheObjects:          7,
		RetiredGenerations:    3,
		FallbackReservedBytes: 4096,
		TrashObjectsDeleted:   3,
	}
	m.SetStoreSnapshot("fallback", snapshot)
	m.SetStoreSnapshot("fallback", snapshot)

	if got := testutil.ToFloat64(m.pressureState.WithLabelValues("fallback", "critical")); got != 1 {
		t.Fatalf("critical pressure gauge = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.degradedObjects.WithLabelValues("fallback")); got != 2 {
		t.Fatalf("degraded object gauge = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.cacheObjects.WithLabelValues("fallback")); got != 7 {
		t.Fatalf("cache object gauge = %v, want 7", got)
	}
	if got := testutil.ToFloat64(m.retiredGenerations.WithLabelValues("fallback")); got != 3 {
		t.Fatalf("retired generation gauge = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.fallbackReservedBytes.WithLabelValues("fallback")); got != 4096 {
		t.Fatalf("fallback reservation gauge = %v, want 4096", got)
	}
	if got := testutil.ToFloat64(m.trashObjectsDeleted.WithLabelValues("fallback")); got != 3 {
		t.Fatalf("trash delete count = %v, want 3", got)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(recorder, request)
	if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), "cache_csi_backend_degraded") {
		t.Fatalf("metrics response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}
