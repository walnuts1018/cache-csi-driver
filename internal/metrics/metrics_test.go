package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPublishMetricsUseBoundedLabels(t *testing.T) {
	t.Parallel()

	m := New()
	m.RecordPublish("hit")
	m.RecordPublish("secret-volume-id")

	if got := testutil.ToFloat64(m.publish.WithLabelValues("hit")); got != 1 {
		t.Fatalf("hit count = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.publish.WithLabelValues("other")); got != 1 {
		t.Fatalf("unrecognized publish result count = %v, want 1", got)
	}
	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(recorder.Body.String(), "secret-volume-id") {
		t.Fatalf("metrics exposed unbounded result label: %s", recorder.Body.String())
	}
}

func TestStoreSnapshotExportsStateAndTrashDelta(t *testing.T) {
	t.Parallel()

	m := New()
	snapshot := StoreSnapshot{
		Ready:               true,
		PressureState:       "reclaiming",
		DegradedObjects:     2,
		CacheObjects:        7,
		RetiredGenerations:  3,
		TrashObjectsDeleted: 3,
	}
	m.SetStoreSnapshot("primary", snapshot)
	m.SetStoreSnapshot("primary", snapshot)

	if got := testutil.ToFloat64(m.pressureState.WithLabelValues("primary", "reclaiming")); got != 1 {
		t.Fatalf("pressure gauge = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.degradedObjects.WithLabelValues("primary")); got != 2 {
		t.Fatalf("degraded object gauge = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.cacheObjects.WithLabelValues("primary")); got != 7 {
		t.Fatalf("cache object gauge = %v, want 7", got)
	}
	if got := testutil.ToFloat64(m.retiredGenerations.WithLabelValues("primary")); got != 3 {
		t.Fatalf("retired generation gauge = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.trashObjectsDeleted.WithLabelValues("primary")); got != 3 {
		t.Fatalf("trash delete count = %v, want 3", got)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(recorder, request)
	if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), "cache_csi_store_ready") {
		t.Fatalf("metrics response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}
