package driver

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestFallbackFailureClassificationSeparatesPolicyAndBackendErrors(t *testing.T) {
	t.Parallel()

	policyConflict, ok := fallbackCauseForError(cache.ErrExclusivePolicyConflict, "cache_acquire_failed")
	if !ok || policyConflict.class != "expected" || policyConflict.reason != "exclusive_conflict" {
		t.Fatalf("policy conflict fallback cause = (%+v, %t)", policyConflict, ok)
	}

	backendFailure, ok := fallbackCauseForError(errors.New("metadata write failed"), "cache_acquire_failed")
	if !ok || backendFailure.class != "unexpected" || backendFailure.reason != "cache_acquire_failed" {
		t.Fatalf("backend fallback cause = (%+v, %t)", backendFailure, ok)
	}

	configurationError := status.Error(codes.InvalidArgument, "invalid cache policy")
	if _, ok := fallbackCauseForError(configurationError, "cache_acquire_failed"); ok {
		t.Fatal("invalid policy should not be converted to fallback")
	}
}

func TestQuotaBackendFailureUsesFallbackAndDegradesStorageHealth(t *testing.T) {
	t.Parallel()

	spec := cachev1alpha1.CacheClassSpec{
		Backend: cachev1alpha1.BackendXFSProject,
		Quota: cachev1alpha1.QuotaPolicy{
			Enabled:         true,
			DefaultMaxBytes: resource.MustParse("1Mi"),
		},
	}
	server, _, _ := newTestServer(t, spec, errors.New("quota configuration failed"))
	observer := metrics.New()
	server.metrics = observer
	request := newPublishRequest(t, server.options.KubeletRoot, "quota-failure-metrics")

	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatalf("publish with fallback after quota failure: %v", err)
	}
	metricsResponse := httptest.NewRecorder()
	observer.Handler().ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, metric := range []string{
		`cache_csi_publish_total{result="fallback"} 1`,
		`cache_csi_fallback_total{class="unexpected",reason="quota_setup_failed"} 1`,
		`cache_csi_backend_fallback_failures_total{reason="quota_setup_failed"} 1`,
	} {
		if !strings.Contains(metricsResponse.Body.String(), metric) {
			t.Errorf("metrics output does not contain %q:\n%s", metric, metricsResponse.Body.String())
		}
	}

	health, err := server.NodeGetStorageHealth(t.Context(), &csi.NodeGetStorageHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	backends := health.GetBackendHealth()
	if len(backends) != 1 || backends[0].GetStatus() != csi.StorageHealthErrorType_STORAGE_DEGRADED || backends[0].GetReason() != "CacheBackendOperationFailed" {
		t.Fatalf("storage health = %+v, want degraded primary backend", backends)
	}

	server.quota = &testQuota{}
	request = newPublishRequest(t, server.options.KubeletRoot, "quota-recovery-success")
	if _, err := server.NodePublishVolume(t.Context(), request); err != nil {
		t.Fatalf("publish after a separate quota operation recovers: %v", err)
	}
	health, err = server.NodeGetStorageHealth(t.Context(), &csi.NodeGetStorageHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	backends = health.GetBackendHealth()
	if len(backends) != 1 || backends[0].GetStatus() != csi.StorageHealthErrorType_STORAGE_DEGRADED || backends[0].GetReason() != "CacheBackendOperationFailed" {
		t.Fatalf("storage health after an unrelated successful publish = %+v, want sticky degradation", backends)
	}
}
