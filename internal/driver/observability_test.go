package driver

import (
	"errors"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestQuotaBackendFailureMakesCacheNodeUnavailable(t *testing.T) {
	t.Parallel()

	spec := cachev1alpha1.CacheClassSpec{
		Storage: cachev1alpha1.StoragePolicy{
			Backend:         cachev1alpha1.BackendXFSProject,
			DefaultMaxBytes: resource.MustParse("1Mi"),
		},
	}
	server, mounts, store := newTestServer(t, spec, errors.New("quota configuration failed"))
	request := newPublishRequest(t, server.options.KubeletRoot, "quota-failure")

	if _, err := server.NodePublishVolume(t.Context(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("publish after quota configuration failure error = %v, want Unavailable", err)
	}
	if mounts.mountCalls != 0 {
		t.Fatalf("mount calls = %d, want no mount after quota setup failure", mounts.mountCalls)
	}
	if _, _, _, _, found, err := store.LeaseDetails(request.GetVolumeId()); err != nil || found {
		t.Fatalf("cache lease found = %t, error = %v; want the failed lease rolled back", found, err)
	}
	health, err := server.NodeGetStorageHealth(t.Context(), &csi.NodeGetStorageHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	backends := health.GetBackendHealth()
	if len(backends) != 1 || backends[0].GetStatus() != csi.StorageHealthErrorType_STORAGE_UNREACHABLE || backends[0].GetReason() != "CacheQuotaUnavailable" {
		t.Fatalf("storage health = %+v, want unavailable quota backend", backends)
	}
}
