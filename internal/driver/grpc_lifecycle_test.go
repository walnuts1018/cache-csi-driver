package driver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestNodeGRPCLifecyclePublishesIdempotentlyAndReleasesLease(t *testing.T) {
	t.Parallel()

	server, mounts, store := newTestServer(t, cachev1alpha1.CacheClassSpec{}, nil)
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	csi.RegisterNodeServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close gRPC client connection: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client := csi.NewNodeClient(connection)
	request := newPublishRequest(t, server.options.KubeletRoot, "grpc-lifecycle-volume")

	if _, err := client.NodePublishVolume(ctx, request); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	if mounts.mountCalls != 1 {
		t.Fatalf("mount calls after first publish = %d, want 1", mounts.mountCalls)
	}
	if _, _, _, _, found, err := store.LeaseDetails(request.GetVolumeId()); err != nil || !found {
		t.Fatalf("lease after publish found = %t, error = %v; want an active lease", found, err)
	}

	if _, err := client.NodePublishVolume(ctx, request); err != nil {
		t.Fatalf("idempotent NodePublishVolume retry: %v", err)
	}
	if mounts.mountCalls != 1 {
		t.Fatalf("mount calls after idempotent retry = %d, want 1", mounts.mountCalls)
	}

	if _, err := client.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   request.GetVolumeId(),
		TargetPath: request.GetTargetPath(),
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if mounts.unmountCalls != 1 {
		t.Fatalf("unmount calls = %d, want 1", mounts.unmountCalls)
	}
	if _, mounted := mounts.mounts[request.GetTargetPath()]; mounted {
		t.Fatal("target remains mounted after NodeUnpublishVolume")
	}
	if _, _, _, _, found, err := store.LeaseDetails(request.GetVolumeId()); err != nil || found {
		t.Fatalf("lease after unpublish found = %t, error = %v; want no lease", found, err)
	}
}
