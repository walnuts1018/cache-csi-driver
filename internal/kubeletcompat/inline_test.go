package kubeletcompat

import (
	"path/filepath"
	"testing"
)

func TestParseInlineCSITargetKubernetes137Fixture(t *testing.T) {
	t.Parallel()

	const kubeletRoot = "/var/lib/kubelet"
	// Kubelet v1.37.1のcsi_mounter.goがinline CSI volumeに割り当てるtarget path形式を固定する。
	const kubernetes137InlineCSITarget = "/var/lib/kubelet/pods/550e8400-e29b-41d4-a716-446655440000/volumes/kubernetes.io~csi/model-cache/mount"

	gotPodUID, gotVolumeName, ok := ParseInlineCSITarget(kubeletRoot, kubernetes137InlineCSITarget)
	if !ok {
		t.Fatal("Kubernetes 1.37 inline CSI target fixture was rejected")
	}
	if gotPodUID != "550e8400-e29b-41d4-a716-446655440000" || gotVolumeName != "model-cache" {
		t.Fatalf("parsed target = (%q, %q), want Kubernetes 1.37 path components", gotPodUID, gotVolumeName)
	}

	invalidTargets := []string{
		filepath.Join(kubeletRoot, "pods", "pod-uid", "volumes", "kubernetes.io~csi", "model-cache"),
		filepath.Join(kubeletRoot, "pods", "pod-uid", "volumes", "kubernetes.io~secret", "model-cache", "mount"),
		filepath.Join(kubeletRoot, "pods", "pod-uid", "volumes", "kubernetes.io~csi", "..", "mount"),
	}
	for _, target := range invalidTargets {
		if _, _, ok := ParseInlineCSITarget(kubeletRoot, target); ok {
			t.Errorf("non-inline or malformed target %q was accepted", target)
		}
	}
}
