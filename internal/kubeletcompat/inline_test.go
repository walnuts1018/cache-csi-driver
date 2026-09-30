package kubeletcompat

import (
	"path/filepath"
	"testing"
)

func TestParsePodTargetAcceptsKubeletPodLayouts(t *testing.T) {
	t.Parallel()

	const kubeletRoot = "/var/lib/kubelet"
	targets := []struct {
		name   string
		target string
		podUID string
	}{
		{
			name:   "Kubernetes 1.37 inline CSI layout",
			target: filepath.Join(kubeletRoot, "pods", "550e8400-e29b-41d4-a716-446655440000", "volumes", "kubernetes.io~csi", "model-cache", "mount"),
			podUID: "550e8400-e29b-41d4-a716-446655440000",
		},
	}
	for _, test := range targets {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gotPodUID, ok := ParsePodTarget(kubeletRoot, test.target)
			if !ok {
				t.Fatalf("Pod target %q was rejected", test.target)
			}
			if gotPodUID != test.podUID {
				t.Fatalf("parsed Pod UID = %q, want %q", gotPodUID, test.podUID)
			}
		})
	}

	invalidTargets := []string{
		filepath.Join(kubeletRoot, "pods"),
		filepath.Join(kubeletRoot, "pods", "pod-uid"),
		filepath.Join(kubeletRoot, "pods", "pod-uid", "mount"),
		filepath.Join(kubeletRoot, "pods", "pod-uid", "plugins", "csi", "mount"),
		filepath.Join(kubeletRoot, "pods", "pod-uid", "volumes", "other-driver", "volume", "mount"),
		filepath.Join(kubeletRoot, "pods", "pod-uid", "volumes", "kubernetes.io~csi", "volume"),
		filepath.Join(kubeletRoot, "pods", "pod-uid", "..", "other", "mount"),
		filepath.Join(kubeletRoot, "pods", "other", "mount"),
		filepath.Join(kubeletRoot, "other", "pod-uid", "mount"),
	}
	for _, target := range invalidTargets {
		if _, ok := ParsePodTarget(kubeletRoot, target); ok {
			t.Errorf("invalid Pod target %q was accepted", target)
		}
	}
}
