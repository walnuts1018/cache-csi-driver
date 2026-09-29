package driver

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func TestIsSingleNodeAccessMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode csi.VolumeCapability_AccessMode_Mode
		want bool
	}{
		{name: "single node writer", mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, want: true},
		{name: "single node reader only", mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY, want: false},
		{name: "single node single writer", mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER, want: false},
		{name: "single node multi writer", mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER, want: false},
		{name: "multi node reader only", mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY, want: false},
		{name: "multi node single writer", mode: csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER, want: false},
		{name: "multi node multi writer", mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, want: false},
		{name: "unknown", mode: csi.VolumeCapability_AccessMode_UNKNOWN, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mode := &csi.VolumeCapability_AccessMode{Mode: tt.mode}
			if got := isSingleNodeAccessMode(mode); got != tt.want {
				t.Fatalf("isSingleNodeAccessMode(%s) = %t, want %t", tt.mode, got, tt.want)
			}
		})
	}
}
