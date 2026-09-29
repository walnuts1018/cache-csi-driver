package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/walnuts1018/cache-csi-driver/internal/kubeletcompat"
)

func (s *Server) validatePublishTarget(target, podUID string) error {
	if !filepath.IsAbs(s.options.KubeletRoot) {
		return errors.New("kubelet root is not configured")
	}
	if podUID == "" {
		return errors.New("pod UID is required")
	}
	targetPodUID, _, ok := kubeletcompat.ParseInlineCSITarget(s.options.KubeletRoot, target)
	if !ok {
		return errors.New("target path must identify an inline CSI volume mount")
	}
	if targetPodUID != podUID {
		return errors.New("target path pod UID does not match pod information")
	}
	return nil
}

func isSingleNodeAccessMode(mode *csi.VolumeCapability_AccessMode) bool {
	if mode == nil {
		return false
	}
	return mode.GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
}

func makeTargetDirectory(path string) error {
	if err := os.Mkdir(path, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("mount target is not a real directory")
	}
	return nil
}

func removeTargetDirectory(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("mount target is not a real directory")
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func fallbackPath(root, volumeID string) string {
	hash := sha256.Sum256([]byte(volumeID))
	return filepath.Join(root, hex.EncodeToString(hash[:]))
}
