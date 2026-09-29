package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func (s *Server) validateTarget(target string) error {
	if !filepath.IsAbs(s.options.KubeletRoot) {
		return errors.New("kubelet root is not configured")
	}
	podsRoot := filepath.Join(filepath.Clean(s.options.KubeletRoot), "pods")
	relative, err := filepath.Rel(podsRoot, target)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("target path must be inside the kubelet pods directory")
	}
	return nil
}

func (s *Server) validatePublishTarget(target, podUID string) error {
	if err := s.validateTarget(target); err != nil {
		return err
	}
	if podUID == "" {
		return errors.New("pod UID is required")
	}
	targetPodUID, _, ok := inlineVolumeTargetParts(s.options.KubeletRoot, target)
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
	switch mode.GetMode() {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
		return true
	case csi.VolumeCapability_AccessMode_UNKNOWN,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		return false
	default:
		return false
	}
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

func matchesInlineVolumeIDTarget(volumeID, kubeletRoot, target string) bool {
	podUID, volumeName, ok := inlineVolumeTargetParts(kubeletRoot, target)
	return ok && volumeID == inlineVolumeID(podUID, volumeName)
}

func inlineVolumeTargetParts(kubeletRoot, target string) (podUID, volumeName string, ok bool) {
	if !filepath.IsAbs(kubeletRoot) || !filepath.IsAbs(target) || filepath.Clean(target) != target {
		return "", "", false
	}
	podsRoot := filepath.Join(filepath.Clean(kubeletRoot), "pods")
	relative, err := filepath.Rel(podsRoot, target)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", "", false
	}
	parts := strings.Split(relative, string(filepath.Separator))
	if len(parts) != 5 || parts[0] == "" || parts[1] != "volumes" || parts[2] != "kubernetes.io~csi" || parts[3] == "" || parts[4] != "mount" {
		return "", "", false
	}
	return parts[0], parts[3], true
}

// inlineVolumeIDはKubeletがPod UIDとvolume nameから作るinline CSI volume IDを再現する。
func inlineVolumeID(podUID, volumeName string) string {
	hash := sha256.Sum256([]byte(podUID + volumeName))
	return "csi-" + hex.EncodeToString(hash[:])
}
