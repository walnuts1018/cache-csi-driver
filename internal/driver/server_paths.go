package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"

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
	targetPodUID, ok := kubeletcompat.ParsePodTarget(s.options.KubeletRoot, target)
	if !ok {
		return errors.New("target path must be inside a kubelet Pod directory")
	}
	if targetPodUID != podUID {
		return errors.New("target path pod UID does not match pod information")
	}
	return ensureNoSymlinkTraversal(target)
}

func ensureNoSymlinkTraversal(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("target path must be clean and absolute")
	}
	current := string(filepath.Separator)
	relative := strings.TrimPrefix(path, current)
	for component := range strings.SplitSeq(relative, string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("target path must not traverse symbolic links")
		}
		if !info.IsDir() {
			return errors.New("target path components must be directories")
		}
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

func makeTerminalFallbackDirectory(root, volumeID string) (string, error) {
	root = filepath.Clean(root)
	if !filepath.IsAbs(root) || volumeID == "" {
		return "", errors.New("terminal fallback source path is invalid")
	}
	path := fallbackPath(root, volumeID)
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return "", err
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("terminal fallback root is not a real directory")
	}
	if err := os.Mkdir(path, 0o777); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("terminal fallback source is not a real directory")
	}
	if err := os.Chmod(path, 0o777); err != nil {
		return "", err
	}
	return path, nil
}

func resetTerminalFallbackDirectory(root, volumeID string) error {
	path, err := makeTerminalFallbackDirectory(root, volumeID)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(path, entry.Name())); err != nil {
			return err
		}
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
