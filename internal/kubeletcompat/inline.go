package kubeletcompat

import (
	"path/filepath"
	"strings"
)

const (
	inlineCSIDirectory = "kubernetes.io~csi"
	inlineCSIMount     = "mount"
)

// ParseInlineCSITargetはKubernetes kubeletがCSI inline volumeに使うtarget pathからPod UIDとvolume nameを取得する。
func ParseInlineCSITarget(kubeletRoot, target string) (podUID, volumeName string, ok bool) {
	if !filepath.IsAbs(kubeletRoot) || !filepath.IsAbs(target) || filepath.Clean(target) != target {
		return "", "", false
	}
	podsRoot := filepath.Join(filepath.Clean(kubeletRoot), "pods")
	relative, err := filepath.Rel(podsRoot, target)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", "", false
	}
	parts := strings.Split(relative, string(filepath.Separator))
	if len(parts) != 5 || parts[0] == "" || parts[1] != "volumes" || parts[2] != inlineCSIDirectory || parts[3] == "" || parts[4] != inlineCSIMount {
		return "", "", false
	}
	return parts[0], parts[3], true
}
