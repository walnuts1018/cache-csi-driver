package kubeletcompat

import (
	"path/filepath"
	"strings"
)

// ParsePodTargetはkubelet root配下のpods directoryにあるtarget pathからPod UIDを取得する。
func ParsePodTarget(kubeletRoot, target string) (podUID string, ok bool) {
	if !filepath.IsAbs(kubeletRoot) || !filepath.IsAbs(target) || filepath.Clean(target) != target {
		return "", false
	}
	podsRoot := filepath.Join(filepath.Clean(kubeletRoot), "pods")
	relative, err := filepath.Rel(podsRoot, target)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", false
	}
	parts := strings.Split(relative, string(filepath.Separator))
	if len(parts) != 5 || parts[0] == "" || parts[1] != "volumes" || parts[2] != "kubernetes.io~csi" || parts[3] == "" || parts[4] != "mount" {
		return "", false
	}
	return parts[0], true
}
