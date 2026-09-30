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
	podUID, targetPath, hasTargetPath := strings.Cut(relative, string(filepath.Separator))
	if podUID == "" || !hasTargetPath || targetPath == "" {
		return "", false
	}
	return podUID, true
}
