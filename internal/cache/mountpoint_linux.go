//go:build linux

package cache

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func validateRootMountpoint(path string, requireSeparateFilesystem bool) error {
	path = filepath.Clean(path)
	hostPath := "/proc/1/root" + path
	var rootStat, parentStat unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, hostPath, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID, &rootStat); err != nil {
		return fmt.Errorf("inspect cache root mount ID in the host mount namespace: %w", err)
	}
	parent := "/proc/1/root" + filepath.Dir(path)
	if err := unix.Statx(unix.AT_FDCWD, parent, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID, &parentStat); err != nil {
		return fmt.Errorf("inspect cache root parent mount ID in the host mount namespace: %w", err)
	}
	if rootStat.Mask&unix.STATX_MNT_ID == 0 || parentStat.Mask&unix.STATX_MNT_ID == 0 {
		return fmt.Errorf("kernel did not provide mount IDs for cache root and parent")
	}
	if rootStat.Mnt_id == parentStat.Mnt_id {
		return fmt.Errorf("cache root %q is not a host filesystem mountpoint", path)
	}
	if requireSeparateFilesystem && rootStat.Dev_major == parentStat.Dev_major && rootStat.Dev_minor == parentStat.Dev_minor {
		return fmt.Errorf("directory backend cache root %q must use a separate filesystem from its parent", path)
	}
	return nil
}
