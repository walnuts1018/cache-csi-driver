//go:build linux

package cache

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func validateRootMountpoint(path string) error {
	var rootStat, parentStat unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID, &rootStat); err != nil {
		return fmt.Errorf("inspect cache root mount ID: %w", err)
	}
	parent := filepath.Dir(path)
	if err := unix.Statx(unix.AT_FDCWD, parent, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID, &parentStat); err != nil {
		return fmt.Errorf("inspect parent mount ID: %w", err)
	}
	if rootStat.Mask&unix.STATX_MNT_ID == 0 || parentStat.Mask&unix.STATX_MNT_ID == 0 {
		return fmt.Errorf("kernel did not provide mount IDs for cache root and parent")
	}
	if rootStat.Mnt_id == parentStat.Mnt_id {
		return fmt.Errorf("cache root %q is not a filesystem mountpoint", path)
	}
	return nil
}
