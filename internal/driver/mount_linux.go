//go:build linux

package driver

import (
	"os"
	"slices"
	"strings"

	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
)

var errNotMounted = unix.EINVAL

func bindMount(source, target string) error {
	return unix.Mount(source, target, "", unix.MS_BIND, "")
}

func remountOptions(target string, readOnly, noExec bool) error {
	flags := uintptr(unix.MS_BIND | unix.MS_REMOUNT | unix.MS_NODEV | unix.MS_NOSUID)
	if readOnly {
		flags |= unix.MS_RDONLY
	}
	if noExec {
		flags |= unix.MS_NOEXEC
	}
	return unix.Mount("", target, "", flags, "")
}

func unmount(target string) error { return unix.Unmount(target, 0) }

func mountedAt(target string) (bool, error) {
	mount, found, err := readMountInfo(target)
	return found, err
}

func sameCacheMount(source, target string, readOnly, noExec bool) (bool, error) {
	mount, mounted, err := readMountInfo(target)
	if err != nil || !mounted {
		return false, err
	}
	if mount.readOnly != readOnly || mount.noExec != noExec || !mount.nodev || !mount.nosuid {
		return false, nil
	}
	return sameMountedSource(source, target)
}

func sameCacheSource(source, target string) (bool, error) {
	mount, mounted, err := readMountInfo(target)
	if err != nil || !mounted {
		return false, err
	}
	if !mount.nodev || !mount.nosuid {
		return false, nil
	}
	return sameMountedSource(source, target)
}

func sameMountedSource(source, target string) (bool, error) {
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return false, err
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		return false, err
	}
	return os.SameFile(sourceInfo, targetInfo), nil
}

type mountInfo struct {
	readOnly bool
	noExec   bool
	nodev    bool
	nosuid   bool
}

func readMountInfo(target string) (mountInfo, bool, error) {
	mounts, err := mountinfo.GetMounts(mountinfo.SingleEntryFilter(target))
	if err != nil {
		return mountInfo{}, false, err
	}
	if len(mounts) == 0 {
		return mountInfo{}, false, nil
	}
	options := strings.Split(mounts[0].Options, ",")
	return mountInfo{
		readOnly: slices.Contains(options, "ro"),
		noExec:   slices.Contains(options, "noexec"),
		nodev:    slices.Contains(options, "nodev"),
		nosuid:   slices.Contains(options, "nosuid"),
	}, true, nil
}

func filesystemReadOnly(path string) (bool, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return false, err
	}
	return stat.Flags&unix.ST_RDONLY != 0, nil
}
