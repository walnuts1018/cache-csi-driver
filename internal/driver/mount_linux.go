//go:build linux

package driver

import (
	"bufio"
	"os"
	"strings"

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
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return mountInfo{}, false, err
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 10 || unescapeMountPath(fields[4]) != target {
			continue
		}
		options := strings.Split(fields[5], ",")
		return mountInfo{
			readOnly: containsString(options, "ro"),
			noExec:   containsString(options, "noexec"),
			nodev:    containsString(options, "nodev"),
			nosuid:   containsString(options, "nosuid"),
		}, true, nil
	}
	if err := scanner.Err(); err != nil {
		return mountInfo{}, false, err
	}
	return mountInfo{}, false, nil
}

func filesystemReadOnly(path string) (bool, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return false, err
	}
	return stat.Flags&unix.ST_RDONLY != 0, nil
}

func unescapeMountPath(path string) string {
	for _, item := range [][2]string{{"\\040", " "}, {"\\011", "\t"}, {"\\012", "\n"}, {"\\134", "\\"}} {
		path = strings.ReplaceAll(path, item[0], item[1])
	}
	return path
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
