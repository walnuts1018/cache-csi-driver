package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
)

func mountFallbackTmpfs(path string, size int64, noExec bool) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("fallback root is not a real directory")
	}
	path, err = canonicalPath(path)
	if err != nil {
		return fmt.Errorf("resolve fallback mount path: %w", err)
	}
	flags := uintptr(unix.MS_NODEV | unix.MS_NOSUID)
	if noExec {
		flags |= unix.MS_NOEXEC
	}
	mounts, err := mountsAtPath(path)
	if err != nil {
		return fmt.Errorf("inspect fallback mount: %w", err)
	}
	if existing := topMountAtPath(mounts); existing != nil && existing.FSType == "tmpfs" {
		flags |= unix.MS_REMOUNT
	}
	options := "size=" + strconv.FormatInt(size, 10) + ",mode=0700"
	if err := unix.Mount("tmpfs", path, "tmpfs", flags, options); err != nil {
		return fmt.Errorf("mount fallback tmpfs: %w", err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("restrict fallback tmpfs root: %w", err)
	}
	mounts, err = mountsAtPath(path)
	if err != nil {
		return fmt.Errorf("verify fallback mount: %w", err)
	}
	mount := topMountAtPath(mounts)
	if mount == nil || mount.FSType != "tmpfs" {
		return errors.New("fallback root is not mounted as tmpfs")
	}
	if strings.Contains(","+mount.Options+",", ",noexec,") != noExec {
		return errors.New("fallback tmpfs noexec option does not match its configured policy")
	}
	requiredOptions := []string{"nodev", "nosuid"}
	if noExec {
		requiredOptions = append(requiredOptions, "noexec")
	}
	for _, required := range requiredOptions {
		if !strings.Contains(","+mount.Options+",", ","+required+",") {
			return fmt.Errorf("fallback tmpfs is missing the %s mount option", required)
		}
	}
	var usage unix.Statfs_t
	if err := unix.Statfs(path, &usage); err != nil {
		return fmt.Errorf("inspect fallback tmpfs capacity: %w", err)
	}
	capacity := uint64(usage.Blocks) * uint64(usage.Bsize)
	if capacity > uint64(size) {
		return fmt.Errorf("fallback tmpfs capacity %d exceeds configured maximum %d", capacity, size)
	}
	return nil
}

func mountsAtPath(path string) ([]*mountinfo.Info, error) {
	return mountinfo.GetMounts(func(mount *mountinfo.Info) (skip, stop bool) {
		return filepath.Clean(mount.Mountpoint) != path, false
	})
}

func topMountAtPath(mounts []*mountinfo.Info) *mountinfo.Info {
	var top *mountinfo.Info
	for _, mount := range mounts {
		if top == nil || mount.ID > top.ID {
			top = mount
		}
	}
	return top
}
