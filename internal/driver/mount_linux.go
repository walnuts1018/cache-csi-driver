//go:build linux

package driver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
)

var errNotMounted = unix.EINVAL

type systemMounter struct{}

func newMounter() mounter { return systemMounter{} }

func (systemMounter) mount(source, target string, readOnly, noExec bool) error {
	return mount(source, target, readOnly, noExec)
}

func (systemMounter) unmount(target string) error { return unmount(target) }

func (systemMounter) mountedAt(target string) (bool, error) { return mountedAt(target) }

func (systemMounter) sameCacheMount(source, target string, readOnly, noExec bool) (bool, error) {
	return sameCacheMount(source, target, readOnly, noExec)
}

func (systemMounter) sameCacheSource(source, target string) (bool, error) {
	return sameCacheSource(source, target)
}

func (systemMounter) sourceMounted(source string) (bool, error) { return sourceMounted(source) }

func (systemMounter) filesystemReadOnly(path string) (bool, error) {
	return filesystemReadOnly(path)
}

func mount(source, target string, readOnly, noExec bool) error {
	detachedMount, err := unix.OpenTree(unix.AT_FDCWD, source, unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC)
	if err != nil {
		return fmt.Errorf("clone cache mount tree: %w", err)
	}
	attributes := unix.MountAttr{Attr_set: unix.MOUNT_ATTR_NODEV | unix.MOUNT_ATTR_NOSUID}
	if readOnly {
		attributes.Attr_set |= unix.MOUNT_ATTR_RDONLY
	}
	if noExec {
		attributes.Attr_set |= unix.MOUNT_ATTR_NOEXEC
	}
	if err := unix.MountSetattr(detachedMount, "", uint(unix.AT_EMPTY_PATH), &attributes); err != nil {
		closeErr := unix.Close(detachedMount)
		return errors.Join(fmt.Errorf("set cache mount attributes: %w", err), wrapMountCloseError(closeErr))
	}
	if err := unix.MoveMount(detachedMount, "", unix.AT_FDCWD, target, unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
		closeErr := unix.Close(detachedMount)
		return errors.Join(fmt.Errorf("attach cache mount: %w", err), wrapMountCloseError(closeErr))
	}
	if err := unix.Close(detachedMount); err != nil {
		return fmt.Errorf("close attached cache mount: %w", err)
	}
	return nil
}

func PreflightMountAPI() (resultErr error) {
	root, err := os.MkdirTemp("", "cache-csi-mount-preflight-")
	if err != nil {
		return fmt.Errorf("create mount preflight directory: %w", err)
	}
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "target")
	if err := os.Mkdir(source, 0o700); err != nil {
		return errors.Join(fmt.Errorf("create mount preflight source: %w", err), os.RemoveAll(root))
	}
	if err := os.Mkdir(target, 0o700); err != nil {
		return errors.Join(fmt.Errorf("create mount preflight target: %w", err), os.RemoveAll(root))
	}
	mounted := false
	defer func() {
		if mounted {
			if err := unmount(target); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("unmount mount preflight target: %w", err))
			}
		}
		if err := os.RemoveAll(root); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove mount preflight directory: %w", err))
		}
	}()

	mountErr := mount(source, target, true, true)
	mounted, err = mountedAt(target)
	if err != nil {
		return fmt.Errorf("inspect mount preflight target: %w", err)
	}
	if mountErr != nil {
		return fmt.Errorf("exercise open_tree, mount_setattr, and move_mount: %w", mountErr)
	}
	if !mounted {
		return errors.New("mount API preflight completed without attaching the test mount")
	}
	same, err := sameCacheMount(source, target, true, true)
	if err != nil {
		return fmt.Errorf("verify mount preflight attributes: %w", err)
	}
	if !same {
		return errors.New("mount API preflight did not apply readonly, nodev, nosuid, and noexec attributes")
	}
	readOnly, err := filesystemReadOnly(target)
	if err != nil {
		return fmt.Errorf("verify mount preflight readonly state: %w", err)
	}
	if !readOnly {
		return errors.New("mount API preflight mount is not readonly")
	}
	if err := unmount(target); err != nil {
		return fmt.Errorf("unmount mount preflight target: %w", err)
	}
	mounted = false
	stillMounted, err := mountedAt(target)
	if err != nil {
		return fmt.Errorf("verify mount preflight cleanup: %w", err)
	}
	if stillMounted {
		return errors.New("mount API preflight target remains mounted after unmount")
	}
	return nil
}

func wrapMountCloseError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close detached cache mount: %w", err)
}

func unmount(target string) error { return unix.Unmount(target, 0) }

func mountedAt(target string) (bool, error) {
	_, found, err := readMountInfo(target)
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

func sourceMounted(source string) (bool, error) {
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return false, err
	}
	mounts, err := mountinfo.GetMounts(nil)
	if err != nil {
		return false, err
	}
	for _, mount := range mounts {
		mountInfo, err := os.Stat(mount.Mountpoint)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return false, err
		}
		if os.SameFile(sourceInfo, mountInfo) {
			return true, nil
		}
	}
	return false, nil
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
