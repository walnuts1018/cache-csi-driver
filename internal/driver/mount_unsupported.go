//go:build !linux

package driver

import (
	"errors"
)

var errNotMounted = errors.New("mount not found")

type systemMounter struct{}

func newMounter() mounter { return systemMounter{} }

func (systemMounter) bindMount(source, target string) error { return bindMount(source, target) }
func (systemMounter) remountOptions(target string, readOnly, noExec bool) error {
	return remountOptions(target, readOnly, noExec)
}
func (systemMounter) unmount(target string) error           { return unmount(target) }
func (systemMounter) mountedAt(target string) (bool, error) { return mountedAt(target) }
func (systemMounter) sameCacheMount(source, target string, readOnly, noExec bool) (bool, error) {
	return sameCacheMount(source, target, readOnly, noExec)
}
func (systemMounter) sameCacheSource(source, target string) (bool, error) {
	return sameCacheSource(source, target)
}
func (systemMounter) sourceMounted(string) (bool, error) {
	return false, errors.New("mount inspection requires Linux")
}
func (systemMounter) filesystemReadOnly(path string) (bool, error) {
	return filesystemReadOnly(path)
}

func bindMount(string, string) error          { return errors.New("bind mounts require Linux") }
func remountOptions(string, bool, bool) error { return errors.New("bind mounts require Linux") }
func unmount(string) error                    { return errors.New("bind mounts require Linux") }
func mountedAt(string) (bool, error)          { return false, errors.New("mount inspection requires Linux") }
func sameCacheMount(string, string, bool, bool) (bool, error) {
	return false, errors.New("mount inspection requires Linux")
}
func sameCacheSource(string, string) (bool, error) {
	return false, errors.New("mount inspection requires Linux")
}
func filesystemReadOnly(string) (bool, error) {
	return false, errors.New("filesystem inspection requires Linux")
}
