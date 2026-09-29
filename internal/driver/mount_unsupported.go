//go:build !linux

package driver

import (
	"errors"
)

var errNotMounted = errors.New("mount not found")

type systemMounter struct{}

func newMounter() mounter { return systemMounter{} }

func (systemMounter) mount(source, target string, readOnly, noExec bool) error {
	return mount(source, target, readOnly, noExec)
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

func mount(string, string, bool, bool) error { return errors.New("bind mounts require Linux") }
func PreflightMountAPI() error               { return errors.New("bind mount preflight requires Linux") }
func unmount(string) error                   { return errors.New("bind mounts require Linux") }
func mountedAt(string) (bool, error)         { return false, errors.New("mount inspection requires Linux") }
func sameCacheMount(string, string, bool, bool) (bool, error) {
	return false, errors.New("mount inspection requires Linux")
}
func sameCacheSource(string, string) (bool, error) {
	return false, errors.New("mount inspection requires Linux")
}
func filesystemReadOnly(string) (bool, error) {
	return false, errors.New("filesystem inspection requires Linux")
}
