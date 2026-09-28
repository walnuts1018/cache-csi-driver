//go:build !linux

package driver

import (
	"errors"
)

var errNotMounted = errors.New("mount not found")

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
func unescapeMountPath(path string) string { return path }
