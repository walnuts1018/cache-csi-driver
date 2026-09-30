//go:build !linux

package cache

import "errors"

func validateRootMountpoint(string, bool) error {
	return errors.New("cache root mountpoint validation is only supported on Linux")
}
