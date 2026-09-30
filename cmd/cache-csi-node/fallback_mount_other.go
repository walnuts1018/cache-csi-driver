//go:build !linux

package main

import "errors"

func mountFallbackTmpfs(string, int64, bool) error {
	return errors.New("fallback tmpfs requires Linux")
}
