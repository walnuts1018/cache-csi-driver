//go:build !linux

package quota

import (
	"context"
	"errors"
)

type XFS struct{ Binary string }

func (XFS) Check(context.Context, string) error {
	return errors.New("XFS project quota requires Linux")
}

func (XFS) Configure(context.Context, string, string, uint32, int64) error {
	return errors.New("XFS project quota requires Linux")
}

func (XFS) AssignProject(context.Context, string, string, uint32) error {
	return errors.New("XFS project quota requires Linux")
}

func (XFS) SetLimit(context.Context, string, uint32, int64) error {
	return errors.New("XFS project quota requires Linux")
}
