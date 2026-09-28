//go:build linux

package quota

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const xfsSuperMagic = 0x58465342

type XFS struct {
	Binary string
}

func (x XFS) AssignProject(ctx context.Context, filesystemRoot, generationPath string, projectID uint32) error {
	if projectID == 0 {
		return errors.New("project ID must be positive")
	}
	if err := x.validate(filesystemRoot); err != nil {
		return err
	}
	command := "project -s -p " + strconv.Quote(generationPath) + " " + strconv.FormatUint(uint64(projectID), 10)
	return x.run(ctx, filesystemRoot, "-c", command)
}

func (x XFS) SetLimit(ctx context.Context, filesystemRoot string, projectID uint32, maxBytes int64) error {
	if projectID == 0 || maxBytes <= 0 {
		return errors.New("project ID and positive quota limit are required")
	}
	if err := x.validate(filesystemRoot); err != nil {
		return err
	}
	command := "limit -p bhard=" + strconv.FormatInt(maxBytes, 10) + "b " + strconv.FormatUint(uint64(projectID), 10)
	return x.run(ctx, filesystemRoot, "-c", command)
}

func (x XFS) validate(root string) error {
	var filesystem unix.Statfs_t
	if err := unix.Statfs(root, &filesystem); err != nil {
		return fmt.Errorf("inspect cache filesystem: %w", err)
	}
	if filesystem.Type != xfsSuperMagic {
		return errors.New("xfs-project quota requires an XFS cache filesystem")
	}
	return nil
}

func (x XFS) run(ctx context.Context, root string, args ...string) error {
	binary := x.Binary
	if binary == "" {
		binary = "xfs_quota"
	}
	commandArgs := append([]string{"-x"}, args...)
	commandArgs = append(commandArgs, root)
	command := exec.CommandContext(ctx, binary, commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("run xfs_quota: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}
