//go:build linux

package quota

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const xfsSuperMagic = 0x58465342

type XFS struct {
	Binary string
}

func (x XFS) Check(ctx context.Context, filesystemRoot string) error {
	if err := x.validate(filesystemRoot); err != nil {
		return err
	}
	state, err := x.runOutput(ctx, filesystemRoot, "-c", "state -p")
	if err != nil {
		return err
	}
	if !projectQuotaEnforced(state) {
		return errors.New("XFS project quota accounting and enforcement must be enabled")
	}
	return nil
}

func (x XFS) AssignProject(ctx context.Context, filesystemRoot, generationPath string, projectID uint32) error {
	if projectID == 0 {
		return errors.New("project ID must be positive")
	}
	if err := x.validate(filesystemRoot); err != nil {
		return err
	}
	if !filepath.IsAbs(generationPath) || strings.ContainsAny(generationPath, "\x00\r\n") {
		return errors.New("generation path must be absolute and must not contain NUL or newline characters")
	}
	info, err := os.Stat(generationPath)
	if err != nil {
		return fmt.Errorf("inspect XFS project generation: %w", err)
	}
	if !info.IsDir() {
		return errors.New("XFS project generation path must be a directory")
	}
	projectFile, err := os.CreateTemp("", "cache-csi-xfs-projects-")
	if err != nil {
		return fmt.Errorf("create XFS project path file: %w", err)
	}
	projectFilePath := projectFile.Name()
	defer func() { _ = os.Remove(projectFilePath) }()
	if _, err := fmt.Fprintf(projectFile, "%d:%s\n", projectID, generationPath); err != nil {
		_ = projectFile.Close()
		return fmt.Errorf("write XFS project path file: %w", err)
	}
	if err := projectFile.Close(); err != nil {
		return fmt.Errorf("close XFS project path file: %w", err)
	}
	command := "project -s " + strconv.FormatUint(uint64(projectID), 10)
	_, err = x.runOutputWithOptions(ctx, filesystemRoot, []string{"-D", projectFilePath}, "-c", command)
	return err
}

func (x XFS) Configure(ctx context.Context, filesystemRoot, generationPath string, projectID uint32, maxBytes int64) error {
	if projectID == 0 || maxBytes <= 0 {
		return errors.New("project ID and positive quota limit are required")
	}
	if err := x.validate(filesystemRoot); err != nil {
		return err
	}
	if err := x.Check(ctx, filesystemRoot); err != nil {
		return err
	}
	if err := x.AssignProject(ctx, filesystemRoot, generationPath, projectID); err != nil {
		return err
	}
	if err := x.SetLimit(ctx, filesystemRoot, projectID, maxBytes); err != nil {
		return err
	}
	report, err := x.runOutput(ctx, filesystemRoot, "-c", "report -p -b -n -N")
	if err != nil {
		return err
	}
	if !projectLimitPresent(report, projectID, maxBytes) {
		return errors.New("XFS project hard quota did not match the requested limit")
	}
	return nil
}

func (x XFS) SetLimit(ctx context.Context, filesystemRoot string, projectID uint32, maxBytes int64) error {
	if projectID == 0 || maxBytes <= 0 {
		return errors.New("project ID and positive quota limit are required")
	}
	if err := x.validate(filesystemRoot); err != nil {
		return err
	}
	command := "limit -p bhard=" + strconv.FormatInt(maxBytes, 10) + " " + strconv.FormatUint(uint64(projectID), 10)
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
	_, err := x.runOutput(ctx, root, args...)
	return err
}

func (x XFS) runOutput(ctx context.Context, root string, args ...string) (string, error) {
	return x.runOutputWithOptions(ctx, root, nil, args...)
}

func (x XFS) runOutputWithOptions(ctx context.Context, root string, options []string, args ...string) (string, error) {
	binary := x.Binary
	if binary == "" {
		binary = "xfs_quota"
	}
	commandArgs := append(options, "-x")
	commandArgs = append(commandArgs, args...)
	commandArgs = append(commandArgs, root)
	command := exec.CommandContext(ctx, binary, commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("run xfs_quota: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}
