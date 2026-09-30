package mountinfo

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/moby/sys/mountinfo"
)

func AtPath(path string) ([]*mountinfo.Info, error) {
	entries, err := mountinfo.GetMounts(nil)
	if err != nil {
		return nil, err
	}
	return AtPathIn(entries, path), nil
}

func AtPathIn(entries []*mountinfo.Info, path string) []*mountinfo.Info {
	path = filepath.Clean(path)
	matched := make([]*mountinfo.Info, 0, 1)
	for _, entry := range entries {
		if filepath.Clean(entry.Mountpoint) == path {
			matched = append(matched, entry)
		}
	}
	return matched
}

func Top(entries []*mountinfo.Info) (*mountinfo.Info, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	var top *mountinfo.Info
	for _, candidate := range entries {
		covered := false
		for _, entry := range entries {
			if entry.ID != candidate.ID && entry.Parent == candidate.ID {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		if top != nil {
			return nil, errors.New("mount stack has multiple topmost entries")
		}
		top = candidate
	}
	if top == nil {
		return nil, errors.New("mount stack has no topmost entry")
	}
	return top, nil
}

func Covering(entries []*mountinfo.Info, path string) (*mountinfo.Info, error) {
	path = filepath.Clean(path)
	var longest int
	var candidates []*mountinfo.Info
	for _, entry := range entries {
		mountpoint := filepath.Clean(entry.Mountpoint)
		if !within(mountpoint, path) {
			continue
		}
		if len(mountpoint) > longest {
			longest = len(mountpoint)
			candidates = candidates[:0]
		}
		if len(mountpoint) == longest {
			candidates = append(candidates, entry)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	top, err := Top(candidates)
	if err != nil {
		return nil, fmt.Errorf("select topmost mount covering %q: %w", path, err)
	}
	return top, nil
}

func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}
