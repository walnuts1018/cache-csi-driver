package cache

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var ErrQuotaPolicyConflict = errors.New("active cache generation cannot change its effective quota")

var ErrExclusivePolicyConflict = errors.New("exclusive cache identity already has an active lease")

var ErrDegradedMetadata = errors.New("cache metadata is degraded")

var ErrPressureReclaimIncomplete = errors.New("cache pressure reclaim is incomplete")

type PressureConfig struct {
	HighFreePercent      int
	LowFreePercent       int
	HighInodeFreePercent int
	LowInodeFreePercent  int
}

type StoreOptions struct {
	Pressure       PressureConfig
	ProjectIDStart uint32
	ProjectIDCount uint32
}

type Store struct {
	root                       string
	rootFS                     *os.Root
	pressure                   PressureConfig
	projectIDStart             uint32
	projectIDCount             uint32
	projectRegistryDamaged     bool
	projectRegistryDirty       bool
	pressureActive             bool
	pressureDetachFailed       map[string]struct{}
	metadataByIdentity         map[string]Metadata
	leaseIndex                 map[string]string
	degraded                   map[string]error
	projectReservations        map[uint32][]projectReservation
	unknownProjectReservations map[string]string
	trashMetadata              map[string]Metadata
	trashCursor                string
	removeTrashEntry           func(string) error
	mu                         sync.Mutex
	stopTrash                  chan struct{}
	trashDone                  chan struct{}
	trashRequests              chan chan error
	closeOnce                  sync.Once
	closeErr                   error
}

func (s *Store) Root() string { return s.root }

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.stopTrash)
		<-s.trashDone
		s.closeErr = s.rootFS.Close()
	})
	return s.closeErr
}

func (s *Store) relative(path string) (string, error) {
	relative, err := filepath.Rel(s.root, filepath.Clean(path))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", fmt.Errorf("path %q is outside cache root", path)
	}
	return relative, nil
}

func (s *Store) readDir(path string) ([]os.DirEntry, error) {
	relative, err := s.relative(path)
	if err != nil {
		return nil, err
	}
	return fs.ReadDir(s.rootFS.FS(), relative)
}

func (s *Store) stat(path string) error {
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	_, err = s.rootFS.Stat(relative)
	return err
}

func (s *Store) ensureDirectory(path string) error {
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	if err := s.rootFS.MkdirAll(relative, 0o700); err != nil {
		return err
	}
	info, err := s.rootFS.Lstat(relative)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q is not a real directory", path)
	}
	return nil
}

func (s *Store) rename(oldPath, newPath string) error {
	oldRelative, err := s.relative(oldPath)
	if err != nil {
		return err
	}
	newRelative, err := s.relative(newPath)
	if err != nil {
		return err
	}
	return s.rootFS.Rename(oldRelative, newRelative)
}

func (s *Store) removeAll(path string) error {
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	return s.rootFS.RemoveAll(relative)
}

func NewStore(root string, options StoreOptions) (*Store, error) {
	if !filepath.IsAbs(root) {
		return nil, errors.New("cache root must be absolute")
	}
	root = filepath.Clean(root)
	if err := ensureDirectory(root); err != nil {
		return nil, fmt.Errorf("create cache root: %w", err)
	}
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("open cache root: %w", err)
	}
	if err := validatePressure(options.Pressure); err != nil {
		_ = rootFS.Close()
		return nil, err
	}
	if options.ProjectIDStart == 0 && options.ProjectIDCount == 0 {
		options.ProjectIDStart = 2_000_000_000
		options.ProjectIDCount = 1_000_000
	}
	if options.ProjectIDStart == 0 || options.ProjectIDCount == 0 || uint64(options.ProjectIDStart)+uint64(options.ProjectIDCount)-1 > uint64(^uint32(0)) {
		_ = rootFS.Close()
		return nil, errors.New("project ID range must be positive and fit within uint32")
	}
	if err := rootFS.MkdirAll(trashDirectoryName, 0o700); err != nil {
		_ = rootFS.Close()
		return nil, fmt.Errorf("create cache trash directory: %w", err)
	}
	store := &Store{
		root:                       root,
		rootFS:                     rootFS,
		pressure:                   options.Pressure,
		projectIDStart:             options.ProjectIDStart,
		projectIDCount:             options.ProjectIDCount,
		pressureDetachFailed:       make(map[string]struct{}),
		metadataByIdentity:         make(map[string]Metadata),
		leaseIndex:                 make(map[string]string),
		degraded:                   make(map[string]error),
		projectReservations:        make(map[uint32][]projectReservation),
		unknownProjectReservations: make(map[string]string),
		trashMetadata:              make(map[string]Metadata),
		stopTrash:                  make(chan struct{}),
		trashDone:                  make(chan struct{}),
		trashRequests:              make(chan chan error),
	}
	if err := store.rebuildIndexes(); err != nil {
		_ = rootFS.Close()
		return nil, fmt.Errorf("rebuild cache indexes: %w", err)
	}
	go store.runTrashCollector()
	return store, nil
}

func ensureDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q is not a real directory", path)
	}
	return nil
}
