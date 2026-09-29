package cache

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrQuotaPolicyConflict = errors.New("active cache generation cannot change its effective quota")

var ErrExclusivePolicyConflict = errors.New("exclusive cache identity already has an active lease")

var ErrDegradedMetadata = errors.New("cache metadata is degraded")

var ErrPressureReclaimIncomplete = errors.New("cache pressure reclaim is incomplete")

var ErrPressureActive = errors.New("cache pool is under pressure")

var ErrFallbackCapacity = errors.New("fallback cache capacity is exhausted")

var ErrFallbackLeaseConflict = errors.New("fallback cache lease ID is already in use")

var ErrStoreNotReady = errors.New("cache store indexes are not ready")

const pressureStateNormal = "normal"

type PressureConfig struct {
	HighFreePercent          int
	LowFreePercent           int
	CriticalFreePercent      int
	HighInodeFreePercent     int
	LowInodeFreePercent      int
	CriticalInodeFreePercent int
}

type StoreOptions struct {
	Pressure          PressureConfig
	ProjectIDStart    uint32
	ProjectIDCount    uint32
	UnmountGeneration func(string) error
}

type Store struct {
	root                       string
	rootFS                     *os.Root
	unmountGeneration          func(string) error
	pressure                   PressureConfig
	projectIDStart             uint32
	projectIDCount             uint32
	projectRegistryDamaged     bool
	projectRegistryDirty       bool
	pressureActive             bool
	pressureState              string
	pressureDetachFailed       map[string]struct{}
	metadataByIdentity         map[string]Metadata
	leaseIndex                 map[string]string
	degraded                   map[string]error
	projectReservations        map[uint32][]projectReservation
	projectOwnersByID          map[uint32]projectReservationKey
	projectIDByGeneration      map[projectReservationKey]uint32
	unknownProjectReservations map[string]string
	trashMetadata              map[string]Metadata
	trashDeleted               uint64
	fallbackReservedBytes      int64
	retiredGenerationCount     int
	trashCursor                string
	removeTrashEntry           func(string) error
	mu                         sync.Mutex
	stopTrash                  chan struct{}
	trashDone                  chan struct{}
	trashRequests              chan chan error
	closeOnce                  sync.Once
	closeErr                   error
	initDone                   chan struct{}
	initErr                    error
	indexReady                 atomic.Bool
	ready                      atomic.Bool
	// lease lockを取得してからidentity lockを取得し、project registry lockの後にmuを取得します。muはfilesystem I/O中に保持しません。
	identityLocks              keyedMutexes
	leaseLocks                 keyedMutexes
	// project IDはcache root全体で一意なため、registry fileの更新を直列化します。
	projectRegistryMu          sync.Mutex
	fallbackMu                 sync.Mutex
	// trashのdetach中にcollectorが作成途中のentryをsnapshotしないようにします。
	trashMu                    sync.Mutex
	recoveryMu                 sync.Mutex
	collectorMu                sync.Mutex
	collectorStarted           bool
	collectorFinished          bool
}

type keyedMutexes struct {
	mu    sync.Mutex
	locks map[string]*keyedMutex
}

type keyedMutex struct {
	mu   sync.Mutex
	refs int
}

func (m *keyedMutexes) lock(key string) func() {
	m.mu.Lock()
	if m.locks == nil {
		m.locks = make(map[string]*keyedMutex)
	}
	lock := m.locks[key]
	if lock == nil {
		lock = &keyedMutex{}
		m.locks[key] = lock
	}
	lock.refs++
	m.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		m.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(m.locks, key)
		}
		m.mu.Unlock()
	}
}

type RuntimeStats struct {
	Ready                 bool
	PressureState         string
	DegradedObjects       int
	CacheObjects          int
	RetiredGenerations    int
	FallbackReservedBytes int64
	TrashObjectsDeleted   uint64
}

func (s *Store) Root() string { return s.root }

func (s *Store) RuntimeStats() RuntimeStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return RuntimeStats{
		Ready:                 s.ready.Load(),
		PressureState:         s.pressureState,
		DegradedObjects:       len(s.degraded),
		CacheObjects:          len(s.metadataByIdentity),
		RetiredGenerations:    s.retiredGenerationCount,
		FallbackReservedBytes: s.fallbackReservedBytes,
		TrashObjectsDeleted:   s.trashDeleted,
	}
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.stopTrash)
		<-s.initDone
		s.collectorMu.Lock()
		if !s.collectorStarted && !s.collectorFinished {
			s.collectorFinished = true
			close(s.trashDone)
		}
		collectorStarted := s.collectorStarted
		s.collectorMu.Unlock()
		if collectorStarted {
			<-s.trashDone
		}
		s.closeErr = s.rootFS.Close()
	})
	return s.closeErr
}

func (s *Store) Ready() bool {
	return s.ready.Load()
}

func (s *Store) WaitForIndexes(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.initDone:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initErr != nil {
		return fmt.Errorf("initialize cache indexes: %w", s.initErr)
	}
	if !s.indexReady.Load() {
		return ErrStoreNotReady
	}
	return nil
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
	return newStoreMode(root, options, true, true)
}

func newStore(root string, options StoreOptions, startTrashCollector bool) (*Store, error) {
	return newStoreMode(root, options, true, startTrashCollector)
}

func NewStoreAsync(root string, options StoreOptions) (*Store, error) {
	return newStoreMode(root, options, false, false)
}

func newStoreMode(root string, options StoreOptions, initializeIndexes, startTrashCollector bool) (*Store, error) {
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
		unmountGeneration:          options.UnmountGeneration,
		pressure:                   options.Pressure,
		pressureState:              pressureStateNormal,
		projectIDStart:             options.ProjectIDStart,
		projectIDCount:             options.ProjectIDCount,
		pressureDetachFailed:       make(map[string]struct{}),
		metadataByIdentity:         make(map[string]Metadata),
		leaseIndex:                 make(map[string]string),
		degraded:                   make(map[string]error),
		projectReservations:        make(map[uint32][]projectReservation),
		projectOwnersByID:          make(map[uint32]projectReservationKey),
		projectIDByGeneration:      make(map[projectReservationKey]uint32),
		unknownProjectReservations: make(map[string]string),
		trashMetadata:              make(map[string]Metadata),
		stopTrash:                  make(chan struct{}),
		trashDone:                  make(chan struct{}),
		trashRequests:              make(chan chan error),
		initDone:                   make(chan struct{}),
	}
	if initializeIndexes {
		if err := store.rebuildIndexes(); err != nil {
			_ = rootFS.Close()
			return nil, fmt.Errorf("rebuild cache indexes: %w", err)
		}
		store.indexReady.Store(true)
		store.ready.Store(true)
		close(store.initDone)
		if startTrashCollector {
			store.startTrashCollector()
		} else {
			store.finishTrashCollector()
		}
	} else {
		go store.initializeIndexes()
	}
	return store, nil
}

func (s *Store) initializeIndexes() {
	defer close(s.initDone)
	for {
		if s.stopRequested() {
			return
		}
		s.trashMu.Lock()
		s.projectRegistryMu.Lock()
		s.mu.Lock()
		s.resetIndexState()
		s.mu.Unlock()
		s.projectRegistryMu.Unlock()
		s.trashMu.Unlock()

		err := s.rebuildIndexes()

		s.mu.Lock()
		s.initErr = err
		s.indexReady.Store(err == nil)
		s.mu.Unlock()
		if err == nil {
			s.startTrashCollector()
			return
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-s.stopTrash:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Store) resetIndexState() {
	clear(s.metadataByIdentity)
	clear(s.leaseIndex)
	clear(s.degraded)
	clear(s.projectReservations)
	clear(s.projectOwnersByID)
	clear(s.projectIDByGeneration)
	clear(s.unknownProjectReservations)
	clear(s.trashMetadata)
	clear(s.pressureDetachFailed)
	s.projectRegistryDamaged = false
	s.projectRegistryDirty = false
	s.pressureActive = false
	s.pressureState = pressureStateNormal
	s.fallbackReservedBytes = 0
	s.retiredGenerationCount = 0
	s.trashCursor = ""
}

func (s *Store) startTrashCollector() {
	s.collectorMu.Lock()
	defer s.collectorMu.Unlock()
	if s.collectorStarted || s.collectorFinished {
		return
	}
	select {
	case <-s.stopTrash:
		s.collectorFinished = true
		close(s.trashDone)
	default:
		s.collectorStarted = true
		go s.runTrashCollector()
	}
}

func (s *Store) finishTrashCollector() {
	s.collectorMu.Lock()
	defer s.collectorMu.Unlock()
	if s.collectorFinished {
		return
	}
	s.collectorFinished = true
	close(s.trashDone)
}

func (s *Store) stopRequested() bool {
	select {
	case <-s.stopTrash:
		return true
	default:
		return false
	}
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
