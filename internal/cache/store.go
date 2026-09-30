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
	Pressure              PressureConfig
	ProjectIDStart        uint32
	ProjectIDCount        uint32
	UnmountGeneration     func(string) error
	RequireRootMountpoint bool
}

type Store struct {
	metadataRepository
	generationManager
	leaseManager
	projectQuotaRegistry
	pressureManager
	trashCollector
	unmountGeneration func(string) error
	mu                sync.Mutex
	closeOnce         sync.Once
	closeErr          error
	initDone          chan struct{}
	initErr           error
	indexReady        atomic.Bool
	ready             atomic.Bool
	// lease lockを取得してからidentity lockを取得し、project registry lockの後にmuを取得します。muはfilesystem I/O中に保持しません。
	recoveryMu sync.Mutex
}

type metadataRepository struct {
	root   string
	rootFS *os.Root
}

type generationManager struct {
	store                  *Store
	metadataByIdentity     map[string]Metadata
	leaseIndex             map[string]string
	degraded               map[string]error
	fallbackReservedBytes  int64
	retiredGenerationCount int
}

type leaseManager struct {
	identityLocks keyedMutexes
	leaseLocks    keyedMutexes
	fallbackMu    sync.Mutex
}

// projectQuotaRegistryはcache metadataをcanonical sourceとしてproject ID予約indexを管理します。永続registryはmetadataから再構築でき、保存失敗はdirty状態のまま再試行します。
type projectQuotaRegistry struct {
	store                      *Store
	projectIDStart             uint32
	projectIDCount             uint32
	projectRegistryDamaged     bool
	projectRegistryDirty       bool
	projectReservations        map[uint32][]projectReservation
	projectOwnersByID          map[uint32]projectReservationKey
	projectIDByGeneration      map[projectReservationKey]uint32
	unknownProjectReservations map[string]string
	projectRegistryMu          sync.Mutex
}

type pressureManager struct {
	pressure             PressureConfig
	pressureActive       bool
	pressureState        string
	pressureDetachFailed map[string]struct{}
}

type trashCollector struct {
	store              *Store
	trashMetadata      map[string]Metadata
	trashDeleted       uint64
	trashCursor        string
	removeTrashEntry   func(string) error
	stopTrash          chan struct{}
	trashDone          chan struct{}
	trashRequests      chan chan error
	quarantineRequests chan quarantineRequest
	trashMu            sync.Mutex
	collectorMu        sync.Mutex
	collectorStarted   bool
	collectorFinished  bool
}

type keyedMutexes struct {
	mu    sync.Mutex
	locks map[string]*keyedMutex
}

type keyedMutex struct {
	mu   sync.Mutex
	refs int
}

func (manager *leaseManager) lockIdentity(identity string) func() {
	return manager.identityLocks.lock(identity)
}

func (manager *leaseManager) lockLease(leaseID string) func() {
	return manager.leaseLocks.lock(leaseID)
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

func (repository *metadataRepository) relative(path string) (string, error) {
	relative, err := filepath.Rel(repository.root, filepath.Clean(path))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", fmt.Errorf("path %q is outside cache root", path)
	}
	return relative, nil
}

func (repository *metadataRepository) readDir(path string) ([]os.DirEntry, error) {
	relative, err := repository.relative(path)
	if err != nil {
		return nil, err
	}
	return fs.ReadDir(repository.rootFS.FS(), relative)
}

func (repository *metadataRepository) stat(path string) error {
	relative, err := repository.relative(path)
	if err != nil {
		return err
	}
	_, err = repository.rootFS.Stat(relative)
	return err
}

func (repository *metadataRepository) ensureDirectory(path string) error {
	relative, err := repository.relative(path)
	if err != nil {
		return err
	}
	if err := repository.rootFS.MkdirAll(relative, 0o700); err != nil {
		return err
	}
	info, err := repository.rootFS.Lstat(relative)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q is not a real directory", path)
	}
	return nil
}

func (repository *metadataRepository) rename(oldPath, newPath string) error {
	oldRelative, err := repository.relative(oldPath)
	if err != nil {
		return err
	}
	newRelative, err := repository.relative(newPath)
	if err != nil {
		return err
	}
	return repository.rootFS.Rename(oldRelative, newRelative)
}

func (repository *metadataRepository) removeAll(path string) error {
	relative, err := repository.relative(path)
	if err != nil {
		return err
	}
	return repository.rootFS.RemoveAll(relative)
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
	if options.RequireRootMountpoint {
		if err := validateRootMountpoint(root); err != nil {
			_ = rootFS.Close()
			return nil, fmt.Errorf("validate cache root mountpoint: %w", err)
		}
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
		metadataRepository:         metadataRepository{root: root, rootFS: rootFS},
		metadataByIdentity:         make(map[string]Metadata),
		leaseIndex:                 make(map[string]string),
		degraded:                   make(map[string]error),
		projectIDStart:             options.ProjectIDStart,
		projectIDCount:             options.ProjectIDCount,
		projectReservations:        make(map[uint32][]projectReservation),
		projectOwnersByID:          make(map[uint32]projectReservationKey),
		projectIDByGeneration:      make(map[projectReservationKey]uint32),
		unknownProjectReservations: make(map[string]string),
		pressure:                   options.Pressure,
		pressureState:              pressureStateNormal,
		pressureDetachFailed:       make(map[string]struct{}),
		trashMetadata:              make(map[string]Metadata),
		stopTrash:                  make(chan struct{}),
		trashDone:                  make(chan struct{}),
		trashRequests:              make(chan chan error),
		quarantineRequests:         make(chan quarantineRequest, 64),
		unmountGeneration:          options.UnmountGeneration,
		initDone:                   make(chan struct{}),
	}
	store.generationManager.store = store
	store.projectQuotaRegistry.store = store
	store.trashCollector.store = store
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

func (collector *trashCollector) startTrashCollector() {
	s := collector.store
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

func (collector *trashCollector) finishTrashCollector() {
	s := collector.store
	s.collectorMu.Lock()
	defer s.collectorMu.Unlock()
	if s.collectorFinished {
		return
	}
	s.collectorFinished = true
	close(s.trashDone)
}

func (collector *trashCollector) stopRequested() bool {
	select {
	case <-collector.stopTrash:
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
