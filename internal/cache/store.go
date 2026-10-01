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
	"syscall"
	"time"
)

var ErrQuotaPolicyConflict = errors.New("active cache generation cannot change its effective quota")

var ErrExclusivePolicyConflict = errors.New("exclusive cache identity already has an active lease")

var ErrDegradedMetadata = errors.New("cache metadata is degraded")

var ErrPressureReclaimIncomplete = errors.New("cache pressure reclaim is incomplete")

var ErrPressureActive = errors.New("cache pool is under pressure")

var ErrStoreNotReady = errors.New("cache store indexes are not ready")

const pressureStateNormal = "normal"

const generationsDirectoryName = "generations"

type PressureConfig struct {
	HighFreePercent      int
	LowFreePercent       int
	HighInodeFreePercent int
	LowInodeFreePercent  int
}

type StoreOptions struct {
	Pressure              PressureConfig
	ProjectIDStart        uint32
	ProjectIDCount        uint32
	ProjectQuotaEnabled   bool
	UnmountGeneration     func(string) error
	IsGenerationMounted   func(string) (bool, error)
	RequireRootMountpoint bool
}

type InitializationState string

const (
	InitializationInitializing    InitializationState = "initializing"
	InitializationReady           InitializationState = "ready"
	InitializationRetryableFailed InitializationState = "retryable-failure"
	InitializationNodeWideFailed  InitializationState = "node-wide-failure"
)

const initializationFailureEscalation = 3

type InitializationStatus struct {
	State InitializationState
	Err   error
}

type Store struct {
	metadataRepository   metadataRepository
	generationManager    generationManager
	leaseManager         leaseManager
	projectQuotaRegistry projectQuotaRegistry
	projectQuotaEnabled  bool
	pressureManager      pressureManager
	trashCollector       trashCollector
	unmountGeneration    func(string) error
	isGenerationMounted  func(string) (bool, error)
	mu                   sync.Mutex
	closeOnce            sync.Once
	closeErr             error
	initDone             chan struct{}
	initState            InitializationState
	initErr              error
	initFailures         uint
	indexReady           atomic.Bool
	ready                atomic.Bool
	// lease lockを取得してからidentity lockを取得し、project registry lockの後にmuを取得します。muはfilesystem I/O中に保持しません。
	recoveryMu sync.Mutex
}

type metadataRepository struct {
	root   string
	rootFS *os.Root
}

type generationManager struct {
	metadataByIdentity     map[string]Metadata
	leaseIndex             map[string]string
	degraded               map[string]error
	retiredGenerationCount int
}

type leaseManager struct {
	identityLocks keyedMutexes
	leaseLocks    keyedMutexes
}

// projectQuotaRegistryはcache metadataをcanonical sourceとしてproject ID予約indexを管理します。永続registryはmetadataから再構築でき、保存失敗はdirty状態のまま再試行します。
type projectQuotaRegistry struct {
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
	pressure       PressureConfig
	pressureActive bool
	pressureState  string
}

type trashCollector struct {
	trashMetadata     map[string]Metadata
	trashDeleted      uint64
	trashCursor       string
	removeTrashEntry  func(string) error
	stopTrash         chan struct{}
	trashDone         chan struct{}
	trashRequests     chan chan error
	trashMu           sync.Mutex
	trashCleanupMu    sync.Mutex
	collectorMu       sync.Mutex
	collectorStarted  bool
	collectorFinished bool
}

type keyedMutexes struct {
	mu    sync.Mutex
	locks map[string]*keyedMutex
}

type keyedMutex struct {
	mu   sync.Mutex
	refs int
}

func (s *Store) lockIdentity(identity string) func() {
	return s.leaseManager.identityLocks.lock(identity)
}

func (s *Store) lockLease(leaseID string) func() {
	return s.leaseManager.leaseLocks.lock(leaseID)
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
	Ready               bool
	PressureState       string
	DegradedObjects     int
	CacheObjects        int
	RetiredGenerations  int
	TrashObjectsDeleted uint64
}

func (s *Store) Root() string { return s.metadataRepository.root }

func (s *Store) RuntimeStats() RuntimeStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return RuntimeStats{
		Ready:               s.ready.Load(),
		PressureState:       s.pressureManager.pressureState,
		DegradedObjects:     len(s.generationManager.degraded),
		CacheObjects:        len(s.generationManager.metadataByIdentity),
		RetiredGenerations:  s.generationManager.retiredGenerationCount,
		TrashObjectsDeleted: s.trashCollector.trashDeleted,
	}
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.trashCollector.stopTrash)
		<-s.initDone
		s.trashCollector.collectorMu.Lock()
		if !s.trashCollector.collectorStarted && !s.trashCollector.collectorFinished {
			s.trashCollector.collectorFinished = true
			close(s.trashCollector.trashDone)
		}
		collectorStarted := s.trashCollector.collectorStarted
		s.trashCollector.collectorMu.Unlock()
		if collectorStarted {
			<-s.trashCollector.trashDone
		}
		s.closeErr = s.metadataRepository.rootFS.Close()
	})
	return s.closeErr
}

func (s *Store) Ready() bool {
	return s.ready.Load()
}

func (s *Store) WaitForIndexes(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		status := s.InitializationStatus()
		switch status.State {
		case InitializationInitializing:
		case InitializationReady:
			return nil
		case InitializationRetryableFailed, InitializationNodeWideFailed:
			return fmt.Errorf("initialize cache indexes: %w", status.Err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.initDone:
			status = s.InitializationStatus()
			if status.State == InitializationReady {
				return nil
			}
			if status.Err != nil {
				return fmt.Errorf("initialize cache indexes: %w", status.Err)
			}
			return ErrStoreNotReady
		case <-ticker.C:
		}
	}
}

func (s *Store) InitializationError() error {
	return s.InitializationStatus().Err
}

func (s *Store) InitializationStatus() InitializationStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return InitializationStatus{State: s.initState, Err: s.initErr}
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

func newStore(root string, options StoreOptions) (*Store, error) {
	return newStoreMode(root, options, true, false)
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
		if err := validateRootMountpoint(root, !options.ProjectQuotaEnabled); err != nil {
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
		metadataRepository:   metadataRepository{root: root, rootFS: rootFS},
		generationManager:    generationManager{metadataByIdentity: make(map[string]Metadata), leaseIndex: make(map[string]string), degraded: make(map[string]error)},
		leaseManager:         leaseManager{},
		projectQuotaRegistry: projectQuotaRegistry{projectIDStart: options.ProjectIDStart, projectIDCount: options.ProjectIDCount, projectReservations: make(map[uint32][]projectReservation), projectOwnersByID: make(map[uint32]projectReservationKey), projectIDByGeneration: make(map[projectReservationKey]uint32), unknownProjectReservations: make(map[string]string)},
		projectQuotaEnabled:  options.ProjectQuotaEnabled,
		pressureManager:      pressureManager{pressure: options.Pressure, pressureState: pressureStateNormal},
		trashCollector:       trashCollector{trashMetadata: make(map[string]Metadata), stopTrash: make(chan struct{}), trashDone: make(chan struct{}), trashRequests: make(chan chan error)},
		unmountGeneration:    options.UnmountGeneration,
		isGenerationMounted:  options.IsGenerationMounted,
		initDone:             make(chan struct{}),
		initState:            InitializationInitializing,
	}
	if initializeIndexes {
		if err := store.rebuildIndexes(); err != nil {
			store.initErr = err
			store.initFailures++
			store.initState = classifyInitializationError(err, store.initFailures)
			_ = rootFS.Close()
			return nil, fmt.Errorf("rebuild cache indexes: %w", err)
		}
		store.indexReady.Store(true)
		store.ready.Store(true)
		store.initState = InitializationReady
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
		s.trashCollector.trashMu.Lock()
		s.projectQuotaRegistry.projectRegistryMu.Lock()
		s.mu.Lock()
		s.resetIndexState()
		s.mu.Unlock()
		s.projectQuotaRegistry.projectRegistryMu.Unlock()
		s.trashCollector.trashMu.Unlock()

		err := s.rebuildIndexes()

		s.mu.Lock()
		s.initErr = err
		s.indexReady.Store(err == nil)
		if err == nil {
			s.initFailures = 0
			s.initState = InitializationReady
		} else {
			s.initFailures++
			s.initState = classifyInitializationError(err, s.initFailures)
		}
		s.mu.Unlock()
		if err == nil {
			s.startTrashCollector()
			return
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-s.trashCollector.stopTrash:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func classifyInitializationError(err error, failures uint) InitializationState {
	for _, temporary := range []error{syscall.EAGAIN, syscall.EBUSY, syscall.EINTR, syscall.EIO, syscall.ENOSPC, syscall.EROFS} {
		if errors.Is(err, temporary) {
			if failures < initializationFailureEscalation {
				return InitializationRetryableFailed
			}
			return InitializationNodeWideFailed
		}
	}
	return InitializationNodeWideFailed
}

func (s *Store) resetIndexState() {
	clear(s.generationManager.metadataByIdentity)
	clear(s.generationManager.leaseIndex)
	clear(s.generationManager.degraded)
	clear(s.projectQuotaRegistry.projectReservations)
	clear(s.projectQuotaRegistry.projectOwnersByID)
	clear(s.projectQuotaRegistry.projectIDByGeneration)
	clear(s.projectQuotaRegistry.unknownProjectReservations)
	clear(s.trashCollector.trashMetadata)
	s.projectQuotaRegistry.projectRegistryDamaged = false
	s.projectQuotaRegistry.projectRegistryDirty = false
	s.pressureManager.pressureActive = false
	s.pressureManager.pressureState = pressureStateNormal
	s.generationManager.retiredGenerationCount = 0
	s.trashCollector.trashCursor = ""
}

func (s *Store) startTrashCollector() {
	s.trashCollector.collectorMu.Lock()
	defer s.trashCollector.collectorMu.Unlock()
	if s.trashCollector.collectorStarted || s.trashCollector.collectorFinished {
		return
	}
	select {
	case <-s.trashCollector.stopTrash:
		s.trashCollector.collectorFinished = true
		close(s.trashCollector.trashDone)
	default:
		s.trashCollector.collectorStarted = true
		go s.trashCollector.run(s)
	}
}

func (s *Store) finishTrashCollector() {
	s.trashCollector.collectorMu.Lock()
	defer s.trashCollector.collectorMu.Unlock()
	if s.trashCollector.collectorFinished {
		return
	}
	s.trashCollector.collectorFinished = true
	close(s.trashCollector.trashDone)
}

func (s *Store) stopRequested() bool {
	select {
	case <-s.trashCollector.stopTrash:
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
