package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"uuid"

	"golang.org/x/sys/unix"
)

const metadataName = ".cache-csi.json"
const trashDirectoryName = ".trash"
const trashBatchSize = 16

var ErrQuotaPolicyConflict = errors.New("active cache generation cannot change its effective quota")

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

type Lease struct {
	ID        string `json:"id"`
	Target    string `json:"target"`
	Namespace string `json:"namespace"`
	PodName   string `json:"podName"`
	PodUID    string `json:"podUID"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
	NoExec    bool   `json:"noExec,omitempty"`
}

type Policy struct {
	ClassName          string        `json:"className"`
	ClassUID           string        `json:"classUID"`
	NoExec             bool          `json:"noExec"`
	SchemaVersion      string        `json:"schemaVersion"`
	CrashRecoveryReuse bool          `json:"crashRecoveryReuse"`
	EvictRunning       bool          `json:"evictRunning"`
	QuotaEnabled       bool          `json:"quotaEnabled"`
	MaxBytes           int64         `json:"maxBytes"`
	Retention          time.Duration `json:"retention"`
}

type policyAlias Policy

type persistedPolicy struct {
	policyAlias
	Retention int64 `json:"retention"`
}

func (policy Policy) MarshalJSON() ([]byte, error) {
	return json.Marshal(persistedPolicy{policyAlias: policyAlias(policy), Retention: int64(policy.Retention)})
}

func (policy *Policy) UnmarshalJSON(data []byte) error {
	var persisted persistedPolicy
	if err := json.Unmarshal(data, &persisted); err != nil {
		return err
	}
	*policy = Policy(persisted.policyAlias)
	policy.Retention = time.Duration(persisted.Retention)
	return nil
}

type Metadata struct {
	Identity        string    `json:"identity"`
	Generation      string    `json:"generation"`
	CreatedAt       time.Time `json:"createdAt"`
	LastUsed        time.Time `json:"lastUsed"`
	Leases          []Lease   `json:"leases,omitempty"`
	Policy          Policy    `json:"policy"`
	Dirty           bool      `json:"dirty"`
	ProjectID       uint32    `json:"projectID,omitempty"`
	ProjectAssigned bool      `json:"projectAssigned,omitempty"`
	QuotaBytes      int64     `json:"quotaBytes,omitempty"`
}

type AcquireOptions struct {
	Identity string
	Lease    Lease
	Policy   Policy
}

type Store struct {
	root           string
	rootFS         *os.Root
	pressure       PressureConfig
	projectIDStart uint32
	projectIDCount uint32
	pressureActive bool
	mu             sync.Mutex
	stopTrash      chan struct{}
	trashDone      chan struct{}
	trashRequests  chan chan error
	closeOnce      sync.Once
	closeErr       error
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

func (s *Store) stat(path string) (os.FileInfo, error) {
	relative, err := s.relative(path)
	if err != nil {
		return nil, err
	}
	return s.rootFS.Stat(relative)
}

func (s *Store) ensureDirectory(path string, mode os.FileMode) error {
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	if err := s.rootFS.MkdirAll(relative, mode); err != nil {
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
	if err := ensureDirectory(root, 0o700); err != nil {
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
		root:           root,
		rootFS:         rootFS,
		pressure:       options.Pressure,
		projectIDStart: options.ProjectIDStart,
		projectIDCount: options.ProjectIDCount,
		stopTrash:      make(chan struct{}),
		trashDone:      make(chan struct{}),
		trashRequests:  make(chan chan error),
	}
	go store.runTrashCollector()
	return store, nil
}

func validatePressure(pressure PressureConfig) error {
	for _, threshold := range []int{pressure.HighFreePercent, pressure.LowFreePercent, pressure.HighInodeFreePercent, pressure.LowInodeFreePercent} {
		if threshold < 0 || threshold > 100 {
			return errors.New("pressure percentages must be between 0 and 100")
		}
	}
	if !validWatermarks(pressure.HighFreePercent, pressure.LowFreePercent) || !validWatermarks(pressure.HighInodeFreePercent, pressure.LowInodeFreePercent) {
		return errors.New("pressure high watermarks must be greater than paired low watermarks")
	}
	return nil
}

func validWatermarks(high, low int) bool {
	return (high == 0 && low == 0) || (high > 0 && low > 0 && high > low)
}

func (s *Store) runTrashCollector() {
	defer close(s.trashDone)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	_ = s.cleanupTrashBatch()
	for {
		select {
		case <-s.stopTrash:
			return
		case <-ticker.C:
			_ = s.cleanupTrashBatch()
		case response := <-s.trashRequests:
			response <- s.cleanupTrashBatch()
		}
	}
}

func (s *Store) CleanupTrash() error {
	response := make(chan error, 1)
	select {
	case <-s.stopTrash:
		return errors.New("cache store is closed")
	case s.trashRequests <- response:
	}
	return <-response
}

func (s *Store) cleanupTrashBatch() error {
	entries, err := s.readDir(filepath.Join(s.root, trashDirectoryName))
	if err != nil {
		return err
	}
	var cleanupErr error
	for _, entry := range entries[:min(len(entries), trashBatchSize)] {
		if err := s.removeAll(filepath.Join(s.root, trashDirectoryName, entry.Name())); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	return cleanupErr
}

func (s *Store) detachToTrash(path string) error {
	trashPath := filepath.Join(s.root, trashDirectoryName, uuid.NewV7().String())
	if err := s.rename(path, trashPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, directory := range []string{filepath.Dir(path), filepath.Join(s.root, trashDirectoryName)} {
		relative, err := s.relative(directory)
		if err != nil {
			return err
		}
		handle, err := s.rootFS.Open(relative)
		if err != nil {
			return err
		}
		syncErr := handle.Sync()
		closeErr := handle.Close()
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func Identity(namespaceUID, cacheClass, cacheClassUID, cacheKey, schema string) (string, error) {
	if namespaceUID == "" || cacheClass == "" || cacheClassUID == "" || cacheKey == "" {
		return "", errors.New("namespace UID, cacheClass, cacheClass UID, and cacheKey are required")
	}
	if strings.ContainsRune(cacheKey, '\x00') {
		return "", errors.New("cacheKey contains an invalid character")
	}
	return stableIdentity(namespaceUID + "\x00" + cacheClass + "\x00" + cacheClassUID + "\x00" + cacheKey + "\x00" + schema), nil
}

func (s *Store) Acquire(options AcquireOptions) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validIdentity(options.Identity) || options.Lease.ID == "" || len(options.Lease.ID) > 1024 || strings.ContainsRune(options.Lease.ID, '\x00') || !filepath.IsAbs(options.Lease.Target) {
		return "", false, errors.New("invalid cache identity, lease ID, or target path")
	}
	if existingIdentity, existing, found, err := s.findLease(options.Lease.ID); err != nil {
		return "", false, err
	} else if found && (existingIdentity != options.Identity || !slices.ContainsFunc(existing.Leases, func(lease Lease) bool { return lease.ID == options.Lease.ID && lease.Target == options.Lease.Target })) {
		return "", false, errors.New("cache lease ID is already in use")
	}
	entry := filepath.Join(s.root, options.Identity)
	if err := s.ensureDirectory(entry, 0o700); err != nil {
		return "", false, fmt.Errorf("create cache entry: %w", err)
	}
	meta, err := s.readMetadata(entry)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", false, fmt.Errorf("read cache metadata: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		if err := s.discardUntrackedGenerations(entry); err != nil {
			return "", false, fmt.Errorf("discard incomplete cache generations: %w", err)
		}
		meta = Metadata{Identity: options.Identity, Generation: uuid.NewV7().String(), CreatedAt: time.Now().UTC(), Policy: options.Policy}
	} else {
		if meta.Identity != options.Identity || meta.Generation == "" {
			return "", false, errors.New("cache metadata identity is inconsistent")
		}
		if len(meta.Leases) > 0 && (meta.Policy.QuotaEnabled != options.Policy.QuotaEnabled || meta.Policy.QuotaEnabled && meta.Policy.MaxBytes != options.Policy.MaxBytes) {
			return "", false, ErrQuotaPolicyConflict
		}
		if len(meta.Leases) == 0 && (meta.Policy.QuotaEnabled != options.Policy.QuotaEnabled || meta.Dirty && !meta.Policy.CrashRecoveryReuse) {
			if err := s.detachToTrash(entry); err != nil {
				return "", false, fmt.Errorf("discard cache before generation transition: %w", err)
			}
			if err := s.ensureDirectory(entry, 0o700); err != nil {
				return "", false, fmt.Errorf("create cache entry after generation transition: %w", err)
			}
			meta = Metadata{Identity: options.Identity, Generation: uuid.NewV7().String(), CreatedAt: time.Now().UTC(), Policy: options.Policy}
		} else {
			return s.acquireExisting(entry, options, meta)
		}
	}
	return s.createLease(entry, options, meta)
}

func (s *Store) acquireExisting(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	generationPath := filepath.Join(entry, "generations", meta.Generation)
	if _, statErr := s.stat(generationPath); errors.Is(statErr, os.ErrNotExist) {
		meta.Generation = uuid.NewV7().String()
		meta.CreatedAt = time.Now().UTC()
		meta.ProjectAssigned = false
		meta.QuotaBytes = 0
		meta.Leases = nil
		meta.Dirty = false
	} else if statErr != nil {
		return "", false, fmt.Errorf("inspect cache generation: %w", statErr)
	}
	for _, lease := range meta.Leases {
		if lease.ID == options.Lease.ID {
			if lease.Target != options.Lease.Target {
				return "", false, errors.New("cache lease already exists for a different target")
			}
			meta.Policy = options.Policy
			index := slices.IndexFunc(meta.Leases, func(existing Lease) bool { return existing.ID == options.Lease.ID })
			meta.Leases[index] = options.Lease
			if err := s.writeMetadata(entry, meta); err != nil {
				return "", false, err
			}
			return filepath.Join(entry, "generations", meta.Generation), false, nil
		}
	}
	meta.Policy = options.Policy
	return s.createLease(entry, options, meta)
}

func (s *Store) createLease(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	generationPath := filepath.Join(entry, "generations", meta.Generation)
	if err := s.ensureDirectory(generationPath, 0o700); err != nil {
		return "", false, fmt.Errorf("create cache generation: %w", err)
	}
	if len(meta.Leases) == 0 {
		relative, err := s.relative(generationPath)
		if err != nil {
			return "", false, err
		}
		if err := s.rootFS.Chmod(relative, 0o700); err != nil {
			return "", false, fmt.Errorf("restrict cache generation before preparation: %w", err)
		}
	}
	meta.LastUsed = time.Now().UTC()
	meta.Leases = append(meta.Leases, options.Lease)
	meta.Dirty = true
	if err := s.writeMetadata(entry, meta); err != nil {
		return "", false, fmt.Errorf("persist cache lease: %w", err)
	}
	return generationPath, true, nil
}

func (s *Store) discardUntrackedGenerations(entry string) error {
	entryContents, err := s.readDir(entry)
	if err != nil {
		return err
	}
	for _, item := range entryContents {
		if strings.HasPrefix(item.Name(), ".metadata-") {
			if err := s.detachToTrash(filepath.Join(entry, item.Name())); err != nil {
				return err
			}
		}
	}
	generations := filepath.Join(entry, "generations")
	entries, err := s.readDir(generations)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, generation := range entries {
		if err := s.detachToTrash(filepath.Join(generations, generation.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) LeaseDetails(leaseID string) (string, Lease, string, Policy, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, meta, found, err := s.findLease(leaseID)
	if err != nil || !found {
		return "", Lease{}, "", Policy{}, found, err
	}
	for _, lease := range meta.Leases {
		if lease.ID == leaseID {
			return identity, lease, filepath.Join(s.root, identity, "generations", meta.Generation), meta.Policy, true, nil
		}
	}
	return "", Lease{}, "", Policy{}, false, nil
}

func (s *Store) QuotaState(identity string, maxBytes int64) (uint32, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validIdentity(identity) || maxBytes <= 0 {
		return 0, false, false, errors.New("valid cache identity and positive quota limit are required")
	}
	entry := filepath.Join(s.root, identity)
	meta, err := s.readMetadata(entry)
	if err != nil {
		return 0, false, false, err
	}
	if meta.ProjectID == 0 {
		projectID, err := s.projectIDLocked(identity)
		if err != nil {
			return 0, false, false, err
		}
		meta.ProjectID = projectID
	}
	if err := s.writeMetadata(entry, meta); err != nil {
		return 0, false, false, err
	}
	return meta.ProjectID, !meta.ProjectAssigned, meta.QuotaBytes != maxBytes, nil
}

func (s *Store) Expose(identity string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validIdentity(identity) {
		return "", errors.New("invalid cache identity")
	}
	entry := filepath.Join(s.root, identity)
	meta, err := s.readMetadata(entry)
	if err != nil {
		return "", err
	}
	generationPath := filepath.Join(entry, "generations", meta.Generation)
	relative, err := s.relative(generationPath)
	if err != nil {
		return "", err
	}
	info, err := s.rootFS.Lstat(relative)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("cache generation is not a real directory")
	}
	if err := s.rootFS.Chmod(relative, 0o777); err != nil {
		return "", fmt.Errorf("expose cache generation: %w", err)
	}
	return generationPath, nil
}

func (s *Store) MarkQuotaApplied(identity string, maxBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := filepath.Join(s.root, identity)
	meta, err := s.readMetadata(entry)
	if err != nil {
		return err
	}
	meta.QuotaBytes = maxBytes
	return s.writeMetadata(entry, meta)
}

func (s *Store) MarkProjectAssigned(identity string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := filepath.Join(s.root, identity)
	meta, err := s.readMetadata(entry)
	if err != nil {
		return err
	}
	meta.ProjectAssigned = true
	return s.writeMetadata(entry, meta)
}

func (s *Store) projectIDLocked(identity string) (uint32, error) {
	used := map[uint32]struct{}{}
	entries, err := s.readDir(s.root)
	if err != nil {
		return 0, err
	}
	for _, current := range entries {
		if !current.IsDir() || current.Name() == identity || current.Name() == trashDirectoryName {
			continue
		}
		other, err := s.readMetadata(filepath.Join(s.root, current.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("read cache metadata while allocating project ID: %w", err)
		}
		if other.ProjectID != 0 {
			used[other.ProjectID] = struct{}{}
		}
	}
	trashEntries, err := s.readDir(filepath.Join(s.root, trashDirectoryName))
	if err != nil {
		return 0, err
	}
	for _, current := range trashEntries {
		if !current.IsDir() {
			continue
		}
		other, err := s.readMetadata(filepath.Join(s.root, trashDirectoryName, current.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("read trashed cache metadata while allocating project ID: %w", err)
		}
		if other.ProjectID != 0 {
			used[other.ProjectID] = struct{}{}
		}
	}
	hash, _ := hex.DecodeString(identity[:8])
	hashValue := uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
	offset := hashValue % s.projectIDCount
	projectID := s.projectIDStart + offset
	for range s.projectIDCount {
		if _, exists := used[projectID]; !exists {
			return projectID, nil
		}
		offset = (offset + 1) % s.projectIDCount
		projectID = s.projectIDStart + offset
	}
	return 0, errors.New("no XFS project IDs are available")
}

func ensureDirectory(path string, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
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

func (s *Store) Release(leaseID, target string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, meta, found, err := s.findLease(leaseID)
	if err != nil || !found {
		return err
	}
	if target != "" && !slices.ContainsFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID && lease.Target == target }) {
		return errors.New("cache lease target does not match")
	}
	meta.Leases = slices.DeleteFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID })
	meta.LastUsed = time.Now().UTC()
	meta.Dirty = len(meta.Leases) > 0
	return s.writeMetadata(filepath.Join(s.root, identity), meta)
}

func (s *Store) findLease(leaseID string) (string, Metadata, bool, error) {
	entries, err := s.readDir(s.root)
	if err != nil {
		return "", Metadata{}, false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		meta, err := s.readMetadata(filepath.Join(s.root, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", Metadata{}, false, fmt.Errorf("read cache metadata while finding lease: %w", err)
		}
		if slices.ContainsFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID }) {
			return entry.Name(), meta, true, nil
		}
	}
	return "", Metadata{}, false, nil
}

func (s *Store) ReleaseTarget(leaseID string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, meta, found, err := s.findLease(leaseID)
	if err != nil || !found {
		return "", found, err
	}
	for _, lease := range meta.Leases {
		if lease.ID == leaseID {
			return lease.Target, true, nil
		}
	}
	return "", false, nil
}

func (s *Store) RecoverLeases(isMounted func(string) (bool, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.readDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(s.root, entry.Name())
		meta, err := s.readMetadata(path)
		if errors.Is(err, os.ErrNotExist) {
			if err := s.discardUntrackedGenerations(path); err != nil {
				return fmt.Errorf("discard incomplete cache generations during recovery: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("read cache metadata during recovery: %w", err)
		}
		wasDirty := meta.Dirty
		active := make([]Lease, 0, len(meta.Leases))
		for _, lease := range meta.Leases {
			mounted, err := isMounted(lease.Target)
			if err != nil {
				return err
			}
			if mounted {
				active = append(active, lease)
			}
		}
		if len(active) == len(meta.Leases) && !(len(active) == 0 && wasDirty && !meta.Policy.CrashRecoveryReuse) {
			continue
		}
		if len(active) == 0 && wasDirty && !meta.Policy.CrashRecoveryReuse {
			if err := s.detachToTrash(path); err != nil {
				return fmt.Errorf("discard cache object after unclean stop: %w", err)
			}
			if err := s.ensureDirectory(path, 0o700); err != nil {
				return fmt.Errorf("create cache object after recovery: %w", err)
			}
			meta = Metadata{
				Identity:   entry.Name(),
				Generation: uuid.NewV7().String(),
				CreatedAt:  time.Now().UTC(),
				LastUsed:   time.Now().UTC(),
				Policy:     meta.Policy,
			}
			if err := s.writeMetadata(path, meta); err != nil {
				return err
			}
			continue
		}
		meta.Leases = active
		generationPath := filepath.Join(path, "generations", meta.Generation)
		if _, statErr := s.stat(generationPath); errors.Is(statErr, os.ErrNotExist) {
			meta.Generation = uuid.NewV7().String()
			meta.CreatedAt = time.Now().UTC()
			meta.ProjectAssigned = false
			meta.QuotaBytes = 0
		} else if statErr != nil {
			return fmt.Errorf("inspect cache generation during recovery: %w", statErr)
		}
		meta.Dirty = len(active) > 0
		if err := s.writeMetadata(path, meta); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Collect(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.readDir(s.root)
	if err != nil {
		return err
	}
	type candidate struct {
		path string
		meta Metadata
	}
	var candidates []candidate
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == trashDirectoryName {
			continue
		}
		path := filepath.Join(s.root, entry.Name())
		meta, err := s.readMetadata(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read cache metadata during collection: %w", err)
		}
		if len(meta.Leases) == 0 {
			candidates = append(candidates, candidate{path, meta})
		}
	}
	fs, err := filesystemUsage(s.root)
	if err != nil {
		return err
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].meta.LastUsed.Before(candidates[j].meta.LastUsed) })
	pressure := s.updatePressure(fs)
	removed := 0
	for _, item := range candidates {
		expired := item.meta.Policy.Retention > 0 && now.Sub(item.meta.LastUsed) >= item.meta.Policy.Retention
		if !expired && !pressure {
			continue
		}
		if err := s.detachToTrash(item.path); err != nil {
			return fmt.Errorf("remove cache object: %w", err)
		}
		removed++
		if removed >= trashBatchSize {
			break
		}
	}
	return nil
}

func (s *Store) underLowWatermark(fs unix.Statfs_t) bool {
	return below(fs.Bavail, fs.Blocks, s.pressure.LowFreePercent) || below(fs.Ffree, fs.Files, s.pressure.LowInodeFreePercent)
}

func (s *Store) updatePressure(fs unix.Statfs_t) bool {
	if !s.pressureActive {
		s.pressureActive = s.underLowWatermark(fs)
		return s.pressureActive
	}
	bytesRecovered := s.pressure.HighFreePercent == 0 || above(fs.Bavail, fs.Blocks, s.pressure.HighFreePercent)
	inodesRecovered := s.pressure.HighInodeFreePercent == 0 || above(fs.Ffree, fs.Files, s.pressure.HighInodeFreePercent)
	if bytesRecovered && inodesRecovered {
		s.pressureActive = false
	}
	return s.pressureActive
}

func (s *Store) MetadataError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.readDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == trashDirectoryName {
			continue
		}
		if _, err := s.readMetadata(filepath.Join(s.root, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("cache metadata is unreadable")
		}
	}
	return nil
}

func (s *Store) PressureVictims() ([]Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	trash, err := s.readDir(filepath.Join(s.root, trashDirectoryName))
	if err != nil {
		return nil, err
	}
	if len(trash) > 0 {
		return nil, nil
	}
	entries, err := s.readDir(s.root)
	if err != nil {
		return nil, err
	}
	fs, err := filesystemUsage(s.root)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		meta Metadata
		path string
	}
	var candidates []candidate
	underPressure := s.updatePressure(fs)
	if underPressure {
		for _, entry := range entries {
			if !entry.IsDir() || entry.Name() == trashDirectoryName {
				continue
			}
			meta, err := s.readMetadata(filepath.Join(s.root, entry.Name()))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("read cache metadata while checking pressure victims: %w", err)
			}
			if len(meta.Leases) == 0 {
				return nil, nil
			}
		}
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == trashDirectoryName {
			continue
		}
		meta, err := s.readMetadata(filepath.Join(s.root, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read cache metadata while listing pressure victims: %w", err)
		}
		if meta.Policy.EvictRunning && len(meta.Leases) > 0 {
			candidates = append(candidates, candidate{meta, filepath.Join(s.root, entry.Name())})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].meta.LastUsed.Before(candidates[j].meta.LastUsed) })
	if underPressure {
		for _, candidate := range candidates {
			if !candidate.meta.Policy.EvictRunning {
				continue
			}
			leases := slices.Clone(candidate.meta.Leases)
			return leases, nil
		}
	}
	return nil, nil
}

func filesystemUsage(path string) (unix.Statfs_t, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return unix.Statfs_t{}, err
	}
	return stat, nil
}

func below(available, total uint64, percent int) bool {
	return percent > 0 && total > 0 && available*100/total < uint64(percent)
}

func above(available, total uint64, percent int) bool {
	return percent > 0 && total > 0 && available*100/total >= uint64(percent)
}

func (s *Store) readMetadata(entry string) (Metadata, error) {
	relative, err := s.relative(entry)
	if err != nil {
		return Metadata{}, err
	}
	data, err := s.rootFS.ReadFile(filepath.Join(relative, metadataName))
	if err != nil {
		return Metadata{}, err
	}
	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return Metadata{}, err
	}
	return meta, nil
}

func (s *Store) writeMetadata(entry string, meta Metadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	relative, err := s.relative(entry)
	if err != nil {
		return err
	}
	temp := filepath.Join(relative, ".metadata-"+uuid.NewV7().String())
	file, err := s.rootFS.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = s.rootFS.Remove(temp)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = s.rootFS.Remove(temp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = s.rootFS.Remove(temp)
		return err
	}
	if err := s.rootFS.Rename(temp, filepath.Join(relative, metadataName)); err != nil {
		_ = s.rootFS.Remove(temp)
		return err
	}
	dir, err := s.rootFS.Open(relative)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func stableIdentity(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func validIdentity(identity string) bool {
	if len(identity) != 64 {
		return false
	}
	_, err := hex.DecodeString(identity)
	return err == nil
}
