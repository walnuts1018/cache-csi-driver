package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
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
	ClassName            string        `json:"className"`
	ClassUID             string        `json:"classUID"`
	NoExec               bool          `json:"noExec"`
	SchemaVersion        string        `json:"schemaVersion"`
	CrashRecoveryReuse   bool          `json:"crashRecoveryReuse"`
	EvictRunning         bool          `json:"evictRunning"`
	QuotaEnabled         bool          `json:"quotaEnabled"`
	MaxBytes             int64         `json:"maxBytes"`
	Retention            time.Duration `json:"retention"`
	HighFreePercent      int           `json:"highFreePercent"`
	LowFreePercent       int           `json:"lowFreePercent"`
	HighInodeFreePercent int           `json:"highInodeFreePercent"`
	LowInodeFreePercent  int           `json:"lowInodeFreePercent"`
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
	root string
	mu   sync.Mutex
}

func (s *Store) Root() string { return s.root }

func NewStore(root string) (*Store, error) {
	if !filepath.IsAbs(root) {
		return nil, errors.New("cache root must be absolute")
	}
	root = filepath.Clean(root)
	if err := ensureDirectory(root, 0o700); err != nil {
		return nil, fmt.Errorf("create cache root: %w", err)
	}
	return &Store{root: root}, nil
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
	if err := ensureDirectory(entry, 0o700); err != nil {
		return "", false, fmt.Errorf("create cache entry: %w", err)
	}
	meta, err := readMetadata(entry)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", false, fmt.Errorf("read cache metadata: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		meta = Metadata{Identity: options.Identity, Generation: uuid.NewV7().String(), CreatedAt: time.Now().UTC(), Policy: options.Policy}
	} else {
		if meta.Identity != options.Identity || meta.Generation == "" {
			return "", false, errors.New("cache metadata identity is inconsistent")
		}
		for _, lease := range meta.Leases {
			if lease.ID == options.Lease.ID {
				if lease.Target != options.Lease.Target {
					return "", false, errors.New("cache lease already exists for a different target")
				}
				meta.Policy = options.Policy
				index := slices.IndexFunc(meta.Leases, func(existing Lease) bool { return existing.ID == options.Lease.ID })
				meta.Leases[index] = options.Lease
				if err := writeMetadata(entry, meta); err != nil {
					return "", false, err
				}
				return filepath.Join(entry, "generations", meta.Generation), false, nil
			}
		}
		if meta.Dirty && len(meta.Leases) == 0 && !meta.Policy.CrashRecoveryReuse {
			if err := os.RemoveAll(filepath.Join(entry, "generations", meta.Generation)); err != nil {
				return "", false, fmt.Errorf("discard dirty cache generation: %w", err)
			}
			meta.Generation = uuid.NewV7().String()
			meta.CreatedAt = time.Now().UTC()
			meta.ProjectAssigned = false
			meta.QuotaBytes = 0
		}
		meta.Policy = options.Policy
	}
	generationPath := filepath.Join(entry, "generations", meta.Generation)
	if err := os.MkdirAll(generationPath, 0o777); err != nil {
		return "", false, fmt.Errorf("create cache generation: %w", err)
	}
	if err := os.Chmod(generationPath, 0o777); err != nil {
		return "", false, fmt.Errorf("set cache generation permissions: %w", err)
	}
	meta.LastUsed = time.Now().UTC()
	meta.Leases = append(meta.Leases, options.Lease)
	meta.Dirty = true
	if err := writeMetadata(entry, meta); err != nil {
		return "", false, fmt.Errorf("persist cache lease: %w", err)
	}
	return generationPath, true, nil
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
	entry := filepath.Join(s.root, identity)
	meta, err := readMetadata(entry)
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
	if err := writeMetadata(entry, meta); err != nil {
		return 0, false, false, err
	}
	return meta.ProjectID, !meta.ProjectAssigned, meta.QuotaBytes != maxBytes, nil
}

func (s *Store) MarkQuotaApplied(identity string, maxBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := filepath.Join(s.root, identity)
	meta, err := readMetadata(entry)
	if err != nil {
		return err
	}
	meta.QuotaBytes = maxBytes
	return writeMetadata(entry, meta)
}

func (s *Store) MarkProjectAssigned(identity string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := filepath.Join(s.root, identity)
	meta, err := readMetadata(entry)
	if err != nil {
		return err
	}
	meta.ProjectAssigned = true
	return writeMetadata(entry, meta)
}

func (s *Store) projectIDLocked(identity string) (uint32, error) {
	used := map[uint32]struct{}{}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return 0, err
	}
	for _, current := range entries {
		if !current.IsDir() || current.Name() == identity {
			continue
		}
		other, err := readMetadata(filepath.Join(s.root, current.Name()))
		if err == nil && other.ProjectID != 0 {
			used[other.ProjectID] = struct{}{}
		}
	}
	hash, _ := hex.DecodeString(identity[:8])
	projectID := uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
	projectID = 10000 + projectID%(^uint32(0)-10000)
	for range len(used) + 1 {
		if _, exists := used[projectID]; !exists {
			return projectID, nil
		}
		projectID++
		if projectID < 10000 {
			projectID = 10000
		}
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
	return writeMetadata(filepath.Join(s.root, identity), meta)
}

func (s *Store) findLease(leaseID string) (string, Metadata, bool, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return "", Metadata{}, false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		meta, err := readMetadata(filepath.Join(s.root, entry.Name()))
		if err != nil {
			continue
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
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(s.root, entry.Name())
		meta, err := readMetadata(path)
		if errors.Is(err, os.ErrNotExist) {
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
		if len(active) == len(meta.Leases) {
			continue
		}
		meta.Leases = active
		if len(active) == 0 && !meta.Policy.CrashRecoveryReuse && wasDirty {
			if err := os.RemoveAll(filepath.Join(path, "generations", meta.Generation)); err != nil {
				return fmt.Errorf("discard cache generation after unclean stop: %w", err)
			}
			meta.Generation = uuid.NewV7().String()
			meta.CreatedAt = time.Now().UTC()
			meta.ProjectAssigned = false
			meta.QuotaBytes = 0
		}
		meta.Dirty = len(active) > 0
		if err := writeMetadata(path, meta); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Collect(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	type candidate struct {
		path string
		meta Metadata
	}
	var candidates []candidate
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(s.root, entry.Name())
		meta, err := readMetadata(path)
		if err == nil && len(meta.Leases) == 0 {
			candidates = append(candidates, candidate{path, meta})
		}
	}
	fs, err := filesystemUsage(s.root)
	if err != nil {
		return err
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].meta.LastUsed.Before(candidates[j].meta.LastUsed) })
	pressure := false
	targetFree, targetInodes := 0, 0
	for _, item := range candidates {
		if below(fs.Bavail, fs.Blocks, item.meta.Policy.LowFreePercent) || below(fs.Ffree, fs.Files, item.meta.Policy.LowInodeFreePercent) {
			pressure = true
			targetFree = max(targetFree, item.meta.Policy.HighFreePercent)
			targetInodes = max(targetInodes, item.meta.Policy.HighInodeFreePercent)
		}
	}
	for _, item := range candidates {
		expired := item.meta.Policy.Retention > 0 && now.Sub(item.meta.LastUsed) >= item.meta.Policy.Retention
		if !expired && !pressure {
			continue
		}
		if err := os.RemoveAll(item.path); err != nil {
			return fmt.Errorf("remove cache object: %w", err)
		}
		if err := unix.Statfs(s.root, &fs); err != nil {
			return err
		}
		if pressure && !below(fs.Bavail, fs.Blocks, targetFree) && !below(fs.Ffree, fs.Files, targetInodes) {
			pressure = false
		}
	}
	return nil
}

func (s *Store) MetadataError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := readMetadata(filepath.Join(s.root, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("cache metadata is unreadable")
		}
	}
	return nil
}

func (s *Store) PressureVictims() ([]Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.root)
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
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		meta, err := readMetadata(filepath.Join(s.root, entry.Name()))
		if err == nil && meta.Policy.EvictRunning && len(meta.Leases) > 0 {
			candidates = append(candidates, candidate{meta, filepath.Join(s.root, entry.Name())})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].meta.LastUsed.Before(candidates[j].meta.LastUsed) })
	for _, candidate := range candidates {
		low := below(fs.Bavail, fs.Blocks, candidate.meta.Policy.LowFreePercent) || below(fs.Ffree, fs.Files, candidate.meta.Policy.LowInodeFreePercent)
		if !low {
			continue
		}
		leases := slices.Clone(candidate.meta.Leases)
		return leases, nil
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

func readMetadata(entry string) (Metadata, error) {
	data, err := os.ReadFile(filepath.Join(entry, metadataName))
	if err != nil {
		return Metadata{}, err
	}
	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return Metadata{}, err
	}
	return meta, nil
}

func writeMetadata(entry string, meta Metadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(entry, ".metadata-*")
	if err != nil {
		return err
	}
	temp := file.Name()
	defer func() { _ = os.Remove(temp) }()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp, filepath.Join(entry, metadataName)); err != nil {
		return err
	}
	dir, err := os.Open(entry)
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
