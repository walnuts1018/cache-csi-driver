package cache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"uuid"
)

var ErrLeaseGenerationRetired = errors.New("cache lease belongs to a retired generation")

type AcquireOptions struct {
	Identity string
	Lease    Lease
	Policy   Policy
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

func FallbackIdentity(volumeID string) (string, error) {
	if volumeID == "" || strings.ContainsRune(volumeID, '\x00') {
		return "", errors.New("valid volume ID is required")
	}
	return stableIdentity("fallback\x00" + volumeID), nil
}

func (s *Store) Acquire(options AcquireOptions) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateAcquireOptions(options); err != nil {
		return "", false, err
	}
	options.Lease.Preparing = true
	if err := s.degraded[options.Identity]; err != nil {
		return "", false, err
	}
	if path, found, err := s.existingLeasePath(options); found || err != nil {
		return path, false, err
	}
	entry := filepath.Join(s.root, options.Identity)
	if err := s.ensureDirectory(entry); err != nil {
		return "", false, fmt.Errorf("create cache entry: %w", err)
	}
	meta, err := s.readMetadata(entry)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.markDegraded(options.Identity, err)
		return "", false, fmt.Errorf("read cache metadata: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		if err := s.discardUntrackedGenerations(entry); err != nil {
			return "", false, fmt.Errorf("discard incomplete cache generations: %w", err)
		}
		meta = Metadata{Identity: options.Identity, Generation: uuid.NewV7().String(), CreatedAt: time.Now().UTC(), Policy: options.Policy}
	} else {
		if err := validateMetadata(options.Identity, meta); err != nil {
			s.markDegraded(options.Identity, err)
			return "", false, err
		}
		for _, lease := range meta.Leases {
			_, existingPolicy, err := leaseGenerationAndPolicy(meta, lease)
			if err != nil {
				s.markDegraded(options.Identity, err)
				return "", false, err
			}
			if options.Policy.SharingPolicy == "Exclusive" || existingPolicy.SharingPolicy == "Exclusive" {
				return "", false, ErrExclusivePolicyConflict
			}
		}
		if s.activeLeaseCount(meta) > 0 && (meta.Policy.QuotaEnabled != options.Policy.QuotaEnabled || meta.Policy.QuotaEnabled && meta.Policy.MaxBytes != options.Policy.MaxBytes) {
			return "", false, ErrQuotaPolicyConflict
		}
		if len(meta.Retired) == 0 && s.activeLeaseCount(meta) == 0 && (meta.Policy.QuotaEnabled != options.Policy.QuotaEnabled || meta.Dirty && !meta.Policy.CrashRecoveryReuse) {
			if err := s.detachToTrash(entry); err != nil {
				return "", false, fmt.Errorf("discard cache before generation transition: %w", err)
			}
			if err := s.ensureDirectory(entry); err != nil {
				return "", false, fmt.Errorf("create cache entry after generation transition: %w", err)
			}
			meta = Metadata{Identity: options.Identity, Generation: uuid.NewV7().String(), CreatedAt: time.Now().UTC(), Policy: options.Policy}
		} else {
			return s.acquireExisting(entry, options, meta)
		}
	}
	return s.createLease(entry, options, meta)
}

func validateAcquireOptions(options AcquireOptions) error {
	if !validIdentity(options.Identity) || options.Lease.ID == "" || len(options.Lease.ID) > 1024 || strings.ContainsRune(options.Lease.ID, '\x00') || !filepath.IsAbs(options.Lease.Target) {
		return errors.New("invalid cache identity, lease ID, or target path")
	}
	if !validSharingPolicy(options.Policy.SharingPolicy) {
		return errors.New("unsupported cache sharing policy")
	}
	if options.Policy.Retention < 0 {
		return errors.New("cache retention must not be negative")
	}
	return nil
}

func (s *Store) existingLeasePath(options AcquireOptions) (string, bool, error) {
	existingIdentity, existing, found, err := s.findLease(options.Lease.ID)
	if err != nil || !found {
		return "", found, err
	}
	if existingIdentity != options.Identity {
		return "", true, errors.New("cache lease ID is already in use")
	}
	leaseIndex := slices.IndexFunc(existing.Leases, func(lease Lease) bool { return lease.ID == options.Lease.ID })
	if leaseIndex < 0 || existing.Leases[leaseIndex].Target != options.Lease.Target {
		return "", true, errors.New("cache lease ID is already in use")
	}
	generation, _, err := leaseGenerationAndPolicy(existing, existing.Leases[leaseIndex])
	if err != nil {
		return "", true, err
	}
	if generation != existing.Generation {
		return "", true, ErrLeaseGenerationRetired
	}
	if !existing.Leases[leaseIndex].Preparing {
		existing.Leases[leaseIndex].Preparing = true
		if err := s.writeMetadata(filepath.Join(s.root, options.Identity), existing); err != nil {
			return "", true, fmt.Errorf("mark cache lease as preparing: %w", err)
		}
	}
	return filepath.Join(s.root, options.Identity, "generations", generation), true, nil
}

func leaseGenerationAndPolicy(meta Metadata, lease Lease) (string, Policy, error) {
	generation := lease.Generation
	if generation == "" {
		generation = meta.Generation
	}
	if generation == meta.Generation {
		return generation, meta.Policy, nil
	}
	for _, retired := range meta.Retired {
		if retired.Generation == generation {
			return generation, retired.Policy, nil
		}
	}
	return "", Policy{}, errors.New("cache lease references an unknown generation")
}

func (s *Store) acquireExisting(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	generationPath := filepath.Join(entry, "generations", meta.Generation)
	if statErr := s.stat(generationPath); errors.Is(statErr, os.ErrNotExist) {
		meta.Leases = slices.DeleteFunc(meta.Leases, func(lease Lease) bool {
			return lease.Generation == "" || lease.Generation == meta.Generation
		})
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
		if lease.ID == options.Lease.ID && (lease.Generation == "" || lease.Generation == meta.Generation) {
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

func (s *Store) activeLeaseCount(meta Metadata) int {
	count := 0
	for _, lease := range meta.Leases {
		if lease.Generation == "" || lease.Generation == meta.Generation {
			count++
		}
	}
	return count
}

func (s *Store) hasPreparingGenerationLease(meta Metadata, generation string) bool {
	return slices.ContainsFunc(meta.Leases, func(lease Lease) bool {
		return lease.Preparing && (lease.Generation == generation || lease.Generation == "" && generation == meta.Generation)
	})
}

func (s *Store) createLease(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	generationPath := filepath.Join(entry, "generations", meta.Generation)
	if err := s.ensureDirectory(generationPath); err != nil {
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
	options.Lease.Generation = meta.Generation
	options.Lease.Preparing = true
	meta.Leases = append(meta.Leases, options.Lease)
	meta.Dirty = true
	if err := s.writeMetadata(entry, meta); err != nil {
		return "", false, fmt.Errorf("persist cache lease: %w", err)
	}
	return generationPath, true, nil
}

// BeginPublish はleaseをPreparing状態にし、pressure reclaimからgenerationを保護します。
func (s *Store) BeginPublish(leaseID, target string) error {
	return s.setLeasePreparing(leaseID, target, true)
}

// CommitPublish はmount成功後にleaseをPublished状態へ変更します。
func (s *Store) CommitPublish(leaseID, target string) error {
	return s.setLeasePreparing(leaseID, target, false)
}

func (s *Store) setLeasePreparing(leaseID, target string, preparing bool) error {
	if leaseID == "" || !filepath.IsAbs(target) {
		return errors.New("valid cache lease ID and absolute target path are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, meta, found, err := s.findLease(leaseID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("cache lease does not exist")
	}
	index := slices.IndexFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID })
	if index < 0 || meta.Leases[index].Target != target {
		return errors.New("cache lease target does not match")
	}
	generation, _, err := leaseGenerationAndPolicy(meta, meta.Leases[index])
	if err != nil {
		return err
	}
	if generation != meta.Generation {
		return ErrLeaseGenerationRetired
	}
	if meta.Leases[index].Preparing == preparing {
		return nil
	}
	meta.Leases[index].Preparing = preparing
	if !preparing {
		meta.LastUsed = time.Now().UTC()
	}
	return s.writeMetadata(filepath.Join(s.root, identity), meta)
}

func (s *Store) LeaseDetails(leaseID string) (string, Lease, string, Policy, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, meta, found, err := s.findLease(leaseID)
	if err != nil || !found {
		return "", Lease{}, "", Policy{}, found, err
	}
	for _, lease := range meta.Leases {
		if lease.ID != leaseID {
			continue
		}
		generation, policy, err := leaseGenerationAndPolicy(meta, lease)
		if err != nil {
			s.markDegraded(identity, err)
			return "", Lease{}, "", Policy{}, false, err
		}
		return identity, lease, filepath.Join(s.root, identity, "generations", generation), policy, true, nil
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
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return 0, false, false, err
		}
		return 0, false, false, err
	}
	if meta.ProjectID == 0 {
		projectID, err := s.projectIDLocked(identity, meta.Generation)
		if err != nil {
			return 0, false, false, err
		}
		meta.ProjectID = projectID
	} else {
		s.addProjectReservation(meta.ProjectID, projectReservation{Identity: identity, Generation: meta.Generation})
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
	meta, err := s.readObjectMetadata(identity)
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
	meta, err := s.readObjectMetadata(identity)
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
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		return err
	}
	meta.ProjectAssigned = true
	return s.writeMetadata(entry, meta)
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
	var generation string
	for _, lease := range meta.Leases {
		if lease.ID == leaseID {
			generation = lease.Generation
			if generation == "" {
				generation = meta.Generation
			}
			break
		}
	}
	meta.Leases = slices.DeleteFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID })
	meta.LastUsed = time.Now().UTC()
	meta.Dirty = s.activeLeaseCount(meta) > 0
	entry := filepath.Join(s.root, identity)
	retiredIndex := slices.IndexFunc(meta.Retired, func(retired RetiredGeneration) bool { return retired.Generation == generation })
	if retiredIndex >= 0 && !s.hasGenerationLeases(meta, generation) {
		retired := meta.Retired[retiredIndex]
		generationPath := filepath.Join(entry, "generations", generation)
		if err := s.detachGenerationToTrash(generationPath, identity, retired); err != nil {
			return fmt.Errorf("detach released cache generation: %w", err)
		}
		meta.Retired = slices.Delete(meta.Retired, retiredIndex, retiredIndex+1)
	}
	if meta.Policy.DiscardOnLastRelease && len(meta.Leases) == 0 {
		if err := s.detachToTrash(entry); err != nil {
			return fmt.Errorf("discard released cache object: %w", err)
		}
		return nil
	}
	return s.writeMetadata(entry, meta)
}

func (s *Store) hasGenerationLeases(meta Metadata, generation string) bool {
	return slices.ContainsFunc(meta.Leases, func(lease Lease) bool {
		return lease.Generation == generation || lease.Generation == "" && generation == meta.Generation
	})
}

func (s *Store) findLease(leaseID string) (string, Metadata, bool, error) {
	identity, exists := s.leaseIndex[leaseID]
	if !exists {
		return "", Metadata{}, false, nil
	}
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		return "", Metadata{}, false, fmt.Errorf("read cache metadata while finding lease: %w", err)
	}
	if slices.ContainsFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID }) {
		return identity, meta, true, nil
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
