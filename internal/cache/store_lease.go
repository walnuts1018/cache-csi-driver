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

func IdentityWithServiceAccount(namespaceUID, serviceAccountUID, cacheClass, cacheClassUID, cacheKey, schema string) (string, error) {
	if namespaceUID == "" || cacheClass == "" || cacheClassUID == "" || cacheKey == "" {
		return "", errors.New("namespace UID, cacheClass, cacheClass UID, and cacheKey are required")
	}
	if serviceAccountUID == "" {
		return "", errors.New("service account UID is required for service-account-scoped cache identity")
	}
	if strings.ContainsRune(serviceAccountUID, '\x00') || strings.ContainsRune(cacheKey, '\x00') {
		return "", errors.New("service account UID or cacheKey contains an invalid character")
	}
	return stableIdentity(namespaceUID + "\x00" + serviceAccountUID + "\x00" + cacheClass + "\x00" + cacheClassUID + "\x00" + cacheKey + "\x00" + schema), nil
}

func (s *Store) Acquire(options AcquireOptions) (string, bool, error) {
	if err := validateAcquireOptions(options); err != nil {
		return "", false, err
	}
	unlockLease := s.lockLease(options.Lease.ID)
	defer unlockLease()
	unlockIdentity := s.lockIdentity(options.Identity)
	defer unlockIdentity()
	return s.acquireLocked(options)
}

func (s *Store) acquireLocked(options AcquireOptions) (string, bool, error) {
	if err := validateAcquireOptions(options); err != nil {
		return "", false, err
	}
	options.Lease.Preparing = true
	s.mu.Lock()
	degradedErr := s.generationManager.degraded[options.Identity]
	s.mu.Unlock()
	if degradedErr != nil {
		return "", false, degradedErr
	}
	if path, found, err := s.existingLeasePath(options); found || err != nil {
		return path, false, err
	}
	if err := s.rejectUnderPressure(); err != nil {
		return "", false, err
	}
	entry := filepath.Join(s.metadataRepository.root, options.Identity)
	entryExists, err := s.prepareAcquireEntry(entry)
	if err != nil {
		return "", false, err
	}
	meta, err := s.metadataRepository.readMetadata(entry)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.markDegraded(options.Identity, err)
		return "", false, fmt.Errorf("read cache metadata: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		return s.acquireWithoutMetadata(entry, entryExists, options)
	}
	return s.acquireFromMetadata(entry, options, meta)
}

func (s *Store) prepareAcquireEntry(entry string) (bool, error) {
	entryExists := true
	if err := s.metadataRepository.stat(entry); errors.Is(err, os.ErrNotExist) {
		entryExists = false
		if err := s.rejectUnderPressure(); err != nil {
			return false, err
		}
	} else if err != nil {
		return false, fmt.Errorf("inspect cache entry: %w", err)
	}
	if err := s.metadataRepository.ensureDirectory(entry); err != nil {
		return false, fmt.Errorf("create cache entry: %w", err)
	}
	return entryExists, nil
}

func (s *Store) acquireWithoutMetadata(entry string, entryExists bool, options AcquireOptions) (string, bool, error) {
	if entryExists {
		if err := s.rejectUnderPressure(); err != nil {
			return "", false, err
		}
	}
	if err := s.discardUntrackedGenerations(entry); err != nil {
		return "", false, fmt.Errorf("discard incomplete cache generations: %w", err)
	}
	meta := Metadata{Identity: options.Identity, Generation: uuid.NewV7().String(), CreatedAt: time.Now().UTC(), Policy: options.Policy}
	return s.createLease(entry, options, meta)
}

func (s *Store) acquireFromMetadata(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	if err := validateMetadata(options.Identity, meta); err != nil {
		s.markDegraded(options.Identity, err)
		return "", false, err
	}
	generationExists, err := s.currentGenerationExists(entry, options.Identity, meta)
	if err != nil {
		return "", false, err
	}
	policyChanged, err := requestedGenerationPolicyChanged(options.Policy, meta.Policy)
	if err != nil {
		return "", false, err
	}
	if err := s.validateSharingPolicy(options, meta); err != nil {
		return "", false, err
	}
	return s.acquireWithPolicy(entry, options, meta, generationExists, policyChanged)
}

func (s *Store) currentGenerationExists(entry, identity string, meta Metadata) (bool, error) {
	generationPath := filepath.Join(entry, generationsDirectoryName, meta.Generation)
	if err := s.metadataRepository.stat(generationPath); errors.Is(err, os.ErrNotExist) {
		if s.activeLeaseCount(meta) > 0 {
			err := fmt.Errorf("active cache leases reference missing generation %q", meta.Generation)
			s.markDegraded(identity, err)
			return false, fmt.Errorf("%w: %v", ErrDegradedMetadata, err)
		}
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("inspect cache generation: %w", err)
	}
	return true, nil
}

func requestedGenerationPolicyChanged(requested, stored Policy) (bool, error) {
	requestedPolicyHash, err := generationPolicyHash(requested)
	if err != nil {
		return false, fmt.Errorf("hash requested cache generation policy: %w", err)
	}
	storedPolicyHash, err := generationPolicyHash(stored)
	if err != nil {
		return false, fmt.Errorf("hash stored cache generation policy: %w", err)
	}
	return requestedPolicyHash != storedPolicyHash, nil
}

func (s *Store) validateSharingPolicy(options AcquireOptions, meta Metadata) error {
	for _, lease := range meta.Leases {
		_, existingPolicy, err := leaseGenerationAndPolicy(meta, lease)
		if err != nil {
			s.markDegraded(options.Identity, err)
			return err
		}
		if options.Policy.SharingPolicy == "Exclusive" || existingPolicy.SharingPolicy == "Exclusive" {
			return ErrExclusivePolicyConflict
		}
	}
	return nil
}

func (s *Store) acquireWithPolicy(entry string, options AcquireOptions, meta Metadata, generationExists, policyChanged bool) (string, bool, error) {
	objectHasLeases := s.activeLeaseCount(meta) > 0
	quotaModeChanged := meta.Policy.QuotaEnabled != options.Policy.QuotaEnabled
	quotaPolicyChanged := quotaModeChanged || meta.Policy.QuotaEnabled && meta.Policy.MaxBytes != options.Policy.MaxBytes
	if quotaPolicyChanged && objectHasLeases && !policyChanged {
		return "", false, ErrQuotaPolicyConflict
	}
	if !objectHasLeases && (policyChanged || quotaModeChanged || meta.Dirty) {
		if err := s.rejectUnderPressure(); err != nil {
			return "", false, err
		}
		return s.discardAndCreateGeneration(entry, options, meta)
	}
	if objectHasLeases && policyChanged {
		if err := s.rejectUnderPressure(); err != nil {
			return "", false, err
		}
		return s.transitionActiveGeneration(entry, options, meta)
	}
	meta, err := s.updateRuntimePolicy(entry, options, meta, objectHasLeases, policyChanged, quotaPolicyChanged)
	if err != nil {
		return "", false, err
	}
	if !generationExists {
		if err := s.rejectUnderPressure(); err != nil {
			return "", false, err
		}
	}
	if generationExists && !objectHasLeases {
		options.Policy = meta.Policy
	}
	return s.acquireExisting(entry, options, meta)
}

func (s *Store) updateRuntimePolicy(entry string, options AcquireOptions, meta Metadata, objectHasLeases, policyChanged, quotaPolicyChanged bool) (Metadata, error) {
	if !objectHasLeases && meta.Policy != options.Policy {
		meta.Policy = options.Policy
		if err := s.writeMetadata(entry, meta); err != nil {
			return meta, fmt.Errorf("update cache policy without changing generation: %w", err)
		}
	}
	if objectHasLeases && !policyChanged && !quotaPolicyChanged && meta.Policy != options.Policy {
		// Runtime policy changes apply to future admissions while published leases
		// retain their mount flags and the quota already attached to their generation.
		meta.Policy = options.Policy
		if err := s.writeMetadata(entry, meta); err != nil {
			return meta, fmt.Errorf("update runtime cache policy: %w", err)
		}
	}
	return meta, nil
}

func (s *Store) discardAndCreateGeneration(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	oldGeneration := RetiredGeneration{
		State:           GenerationStateRetired,
		Generation:      meta.Generation,
		ProjectID:       meta.ProjectID,
		ProjectAssigned: meta.ProjectAssigned,
		QuotaBytes:      meta.QuotaBytes,
		Policy:          meta.Policy,
	}
	if err := s.detachGenerationToTrash(filepath.Join(entry, generationsDirectoryName, meta.Generation), options.Identity, oldGeneration); err != nil {
		return "", false, fmt.Errorf("discard cache generation before policy update: %w", err)
	}
	meta.Generation = uuid.NewV7().String()
	meta.GenerationState = GenerationStateActive
	meta.CreatedAt = time.Now().UTC()
	meta.Policy = options.Policy
	meta.Dirty = false
	meta.ProjectID = 0
	meta.ProjectAssigned = false
	meta.QuotaBytes = 0
	path, created, err := s.createLease(entry, options, meta)
	if err != nil {
		relative, relativeErr := s.metadataRepository.relative(filepath.Join(entry, generationsDirectoryName, meta.Generation))
		if relativeErr == nil {
			err = errors.Join(err, s.metadataRepository.rootFS.RemoveAll(relative))
		}
		return "", false, fmt.Errorf("create cache generation after policy update: %w", err)
	}
	return path, created, nil
}

func (s *Store) transitionActiveGeneration(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	oldGeneration := meta.Generation
	retired := RetiredGeneration{
		State:           GenerationStateRetiring,
		Generation:      oldGeneration,
		ProjectID:       meta.ProjectID,
		ProjectAssigned: meta.ProjectAssigned,
		QuotaBytes:      meta.QuotaBytes,
		Policy:          meta.Policy,
	}
	for index := range meta.Leases {
		if meta.Leases[index].Generation == "" || meta.Leases[index].Generation == oldGeneration {
			meta.Leases[index].Generation = oldGeneration
		}
	}
	meta.Retired = append(meta.Retired, retired)
	meta.Generation = uuid.NewV7().String()
	meta.GenerationState = GenerationStateActive
	meta.Policy = options.Policy
	meta.CreatedAt = time.Now().UTC()
	meta.Dirty = false
	meta.ProjectID = 0
	meta.ProjectAssigned = false
	meta.QuotaBytes = 0
	path, created, err := s.createLease(entry, options, meta)
	if err != nil {
		relative, relativeErr := s.metadataRepository.relative(filepath.Join(entry, generationsDirectoryName, meta.Generation))
		if relativeErr == nil {
			err = errors.Join(err, s.metadataRepository.rootFS.RemoveAll(relative))
		}
		return "", false, fmt.Errorf("create cache generation while retiring incompatible content: %w", err)
	}
	return path, created, nil
}

func (s *Store) rejectUnderPressure() error {
	usage, err := filesystemUsage(s.metadataRepository.root)
	if err != nil {
		return fmt.Errorf("inspect cache pool pressure: %w", err)
	}
	s.mu.Lock()
	pressureActive := s.updatePressure(usage)
	s.mu.Unlock()
	if pressureActive {
		return ErrPressureActive
	}
	return nil
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
		if err := s.writeMetadata(filepath.Join(s.metadataRepository.root, options.Identity), existing); err != nil {
			return "", true, fmt.Errorf("mark cache lease as preparing: %w", err)
		}
	}
	return filepath.Join(s.metadataRepository.root, options.Identity, generationsDirectoryName, generation), true, nil
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
	generationPath := filepath.Join(entry, generationsDirectoryName, meta.Generation)
	newGeneration := false
	if statErr := s.metadataRepository.stat(generationPath); errors.Is(statErr, os.ErrNotExist) {
		if s.activeLeaseCount(meta) > 0 {
			err := fmt.Errorf("active cache leases reference missing generation %q", meta.Generation)
			s.markDegraded(options.Identity, err)
			return "", false, fmt.Errorf("%w: %v", ErrDegradedMetadata, err)
		}
		meta.Generation = uuid.NewV7().String()
		meta.CreatedAt = time.Now().UTC()
		meta.ProjectAssigned = false
		meta.QuotaBytes = 0
		meta.Dirty = false
		newGeneration = true
	} else if statErr != nil {
		return "", false, fmt.Errorf("inspect cache generation: %w", statErr)
	}
	for _, lease := range meta.Leases {
		if lease.ID == options.Lease.ID && (lease.Generation == "" || lease.Generation == meta.Generation) {
			if lease.Target != options.Lease.Target {
				return "", false, errors.New("cache lease already exists for a different target")
			}
			index := slices.IndexFunc(meta.Leases, func(existing Lease) bool { return existing.ID == options.Lease.ID })
			meta.Leases[index] = options.Lease
			if err := s.writeMetadata(entry, meta); err != nil {
				return "", false, err
			}
			return filepath.Join(entry, generationsDirectoryName, meta.Generation), false, nil
		}
	}
	if newGeneration {
		meta.Policy = options.Policy
	}
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

func (s *Store) createLease(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	generationPath := filepath.Join(entry, generationsDirectoryName, meta.Generation)
	if err := s.metadataRepository.ensureDirectory(generationPath); err != nil {
		return "", false, fmt.Errorf("create cache generation: %w", err)
	}
	if s.activeLeaseCount(meta) == 0 {
		relative, err := s.metadataRepository.relative(generationPath)
		if err != nil {
			return "", false, err
		}
		if err := s.metadataRepository.rootFS.Chmod(relative, 0o700); err != nil {
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
	unlockLease := s.lockLease(leaseID)
	defer unlockLease()
	identity, exists := s.identityForLease(leaseID)
	if !exists {
		return errors.New("cache lease does not exist")
	}
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
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
	return s.writeMetadata(filepath.Join(s.metadataRepository.root, identity), meta)
}

func (s *Store) LeaseDetails(leaseID string) (string, Lease, string, Policy, bool, error) {
	unlockLease := s.lockLease(leaseID)
	defer unlockLease()
	identity, exists := s.identityForLease(leaseID)
	if !exists {
		return "", Lease{}, "", Policy{}, false, nil
	}
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
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
		return identity, lease, filepath.Join(s.metadataRepository.root, identity, generationsDirectoryName, generation), policy, true, nil
	}
	return "", Lease{}, "", Policy{}, false, nil
}

func (s *Store) QuotaState(identity string, maxBytes int64) (uint32, bool, bool, error) {
	if !validIdentity(identity) || maxBytes <= 0 {
		return 0, false, false, errors.New("valid cache identity and positive quota limit are required")
	}
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
	entry := filepath.Join(s.metadataRepository.root, identity)
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return 0, false, false, err
		}
		return 0, false, false, err
	}
	if meta.ProjectID == 0 {
		projectID, err := s.projectID(identity, meta.Generation)
		if err != nil {
			return 0, false, false, err
		}
		meta.ProjectID = projectID
	} else {
		s.projectQuotaRegistry.projectRegistryMu.Lock()
		s.mu.Lock()
		s.addProjectReservation(meta.ProjectID, projectReservation{Identity: identity, Generation: meta.Generation})
		s.mu.Unlock()
		if err := s.persistProjectReservationsLocked(); err != nil {
			s.projectQuotaRegistry.projectRegistryMu.Unlock()
			return 0, false, false, fmt.Errorf("synchronize project ID reservation before quota reuse: %w", err)
		}
		s.projectQuotaRegistry.projectRegistryMu.Unlock()
	}
	if err := s.writeMetadata(entry, meta); err != nil {
		return 0, false, false, err
	}
	return meta.ProjectID, !meta.ProjectAssigned, meta.QuotaBytes != maxBytes, nil
}

func (s *Store) Expose(identity string) (string, error) {
	if !validIdentity(identity) {
		return "", errors.New("invalid cache identity")
	}
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
	entry := filepath.Join(s.metadataRepository.root, identity)
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		return "", err
	}
	generationPath := filepath.Join(entry, generationsDirectoryName, meta.Generation)
	relative, err := s.metadataRepository.relative(generationPath)
	if err != nil {
		return "", err
	}
	info, err := s.metadataRepository.rootFS.Lstat(relative)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("cache generation is not a real directory")
	}
	if err := s.metadataRepository.rootFS.Chmod(relative, 0o777); err != nil {
		return "", fmt.Errorf("expose cache generation: %w", err)
	}
	return generationPath, nil
}

func (s *Store) MarkQuotaApplied(identity string, maxBytes int64) error {
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
	entry := filepath.Join(s.metadataRepository.root, identity)
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		return err
	}
	meta.QuotaBytes = maxBytes
	return s.writeMetadata(entry, meta)
}

func (s *Store) MarkProjectAssigned(identity string) error {
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
	entry := filepath.Join(s.metadataRepository.root, identity)
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		return err
	}
	meta.ProjectAssigned = true
	return s.writeMetadata(entry, meta)
}

func (s *Store) Release(leaseID, target string) error {
	unlockLease := s.lockLease(leaseID)
	defer unlockLease()
	identity, exists := s.identityForLease(leaseID)
	if !exists {
		return nil
	}
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
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
	entry := filepath.Join(s.metadataRepository.root, identity)
	retiredIndex := slices.IndexFunc(meta.Retired, func(retired RetiredGeneration) bool { return retired.Generation == generation })
	if retiredIndex >= 0 && !s.hasGenerationLeases(meta, generation) {
		meta.Retired[retiredIndex].State = GenerationStateRetired
		if err := s.writeMetadata(filepath.Join(s.metadataRepository.root, identity), meta); err != nil {
			s.markDegraded(identity, err)
			return fmt.Errorf("record cache generation retirement: %w", err)
		}
		retired := meta.Retired[retiredIndex]
		generationPath := filepath.Join(entry, generationsDirectoryName, generation)
		if err := s.detachGenerationToTrash(generationPath, identity, retired); err != nil {
			s.markDegraded(identity, err)
			return fmt.Errorf("detach released cache generation: %w", err)
		}
		meta.Retired = slices.Delete(meta.Retired, retiredIndex, retiredIndex+1)
	}
	if err := s.writeMetadata(entry, meta); err != nil {
		s.markDegraded(identity, err)
		return err
	}
	return nil
}

func (s *Store) hasGenerationLeases(meta Metadata, generation string) bool {
	return slices.ContainsFunc(meta.Leases, func(lease Lease) bool {
		return lease.Generation == generation || lease.Generation == "" && generation == meta.Generation
	})
}

func (s *Store) findLease(leaseID string) (string, Metadata, bool, error) {
	identity, exists := s.identityForLease(leaseID)
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

func (s *Store) identityForLease(leaseID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, exists := s.generationManager.leaseIndex[leaseID]
	return identity, exists
}

func (s *Store) ReleaseTarget(leaseID string) (string, bool, error) {
	unlockLease := s.lockLease(leaseID)
	defer unlockLease()
	identity, exists := s.identityForLease(leaseID)
	if !exists {
		return "", false, nil
	}
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
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
