package cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"uuid"
)

func (s *Store) rebuildIndexes() error {
	registryNeedsRebuild := false
	if s.projectQuotaEnabled {
		var err error
		registryNeedsRebuild, err = s.loadProjectRegistry()
		if err != nil {
			return fmt.Errorf("read XFS project ID registry: %w", err)
		}
	}

	entries, err := s.metadataRepository.readDir(s.metadataRepository.root)
	if err != nil {
		return err
	}
	if err := s.rebuildCanonicalIndexes(entries); err != nil {
		return err
	}

	trashEntries, err := s.metadataRepository.readDir(filepath.Join(s.metadataRepository.root, trashDirectoryName))
	if err != nil {
		return err
	}
	if err := s.rebuildTrashIndexes(trashEntries); err != nil {
		return err
	}
	if registryNeedsRebuild {
		s.reserveDegradedProjectIDs()
	}
	if s.projectQuotaEnabled {
		return s.reconcileProjectReservations(entries, trashEntries)
	}
	return nil
}

func (s *Store) rebuildCanonicalIndexes(entries []os.DirEntry) error {
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == trashDirectoryName {
			continue
		}
		path := filepath.Join(s.metadataRepository.root, entry.Name())
		meta, err := s.metadataRepository.readMetadata(path)
		if errors.Is(err, os.ErrNotExist) {
			generations, generationErr := s.metadataRepository.readDir(filepath.Join(path, generationsDirectoryName))
			if generationErr == nil && hasGenerationDirectory(generations) {
				s.markDegraded(entry.Name(), errors.New("cache metadata is missing while generations remain"))
			} else if generationErr != nil && !errors.Is(generationErr, os.ErrNotExist) {
				return fmt.Errorf("inspect cache generations for %s: %w", entry.Name(), generationErr)
			}
			continue
		}
		if err != nil {
			if isFilesystemOperationError(err) {
				return fmt.Errorf("read cache metadata for %s: %w", entry.Name(), err)
			}
			s.markDegraded(entry.Name(), err)
			continue
		}
		if err := validateMetadata(entry.Name(), meta); err != nil {
			s.indexDegradedLeaseIDs(entry.Name(), meta)
			s.markDegraded(entry.Name(), err)
			continue
		}
		if !s.projectQuotaEnabled && metadataHasProjectQuota(meta) {
			return fmt.Errorf("cache object %s uses XFS project quota state but the node backend is directory", entry.Name())
		}
		s.indexMetadata(meta)
	}
	return nil
}

func (s *Store) rebuildTrashIndexes(trashEntries []os.DirEntry) error {
	for _, entry := range trashEntries {
		if !entry.IsDir() {
			continue
		}
		trashPath := filepath.Join(s.metadataRepository.root, trashDirectoryName, entry.Name())
		meta, err := s.metadataRepository.readMetadata(trashPath)
		if err == nil {
			err = validateMetadata(meta.Identity, meta)
		}
		if err != nil {
			if isFilesystemOperationError(err) {
				return fmt.Errorf("read cache trash metadata %s: %w", entry.Name(), err)
			}
			s.markDegraded(filepath.Join(trashDirectoryName, entry.Name()), err)
			if s.projectQuotaEnabled && validIdentity(meta.Identity) {
				s.addUnknownProjectReservation(meta.Identity, entry.Name())
			}
			continue
		}
		s.projectQuotaRegistry.projectRegistryMu.Lock()
		s.mu.Lock()
		s.trashCollector.trashMetadata[entry.Name()] = meta
		s.addMetadataReservations(meta, entry.Name())
		s.mu.Unlock()
		s.projectQuotaRegistry.projectRegistryMu.Unlock()
	}
	return nil
}

func (s *Store) reserveDegradedProjectIDs() {
	s.mu.Lock()
	degradedIdentities := make([]string, 0, len(s.generationManager.degraded))
	for identity := range s.generationManager.degraded {
		degradedIdentities = append(degradedIdentities, identity)
	}
	s.mu.Unlock()
	for _, identity := range degradedIdentities {
		if filepath.Dir(identity) != "." {
			continue
		}
		s.projectQuotaRegistry.projectRegistryMu.Lock()
		s.mu.Lock()
		s.addUnknownProjectReservation(identity, "")
		s.mu.Unlock()
		s.projectQuotaRegistry.projectRegistryMu.Unlock()
	}
}

func isFilesystemOperationError(err error) bool {
	_, ok := errors.AsType[*os.PathError](err)
	return ok
}

func metadataHasProjectQuota(meta Metadata) bool {
	if meta.Policy.QuotaEnabled || meta.ProjectAssigned || meta.ProjectID != 0 || meta.QuotaBytes != 0 {
		return true
	}
	return slices.ContainsFunc(meta.Retired, func(generation RetiredGeneration) bool {
		return generation.ProjectAssigned || generation.ProjectID != 0 || generation.QuotaBytes != 0
	})
}

func hasGenerationDirectory(entries []os.DirEntry) bool {
	return slices.ContainsFunc(entries, func(entry os.DirEntry) bool { return entry.IsDir() })
}

func hasUntrackedObjectPayload(entries []os.DirEntry) bool {
	return slices.ContainsFunc(entries, func(entry os.DirEntry) bool {
		return entry.Name() != generationsDirectoryName && !strings.HasPrefix(entry.Name(), ".metadata-")
	})
}

func (s *Store) discardUntrackedGenerations(entry string) error {
	generations := filepath.Join(entry, generationsDirectoryName)
	entries, err := s.metadataRepository.readDir(generations)
	if errors.Is(err, os.ErrNotExist) {
		entries = nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.markDegraded(filepath.Base(entry), err)
		return err
	}
	if hasGenerationDirectory(entries) {
		cause := errors.New("cache metadata is missing while generations remain")
		s.markDegraded(filepath.Base(entry), cause)
		return fmt.Errorf("%w: %v", ErrDegradedMetadata, cause)
	}
	entryContents, err := s.metadataRepository.readDir(entry)
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
	for _, generation := range entries {
		if generation.IsDir() {
			continue
		}
		if err := s.detachToTrash(filepath.Join(generations, generation.Name())); err != nil {
			return err
		}
	}
	return nil
}

type degradedTargetMatch struct {
	identity string
	source   string
}

func (s *Store) FindDegradedGenerationForTarget(target string, sameSource func(source, target string) (bool, error)) (string, string, bool, error) {
	if target == "" || !filepath.IsAbs(target) || sameSource == nil {
		return "", "", false, errors.New("absolute target path and source matcher are required")
	}
	match, err := s.findDegradedCanonicalTarget(target, sameSource)
	if err != nil {
		return "", "", false, err
	}
	match, err = s.findDegradedTrashTarget(target, sameSource, match)
	if err != nil {
		return "", "", false, err
	}
	return match.identity, match.source, match.source != "", nil
}

func (s *Store) findDegradedCanonicalTarget(target string, sameSource func(source, target string) (bool, error)) (degradedTargetMatch, error) {
	s.mu.Lock()
	identities := make([]string, 0, len(s.generationManager.degraded))
	for identity := range s.generationManager.degraded {
		if validIdentity(identity) {
			identities = append(identities, identity)
		}
	}
	s.mu.Unlock()
	var match degradedTargetMatch
	for _, identity := range identities {
		unlockIdentity := s.lockIdentity(identity)
		s.mu.Lock()
		_, degraded := s.generationManager.degraded[identity]
		s.mu.Unlock()
		if !degraded {
			unlockIdentity()
			continue
		}
		objectPath := filepath.Join(s.metadataRepository.root, identity)
		generations, err := s.metadataRepository.readDir(filepath.Join(objectPath, generationsDirectoryName))
		if errors.Is(err, os.ErrNotExist) {
			generations = nil
		} else if err != nil {
			unlockIdentity()
			return degradedTargetMatch{}, fmt.Errorf("inspect degraded cache generations: %w", err)
		}
		for _, source := range generationSourcePaths(objectPath, generations) {
			match, err = matchDegradedTarget(match, identity, source, target, sameSource, "degraded cache generation")
			if err != nil {
				unlockIdentity()
				return degradedTargetMatch{}, err
			}
		}
		unlockIdentity()
	}
	return match, nil
}

func (s *Store) findDegradedTrashTarget(target string, sameSource func(source, target string) (bool, error), match degradedTargetMatch) (degradedTargetMatch, error) {
	s.trashCollector.trashCleanupMu.Lock()
	defer s.trashCollector.trashCleanupMu.Unlock()
	s.trashCollector.trashMu.Lock()
	defer s.trashCollector.trashMu.Unlock()
	trashEntries, err := s.metadataRepository.readDir(filepath.Join(s.metadataRepository.root, trashDirectoryName))
	if err != nil {
		return degradedTargetMatch{}, fmt.Errorf("inspect cache trash while locating mounted generation: %w", err)
	}
	for _, entry := range trashEntries {
		if !entry.IsDir() {
			continue
		}
		trashPath := filepath.Join(s.metadataRepository.root, trashDirectoryName, entry.Name())
		generations, err := s.metadataRepository.readDir(filepath.Join(trashPath, generationsDirectoryName))
		if errors.Is(err, os.ErrNotExist) {
			generations = nil
		} else if err != nil {
			return degradedTargetMatch{}, fmt.Errorf("inspect quarantined cache generations: %w", err)
		}
		for _, source := range generationSourcePaths(trashPath, generations) {
			match, err = matchDegradedTarget(match, "", source, target, sameSource, "quarantined cache generation")
			if err != nil {
				return degradedTargetMatch{}, err
			}
		}
	}
	return match, nil
}

func generationSourcePaths(objectPath string, generations []os.DirEntry) []string {
	sources := make([]string, 0, len(generations))
	for _, generation := range generations {
		if generation.IsDir() {
			sources = append(sources, filepath.Join(objectPath, generationsDirectoryName, generation.Name()))
		}
	}
	if len(sources) != 0 {
		return sources
	}
	return []string{objectPath}
}

func matchDegradedTarget(current degradedTargetMatch, identity, source, target string, sameSource func(source, target string) (bool, error), description string) (degradedTargetMatch, error) {
	matches, err := sameSource(source, target)
	if errors.Is(err, os.ErrNotExist) || err == nil && !matches {
		return current, nil
	}
	if err != nil {
		return degradedTargetMatch{}, fmt.Errorf("match %s: %w", description, err)
	}
	if current.source != "" {
		return degradedTargetMatch{}, errors.New("target matches multiple cache generations")
	}
	return degradedTargetMatch{identity: identity, source: source}, nil
}

func (s *Store) QuarantineDegradedObject(identity string, sourceMounted func(source string) (bool, error)) error {
	if !validIdentity(identity) || sourceMounted == nil {
		return errors.New("valid degraded cache identity and mount inspector are required")
	}
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
	s.mu.Lock()
	if _, degraded := s.generationManager.degraded[identity]; !degraded {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	objectPath := filepath.Join(s.metadataRepository.root, identity)
	generations, err := s.metadataRepository.readDir(filepath.Join(objectPath, generationsDirectoryName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect degraded cache generations before cleanup: %w", err)
	}
	for _, generation := range generations {
		if !generation.IsDir() {
			continue
		}
		source := filepath.Join(objectPath, generationsDirectoryName, generation.Name())
		mounted, err := sourceMounted(source)
		if err != nil {
			return s.detachAndCreateFreshObject(objectPath, identity, true, s.policyForDegradedObject(objectPath, identity))
		}
		if mounted {
			return s.detachAndCreateFreshObject(objectPath, identity, true, s.policyForDegradedObject(objectPath, identity))
		}
	}
	if !hasGenerationDirectory(generations) {
		mounted, err := sourceMounted(objectPath)
		if err != nil {
			return s.detachAndCreateFreshObject(objectPath, identity, true, s.policyForDegradedObject(objectPath, identity))
		}
		if mounted {
			return s.detachAndCreateFreshObject(objectPath, identity, true, s.policyForDegradedObject(objectPath, identity))
		}
	}
	return s.detachAndCreateFreshObject(objectPath, identity, false, s.policyForDegradedObject(objectPath, identity))
}

func (s *Store) RecoverDegraded(ctx context.Context, verifyMount func(source string, lease Lease, policy Policy) (bool, error)) error {
	if verifyMount == nil {
		return errors.New("mount verifier is not configured")
	}
	s.mu.Lock()
	identities := make([]string, 0, len(s.generationManager.degraded))
	for identity := range s.generationManager.degraded {
		if validIdentity(identity) {
			identities = append(identities, identity)
		}
	}
	s.mu.Unlock()
	slices.Sort(identities)

	var recoveryErr error
	for _, identity := range identities {
		if err := ctx.Err(); err != nil {
			return errors.Join(recoveryErr, err)
		}
		unlockIdentity := s.lockIdentity(identity)
		s.mu.Lock()
		_, degraded := s.generationManager.degraded[identity]
		s.mu.Unlock()
		if degraded {
			path := filepath.Join(s.metadataRepository.root, identity)
			if err := s.quarantineDegradedObjectChecked(ctx, path, identity, verifyMount); err != nil {
				recoveryErr = errors.Join(recoveryErr, fmt.Errorf("recover degraded cache %s: %w", identity, err))
			}
		}
		unlockIdentity()
	}
	return recoveryErr
}

func (s *Store) RecoverLeases(verifyMount func(source string, lease Lease, policy Policy) (bool, error)) error {
	return s.RecoverLeasesContext(context.Background(), verifyMount)
}

func (s *Store) RecoverLeasesContext(ctx context.Context, verifyMount func(source string, lease Lease, policy Policy) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if verifyMount == nil {
		return errors.New("mount verifier is not configured")
	}
	entries, err := s.metadataRepository.readDir(s.metadataRepository.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() != trashDirectoryName {
			unlockIdentity := s.lockIdentity(entry.Name())
			if err := s.recoverObjectLeases(ctx, filepath.Join(s.metadataRepository.root, entry.Name()), entry.Name(), verifyMount); err != nil {
				unlockIdentity()
				return err
			}
			unlockIdentity()
		}
	}
	s.ready.Store(true)
	return nil
}

func (s *Store) recoverObjectLeases(ctx context.Context, path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	meta, err := s.metadataRepository.readMetadata(path)
	if errors.Is(err, os.ErrNotExist) {
		return s.recoverMissingMetadataObject(ctx, path, identity, verifyMount)
	}
	if err == nil {
		err = validateMetadata(identity, meta)
	}
	if err != nil {
		s.markDegraded(identity, err)
		return s.quarantineForRecovery(ctx, path, identity, verifyMount)
	}
	return s.recoverValidObjectLeases(ctx, path, identity, meta, verifyMount)
}

func (s *Store) recoverMissingMetadataObject(ctx context.Context, path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	generations, err := s.metadataRepository.readDir(filepath.Join(path, generationsDirectoryName))
	if errors.Is(err, os.ErrNotExist) || err == nil && !hasGenerationDirectory(generations) {
		contents, readErr := s.metadataRepository.readDir(path)
		if readErr != nil {
			s.markDegraded(identity, readErr)
			return nil
		}
		if hasUntrackedObjectPayload(contents) {
			s.markDegraded(identity, errors.New("cache metadata is missing while object data remains"))
			return s.quarantineForRecovery(ctx, path, identity, verifyMount)
		}
		if err := s.discardUntrackedGenerations(path); err != nil {
			s.markDegraded(identity, err)
		}
		return nil
	}
	if err != nil {
		s.markDegraded(identity, err)
		return nil
	}
	s.markDegraded(identity, errors.New("cache metadata is missing while generations remain"))
	return s.quarantineForRecovery(ctx, path, identity, verifyMount)
}

func (s *Store) recoverValidObjectLeases(ctx context.Context, path, identity string, meta Metadata, verifyMount func(string, Lease, Policy) (bool, error)) error {
	generationPath := filepath.Join(path, generationsDirectoryName, meta.Generation)
	if err := s.metadataRepository.stat(generationPath); errors.Is(err, os.ErrNotExist) && s.activeLeaseCount(meta) > 0 {
		s.markDegraded(identity, fmt.Errorf("active cache leases reference missing generation %q", meta.Generation))
		return s.quarantineDegradedObjectChecked(ctx, path, identity, verifyMount)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.markDegraded(identity, err)
		return fmt.Errorf("inspect current cache generation during lease recovery: %w", err)
	}
	wasDirty := meta.Dirty
	hadPreparingLease := slices.ContainsFunc(meta.Leases, func(lease Lease) bool { return lease.Preparing })
	active, uncertain, err := s.verifyRecoveredLeases(ctx, path, identity, meta, verifyMount)
	if err != nil {
		return err
	}
	if uncertain {
		return nil
	}
	allLeasesActive := len(active) == len(meta.Leases)
	if allLeasesActive && (len(active) > 0 || !wasDirty) {
		meta.Leases = active
		if hadPreparingLease || metadataNeedsNormalization(meta) {
			if err := s.writeMetadata(path, meta); err != nil {
				s.markDegraded(identity, err)
			}
		} else {
			s.indexMetadata(meta)
		}
		return nil
	}
	if len(active) == 0 && wasDirty {
		return s.recoverDirtyObject(path, identity, meta)
	}
	meta.Leases = active
	retiredHandled, err := s.recoverRetiredGenerations(ctx, path, identity, &meta, active)
	if err != nil {
		return err
	}
	if retiredHandled {
		return nil
	}
	generationPath = filepath.Join(path, generationsDirectoryName, meta.Generation)
	if statErr := s.metadataRepository.stat(generationPath); errors.Is(statErr, os.ErrNotExist) {
		meta.Leases = slices.DeleteFunc(meta.Leases, func(lease Lease) bool { return lease.Generation == "" || lease.Generation == meta.Generation })
		meta.Generation = uuid.NewV7().String()
		meta.CreatedAt = time.Now().UTC()
		meta.ProjectAssigned = false
		meta.QuotaBytes = 0
	} else if statErr != nil {
		s.markDegraded(identity, statErr)
		return nil
	}
	meta.Dirty = len(active) > 0
	if err := s.writeMetadata(path, meta); err != nil {
		s.markDegraded(identity, err)
	}
	return nil
}

func (s *Store) verifyRecoveredLeases(ctx context.Context, path, identity string, meta Metadata, verifyMount func(string, Lease, Policy) (bool, error)) ([]Lease, bool, error) {
	active := make([]Lease, 0, len(meta.Leases))
	for _, lease := range meta.Leases {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		generation, policy, err := leaseGenerationAndPolicy(meta, lease)
		if err != nil {
			s.markDegraded(identity, err)
			return nil, true, nil
		}
		mounted, err := verifyMount(filepath.Join(path, generationsDirectoryName, generation), lease, policy)
		if err != nil {
			s.markDegraded(identity, err)
			return nil, true, nil
		}
		if mounted {
			lease.Preparing = false
			active = append(active, lease)
		}
	}
	return active, false, nil
}

func (s *Store) recoverDirtyObject(path, identity string, meta Metadata) error {
	if err := s.detachToTrash(path); err != nil {
		s.markDegraded(identity, err)
		return nil
	}
	if err := s.metadataRepository.ensureDirectory(path); err != nil {
		return fmt.Errorf("create cache object after recovery: %w", err)
	}
	meta = Metadata{
		Identity:        identity,
		Generation:      uuid.NewV7().String(),
		GenerationState: GenerationStateActive,
		CreatedAt:       time.Now().UTC(),
		LastUsed:        time.Now().UTC(),
		Policy:          meta.Policy,
	}
	if err := s.writeMetadata(path, meta); err != nil {
		s.markDegraded(identity, err)
	}
	return nil
}

func (s *Store) recoverRetiredGenerations(ctx context.Context, path, identity string, meta *Metadata, active []Lease) (bool, error) {
	for index := 0; index < len(meta.Retired); {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		retired := meta.Retired[index]
		if slices.ContainsFunc(active, func(lease Lease) bool { return lease.Generation == retired.Generation }) {
			meta.Retired[index].State = GenerationStateRetiring
			index++
			continue
		}
		if meta.Retired[index].State != GenerationStateRetired {
			meta.Retired[index].State = GenerationStateRetired
			if err := s.writeMetadata(path, *meta); err != nil {
				s.markDegraded(identity, err)
				return true, nil
			}
		}
		if err := s.detachGenerationToTrash(filepath.Join(path, generationsDirectoryName, retired.Generation), identity, retired); err != nil {
			s.markDegraded(identity, err)
			return true, nil
		}
		meta.Retired = slices.Delete(meta.Retired, index, index+1)
	}
	return false, nil
}

func (s *Store) quarantineForRecovery(ctx context.Context, path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) error {
	return s.quarantineDegradedObjectChecked(ctx, path, identity, verifyMount)
}

func (s *Store) quarantineDegradedObjectChecked(ctx context.Context, path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) error {
	if err := s.metadataRepository.stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.clearDegraded(identity)
			return nil
		}
		s.markDegraded(identity, err)
		return fmt.Errorf("inspect degraded cache object: %w", err)
	}
	generationsPath := filepath.Join(path, generationsDirectoryName)
	generations, err := s.metadataRepository.readDir(generationsPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.markDegraded(identity, err)
		return fmt.Errorf("inspect degraded cache generations: %w", err)
	}
	preserveMounts := false
	for _, generation := range generations {
		if !generation.IsDir() {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		source := filepath.Join(generationsPath, generation.Name())
		mounted, err := verifyMount(source, Lease{}, Policy{})
		if err != nil {
			s.markDegraded(identity, err)
			preserveMounts = true
			continue
		}
		if mounted {
			preserveMounts = true
		}
	}
	if !hasGenerationDirectory(generations) {
		mounted, err := verifyMount(path, Lease{}, Policy{})
		if err != nil {
			s.markDegraded(identity, err)
			preserveMounts = true
		} else if mounted {
			preserveMounts = true
		}
	}
	policy := s.policyForDegradedObject(path, identity)
	if meta, readErr := s.metadataRepository.readMetadata(path); readErr == nil && validateMetadata(identity, meta) == nil {
		for _, lease := range meta.Leases {
			generation, leasePolicy, err := leaseGenerationAndPolicy(meta, lease)
			if err != nil {
				preserveMounts = true
				continue
			}
			source := filepath.Join(generationsPath, generation)
			if statErr := s.metadataRepository.stat(source); errors.Is(statErr, os.ErrNotExist) && generation == meta.Generation {
				preserveMounts = true
				if _, verifyErr := verifyMount(source, lease, leasePolicy); verifyErr != nil {
					s.markDegraded(identity, verifyErr)
					preserveMounts = true
				}
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.detachAndCreateFreshObject(path, identity, preserveMounts, policy)
}

func (s *Store) policyForDegradedObject(path, identity string) Policy {
	meta, err := s.metadataRepository.readMetadata(path)
	if err != nil || validateMetadata(identity, meta) != nil {
		return Policy{}
	}
	return meta.Policy
}

func (s *Store) detachAndCreateFreshObject(path, identity string, preserveMounts bool, policy Policy) error {
	var err error
	if preserveMounts {
		err = s.detachToTrashPreservingMounts(path)
	} else {
		err = s.detachToTrash(path)
	}
	if err != nil {
		s.markDegraded(identity, err)
		return fmt.Errorf("quarantine degraded cache object: %w", err)
	}
	if err := s.metadataRepository.ensureDirectory(path); err != nil {
		s.markDegraded(identity, err)
		return fmt.Errorf("create cache object after quarantine: %w", err)
	}
	meta := Metadata{
		Identity:   identity,
		Generation: uuid.NewV7().String(),
		CreatedAt:  time.Now().UTC(),
		LastUsed:   time.Now().UTC(),
		Policy:     policy,
	}
	if err := s.metadataRepository.ensureDirectory(filepath.Join(path, generationsDirectoryName, meta.Generation)); err != nil {
		s.markDegraded(identity, err)
		return fmt.Errorf("create empty cache generation after quarantine: %w", err)
	}
	if err := s.writeMetadata(path, meta); err != nil {
		s.markDegraded(identity, err)
		return fmt.Errorf("persist empty cache generation after quarantine: %w", err)
	}
	return nil
}
