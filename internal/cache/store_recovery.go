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
	registryMissing := s.loadProjectRegistry()

	entries, err := s.readDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == trashDirectoryName {
			continue
		}
		path := filepath.Join(s.root, entry.Name())
		meta, err := s.readMetadata(path)
		if errors.Is(err, os.ErrNotExist) {
			generations, generationErr := s.readDir(filepath.Join(path, "generations"))
			if generationErr == nil && hasGenerationDirectory(generations) {
				s.markDegraded(entry.Name(), errors.New("cache metadata is missing while generations remain"))
			} else if generationErr != nil && !errors.Is(generationErr, os.ErrNotExist) {
				s.markDegraded(entry.Name(), generationErr)
			}
			continue
		}
		if err != nil {
			s.markDegraded(entry.Name(), err)
			continue
		}
		if err := validateMetadata(entry.Name(), meta); err != nil {
			s.indexDegradedLeaseIDs(entry.Name(), meta)
			s.markDegraded(entry.Name(), err)
			continue
		}
		s.indexObjectMetadata(meta)
	}

	trashEntries, err := s.readDir(filepath.Join(s.root, trashDirectoryName))
	if err != nil {
		return err
	}
	for _, entry := range trashEntries {
		if !entry.IsDir() {
			continue
		}
		trashPath := filepath.Join(s.root, trashDirectoryName, entry.Name())
		meta, err := s.readMetadata(trashPath)
		if err == nil {
			err = validateMetadata(meta.Identity, meta)
		}
		if err != nil {
			s.markDegraded(filepath.Join(trashDirectoryName, entry.Name()), err)
			if validIdentity(meta.Identity) {
				s.addUnknownProjectReservation(meta.Identity, entry.Name())
			}
			continue
		}
		s.trashMetadata[entry.Name()] = meta
		s.addMetadataReservations(meta, entry.Name())
	}
	if registryMissing && len(s.degraded) > 0 {
		for identity := range s.degraded {
			if filepath.Dir(identity) == "." {
				s.addUnknownProjectReservation(identity, "")
			}
		}
	}
	if err := s.reconcileProjectReservations(entries, trashEntries); err != nil {
		return err
	}
	return nil
}

func hasGenerationDirectory(entries []os.DirEntry) bool {
	return slices.ContainsFunc(entries, func(entry os.DirEntry) bool { return entry.IsDir() })
}

func hasUntrackedObjectPayload(entries []os.DirEntry) bool {
	return slices.ContainsFunc(entries, func(entry os.DirEntry) bool {
		return entry.Name() != "generations" && !strings.HasPrefix(entry.Name(), ".metadata-")
	})
}

func (s *Store) discardUntrackedGenerations(entry string) error {
	generations := filepath.Join(entry, "generations")
	entries, err := s.readDir(generations)
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

func (s *Store) FindDegradedGenerationForTarget(target string, sameSource func(source, target string) (bool, error)) (string, string, bool, error) {
	if target == "" || !filepath.IsAbs(target) || sameSource == nil {
		return "", "", false, errors.New("absolute target path and source matcher are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var matchedIdentity, matchedSource string
	for identity := range s.degraded {
		if !validIdentity(identity) {
			continue
		}
		generations, err := s.readDir(filepath.Join(s.root, identity, "generations"))
		if errors.Is(err, os.ErrNotExist) {
			generations = nil
		} else if err != nil {
			return "", "", false, fmt.Errorf("inspect degraded cache generations: %w", err)
		}
		for _, generation := range generations {
			if !generation.IsDir() {
				continue
			}
			source := filepath.Join(s.root, identity, "generations", generation.Name())
			matches, err := sameSource(source, target)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return "", "", false, fmt.Errorf("match degraded cache generation: %w", err)
			}
			if !matches {
				continue
			}
			if matchedIdentity != "" {
				return "", "", false, errors.New("target matches multiple degraded cache generations")
			}
			matchedIdentity = identity
			matchedSource = source
		}
		if !hasGenerationDirectory(generations) {
			source := filepath.Join(s.root, identity)
			matches, err := sameSource(source, target)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return "", "", false, fmt.Errorf("match degraded cache object: %w", err)
			}
			if matches {
				if matchedIdentity != "" {
					return "", "", false, errors.New("target matches multiple degraded cache generations")
				}
				matchedIdentity = identity
				matchedSource = source
			}
		}
	}
	return matchedIdentity, matchedSource, matchedIdentity != "", nil
}

func (s *Store) CleanupDegradedObject(identity string, sourceMounted func(source string) (bool, error)) error {
	if !validIdentity(identity) || sourceMounted == nil {
		return errors.New("valid degraded cache identity and mount inspector are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, degraded := s.degraded[identity]; !degraded {
		return nil
	}
	objectPath := filepath.Join(s.root, identity)
	generations, err := s.readDir(filepath.Join(objectPath, "generations"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect degraded cache generations before cleanup: %w", err)
	}
	for _, generation := range generations {
		if !generation.IsDir() {
			continue
		}
		source := filepath.Join(objectPath, "generations", generation.Name())
		mounted, err := sourceMounted(source)
		if err != nil {
			return fmt.Errorf("verify degraded cache generation before cleanup: %w", err)
		}
		if mounted {
			return nil
		}
	}
	if !hasGenerationDirectory(generations) {
		mounted, err := sourceMounted(objectPath)
		if err != nil {
			return fmt.Errorf("verify degraded cache object before cleanup: %w", err)
		}
		if mounted {
			return nil
		}
	}
	if err := s.detachToTrash(objectPath); err != nil {
		return fmt.Errorf("quarantine unmounted degraded cache: %w", err)
	}
	return nil
}

func (s *Store) RecoverDegraded(ctx context.Context, verifyMount func(source string, lease Lease, policy Policy) (bool, error)) error {
	if verifyMount == nil {
		return errors.New("mount verifier is not configured")
	}
	s.mu.Lock()
	identities := make([]string, 0, len(s.degraded))
	for identity := range s.degraded {
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
		s.mu.Lock()
		if _, degraded := s.degraded[identity]; degraded {
			path := filepath.Join(s.root, identity)
			if err := s.quarantineDegradedObjectChecked(ctx, path, identity, verifyMount); err != nil {
				recoveryErr = errors.Join(recoveryErr, fmt.Errorf("recover degraded cache %s: %w", identity, err))
			}
		}
		s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if verifyMount == nil {
		return errors.New("mount verifier is not configured")
	}
	entries, err := s.readDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() != trashDirectoryName {
			if err := s.recoverObjectLeases(ctx, filepath.Join(s.root, entry.Name()), entry.Name(), verifyMount); err != nil {
				return err
			}
		}
	}
	s.ready.Store(true)
	return nil
}

func (s *Store) recoverObjectLeases(ctx context.Context, path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	meta, err := s.readMetadata(path)
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
	generations, err := s.readDir(filepath.Join(path, "generations"))
	if errors.Is(err, os.ErrNotExist) || err == nil && !hasGenerationDirectory(generations) {
		contents, readErr := s.readDir(path)
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
	wasDirty := meta.Dirty
	hadPreparingLease := slices.ContainsFunc(meta.Leases, func(lease Lease) bool { return lease.Preparing })
	active, uncertain, err := s.verifyRecoveredLeases(ctx, path, identity, meta, verifyMount)
	if err != nil {
		return err
	}
	if uncertain {
		return nil
	}
	if meta.Policy.DiscardOnLastRelease {
		if len(active) == 0 {
			if err := s.detachToTrash(path); err != nil {
				s.markDegraded(identity, err)
			}
			return nil
		}
		meta.Leases = active
		meta.Dirty = true
		if err := s.writeMetadata(path, meta); err != nil {
			s.markDegraded(identity, err)
		}
		return nil
	}
	allLeasesActive := len(active) == len(meta.Leases)
	if allLeasesActive && (len(active) > 0 || !wasDirty || meta.Policy.CrashRecoveryReuse) {
		meta.Leases = active
		if hadPreparingLease {
			if err := s.writeMetadata(path, meta); err != nil {
				s.markDegraded(identity, err)
			}
		} else {
			s.indexObjectMetadata(meta)
		}
		return nil
	}
	if len(active) == 0 && wasDirty && !meta.Policy.CrashRecoveryReuse {
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
	generationPath := filepath.Join(path, "generations", meta.Generation)
	if statErr := s.stat(generationPath); errors.Is(statErr, os.ErrNotExist) {
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
		mounted, err := verifyMount(filepath.Join(path, "generations", generation), lease, policy)
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
	if err := s.ensureDirectory(path); err != nil {
		return fmt.Errorf("create cache object after recovery: %w", err)
	}
	meta = Metadata{
		Identity:   identity,
		Generation: uuid.NewV7().String(),
		CreatedAt:  time.Now().UTC(),
		LastUsed:   time.Now().UTC(),
		Policy:     meta.Policy,
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
			index++
			continue
		}
		if err := s.detachGenerationToTrash(filepath.Join(path, "generations", retired.Generation), identity, retired); err != nil {
			s.markDegraded(identity, err)
			return true, nil
		}
		meta.Retired = slices.Delete(meta.Retired, index, index+1)
	}
	return false, nil
}

func (s *Store) quarantineForRecovery(ctx context.Context, path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) error {
	if err := s.quarantineDegradedObjectChecked(ctx, path, identity, verifyMount); err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return err
	}
	return nil
}

func (s *Store) quarantineDegradedObjectChecked(ctx context.Context, path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) error {
	if err := s.stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			delete(s.degraded, identity)
			return nil
		}
		s.markDegraded(identity, err)
		return fmt.Errorf("inspect degraded cache object: %w", err)
	}
	generationsPath := filepath.Join(path, "generations")
	generations, err := s.readDir(generationsPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.markDegraded(identity, err)
		return fmt.Errorf("inspect degraded cache generations: %w", err)
	}
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
			return fmt.Errorf("verify degraded cache generation mount: %w", err)
		}
		if mounted {
			s.markDegraded(identity, errors.New("cache generation remains mounted with unreadable metadata"))
			return nil
		}
	}
	if !hasGenerationDirectory(generations) {
		mounted, err := verifyMount(path, Lease{}, Policy{})
		if err != nil {
			s.markDegraded(identity, err)
			return fmt.Errorf("verify degraded cache object mount: %w", err)
		}
		if mounted {
			s.markDegraded(identity, errors.New("cache object remains mounted with unreadable metadata"))
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.detachToTrash(path); err != nil {
		s.markDegraded(identity, err)
		return fmt.Errorf("quarantine unmounted degraded cache: %w", err)
	}
	return nil
}
