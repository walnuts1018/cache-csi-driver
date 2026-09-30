package cache

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"golang.org/x/sys/unix"
)

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

func (s *Store) Collect(now time.Time) error {
	s.mu.Lock()
	identities := make([]string, 0, len(s.generationManager.metadataByIdentity))
	for identity := range s.generationManager.metadataByIdentity {
		identities = append(identities, identity)
	}
	type candidate struct {
		identity string
		path     string
		meta     Metadata
	}
	var candidates []candidate
	for _, identity := range identities {
		indexed := s.generationManager.metadataByIdentity[identity]
		if len(indexed.Leases) != 0 || indexed.Policy.Retention <= 0 || now.Sub(indexed.LastUsed) < indexed.Policy.Retention {
			continue
		}
		candidates = append(candidates, candidate{identity: identity, path: filepath.Join(s.metadataRepository.root, identity), meta: indexed})
	}
	s.mu.Unlock()
	slices.SortFunc(candidates, func(left, right candidate) int { return left.meta.LastUsed.Compare(right.meta.LastUsed) })
	for _, item := range candidates[:min(len(candidates), trashBatchSize)] {
		unlockIdentity := s.lockIdentity(item.identity)
		meta, err := s.readObjectMetadata(item.identity)
		if err != nil || len(meta.Leases) != 0 || meta.Policy.Retention <= 0 || now.Sub(meta.LastUsed) < meta.Policy.Retention {
			unlockIdentity()
			continue
		}
		if err := s.detachToTrash(item.path); err != nil {
			unlockIdentity()
			return fmt.Errorf("remove cache object: %w", err)
		}
		unlockIdentity()
	}
	return nil
}

func (s *Store) ObservePressure() (bool, error) {
	usage, err := filesystemUsage(s.metadataRepository.root)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	active := s.updatePressure(usage)
	s.mu.Unlock()
	return active, nil
}

func (s *Store) ReclaimPressure(ctx context.Context) error {
	attemptedTrash := make(map[string]struct{})
	var incomplete error
	usage, err := filesystemUsage(s.metadataRepository.root)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if !s.updatePressure(usage) {
		s.mu.Unlock()
		return nil
	}
	candidates := s.unusedPressureCandidates()
	s.mu.Unlock()
	slices.SortFunc(candidates, func(left, right pressureCandidate) int { return left.lastUsed.Compare(right.lastUsed) })
	nextCandidate := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		usage, err = filesystemUsage(s.metadataRepository.root)
		if err != nil {
			return err
		}
		s.mu.Lock()
		if !s.updatePressure(usage) {
			s.mu.Unlock()
			return pressureCleanupResult(incomplete)
		}
		s.mu.Unlock()

		if err := s.cleanupTrashUntilAttempted(ctx, attemptedTrash); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			incomplete = errors.Join(incomplete, err)
		}

		usage, err = filesystemUsage(s.metadataRepository.root)
		if err != nil {
			return err
		}
		s.mu.Lock()
		if !s.updatePressure(usage) {
			s.mu.Unlock()
			return pressureCleanupResult(incomplete)
		}
		if nextCandidate >= len(candidates) {
			s.mu.Unlock()
			return pressureCleanupResult(incomplete)
		}
		item := candidates[nextCandidate]
		nextCandidate++
		s.mu.Unlock()
		unlockIdentity := s.lockIdentity(item.identity)
		meta, err := s.readObjectMetadata(item.identity)
		if err != nil || len(meta.Leases) != 0 || !meta.LastUsed.Equal(item.lastUsed) {
			unlockIdentity()
			continue
		}
		usage, err = filesystemUsage(s.metadataRepository.root)
		if err != nil {
			unlockIdentity()
			return err
		}
		s.mu.Lock()
		if !s.updatePressure(usage) {
			s.mu.Unlock()
			unlockIdentity()
			return pressureCleanupResult(incomplete)
		}
		s.mu.Unlock()
		if err := s.detachToTrash(item.path); err != nil {
			incomplete = errors.Join(incomplete, fmt.Errorf("detach unused cache during pressure reclaim: %w", err))
			unlockIdentity()
			continue
		}
		unlockIdentity()
	}
}

type pressureCandidate struct {
	identity string
	path     string
	lastUsed time.Time
}

func (s *Store) unusedPressureCandidates() []pressureCandidate {
	candidates := make([]pressureCandidate, 0, len(s.generationManager.metadataByIdentity))
	for identity, meta := range s.generationManager.metadataByIdentity {
		if len(meta.Leases) != 0 {
			continue
		}
		candidates = append(candidates, pressureCandidate{identity: identity, path: filepath.Join(s.metadataRepository.root, identity), lastUsed: meta.LastUsed})
	}
	return candidates
}

func pressureCleanupResult(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrPressureReclaimIncomplete, err)
}

func (manager *pressureManager) underLowWatermark(fs unix.Statfs_t) bool {
	return below(fs.Bavail, fs.Blocks, manager.pressure.LowFreePercent) || below(fs.Ffree, fs.Files, manager.pressure.LowInodeFreePercent)
}

func (manager *pressureManager) updatePressure(fs unix.Statfs_t) bool {
	if !manager.pressureActive {
		manager.pressureActive = manager.underLowWatermark(fs)
		manager.updatePressureState()
		return manager.pressureActive
	}
	bytesRecovered := manager.pressure.HighFreePercent == 0 || above(fs.Bavail, fs.Blocks, manager.pressure.HighFreePercent)
	inodesRecovered := manager.pressure.HighInodeFreePercent == 0 || above(fs.Ffree, fs.Files, manager.pressure.HighInodeFreePercent)
	if bytesRecovered && inodesRecovered {
		manager.pressureActive = false
	}
	manager.updatePressureState()
	return manager.pressureActive
}

func (manager *pressureManager) updatePressureState() {
	manager.pressureState = pressureStateNormal
	if !manager.pressureActive {
		return
	}
	manager.pressureState = "reclaiming"
}

func (s *Store) updatePressure(fs unix.Statfs_t) bool {
	return s.pressureManager.updatePressure(fs)
}

func (s *Store) MetadataError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, err := range s.generationManager.degraded {
		return err
	}
	if s.projectQuotaRegistry.projectRegistryDamaged {
		return fmt.Errorf("%w: project ID reservation registry is damaged", ErrDegradedMetadata)
	}
	return nil
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
