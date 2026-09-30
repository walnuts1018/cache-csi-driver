package cache

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"
	"uuid"

	"golang.org/x/sys/unix"
)

func validatePressure(pressure PressureConfig) error {
	for _, threshold := range []int{pressure.HighFreePercent, pressure.LowFreePercent, pressure.CriticalFreePercent, pressure.HighInodeFreePercent, pressure.LowInodeFreePercent, pressure.CriticalInodeFreePercent} {
		if threshold < 0 || threshold > 100 {
			return errors.New("pressure percentages must be between 0 and 100")
		}
	}
	if !validWatermarks(pressure.HighFreePercent, pressure.LowFreePercent) || !validWatermarks(pressure.HighInodeFreePercent, pressure.LowInodeFreePercent) {
		return errors.New("pressure high watermarks must be greater than paired low watermarks")
	}
	if !validCriticalWatermark(pressure.LowFreePercent, pressure.CriticalFreePercent) || !validCriticalWatermark(pressure.LowInodeFreePercent, pressure.CriticalInodeFreePercent) {
		return errors.New("critical pressure watermarks must be lower than paired low watermarks")
	}
	return nil
}

func validCriticalWatermark(low, critical int) bool {
	return critical == 0 || low > critical
}

func validWatermarks(high, low int) bool {
	return (high == 0 && low == 0) || (high > 0 && low > 0 && high > low)
}

func (s *Store) Collect(now time.Time) error {
	s.mu.Lock()
	identities := make([]string, 0, len(s.metadataByIdentity))
	for identity := range s.metadataByIdentity {
		identities = append(identities, identity)
	}
	type candidate struct {
		identity string
		path     string
		meta     Metadata
	}
	var candidates []candidate
	for _, identity := range identities {
		indexed := s.metadataByIdentity[identity]
		if len(indexed.Leases) != 0 || indexed.Policy.Retention <= 0 || now.Sub(indexed.LastUsed) < indexed.Policy.Retention {
			continue
		}
		candidates = append(candidates, candidate{identity: identity, path: filepath.Join(s.root, identity), meta: indexed})
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

func (s *Store) ReclaimPressure(ctx context.Context) error {
	attemptedTrash := make(map[string]struct{})
	var incomplete error
	s.mu.Lock()
	clear(s.pressureDetachFailed)
	s.mu.Unlock()
	usage, err := filesystemUsage(s.root)
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
		usage, err = filesystemUsage(s.root)
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

		usage, err = filesystemUsage(s.root)
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
		usage, err = filesystemUsage(s.root)
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
			s.mu.Lock()
			s.pressureDetachFailed[item.identity] = struct{}{}
			s.mu.Unlock()
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

type pressureVictimCandidate struct {
	identity string
	meta     Metadata
	path     string
}

func (s *Store) unusedPressureCandidates() []pressureCandidate {
	candidates := make([]pressureCandidate, 0, len(s.metadataByIdentity))
	for identity, meta := range s.metadataByIdentity {
		if len(meta.Leases) != 0 {
			continue
		}
		candidates = append(candidates, pressureCandidate{identity: identity, path: filepath.Join(s.root, identity), lastUsed: meta.LastUsed})
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
		manager.updatePressureState(fs)
		return manager.pressureActive
	}
	bytesRecovered := manager.pressure.HighFreePercent == 0 || above(fs.Bavail, fs.Blocks, manager.pressure.HighFreePercent)
	inodesRecovered := manager.pressure.HighInodeFreePercent == 0 || above(fs.Ffree, fs.Files, manager.pressure.HighInodeFreePercent)
	if bytesRecovered && inodesRecovered {
		manager.pressureActive = false
	}
	manager.updatePressureState(fs)
	return manager.pressureActive
}

func (manager *pressureManager) updatePressureState(fs unix.Statfs_t) {
	manager.pressureState = pressureStateNormal
	if !manager.pressureActive {
		return
	}
	manager.pressureState = "reclaiming"
	if manager.underCriticalWatermark(fs) {
		manager.pressureState = "critical"
	}
}

func (manager *pressureManager) underCriticalWatermark(fs unix.Statfs_t) bool {
	return below(fs.Bavail, fs.Blocks, manager.pressure.CriticalFreePercent) || below(fs.Ffree, fs.Files, manager.pressure.CriticalInodeFreePercent)
}

func (s *Store) MetadataError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, err := range s.degraded {
		return err
	}
	if s.projectRegistryDamaged {
		return fmt.Errorf("%w: project ID reservation registry is damaged", ErrDegradedMetadata)
	}
	return nil
}

func (s *Store) PressureVictims() ([]PressureVictim, error) {
	fs, err := filesystemUsage(s.root)
	if err != nil {
		return nil, err
	}
	underPressure, criticalPressure, identities, candidates, unusedCachePending := s.pressureVictimSnapshot(fs)
	if !underPressure || unusedCachePending {
		return nil, nil
	}
	slices.SortFunc(candidates, func(left, right pressureVictimCandidate) int {
		return left.meta.LastUsed.Compare(right.meta.LastUsed)
	})
	for _, candidate := range candidates {
		victims, pressureCleared, err := s.retirePressureCandidate(candidate)
		if err != nil {
			return nil, err
		}
		if pressureCleared {
			return nil, nil
		}
		if len(victims) > 0 {
			return victims, nil
		}
	}
	return s.retiredPressureVictims(identities, criticalPressure)
}

func (s *Store) pressureVictimSnapshot(fs unix.Statfs_t) (bool, bool, []string, []pressureVictimCandidate, bool) {
	s.mu.Lock()
	underPressure := s.updatePressure(fs)
	if !underPressure {
		s.mu.Unlock()
		return false, false, nil, nil, false
	}
	criticalPressure := s.underCriticalWatermark(fs)
	identities := make([]string, 0, len(s.metadataByIdentity))
	candidates := make([]pressureVictimCandidate, 0)
	for identity, meta := range s.metadataByIdentity {
		identities = append(identities, identity)
		if len(meta.Leases) == 0 {
			if _, detachFailed := s.pressureDetachFailed[identity]; !detachFailed {
				s.mu.Unlock()
				return true, criticalPressure, identities, nil, true
			}
		}
		if policyAllowsPressureTermination(meta.Policy.PressurePolicy) && s.activeLeaseCount(meta) > 0 && !s.hasPreparingGenerationLease(meta, meta.Generation) {
			candidates = append(candidates, pressureVictimCandidate{identity: identity, meta: meta, path: filepath.Join(s.root, identity)})
		}
	}
	s.mu.Unlock()
	return true, criticalPressure, identities, candidates, false
}

func (s *Store) retirePressureCandidate(candidate pressureVictimCandidate) ([]PressureVictim, bool, error) {
	unlockIdentity := s.lockIdentity(candidate.identity)
	defer unlockIdentity()
	meta, err := s.readObjectMetadata(candidate.identity)
	if err != nil || s.activeLeaseCount(meta) == 0 || !policyAllowsPressureTermination(meta.Policy.PressurePolicy) || s.hasPreparingGenerationLease(meta, meta.Generation) {
		return nil, false, nil
	}
	fs, err := filesystemUsage(s.root)
	if err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	underPressure := s.updatePressure(fs)
	criticalPressure := s.underCriticalWatermark(fs)
	s.mu.Unlock()
	if !underPressure {
		return nil, true, nil
	}
	victims := make([]PressureVictim, 0, len(meta.Leases))
	for index := range meta.Leases {
		if meta.Leases[index].Generation == "" || meta.Leases[index].Generation == meta.Generation {
			meta.Leases[index].Generation = meta.Generation
			victims = append(victims, PressureVictim{Lease: meta.Leases[index], ForceDelete: shouldForceDelete(meta.Policy.PressurePolicy, criticalPressure)})
		}
	}
	if len(victims) == 0 {
		return nil, false, nil
	}
	meta.Retired = append(meta.Retired, RetiredGeneration{
		State:           GenerationStateRetiring,
		Generation:      meta.Generation,
		ProjectID:       meta.ProjectID,
		ProjectAssigned: meta.ProjectAssigned,
		QuotaBytes:      meta.QuotaBytes,
		Policy:          meta.Policy,
	})
	meta.Generation = uuid.NewV7().String()
	meta.GenerationState = GenerationStateActive
	meta.CreatedAt = time.Now().UTC()
	replacementPath := filepath.Join(candidate.path, "generations", meta.Generation)
	if err := s.ensureDirectory(replacementPath); err != nil {
		return nil, false, fmt.Errorf("create replacement cache generation before Pod eviction: %w", err)
	}
	meta.ProjectID = 0
	meta.ProjectAssigned = false
	meta.QuotaBytes = 0
	meta.Dirty = false
	if err := s.writeMetadata(candidate.path, meta); err != nil {
		_ = s.detachToTrash(replacementPath)
		return nil, false, fmt.Errorf("retire cache generation before Pod eviction: %w", err)
	}
	return victims, false, nil
}

func (s *Store) retiredPressureVictims(identities []string, criticalPressure bool) ([]PressureVictim, error) {
	for _, identity := range identities {
		unlockIdentity := s.lockIdentity(identity)
		meta, err := s.readObjectMetadata(identity)
		if err != nil {
			unlockIdentity()
			continue
		}
		for _, retired := range meta.Retired {
			if retired.State == GenerationStateRetired || !s.hasGenerationLeases(meta, retired.Generation) || !policyAllowsPressureTermination(retired.Policy.PressurePolicy) || s.hasPreparingGenerationLease(meta, retired.Generation) {
				continue
			}
			victims := make([]PressureVictim, 0)
			for _, lease := range meta.Leases {
				if lease.Generation == retired.Generation && !lease.Preparing {
					victims = append(victims, PressureVictim{Lease: lease, ForceDelete: shouldForceDelete(retired.Policy.PressurePolicy, criticalPressure)})
				}
			}
			if len(victims) > 0 {
				unlockIdentity()
				return victims, nil
			}
		}
		unlockIdentity()
	}
	return nil, nil
}

func policyAllowsPressureTermination(policy string) bool {
	return policy == PressurePolicyEvict || policy == PressurePolicyForceDelete
}

func shouldForceDelete(policy string, criticalPressure bool) bool {
	return policy == PressurePolicyForceDelete && criticalPressure
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
