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
		unlockIdentity := s.identityLocks.lock(item.identity)
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
		unlockIdentity := s.identityLocks.lock(item.identity)
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

func (s *Store) unusedPressureCandidates() []pressureCandidate {
	candidates := make([]pressureCandidate, 0, len(s.metadataByIdentity))
	for identity, meta := range s.metadataByIdentity {
		if len(meta.Leases) != 0 {
			continue
		}
		candidates = append(candidates, pressureCandidate{identity: identity, path: filepath.Join(s.root, identity), lastUsed: meta.LastUsed})
	}
	slices.SortFunc(candidates, func(left, right pressureCandidate) int { return left.lastUsed.Compare(right.lastUsed) })
	return candidates
}

func pressureCleanupResult(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrPressureReclaimIncomplete, err)
}

func (s *Store) underLowWatermark(fs unix.Statfs_t) bool {
	return below(fs.Bavail, fs.Blocks, s.pressure.LowFreePercent) || below(fs.Ffree, fs.Files, s.pressure.LowInodeFreePercent)
}

func (s *Store) pressureActiveLocked() (bool, error) {
	usage, err := filesystemUsage(s.root)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updatePressure(usage), nil
}

func (s *Store) updatePressure(fs unix.Statfs_t) bool {
	if !s.pressureActive {
		s.pressureActive = s.underLowWatermark(fs)
		s.updatePressureState(fs)
		return s.pressureActive
	}
	bytesRecovered := s.pressure.HighFreePercent == 0 || above(fs.Bavail, fs.Blocks, s.pressure.HighFreePercent)
	inodesRecovered := s.pressure.HighInodeFreePercent == 0 || above(fs.Ffree, fs.Files, s.pressure.HighInodeFreePercent)
	if bytesRecovered && inodesRecovered {
		s.pressureActive = false
	}
	s.updatePressureState(fs)
	return s.pressureActive
}

func (s *Store) updatePressureState(fs unix.Statfs_t) {
	s.pressureState = pressureStateNormal
	if !s.pressureActive {
		return
	}
	s.pressureState = "reclaiming"
	if s.underCriticalWatermark(fs) {
		s.pressureState = "critical"
	}
}

func (s *Store) underCriticalWatermark(fs unix.Statfs_t) bool {
	return below(fs.Bavail, fs.Blocks, s.pressure.CriticalFreePercent) || below(fs.Ffree, fs.Files, s.pressure.CriticalInodeFreePercent)
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
	s.mu.Lock()
	type candidate struct {
		identity string
		meta     Metadata
		path     string
	}
	var candidates []candidate
	underPressure := s.updatePressure(fs)
	if !underPressure {
		s.mu.Unlock()
		return nil, nil
	}
	criticalPressure := s.underCriticalWatermark(fs)
	identities := make([]string, 0, len(s.metadataByIdentity))
	for identity, meta := range s.metadataByIdentity {
		identities = append(identities, identity)
		if underPressure && len(meta.Leases) == 0 {
			if _, detachFailed := s.pressureDetachFailed[identity]; !detachFailed {
				s.mu.Unlock()
				return nil, nil
			}
		}
		if policyAllowsPressureTermination(meta.Policy.PressurePolicy) && s.activeLeaseCount(meta) > 0 && !s.hasPreparingGenerationLease(meta, meta.Generation) {
			candidates = append(candidates, candidate{identity: identity, meta: meta, path: filepath.Join(s.root, identity)})
		}
	}
	slices.SortFunc(candidates, func(left, right candidate) int { return left.meta.LastUsed.Compare(right.meta.LastUsed) })
	s.mu.Unlock()
	if underPressure {
		for _, candidate := range candidates {
			unlockIdentity := s.identityLocks.lock(candidate.identity)
			meta, err := s.readObjectMetadata(candidate.identity)
			if err != nil || len(meta.Leases) == 0 || !policyAllowsPressureTermination(meta.Policy.PressurePolicy) || s.hasPreparingGenerationLease(meta, meta.Generation) {
				unlockIdentity()
				continue
			}
			fs, err := filesystemUsage(s.root)
			if err != nil {
				unlockIdentity()
				return nil, err
			}
			s.mu.Lock()
			underPressure = s.updatePressure(fs)
			criticalPressure = s.underCriticalWatermark(fs)
			s.mu.Unlock()
			if !underPressure {
				unlockIdentity()
				return nil, nil
			}
			victims := make([]PressureVictim, 0, len(meta.Leases))
			for index := range meta.Leases {
				if meta.Leases[index].Generation == "" || meta.Leases[index].Generation == meta.Generation {
					meta.Leases[index].Generation = meta.Generation
					victims = append(victims, PressureVictim{Lease: meta.Leases[index], ForceDelete: shouldForceDelete(meta.Policy.PressurePolicy, criticalPressure)})
				}
			}
			if len(victims) == 0 {
				unlockIdentity()
				continue
			}
			meta.Retired = append(meta.Retired, RetiredGeneration{
				Generation:      meta.Generation,
				ProjectID:       meta.ProjectID,
				ProjectAssigned: meta.ProjectAssigned,
				QuotaBytes:      meta.QuotaBytes,
				Policy:          meta.Policy,
			})
			meta.Generation = uuid.NewV7().String()
			meta.CreatedAt = time.Now().UTC()
			replacementPath := filepath.Join(candidate.path, "generations", meta.Generation)
			if err := s.ensureDirectory(replacementPath); err != nil {
				unlockIdentity()
				return nil, fmt.Errorf("create replacement cache generation before Pod eviction: %w", err)
			}
			meta.ProjectID = 0
			meta.ProjectAssigned = false
			meta.QuotaBytes = 0
			meta.Dirty = false
			if err := s.writeMetadata(candidate.path, meta); err != nil {
				_ = s.detachToTrash(replacementPath)
				unlockIdentity()
				return nil, fmt.Errorf("retire cache generation before Pod eviction: %w", err)
			}
			unlockIdentity()
			return victims, nil
		}
		for _, identity := range identities {
			unlockIdentity := s.identityLocks.lock(identity)
			meta, err := s.readObjectMetadata(identity)
			if err != nil {
				unlockIdentity()
				continue
			}
			for _, retired := range meta.Retired {
				if !policyAllowsPressureTermination(retired.Policy.PressurePolicy) {
					continue
				}
				if s.hasPreparingGenerationLease(meta, retired.Generation) {
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
