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
	defer s.mu.Unlock()
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
		meta, err := s.readObjectMetadata(identity)
		if err != nil {
			continue
		}
		if len(meta.Leases) == 0 && meta.Policy.Retention > 0 && now.Sub(meta.LastUsed) >= meta.Policy.Retention {
			candidates = append(candidates, candidate{identity: identity, path: filepath.Join(s.root, identity), meta: meta})
		}
	}
	slices.SortFunc(candidates, func(left, right candidate) int { return left.meta.LastUsed.Compare(right.meta.LastUsed) })
	for _, item := range candidates[:min(len(candidates), trashBatchSize)] {
		meta, err := s.readObjectMetadata(item.identity)
		if err != nil || len(meta.Leases) != 0 || meta.Policy.Retention <= 0 || now.Sub(meta.LastUsed) < meta.Policy.Retention {
			continue
		}
		if err := s.detachToTrash(item.path); err != nil {
			return fmt.Errorf("remove cache object: %w", err)
		}
	}
	return nil
}

func (s *Store) ReclaimPressure(ctx context.Context) error {
	attemptedTrash := make(map[string]struct{})
	excluded := make(map[string]struct{})
	var incomplete error
	s.mu.Lock()
	clear(s.pressureDetachFailed)
	s.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		usage, err := filesystemUsage(s.root)
		if err != nil {
			s.mu.Unlock()
			return err
		}
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

		s.mu.Lock()
		usage, err = filesystemUsage(s.root)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		if !s.updatePressure(usage) {
			s.mu.Unlock()
			return pressureCleanupResult(incomplete)
		}
		candidates := s.unusedPressureCandidates(excluded)
		if len(candidates) == 0 {
			s.mu.Unlock()
			return pressureCleanupResult(incomplete)
		}
		item := candidates[0]
		if err := s.detachToTrash(item.path); err != nil {
			excluded[item.identity] = struct{}{}
			s.pressureDetachFailed[item.identity] = struct{}{}
			incomplete = errors.Join(incomplete, fmt.Errorf("detach unused cache during pressure reclaim: %w", err))
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()
	}
}

type pressureCandidate struct {
	identity string
	path     string
	lastUsed time.Time
}

func (s *Store) unusedPressureCandidates(excluded map[string]struct{}) []pressureCandidate {
	candidates := make([]pressureCandidate, 0)
	for identity := range s.metadataByIdentity {
		if _, skip := excluded[identity]; skip {
			continue
		}
		meta, err := s.readObjectMetadata(identity)
		if err != nil || len(meta.Leases) != 0 {
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
	for _, err := range s.degraded {
		return err
	}
	if s.projectRegistryDamaged {
		return fmt.Errorf("%w: project ID reservation registry is damaged", ErrDegradedMetadata)
	}
	return nil
}

func (s *Store) PressureVictims() ([]Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fs, err := filesystemUsage(s.root)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		identity string
		meta     Metadata
		path     string
	}
	var candidates []candidate
	underPressure := s.updatePressure(fs)
	if !underPressure {
		return nil, nil
	}
	identities := make([]string, 0, len(s.metadataByIdentity))
	for identity := range s.metadataByIdentity {
		identities = append(identities, identity)
	}
	for _, identity := range identities {
		meta, err := s.readObjectMetadata(identity)
		if err != nil {
			continue
		}
		if underPressure && len(meta.Leases) == 0 {
			if _, detachFailed := s.pressureDetachFailed[identity]; !detachFailed {
				return nil, nil
			}
		}
		if meta.Policy.EvictRunning && s.activeLeaseCount(meta) > 0 && !s.hasPreparingGenerationLease(meta, meta.Generation) {
			candidates = append(candidates, candidate{identity: identity, meta: meta, path: filepath.Join(s.root, identity)})
		}
	}
	slices.SortFunc(candidates, func(left, right candidate) int { return left.meta.LastUsed.Compare(right.meta.LastUsed) })
	if underPressure {
		for _, candidate := range candidates {
			if s.hasPreparingGenerationLease(candidate.meta, candidate.meta.Generation) {
				continue
			}
			leases := make([]Lease, 0, len(candidate.meta.Leases))
			for index := range candidate.meta.Leases {
				if candidate.meta.Leases[index].Generation == "" || candidate.meta.Leases[index].Generation == candidate.meta.Generation {
					candidate.meta.Leases[index].Generation = candidate.meta.Generation
					leases = append(leases, candidate.meta.Leases[index])
				}
			}
			if len(leases) == 0 {
				continue
			}
			candidate.meta.Retired = append(candidate.meta.Retired, RetiredGeneration{
				Generation:      candidate.meta.Generation,
				ProjectID:       candidate.meta.ProjectID,
				ProjectAssigned: candidate.meta.ProjectAssigned,
				QuotaBytes:      candidate.meta.QuotaBytes,
				Policy:          candidate.meta.Policy,
			})
			candidate.meta.Generation = uuid.NewV7().String()
			candidate.meta.CreatedAt = time.Now().UTC()
			replacementPath := filepath.Join(candidate.path, "generations", candidate.meta.Generation)
			if err := s.ensureDirectory(replacementPath); err != nil {
				return nil, fmt.Errorf("create replacement cache generation before Pod eviction: %w", err)
			}
			candidate.meta.ProjectID = 0
			candidate.meta.ProjectAssigned = false
			candidate.meta.QuotaBytes = 0
			candidate.meta.Dirty = false
			if err := s.writeMetadata(candidate.path, candidate.meta); err != nil {
				_ = s.detachToTrash(replacementPath)
				return nil, fmt.Errorf("retire cache generation before Pod eviction: %w", err)
			}
			return leases, nil
		}
		for _, identity := range identities {
			meta, err := s.readObjectMetadata(identity)
			if err != nil {
				continue
			}
			for _, retired := range meta.Retired {
				if !retired.Policy.EvictRunning {
					continue
				}
				if s.hasPreparingGenerationLease(meta, retired.Generation) {
					continue
				}
				leases := make([]Lease, 0)
				for _, lease := range meta.Leases {
					if lease.Generation == retired.Generation && !lease.Preparing {
						leases = append(leases, lease)
					}
				}
				if len(leases) > 0 {
					return leases, nil
				}
			}
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
