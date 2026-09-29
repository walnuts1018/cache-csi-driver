package cache

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
	"uuid"
)

const trashDirectoryName = ".trash"

const trashBatchSize = 16

func (s *Store) runTrashCollector() {
	defer s.finishTrashCollector()
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

func (s *Store) CleanupTrash(ctx context.Context) error {
	response := make(chan error, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopTrash:
		return errors.New("cache store is closed")
	case <-s.trashDone:
		return errors.New("cache store has no trash collector")
	case s.trashRequests <- response:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-response:
		return err
	}
}

func (s *Store) cleanupTrashBatch() error {
	_, err := s.cleanupTrashBatchSkipping(nil)
	return err
}

func (s *Store) cleanupTrashBatchSkipping(skip map[string]struct{}) (map[string]struct{}, error) {
	// detach処理と同じmutex下でsnapshotし、作成途中のtrash entryを削除対象に含めない。
	s.mu.Lock()
	entries, err := s.readDir(filepath.Join(s.root, trashDirectoryName))
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	trashIDs := make([]string, 0, min(len(entries), trashBatchSize))
	if len(entries) > 0 {
		start := sort.Search(len(entries), func(index int) bool { return entries[index].Name() > s.trashCursor })
		for offset := range entries {
			entry := entries[(start+offset)%len(entries)]
			if _, alreadyAttempted := skip[entry.Name()]; alreadyAttempted {
				continue
			}
			trashIDs = append(trashIDs, entry.Name())
			if len(trashIDs) == trashBatchSize {
				break
			}
		}
		if len(trashIDs) > 0 {
			s.trashCursor = trashIDs[len(trashIDs)-1]
		}
	}
	s.mu.Unlock()

	attempted := make(map[string]struct{}, len(trashIDs))
	var cleanupErr error
	for _, trashID := range trashIDs {
		attempted[trashID] = struct{}{}
		if err := s.removeTrash(filepath.Join(s.root, trashDirectoryName, trashID)); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		s.mu.Lock()
		s.trashDeleted++
		reservationsBeforeCleanup := make(map[uint32][]projectReservation, len(s.projectReservations))
		for projectID, reservations := range s.projectReservations {
			reservationsBeforeCleanup[projectID] = slices.Clone(reservations)
		}
		unknownReservationsBeforeCleanup := maps.Clone(s.unknownProjectReservations)
		for projectID, reservations := range s.projectReservations {
			kept := make([]projectReservation, 0, len(reservations))
			for _, reservation := range reservations {
				if reservation.TrashID != trashID {
					kept = append(kept, reservation)
					continue
				}
				if meta, exists := s.metadataByIdentity[reservation.Identity]; exists && metadataHasProjectReservation(meta, projectID, reservation.Generation) {
					if reservation.TrashID != "" {
						reservation.TrashID = ""
						s.projectRegistryDirty = true
					}
					kept = append(kept, reservation)
				}
			}
			if len(kept) != len(reservations) {
				s.projectRegistryDirty = true
			}
			if len(kept) == 0 {
				delete(s.projectReservations, projectID)
			} else {
				s.projectReservations[projectID] = kept
			}
		}
		s.rebuildProjectReservationIndexes()
		delete(s.trashMetadata, trashID)
		delete(s.degraded, filepath.Join(trashDirectoryName, trashID))
		for identity, reservedTrashID := range s.unknownProjectReservations {
			if reservedTrashID == trashID {
				delete(s.unknownProjectReservations, identity)
				s.projectRegistryDirty = true
			}
		}
		if err := s.persistProjectReservations(); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("persist project ID registry after trash cleanup: %w", err))
			s.projectReservations = reservationsBeforeCleanup
			s.unknownProjectReservations = unknownReservationsBeforeCleanup
			s.rebuildProjectReservationIndexes()
			s.projectRegistryDirty = true
		}
		s.mu.Unlock()
	}
	return attempted, cleanupErr
}

func (s *Store) removeTrash(path string) error {
	if s.removeTrashEntry != nil {
		return s.removeTrashEntry(path)
	}
	return s.removeAll(path)
}

func (s *Store) cleanupTrashUntilAttempted(ctx context.Context, attempted map[string]struct{}) error {
	if attempted == nil {
		attempted = make(map[string]struct{})
	}
	var cleanupErr error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(cleanupErr, err)
		}
		batch, err := s.cleanupTrashBatchSkipping(attempted)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
		if len(batch) == 0 {
			return cleanupErr
		}
		for trashID := range batch {
			attempted[trashID] = struct{}{}
		}
	}
}

func (s *Store) detachToTrash(path string) error {
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	isObject := filepath.Dir(relative) == "." && filepath.Base(relative) != trashDirectoryName
	identity := filepath.Base(relative)
	var meta Metadata
	if isObject {
		if err := s.unmountGenerationMounts(path); err != nil {
			return fmt.Errorf("unmount cache generation before trash detach: %w", err)
		}
		meta, _ = s.readMetadata(path)
	}
	trashPath := filepath.Join(s.root, trashDirectoryName, uuid.NewV7().String())
	trashID := filepath.Base(trashPath)
	if isObject {
		if err := s.reserveObjectForTrash(identity, trashID, meta); err != nil {
			return err
		}
	}
	if err := s.rename(path, trashPath); err != nil {
		if isObject {
			err = errors.Join(err, s.restoreObjectTrashReservation(identity, trashID))
		}
		return err
	}
	if isObject {
		s.indexObjectInTrash(identity, trashID, meta)
	}
	return errors.Join(s.syncDirectory(filepath.Dir(path)), s.syncDirectory(filepath.Join(s.root, trashDirectoryName)))
}

func (s *Store) unmountGenerationMounts(path string) error {
	if s.unmountGeneration == nil {
		return nil
	}
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	parts := strings.Split(relative, string(filepath.Separator))
	if len(parts) == 3 && parts[1] == "generations" {
		return s.unmountGeneration(path)
	}
	if len(parts) != 1 {
		return nil
	}
	entries, err := s.readDir(filepath.Join(path, "generations"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if err := s.unmountGeneration(filepath.Join(path, "generations", entry.Name())); err != nil {
			return fmt.Errorf("unmount generation %q: %w", entry.Name(), err)
		}
	}
	return nil
}

func (s *Store) reserveObjectForTrash(identity, trashID string, meta Metadata) error {
	for projectID, reservations := range s.projectReservations {
		for index, reservation := range reservations {
			if reservation.Identity == identity && reservation.TrashID != trashID {
				reservation.TrashID = trashID
				reservations[index] = reservation
				s.projectRegistryDirty = true
			}
		}
		s.projectReservations[projectID] = reservations
	}
	if _, damaged := s.degraded[identity]; damaged || meta.Identity != identity {
		s.addUnknownProjectReservation(identity, trashID)
	}
	if err := s.persistProjectReservations(); err != nil {
		return fmt.Errorf("persist project ID reservation before cache quarantine: %w", err)
	}
	return nil
}

func (s *Store) restoreObjectTrashReservation(identity, trashID string) error {
	for projectID, reservations := range s.projectReservations {
		for index, reservation := range reservations {
			if reservation.Identity == identity && reservation.TrashID == trashID {
				reservation.TrashID = ""
				reservations[index] = reservation
				s.projectRegistryDirty = true
			}
		}
		s.projectReservations[projectID] = reservations
	}
	if s.unknownProjectReservations[identity] == trashID {
		s.addUnknownProjectReservation(identity, "")
	}
	if err := s.persistProjectReservations(); err != nil {
		return fmt.Errorf("restore project ID reservation after failed cache quarantine: %w", err)
	}
	return nil
}

func (s *Store) indexObjectInTrash(identity, trashID string, meta Metadata) {
	if previous, exists := s.metadataByIdentity[identity]; exists {
		s.removeFallbackReservation(previous)
		s.retiredGenerationCount -= len(previous.Retired)
	}
	delete(s.metadataByIdentity, identity)
	maps.DeleteFunc(s.leaseIndex, func(_ string, leaseIdentity string) bool { return leaseIdentity == identity })
	if meta.Identity == identity {
		s.trashMetadata[trashID] = meta
	}
	delete(s.degraded, identity)
}

func (s *Store) syncDirectory(path string) error {
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	handle, err := s.rootFS.Open(relative)
	if err != nil {
		return err
	}
	syncErr := handle.Sync()
	closeErr := handle.Close()
	return errors.Join(syncErr, closeErr)
}

func (s *Store) detachGenerationToTrash(path string, identity string, retired RetiredGeneration) error {
	if err := s.stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := s.unmountGenerationMounts(path); err != nil {
		return fmt.Errorf("unmount retired cache generation before trash detach: %w", err)
	}
	trashPath := filepath.Join(s.root, trashDirectoryName, uuid.NewV7().String())
	trashID := filepath.Base(trashPath)
	var previous projectReservation
	var hadPrevious bool
	if retired.ProjectID != 0 {
		previous, hadPrevious = s.projectReservation(retired.ProjectID, identity, retired.Generation)
		s.addProjectReservation(retired.ProjectID, projectReservation{Identity: identity, Generation: retired.Generation, TrashID: trashID})
		if err := s.persistProjectReservations(); err != nil {
			restoreErr := s.restoreProjectReservation(retired.ProjectID, identity, retired.Generation, previous, hadPrevious)
			return errors.Join(fmt.Errorf("persist project ID reservation before retired generation detach: %w", err), restoreErr)
		}
	}
	if err := s.ensureDirectory(trashPath); err != nil {
		return errors.Join(err, s.restoreProjectReservation(retired.ProjectID, identity, retired.Generation, previous, hadPrevious))
	}
	trashDirectory := filepath.Join(s.root, trashDirectoryName)
	if err := s.syncDirectory(trashDirectory); err != nil {
		return errors.Join(err, s.restoreProjectReservation(retired.ProjectID, identity, retired.Generation, previous, hadPrevious))
	}
	if err := s.rename(path, filepath.Join(trashPath, "generation")); err != nil {
		removeErr := s.removeAll(trashPath)
		restoreErr := s.restoreProjectReservation(retired.ProjectID, identity, retired.Generation, previous, hadPrevious)
		if errors.Is(err, os.ErrNotExist) {
			return errors.Join(removeErr, restoreErr)
		}
		return errors.Join(err, removeErr, restoreErr)
	}
	if err := errors.Join(s.syncDirectory(filepath.Dir(path)), s.syncDirectory(trashPath)); err != nil {
		return err
	}
	// project IDの予約はrename前に永続化し、trash entryのmetadataで処理を完了する。
	if err := s.writeMetadata(trashPath, Metadata{Identity: identity, Generation: retired.Generation, ProjectID: retired.ProjectID, ProjectAssigned: retired.ProjectAssigned, QuotaBytes: retired.QuotaBytes, Policy: retired.Policy}); err != nil {
		return err
	}
	return nil
}
