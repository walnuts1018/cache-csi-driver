package cache

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"uuid"
)

const trashDirectoryName = ".trash"

const trashBatchSize = 16

func (collector *trashCollector) run(store *Store) {
	defer store.finishTrashCollector()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	_ = store.retryProjectRegistry()
	_ = store.cleanupTrashBatch()
	for {
		select {
		case <-collector.stopTrash:
			return
		case <-ticker.C:
			_ = store.retryProjectRegistry()
			_ = store.cleanupTrashBatch()
		case response := <-collector.trashRequests:
			response <- errors.Join(store.retryProjectRegistry(), store.cleanupTrashBatch())
		}
	}
}

func (s *Store) CleanupTrash(ctx context.Context) error {
	response := make(chan error, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.trashCollector.stopTrash:
		return errors.New("cache store is closed")
	case <-s.trashCollector.trashDone:
		return errors.New("cache store has no trash collector")
	case s.trashCollector.trashRequests <- response:
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
	s.trashCollector.trashCleanupMu.Lock()
	defer s.trashCollector.trashCleanupMu.Unlock()
	// detach処理と同じmutex下でsnapshotし、作成途中のtrash entryを削除対象に含めない。
	s.trashCollector.trashMu.Lock()
	entries, err := s.metadataRepository.readDir(filepath.Join(s.metadataRepository.root, trashDirectoryName))
	if err != nil {
		s.trashCollector.trashMu.Unlock()
		return nil, err
	}
	trashIDs := make([]string, 0, min(len(entries), trashBatchSize))
	if len(entries) > 0 {
		start := sort.Search(len(entries), func(index int) bool { return entries[index].Name() > s.trashCollector.trashCursor })
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
			s.trashCollector.trashCursor = trashIDs[len(trashIDs)-1]
		}
	}
	s.trashCollector.trashMu.Unlock()

	attempted := make(map[string]struct{}, len(trashIDs))
	var cleanupErr error
	for _, trashID := range trashIDs {
		attempted[trashID] = struct{}{}
		if err := s.removeTrash(filepath.Join(s.metadataRepository.root, trashDirectoryName, trashID)); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		s.projectQuotaRegistry.projectRegistryMu.Lock()
		s.mu.Lock()
		s.trashCollector.trashDeleted++
		for projectID, reservations := range s.projectQuotaRegistry.projectReservations {
			kept := make([]projectReservation, 0, len(reservations))
			for _, reservation := range reservations {
				if reservation.TrashID != trashID {
					kept = append(kept, reservation)
					continue
				}
				if meta, exists := s.generationManager.metadataByIdentity[reservation.Identity]; exists && metadataHasProjectReservation(meta, projectID, reservation.Generation) {
					if reservation.TrashID != "" {
						reservation.TrashID = ""
						s.projectQuotaRegistry.projectRegistryDirty = true
					}
					kept = append(kept, reservation)
				}
			}
			if len(kept) != len(reservations) {
				s.projectQuotaRegistry.projectRegistryDirty = true
			}
			if len(kept) == 0 {
				delete(s.projectQuotaRegistry.projectReservations, projectID)
			} else {
				s.projectQuotaRegistry.projectReservations[projectID] = kept
			}
		}
		s.rebuildProjectReservationIndexes()
		delete(s.trashCollector.trashMetadata, trashID)
		delete(s.generationManager.degraded, filepath.Join(trashDirectoryName, trashID))
		for identity, reservedTrashID := range s.projectQuotaRegistry.unknownProjectReservations {
			if reservedTrashID == trashID {
				delete(s.projectQuotaRegistry.unknownProjectReservations, identity)
				s.projectQuotaRegistry.projectRegistryDirty = true
			}
		}
		s.mu.Unlock()
		if err := s.persistProjectReservationsLocked(); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("persist project ID registry after trash cleanup: %w", err))
		}
		s.projectQuotaRegistry.projectRegistryMu.Unlock()
	}
	return attempted, cleanupErr
}

func (s *Store) removeTrash(path string) error {
	if s.trashCollector.removeTrashEntry != nil {
		return s.trashCollector.removeTrashEntry(path)
	}
	return s.metadataRepository.removeAll(path)
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
	s.trashCollector.trashMu.Lock()
	defer s.trashCollector.trashMu.Unlock()
	relative, err := s.metadataRepository.relative(path)
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
		meta, _ = s.metadataRepository.readMetadata(path)
	}
	trashPath := filepath.Join(s.metadataRepository.root, trashDirectoryName, uuid.NewV7().String())
	trashID := filepath.Base(trashPath)
	if isObject {
		if err := s.reserveObjectForTrash(identity, trashID, meta); err != nil {
			return err
		}
	}
	if err := s.metadataRepository.rename(path, trashPath); err != nil {
		if isObject {
			err = errors.Join(err, s.restoreObjectTrashReservation(identity, trashID))
		}
		return err
	}
	if isObject {
		s.mu.Lock()
		s.indexObjectInTrash(identity, trashID, meta)
		s.mu.Unlock()
	}
	return errors.Join(s.metadataRepository.syncDirectory(filepath.Dir(path)), s.metadataRepository.syncDirectory(filepath.Join(s.metadataRepository.root, trashDirectoryName)))
}

func (s *Store) unmountGenerationMounts(path string) error {
	if s.unmountGeneration == nil {
		return nil
	}
	relative, err := s.metadataRepository.relative(path)
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
	entries, err := s.metadataRepository.readDir(filepath.Join(path, "generations"))
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
	s.projectQuotaRegistry.projectRegistryMu.Lock()
	defer s.projectQuotaRegistry.projectRegistryMu.Unlock()
	s.mu.Lock()
	for projectID, reservations := range s.projectQuotaRegistry.projectReservations {
		for index, reservation := range reservations {
			if reservation.Identity == identity && reservation.TrashID != trashID {
				reservation.TrashID = trashID
				reservations[index] = reservation
				s.projectQuotaRegistry.projectRegistryDirty = true
			}
		}
		s.projectQuotaRegistry.projectReservations[projectID] = reservations
	}
	if _, damaged := s.generationManager.degraded[identity]; damaged || meta.Identity != identity {
		s.addUnknownProjectReservation(identity, trashID)
	}
	s.mu.Unlock()
	if err := s.persistProjectReservationsLocked(); err != nil {
		return fmt.Errorf("persist project ID reservation before cache quarantine: %w", err)
	}
	return nil
}

func (s *Store) restoreObjectTrashReservation(identity, trashID string) error {
	s.projectQuotaRegistry.projectRegistryMu.Lock()
	defer s.projectQuotaRegistry.projectRegistryMu.Unlock()
	s.mu.Lock()
	for projectID, reservations := range s.projectQuotaRegistry.projectReservations {
		for index, reservation := range reservations {
			if reservation.Identity == identity && reservation.TrashID == trashID {
				reservation.TrashID = ""
				reservations[index] = reservation
				s.projectQuotaRegistry.projectRegistryDirty = true
			}
		}
		s.projectQuotaRegistry.projectReservations[projectID] = reservations
	}
	if s.projectQuotaRegistry.unknownProjectReservations[identity] == trashID {
		s.addUnknownProjectReservation(identity, "")
	}
	s.mu.Unlock()
	if err := s.persistProjectReservationsLocked(); err != nil {
		return fmt.Errorf("restore project ID reservation after failed cache quarantine: %w", err)
	}
	return nil
}

func (s *Store) indexObjectInTrash(identity, trashID string, meta Metadata) {
	if previous, exists := s.generationManager.metadataByIdentity[identity]; exists {
		s.removeFallbackReservation(previous)
		s.generationManager.retiredGenerationCount -= len(previous.Retired)
	}
	delete(s.generationManager.metadataByIdentity, identity)
	maps.DeleteFunc(s.generationManager.leaseIndex, func(_ string, leaseIdentity string) bool { return leaseIdentity == identity })
	if meta.Identity == identity {
		s.trashCollector.trashMetadata[trashID] = meta
	}
	delete(s.generationManager.degraded, identity)
}

func (repository *metadataRepository) syncDirectory(path string) error {
	relative, err := repository.relative(path)
	if err != nil {
		return err
	}
	handle, err := repository.rootFS.Open(relative)
	if err != nil {
		return err
	}
	syncErr := handle.Sync()
	closeErr := handle.Close()
	return errors.Join(syncErr, closeErr)
}

func (s *Store) detachGenerationToTrash(path string, identity string, retired RetiredGeneration) error {
	s.trashCollector.trashMu.Lock()
	defer s.trashCollector.trashMu.Unlock()
	if err := s.metadataRepository.stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := s.unmountGenerationMounts(path); err != nil {
		return fmt.Errorf("unmount retired cache generation before trash detach: %w", err)
	}
	trashPath := filepath.Join(s.metadataRepository.root, trashDirectoryName, uuid.NewV7().String())
	trashID := filepath.Base(trashPath)
	var previous projectReservation
	var hadPrevious bool
	if retired.ProjectID != 0 {
		s.projectQuotaRegistry.projectRegistryMu.Lock()
		s.mu.Lock()
		previous, hadPrevious = s.projectReservationLocked(retired.ProjectID, identity, retired.Generation)
		s.addProjectReservation(retired.ProjectID, projectReservation{Identity: identity, Generation: retired.Generation, TrashID: trashID})
		s.mu.Unlock()
		if err := s.persistProjectReservationsLocked(); err != nil {
			restoreErr := s.restoreProjectReservationLocked(retired.ProjectID, identity, retired.Generation, previous, hadPrevious)
			s.projectQuotaRegistry.projectRegistryMu.Unlock()
			return errors.Join(fmt.Errorf("persist project ID reservation before retired generation detach: %w", err), restoreErr)
		}
		s.projectQuotaRegistry.projectRegistryMu.Unlock()
	}
	if err := s.metadataRepository.ensureDirectory(trashPath); err != nil {
		return errors.Join(err, s.restoreProjectReservation(retired.ProjectID, identity, retired.Generation, previous, hadPrevious))
	}
	trashDirectory := filepath.Join(s.metadataRepository.root, trashDirectoryName)
	if err := s.metadataRepository.syncDirectory(trashDirectory); err != nil {
		return errors.Join(err, s.restoreProjectReservation(retired.ProjectID, identity, retired.Generation, previous, hadPrevious))
	}
	if err := s.metadataRepository.rename(path, filepath.Join(trashPath, "generation")); err != nil {
		removeErr := s.metadataRepository.removeAll(trashPath)
		restoreErr := s.restoreProjectReservation(retired.ProjectID, identity, retired.Generation, previous, hadPrevious)
		if errors.Is(err, os.ErrNotExist) {
			return errors.Join(removeErr, restoreErr)
		}
		return errors.Join(err, removeErr, restoreErr)
	}
	if err := errors.Join(s.metadataRepository.syncDirectory(filepath.Dir(path)), s.metadataRepository.syncDirectory(trashPath)); err != nil {
		return err
	}
	// project IDの予約はrename前に永続化し、trash entryのmetadataで処理を完了する。
	if err := s.writeMetadata(trashPath, Metadata{Identity: identity, Generation: retired.Generation, ProjectID: retired.ProjectID, ProjectAssigned: retired.ProjectAssigned, QuotaBytes: retired.QuotaBytes, Policy: retired.Policy}); err != nil {
		return err
	}
	return nil
}
