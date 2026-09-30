package cache

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"uuid"
)

const projectRegistryName = ".project-ids.json"

type projectReservation struct {
	Identity   string `json:"identity"`
	Generation string `json:"generation"`
	TrashID    string `json:"trashID,omitempty"`
}

type projectReservationKey struct {
	identity   string
	generation string
}

type projectReservationDocument struct {
	FormatVersion       int                         `json:"formatVersion"`
	Reservations        []projectIDReservation      `json:"reservations"`
	UnknownReservations []unknownProjectReservation `json:"unknownReservations,omitempty"`
}

type projectIDReservation struct {
	ProjectID  uint32 `json:"projectID"`
	Identity   string `json:"identity"`
	Generation string `json:"generation"`
	TrashID    string `json:"trashID,omitempty"`
}

type unknownProjectReservation struct {
	Identity string `json:"identity"`
	TrashID  string `json:"trashID,omitempty"`
}

func (store *Store) ProjectRegistryError() error {
	registry := &store.projectQuotaRegistry
	registry.projectRegistryMu.Lock()
	defer registry.projectRegistryMu.Unlock()

	store.mu.Lock()
	damaged := registry.projectRegistryDamaged
	unknownReservations := len(registry.unknownProjectReservations)
	dirty := registry.projectRegistryDirty
	store.mu.Unlock()

	if damaged {
		return errors.New("project ID reservation registry is damaged")
	}
	if unknownReservations > 0 {
		return errors.New("project ID reservations remain for quarantined cache objects")
	}
	if dirty {
		if err := registry.persistProjectReservationsLocked(store); err != nil {
			return fmt.Errorf("persist project ID reservation registry: %w", err)
		}
	}
	return nil
}

func (registry *projectQuotaRegistry) loadProjectRegistry(store *Store) bool {
	registry.projectRegistryMu.Lock()
	defer registry.projectRegistryMu.Unlock()
	data, err := store.metadataRepository.rootFS.ReadFile(projectRegistryName)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	if err != nil {
		store.mu.Lock()
		registry.projectRegistryDamaged = true
		store.mu.Unlock()
		return false
	}
	var document projectReservationDocument
	if err := json.Unmarshal(data, &document); err != nil {
		store.mu.Lock()
		registry.projectRegistryDamaged = true
		store.mu.Unlock()
		return false
	}
	if document.FormatVersion != 0 && document.FormatVersion != storeFormatVersion {
		store.mu.Lock()
		registry.projectRegistryDamaged = true
		store.mu.Unlock()
		return false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, item := range document.Reservations {
		if item.ProjectID == 0 || !validIdentity(item.Identity) || item.Generation == "" {
			registry.projectRegistryDamaged = true
			continue
		}
		registry.addProjectReservation(item.ProjectID, projectReservation{Identity: item.Identity, Generation: item.Generation, TrashID: item.TrashID})
	}
	for _, reservation := range document.UnknownReservations {
		if !validIdentity(reservation.Identity) {
			registry.projectRegistryDamaged = true
			continue
		}
		registry.unknownProjectReservations[reservation.Identity] = reservation.TrashID
	}
	registry.projectRegistryDirty = false
	return false
}

func (registry *projectQuotaRegistry) reconcileProjectReservations(store *Store, entries, trashEntries []os.DirEntry) error {
	objects := make(map[string]os.DirEntry, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != trashDirectoryName {
			objects[entry.Name()] = entry
		}
	}
	trash := make(map[string]os.DirEntry, len(trashEntries))
	for _, entry := range trashEntries {
		if entry.IsDir() {
			trash[entry.Name()] = entry
		}
	}

	registry.projectRegistryMu.Lock()
	defer registry.projectRegistryMu.Unlock()
	store.mu.Lock()
	for projectID, reservations := range registry.projectReservations {
		kept := make([]projectReservation, 0, len(reservations))
		for _, reservation := range reservations {
			previous := reservation
			reservation, retain := registry.reconcileProjectReservation(store, projectID, reservation, objects, trash)
			if previous != reservation {
				registry.projectRegistryDirty = true
			}
			if retain {
				kept = append(kept, reservation)
			}
		}
		if len(kept) == 0 {
			if len(reservations) != 0 {
				registry.projectRegistryDirty = true
			}
			delete(registry.projectReservations, projectID)
		} else {
			if len(kept) != len(reservations) {
				registry.projectRegistryDirty = true
			}
			registry.projectReservations[projectID] = kept
		}
	}
	registry.rebuildProjectReservationIndexes()
	for identity, trashID := range registry.unknownProjectReservations {
		previous := trashID
		trashID, retain := registry.reconcileUnknownProjectReservation(store, identity, trashID, objects, trash)
		if previous != trashID || !retain {
			registry.projectRegistryDirty = true
		}
		if retain {
			registry.unknownProjectReservations[identity] = trashID
		} else {
			delete(registry.unknownProjectReservations, identity)
		}
	}

	for _, meta := range store.generationManager.metadataByIdentity {
		registry.addMetadataReservations(meta, "")
	}
	store.mu.Unlock()
	if err := registry.persistProjectReservationsLocked(store); err != nil {
		return fmt.Errorf("persist rebuilt project ID registry: %w", err)
	}
	return nil
}

func (registry *projectQuotaRegistry) reconcileProjectReservation(store *Store, projectID uint32, reservation projectReservation, objects, trash map[string]os.DirEntry) (projectReservation, bool) {
	if reservation.TrashID != "" {
		if _, exists := trash[reservation.TrashID]; exists {
			return reservation, true
		}
		if meta, exists := store.generationManager.metadataByIdentity[reservation.Identity]; exists && metadataHasProjectReservation(meta, projectID, reservation.Generation) {
			reservation.TrashID = ""
			return reservation, true
		}
		if _, damaged := store.generationManager.degraded[reservation.Identity]; damaged {
			if _, objectExists := objects[reservation.Identity]; objectExists {
				reservation.TrashID = ""
				return reservation, true
			}
		}
		return reservation, false
	}
	if _, damaged := store.generationManager.degraded[reservation.Identity]; damaged {
		_, exists := objects[reservation.Identity]
		return reservation, exists
	}
	if meta, exists := store.generationManager.metadataByIdentity[reservation.Identity]; exists {
		return reservation, metadataHasProjectReservation(meta, projectID, reservation.Generation)
	}
	if _, exists := objects[reservation.Identity]; exists {
		return reservation, true
	}
	for trashID, meta := range store.trashCollector.trashMetadata {
		if meta.Identity == reservation.Identity && metadataHasProjectReservation(meta, projectID, reservation.Generation) {
			reservation.TrashID = trashID
			return reservation, true
		}
	}
	return reservation, false
}

func (registry *projectQuotaRegistry) reconcileUnknownProjectReservation(store *Store, identity, trashID string, objects, trash map[string]os.DirEntry) (string, bool) {
	if trashID != "" {
		if _, exists := trash[trashID]; exists {
			return trashID, true
		}
		_, objectExists := objects[identity]
		return "", objectExists
	}
	if _, objectExists := objects[identity]; objectExists {
		return "", true
	}
	for candidateTrashID, meta := range store.trashCollector.trashMetadata {
		if meta.Identity == identity {
			return candidateTrashID, true
		}
	}
	return "", false
}

func metadataHasProjectReservation(meta Metadata, projectID uint32, generation string) bool {
	if meta.ProjectID == projectID && meta.Generation == generation {
		return true
	}
	return slices.ContainsFunc(meta.Retired, func(retired RetiredGeneration) bool {
		return retired.ProjectID == projectID && retired.Generation == generation
	})
}

func (registry *projectQuotaRegistry) addMetadataReservations(meta Metadata, trashID string) {
	if meta.ProjectID != 0 {
		registry.addProjectReservation(meta.ProjectID, projectReservation{Identity: meta.Identity, Generation: meta.Generation, TrashID: trashID})
	}
	for _, retired := range meta.Retired {
		if retired.ProjectID != 0 {
			registry.addProjectReservation(retired.ProjectID, projectReservation{Identity: meta.Identity, Generation: retired.Generation, TrashID: trashID})
		}
	}
}

func (registry *projectQuotaRegistry) addProjectReservation(projectID uint32, reservation projectReservation) {
	key := projectReservationKey{identity: reservation.Identity, generation: reservation.Generation}
	if existing, found := registry.projectOwnersByID[projectID]; found && existing != key {
		registry.projectRegistryDamaged = true
	}
	if existing, found := registry.projectIDByGeneration[key]; found && existing != projectID {
		registry.projectRegistryDamaged = true
	}
	for index, existing := range registry.projectReservations[projectID] {
		if existing.Identity == key.identity && existing.Generation == key.generation {
			if existing != reservation {
				registry.projectReservations[projectID][index] = reservation
				registry.projectRegistryDirty = true
			}
			return
		}
	}
	registry.projectReservations[projectID] = append(registry.projectReservations[projectID], reservation)
	if _, found := registry.projectOwnersByID[projectID]; !found {
		registry.projectOwnersByID[projectID] = key
	}
	if _, found := registry.projectIDByGeneration[key]; !found {
		registry.projectIDByGeneration[key] = projectID
	}
	registry.projectRegistryDirty = true
}

func (registry *projectQuotaRegistry) rebuildProjectReservationIndexes() {
	clear(registry.projectOwnersByID)
	clear(registry.projectIDByGeneration)
	for projectID, reservations := range registry.projectReservations {
		for _, reservation := range reservations {
			key := projectReservationKey{identity: reservation.Identity, generation: reservation.Generation}
			if owner, found := registry.projectOwnersByID[projectID]; found && owner != key {
				registry.projectRegistryDamaged = true
			} else {
				registry.projectOwnersByID[projectID] = key
			}
			if existing, found := registry.projectIDByGeneration[key]; found && existing != projectID {
				registry.projectRegistryDamaged = true
			} else {
				registry.projectIDByGeneration[key] = projectID
			}
		}
	}
}

func (registry *projectQuotaRegistry) syncProjectReservationIndexes(projectID uint32) {
	reservations := registry.projectReservations[projectID]
	if len(reservations) == 0 {
		delete(registry.projectOwnersByID, projectID)
	} else {
		reservation := reservations[0]
		registry.projectOwnersByID[projectID] = projectReservationKey{identity: reservation.Identity, generation: reservation.Generation}
	}
	for key, indexedProjectID := range registry.projectIDByGeneration {
		if indexedProjectID == projectID && !slices.ContainsFunc(reservations, func(reservation projectReservation) bool {
			return reservation.Identity == key.identity && reservation.Generation == key.generation
		}) {
			delete(registry.projectIDByGeneration, key)
		}
	}
	for _, reservation := range reservations {
		key := projectReservationKey{identity: reservation.Identity, generation: reservation.Generation}
		if _, found := registry.projectIDByGeneration[key]; !found {
			registry.projectIDByGeneration[key] = projectID
		}
	}
}

func (registry *projectQuotaRegistry) addUnknownProjectReservation(identity, trashID string) {
	if existing, exists := registry.unknownProjectReservations[identity]; exists && existing == trashID {
		return
	}
	registry.unknownProjectReservations[identity] = trashID
	registry.projectRegistryDirty = true
}

func (registry *projectQuotaRegistry) persistProjectReservationsLocked(store *Store) (resultErr error) {
	store.mu.Lock()
	if registry.projectRegistryDamaged || !registry.projectRegistryDirty {
		store.mu.Unlock()
		return nil
	}
	reservations := make([]projectIDReservation, 0)
	for projectID, owners := range registry.projectReservations {
		for _, owner := range owners {
			reservations = append(reservations, projectIDReservation{ProjectID: projectID, Identity: owner.Identity, Generation: owner.Generation, TrashID: owner.TrashID})
		}
	}
	unknownReservations := make([]unknownProjectReservation, 0, len(registry.unknownProjectReservations))
	for identity, trashID := range registry.unknownProjectReservations {
		unknownReservations = append(unknownReservations, unknownProjectReservation{Identity: identity, TrashID: trashID})
	}
	store.mu.Unlock()
	defer func() {
		store.mu.Lock()
		if resultErr == nil {
			registry.projectRegistryDirty = false
		} else {
			registry.projectRegistryDirty = true
		}
		store.mu.Unlock()
	}()
	sort.Slice(unknownReservations, func(i, j int) bool { return unknownReservations[i].Identity < unknownReservations[j].Identity })
	sort.Slice(reservations, func(i, j int) bool {
		if reservations[i].ProjectID == reservations[j].ProjectID {
			if reservations[i].Identity == reservations[j].Identity {
				return reservations[i].Generation < reservations[j].Generation
			}
			return reservations[i].Identity < reservations[j].Identity
		}
		return reservations[i].ProjectID < reservations[j].ProjectID
	})
	data, err := json.Marshal(projectReservationDocument{
		FormatVersion:       storeFormatVersion,
		Reservations:        reservations,
		UnknownReservations: unknownReservations,
	})
	if err != nil {
		return err
	}
	temp := ".project-ids-" + uuid.NewV7().String()
	file, err := store.metadataRepository.rootFS.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = store.metadataRepository.rootFS.Remove(temp)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = store.metadataRepository.rootFS.Remove(temp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = store.metadataRepository.rootFS.Remove(temp)
		return err
	}
	if err := store.metadataRepository.rootFS.Rename(temp, projectRegistryName); err != nil {
		_ = store.metadataRepository.rootFS.Remove(temp)
		return err
	}
	directory, err := store.metadataRepository.rootFS.Open(".")
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	return nil
}

func (registry *projectQuotaRegistry) retryProjectRegistry(store *Store) error {
	registry.projectRegistryMu.Lock()
	defer registry.projectRegistryMu.Unlock()
	return registry.persistProjectReservationsLocked(store)
}

func (registry *projectQuotaRegistry) projectReservationLocked(projectID uint32, identity, generation string) (projectReservation, bool) {
	for _, reservation := range registry.projectReservations[projectID] {
		if reservation.Identity == identity && reservation.Generation == generation {
			return reservation, true
		}
	}
	return projectReservation{}, false
}

func (registry *projectQuotaRegistry) restoreProjectReservation(store *Store, projectID uint32, identity, generation string, previous projectReservation, hadPrevious bool) error {
	if projectID == 0 {
		return nil
	}
	registry.projectRegistryMu.Lock()
	defer registry.projectRegistryMu.Unlock()
	return registry.restoreProjectReservationLocked(store, projectID, identity, generation, previous, hadPrevious)
}

func (registry *projectQuotaRegistry) restoreProjectReservationLocked(store *Store, projectID uint32, identity, generation string, previous projectReservation, hadPrevious bool) error {
	store.mu.Lock()
	reservations := registry.projectReservations[projectID]
	index := slices.IndexFunc(reservations, func(reservation projectReservation) bool {
		return reservation.Identity == identity && reservation.Generation == generation
	})
	if hadPrevious {
		if index < 0 {
			registry.projectReservations[projectID] = append(reservations, previous)
			registry.projectRegistryDirty = true
		} else {
			if reservations[index] != previous {
				registry.projectRegistryDirty = true
			}
			reservations[index] = previous
			registry.projectReservations[projectID] = reservations
		}
	} else if index >= 0 {
		registry.projectRegistryDirty = true
		reservations = slices.Delete(reservations, index, index+1)
		if len(reservations) == 0 {
			delete(registry.projectReservations, projectID)
		} else {
			registry.projectReservations[projectID] = reservations
		}
	}
	registry.rebuildProjectReservationIndexes()
	store.mu.Unlock()
	if err := registry.persistProjectReservationsLocked(store); err != nil {
		return fmt.Errorf("restore project ID reservation after failed retired generation detach: %w", err)
	}
	return nil
}

func (registry *projectQuotaRegistry) projectID(store *Store, identity, generation string) (uint32, error) {
	registry.projectRegistryMu.Lock()
	defer registry.projectRegistryMu.Unlock()

	store.mu.Lock()
	if registry.projectRegistryDamaged {
		store.mu.Unlock()
		return 0, errors.New("project ID allocation is disabled because the reservation registry is damaged")
	}
	if len(registry.unknownProjectReservations) > 0 {
		store.mu.Unlock()
		return 0, errors.New("project ID allocation is unavailable while damaged cache projects are quarantined")
	}
	dirty := registry.projectRegistryDirty
	store.mu.Unlock()
	if dirty {
		if err := registry.persistProjectReservationsLocked(store); err != nil {
			return 0, fmt.Errorf("persist pending project ID registry update: %w", err)
		}
	}
	key := projectReservationKey{identity: identity, generation: generation}
	store.mu.Lock()
	existingProjectID, hasReservation := registry.projectIDByGeneration[key]
	store.mu.Unlock()
	if hasReservation {
		return existingProjectID, nil
	}
	hash, _ := hex.DecodeString(identity[:8])
	hashValue := uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
	offset := hashValue % registry.projectIDCount
	projectID := registry.projectIDStart + offset
	for range registry.projectIDCount {
		store.mu.Lock()
		if _, exists := registry.projectReservations[projectID]; !exists {
			registry.addProjectReservation(projectID, projectReservation{Identity: identity, Generation: generation})
			store.mu.Unlock()
			if err := registry.persistProjectReservationsLocked(store); err != nil {
				return 0, fmt.Errorf("reserve XFS project ID: %w", err)
			}
			return projectID, nil
		}
		store.mu.Unlock()
		offset = (offset + 1) % registry.projectIDCount
		projectID = registry.projectIDStart + offset
	}
	return 0, errors.New("no XFS project IDs are available")
}

func (s *Store) loadProjectRegistry() bool {
	return s.projectQuotaRegistry.loadProjectRegistry(s)
}
func (s *Store) reconcileProjectReservations(entries, trashEntries []os.DirEntry) error {
	return s.projectQuotaRegistry.reconcileProjectReservations(s, entries, trashEntries)
}
func (s *Store) addMetadataReservations(meta Metadata, trashID string) {
	s.projectQuotaRegistry.addMetadataReservations(meta, trashID)
}
func (s *Store) addProjectReservation(projectID uint32, reservation projectReservation) {
	s.projectQuotaRegistry.addProjectReservation(projectID, reservation)
}
func (s *Store) rebuildProjectReservationIndexes() {
	s.projectQuotaRegistry.rebuildProjectReservationIndexes()
}
func (s *Store) syncProjectReservationIndexes(projectID uint32) {
	s.projectQuotaRegistry.syncProjectReservationIndexes(projectID)
}
func (s *Store) addUnknownProjectReservation(identity, trashID string) {
	s.projectQuotaRegistry.addUnknownProjectReservation(identity, trashID)
}
func (s *Store) persistProjectReservationsLocked() (resultErr error) {
	return s.projectQuotaRegistry.persistProjectReservationsLocked(s)
}
func (s *Store) retryProjectRegistry() error {
	return s.projectQuotaRegistry.retryProjectRegistry(s)
}
func (s *Store) projectReservationLocked(projectID uint32, identity, generation string) (projectReservation, bool) {
	return s.projectQuotaRegistry.projectReservationLocked(projectID, identity, generation)
}
func (s *Store) restoreProjectReservation(projectID uint32, identity, generation string, previous projectReservation, hadPrevious bool) error {
	return s.projectQuotaRegistry.restoreProjectReservation(s, projectID, identity, generation, previous, hadPrevious)
}
func (s *Store) restoreProjectReservationLocked(projectID uint32, identity, generation string, previous projectReservation, hadPrevious bool) error {
	return s.projectQuotaRegistry.restoreProjectReservationLocked(s, projectID, identity, generation, previous, hadPrevious)
}
func (s *Store) projectID(identity, generation string) (uint32, error) {
	return s.projectQuotaRegistry.projectID(s, identity, generation)
}
