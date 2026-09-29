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

func (s *Store) loadProjectRegistry() bool {
	data, err := s.rootFS.ReadFile(projectRegistryName)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	if err != nil {
		s.projectRegistryDamaged = true
		return false
	}
	var document projectReservationDocument
	if err := json.Unmarshal(data, &document); err != nil {
		s.projectRegistryDamaged = true
		return false
	}
	if document.FormatVersion != 0 && document.FormatVersion != storeFormatVersion {
		s.projectRegistryDamaged = true
		return false
	}
	for _, item := range document.Reservations {
		if item.ProjectID == 0 || !validIdentity(item.Identity) || item.Generation == "" {
			s.projectRegistryDamaged = true
			continue
		}
		s.addProjectReservation(item.ProjectID, projectReservation{Identity: item.Identity, Generation: item.Generation, TrashID: item.TrashID})
	}
	for _, reservation := range document.UnknownReservations {
		if !validIdentity(reservation.Identity) {
			s.projectRegistryDamaged = true
			continue
		}
		s.unknownProjectReservations[reservation.Identity] = reservation.TrashID
	}
	s.projectRegistryDirty = false
	return false
}

func (s *Store) reconcileProjectReservations(entries, trashEntries []os.DirEntry) error {
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

	for projectID, reservations := range s.projectReservations {
		kept := make([]projectReservation, 0, len(reservations))
		for _, reservation := range reservations {
			previous := reservation
			reservation, retain := s.reconcileProjectReservation(projectID, reservation, objects, trash)
			if previous != reservation {
				s.projectRegistryDirty = true
			}
			if retain {
				kept = append(kept, reservation)
			}
		}
		if len(kept) == 0 {
			if len(reservations) != 0 {
				s.projectRegistryDirty = true
			}
			delete(s.projectReservations, projectID)
		} else {
			if len(kept) != len(reservations) {
				s.projectRegistryDirty = true
			}
			s.projectReservations[projectID] = kept
		}
	}
	s.rebuildProjectReservationIndexes()
	for identity, trashID := range s.unknownProjectReservations {
		previous := trashID
		trashID, retain := s.reconcileUnknownProjectReservation(identity, trashID, objects, trash)
		if previous != trashID || !retain {
			s.projectRegistryDirty = true
		}
		if retain {
			s.unknownProjectReservations[identity] = trashID
		} else {
			delete(s.unknownProjectReservations, identity)
		}
	}

	for _, meta := range s.metadataByIdentity {
		s.addMetadataReservations(meta, "")
	}
	if err := s.persistProjectReservations(); err != nil {
		return fmt.Errorf("persist rebuilt project ID registry: %w", err)
	}
	return nil
}

func (s *Store) reconcileProjectReservation(projectID uint32, reservation projectReservation, objects, trash map[string]os.DirEntry) (projectReservation, bool) {
	if reservation.TrashID != "" {
		if _, exists := trash[reservation.TrashID]; exists {
			return reservation, true
		}
		if meta, exists := s.metadataByIdentity[reservation.Identity]; exists && metadataHasProjectReservation(meta, projectID, reservation.Generation) {
			reservation.TrashID = ""
			return reservation, true
		}
		if _, damaged := s.degraded[reservation.Identity]; damaged {
			if _, objectExists := objects[reservation.Identity]; objectExists {
				reservation.TrashID = ""
				return reservation, true
			}
		}
		return reservation, false
	}
	if _, damaged := s.degraded[reservation.Identity]; damaged {
		_, exists := objects[reservation.Identity]
		return reservation, exists
	}
	if meta, exists := s.metadataByIdentity[reservation.Identity]; exists {
		return reservation, metadataHasProjectReservation(meta, projectID, reservation.Generation)
	}
	if _, exists := objects[reservation.Identity]; exists {
		return reservation, true
	}
	for trashID, meta := range s.trashMetadata {
		if meta.Identity == reservation.Identity && metadataHasProjectReservation(meta, projectID, reservation.Generation) {
			reservation.TrashID = trashID
			return reservation, true
		}
	}
	return reservation, false
}

func (s *Store) reconcileUnknownProjectReservation(identity, trashID string, objects, trash map[string]os.DirEntry) (string, bool) {
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
	for candidateTrashID, meta := range s.trashMetadata {
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

func (s *Store) addMetadataReservations(meta Metadata, trashID string) {
	if meta.ProjectID != 0 {
		s.addProjectReservation(meta.ProjectID, projectReservation{Identity: meta.Identity, Generation: meta.Generation, TrashID: trashID})
	}
	for _, retired := range meta.Retired {
		if retired.ProjectID != 0 {
			s.addProjectReservation(retired.ProjectID, projectReservation{Identity: meta.Identity, Generation: retired.Generation, TrashID: trashID})
		}
	}
}

func (s *Store) addProjectReservation(projectID uint32, reservation projectReservation) {
	key := projectReservationKey{identity: reservation.Identity, generation: reservation.Generation}
	if existing, found := s.projectOwnersByID[projectID]; found && existing != key {
		s.projectRegistryDamaged = true
	}
	if existing, found := s.projectIDByGeneration[key]; found && existing != projectID {
		s.projectRegistryDamaged = true
	}
	for index, existing := range s.projectReservations[projectID] {
		if existing.Identity == key.identity && existing.Generation == key.generation {
			if existing != reservation {
				s.projectReservations[projectID][index] = reservation
				s.projectRegistryDirty = true
			}
			return
		}
	}
	s.projectReservations[projectID] = append(s.projectReservations[projectID], reservation)
	if _, found := s.projectOwnersByID[projectID]; !found {
		s.projectOwnersByID[projectID] = key
	}
	if _, found := s.projectIDByGeneration[key]; !found {
		s.projectIDByGeneration[key] = projectID
	}
	s.projectRegistryDirty = true
}

func (s *Store) rebuildProjectReservationIndexes() {
	clear(s.projectOwnersByID)
	clear(s.projectIDByGeneration)
	for projectID, reservations := range s.projectReservations {
		for _, reservation := range reservations {
			key := projectReservationKey{identity: reservation.Identity, generation: reservation.Generation}
			if owner, found := s.projectOwnersByID[projectID]; found && owner != key {
				s.projectRegistryDamaged = true
			} else {
				s.projectOwnersByID[projectID] = key
			}
			if existing, found := s.projectIDByGeneration[key]; found && existing != projectID {
				s.projectRegistryDamaged = true
			} else {
				s.projectIDByGeneration[key] = projectID
			}
		}
	}
}

func (s *Store) syncProjectReservationIndexes(projectID uint32) {
	reservations := s.projectReservations[projectID]
	if len(reservations) == 0 {
		delete(s.projectOwnersByID, projectID)
	} else {
		reservation := reservations[0]
		s.projectOwnersByID[projectID] = projectReservationKey{identity: reservation.Identity, generation: reservation.Generation}
	}
	for key, indexedProjectID := range s.projectIDByGeneration {
		if indexedProjectID == projectID && !slices.ContainsFunc(reservations, func(reservation projectReservation) bool {
			return reservation.Identity == key.identity && reservation.Generation == key.generation
		}) {
			delete(s.projectIDByGeneration, key)
		}
	}
	for _, reservation := range reservations {
		key := projectReservationKey{identity: reservation.Identity, generation: reservation.Generation}
		if _, found := s.projectIDByGeneration[key]; !found {
			s.projectIDByGeneration[key] = projectID
		}
	}
}

func (s *Store) addUnknownProjectReservation(identity, trashID string) {
	if existing, exists := s.unknownProjectReservations[identity]; exists && existing == trashID {
		return
	}
	s.unknownProjectReservations[identity] = trashID
	s.projectRegistryDirty = true
}

func (s *Store) persistProjectReservations() error {
	if s.projectRegistryDamaged || !s.projectRegistryDirty {
		return nil
	}
	reservations := make([]projectIDReservation, 0)
	for projectID, owners := range s.projectReservations {
		for _, owner := range owners {
			reservations = append(reservations, projectIDReservation{ProjectID: projectID, Identity: owner.Identity, Generation: owner.Generation, TrashID: owner.TrashID})
		}
	}
	unknownReservations := make([]unknownProjectReservation, 0, len(s.unknownProjectReservations))
	for identity, trashID := range s.unknownProjectReservations {
		unknownReservations = append(unknownReservations, unknownProjectReservation{Identity: identity, TrashID: trashID})
	}
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
	file, err := s.rootFS.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = s.rootFS.Remove(temp)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = s.rootFS.Remove(temp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = s.rootFS.Remove(temp)
		return err
	}
	if err := s.rootFS.Rename(temp, projectRegistryName); err != nil {
		_ = s.rootFS.Remove(temp)
		return err
	}
	directory, err := s.rootFS.Open(".")
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
	s.projectRegistryDirty = false
	return nil
}

func (s *Store) projectReservation(projectID uint32, identity, generation string) (projectReservation, bool) {
	for _, reservation := range s.projectReservations[projectID] {
		if reservation.Identity == identity && reservation.Generation == generation {
			return reservation, true
		}
	}
	return projectReservation{}, false
}

func (s *Store) restoreProjectReservation(projectID uint32, identity, generation string, previous projectReservation, hadPrevious bool) error {
	if projectID == 0 {
		return nil
	}
	reservations := s.projectReservations[projectID]
	index := slices.IndexFunc(reservations, func(reservation projectReservation) bool {
		return reservation.Identity == identity && reservation.Generation == generation
	})
	if hadPrevious {
		if index < 0 {
			s.projectReservations[projectID] = append(reservations, previous)
			s.projectRegistryDirty = true
		} else {
			if reservations[index] != previous {
				s.projectRegistryDirty = true
			}
			reservations[index] = previous
			s.projectReservations[projectID] = reservations
		}
	} else if index >= 0 {
		s.projectRegistryDirty = true
		reservations = slices.Delete(reservations, index, index+1)
		if len(reservations) == 0 {
			delete(s.projectReservations, projectID)
		} else {
			s.projectReservations[projectID] = reservations
		}
	}
	s.rebuildProjectReservationIndexes()
	if err := s.persistProjectReservations(); err != nil {
		return fmt.Errorf("restore project ID reservation after failed retired generation detach: %w", err)
	}
	return nil
}

func (s *Store) projectIDLocked(identity, generation string) (uint32, error) {
	if s.projectRegistryDamaged {
		return 0, errors.New("project ID allocation is disabled because the reservation registry is damaged")
	}
	if len(s.unknownProjectReservations) > 0 {
		return 0, errors.New("project ID allocation is unavailable while damaged cache projects are quarantined")
	}
	if s.projectRegistryDirty {
		if err := s.persistProjectReservations(); err != nil {
			return 0, fmt.Errorf("persist pending project ID registry update: %w", err)
		}
	}
	hash, _ := hex.DecodeString(identity[:8])
	hashValue := uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
	offset := hashValue % s.projectIDCount
	projectID := s.projectIDStart + offset
	for range s.projectIDCount {
		if _, exists := s.projectReservations[projectID]; !exists {
			previous, hadPrevious := s.projectReservation(projectID, identity, generation)
			s.addProjectReservation(projectID, projectReservation{Identity: identity, Generation: generation})
			if err := s.persistProjectReservations(); err != nil {
				restoreErr := s.restoreProjectReservation(projectID, identity, generation, previous, hadPrevious)
				return 0, errors.Join(fmt.Errorf("reserve XFS project ID: %w", err), restoreErr)
			}
			return projectID, nil
		}
		offset = (offset + 1) % s.projectIDCount
		projectID = s.projectIDStart + offset
	}
	return 0, errors.New("no XFS project IDs are available")
}
