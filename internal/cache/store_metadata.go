package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"uuid"
)

const metadataName = ".cache-csi.json"
const storeFormatVersion = 1

const SharingPolicyShared = "Shared"

const SharingPolicyExclusive = "Exclusive"

type GenerationState string

const (
	GenerationStateActive   GenerationState = "Active"
	GenerationStateRetiring GenerationState = "Retiring"
	GenerationStateRetired  GenerationState = "Retired"
)

type Lease struct {
	ID         string `json:"id"`
	Target     string `json:"target"`
	Generation string `json:"generation,omitempty"`
	Namespace  string `json:"namespace"`
	PodName    string `json:"podName"`
	PodUID     string `json:"podUID"`
	ReadOnly   bool   `json:"readOnly,omitempty"`
	NoExec     bool   `json:"noExec,omitempty"`
	Preparing  bool   `json:"preparing,omitzero"`
}

type RetiredGeneration struct {
	State           GenerationState `json:"state,omitempty"`
	Generation      string          `json:"generation"`
	ProjectID       uint32          `json:"projectID,omitempty"`
	ProjectAssigned bool            `json:"projectAssigned,omitempty"`
	QuotaBytes      int64           `json:"quotaBytes,omitempty"`
	Policy          Policy          `json:"policy"`
}

type Policy struct {
	ClassName     string        `json:"className"`
	ClassUID      string        `json:"classUID"`
	SharingPolicy string        `json:"sharingPolicy,omitempty"`
	NoExec        bool          `json:"noExec"`
	SchemaVersion string        `json:"schemaVersion"`
	QuotaEnabled  bool          `json:"quotaEnabled"`
	MaxBytes      int64         `json:"maxBytes"`
	Retention     time.Duration `json:"retention"`
}

type policyAlias Policy

type persistedPolicy struct {
	policyAlias
	Retention int64 `json:"retention"`
}

func (policy Policy) MarshalJSON() ([]byte, error) {
	return json.Marshal(persistedPolicy{policyAlias: policyAlias(policy), Retention: int64(policy.Retention)})
}

func (policy *Policy) UnmarshalJSON(data []byte) error {
	var persisted persistedPolicy
	if err := json.Unmarshal(data, &persisted); err != nil {
		return err
	}
	*policy = Policy(persisted.policyAlias)
	policy.Retention = time.Duration(persisted.Retention)
	return nil
}

type Metadata struct {
	FormatVersion   int                 `json:"formatVersion"`
	Identity        string              `json:"identity"`
	Generation      string              `json:"generation"`
	GenerationState GenerationState     `json:"generationState,omitempty"`
	PolicyHash      string              `json:"policyHash,omitempty"`
	CreatedAt       time.Time           `json:"createdAt"`
	LastUsed        time.Time           `json:"lastUsed"`
	Leases          []Lease             `json:"leases,omitempty"`
	Policy          Policy              `json:"policy"`
	Dirty           bool                `json:"dirty"`
	ProjectID       uint32              `json:"projectID,omitempty"`
	ProjectAssigned bool                `json:"projectAssigned,omitempty"`
	QuotaBytes      int64               `json:"quotaBytes,omitempty"`
	Retired         []RetiredGeneration `json:"retired,omitempty"`
}

func (manager *generationManager) indexObjectMetadata(store *Store, meta Metadata) {
	_, recoveringDegraded := manager.degraded[meta.Identity]
	if previous, exists := manager.metadataByIdentity[meta.Identity]; exists {
		manager.retiredGenerationCount -= len(previous.Retired)
		for _, lease := range previous.Leases {
			delete(manager.leaseIndex, lease.ID)
		}
	} else if recoveringDegraded {
		for leaseID, identity := range manager.leaseIndex {
			if identity != meta.Identity || slices.ContainsFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID }) {
				continue
			}
			delete(manager.leaseIndex, leaseID)
		}
	}
	manager.metadataByIdentity[meta.Identity] = meta
	manager.retiredGenerationCount += len(meta.Retired)
	delete(manager.degraded, meta.Identity)
	for _, lease := range meta.Leases {
		manager.leaseIndex[lease.ID] = meta.Identity
	}
	for projectID, reservations := range store.projectQuotaRegistry.projectReservations {
		previousLength := len(reservations)
		kept := slices.DeleteFunc(reservations, func(reservation projectReservation) bool {
			return reservation.Identity == meta.Identity && reservation.TrashID == "" && !metadataHasProjectReservation(meta, projectID, reservation.Generation)
		})
		if len(kept) != previousLength {
			store.projectQuotaRegistry.projectRegistryDirty = true
		}
		if len(kept) == 0 {
			delete(store.projectQuotaRegistry.projectReservations, projectID)
		} else {
			store.projectQuotaRegistry.projectReservations[projectID] = kept
		}
		if len(kept) != previousLength {
			store.syncProjectReservationIndexes(projectID)
		}
	}
	for _, retired := range meta.Retired {
		if retired.ProjectID == 0 {
			continue
		}
		store.addProjectReservation(retired.ProjectID, projectReservation{Identity: meta.Identity, Generation: retired.Generation})
	}
	if meta.ProjectID != 0 {
		store.addProjectReservation(meta.ProjectID, projectReservation{Identity: meta.Identity, Generation: meta.Generation})
	}
}

func (manager *generationManager) indexMetadata(store *Store, meta Metadata) {
	store.projectQuotaRegistry.projectRegistryMu.Lock()
	defer store.projectQuotaRegistry.projectRegistryMu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	store.generationManager.indexObjectMetadata(store, meta)
}

func (manager *generationManager) markDegraded(store *Store, identity string, cause error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	manager.markDegradedLocked(identity, cause)
}

func (manager *generationManager) markDegradedLocked(identity string, cause error) {
	if previous, exists := manager.metadataByIdentity[identity]; exists {
		manager.retiredGenerationCount -= len(previous.Retired)
	}
	delete(manager.metadataByIdentity, identity)
	manager.degraded[identity] = fmt.Errorf("%w: %v", ErrDegradedMetadata, cause)
}

func (manager *generationManager) indexDegradedLeaseIDs(store *Store, identity string, meta Metadata) {
	if meta.Identity != identity {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, lease := range meta.Leases {
		if lease.ID == "" || len(lease.ID) > 1024 || strings.ContainsRune(lease.ID, '\x00') {
			continue
		}
		manager.leaseIndex[lease.ID] = identity
	}
}

func validateMetadata(identity string, meta Metadata) error {
	if meta.FormatVersion != storeFormatVersion {
		return fmt.Errorf("unsupported cache metadata format version %d", meta.FormatVersion)
	}
	if meta.Identity != identity || meta.Generation == "" {
		return errors.New("cache metadata identity or generation is inconsistent")
	}
	if meta.GenerationState != GenerationStateActive {
		return errors.New("cache metadata has an unsupported active generation state")
	}
	policyHash, err := generationPolicyHash(meta.Policy)
	if err != nil {
		return fmt.Errorf("hash cache metadata policy: %w", err)
	}
	if meta.PolicyHash == "" || meta.PolicyHash != policyHash {
		return errors.New("cache metadata policy hash does not match its content compatibility policy")
	}
	if meta.Policy.Retention < 0 {
		return errors.New("cache metadata has a negative retention")
	}
	if !validSharingPolicy(meta.Policy.SharingPolicy) {
		return errors.New("cache metadata has an unsupported sharing policy")
	}
	for _, retired := range meta.Retired {
		if retired.Generation == "" || retired.State != GenerationStateRetiring && retired.State != GenerationStateRetired || retired.Policy.Retention < 0 || !validSharingPolicy(retired.Policy.SharingPolicy) {
			return errors.New("cache metadata has an invalid retired generation policy")
		}
		hasLeases := slices.ContainsFunc(meta.Leases, func(lease Lease) bool {
			generation := lease.Generation
			if generation == "" {
				generation = meta.Generation
			}
			return generation == retired.Generation
		})
		if retired.State == GenerationStateRetiring && !hasLeases || retired.State == GenerationStateRetired && hasLeases {
			return errors.New("cache metadata retired generation state does not match its leases")
		}
	}
	return nil
}

func metadataNeedsNormalization(meta Metadata) bool {
	return meta.FormatVersion != storeFormatVersion || meta.GenerationState != GenerationStateActive || meta.PolicyHash == "" || slices.ContainsFunc(meta.Retired, func(retired RetiredGeneration) bool { return retired.State == "" })
}

func generationPolicyHash(policy Policy) (string, error) {
	data, err := json.Marshal(struct {
		SchemaVersion string `json:"schemaVersion"`
	}{SchemaVersion: policy.SchemaVersion})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func validSharingPolicy(policy string) bool {
	return policy == "" || policy == SharingPolicyShared || policy == SharingPolicyExclusive
}

func (component *generationManager) readObjectMetadata(store *Store, identity string) (Metadata, error) {
	store.mu.Lock()
	if err := store.generationManager.degraded[identity]; err != nil {
		store.mu.Unlock()
		return Metadata{}, err
	}
	store.mu.Unlock()
	meta, err := store.metadataRepository.readMetadata(filepath.Join(store.metadataRepository.root, identity))
	if err != nil {
		store.markDegraded(identity, err)
		store.mu.Lock()
		degradedErr := store.generationManager.degraded[identity]
		store.mu.Unlock()
		return Metadata{}, fmt.Errorf("read cache metadata: %w", degradedErr)
	}
	if err := validateMetadata(identity, meta); err != nil {
		store.markDegraded(identity, err)
		store.mu.Lock()
		degradedErr := store.generationManager.degraded[identity]
		store.mu.Unlock()
		return Metadata{}, degradedErr
	}
	store.indexMetadata(meta)
	return meta, nil
}

func (repository *metadataRepository) readMetadata(entry string) (Metadata, error) {
	relative, err := repository.relative(entry)
	if err != nil {
		return Metadata{}, err
	}
	data, err := repository.rootFS.ReadFile(filepath.Join(relative, metadataName))
	if err != nil {
		return Metadata{}, err
	}
	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return Metadata{}, err
	}
	return meta, nil
}

func (repository *metadataRepository) writeAtomicMetadata(relative string, data []byte) error {
	temp := filepath.Join(relative, ".metadata-"+uuid.NewV7().String())
	file, err := repository.rootFS.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = repository.rootFS.Remove(temp)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = repository.rootFS.Remove(temp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = repository.rootFS.Remove(temp)
		return err
	}
	if err := repository.rootFS.Rename(temp, filepath.Join(relative, metadataName)); err != nil {
		_ = repository.rootFS.Remove(temp)
		return err
	}
	directory, err := repository.rootFS.Open(relative)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

func (s *Store) writeMetadata(entry string, meta Metadata) error {
	meta.FormatVersion = storeFormatVersion
	meta.GenerationState = GenerationStateActive
	for index := range meta.Retired {
		if meta.Retired[index].State != "" {
			continue
		}
		if s.hasGenerationLeases(meta, meta.Retired[index].Generation) {
			meta.Retired[index].State = GenerationStateRetiring
		} else {
			meta.Retired[index].State = GenerationStateRetired
		}
	}
	policyHash, err := generationPolicyHash(meta.Policy)
	if err != nil {
		return fmt.Errorf("hash cache generation policy: %w", err)
	}
	meta.PolicyHash = policyHash
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	relative, err := s.metadataRepository.relative(entry)
	if err != nil {
		return err
	}
	if filepath.Dir(relative) == "." && (filepath.Base(relative) != meta.Identity || meta.Generation == "") {
		return errors.New("cache metadata identity or generation is inconsistent")
	}
	if err := s.metadataRepository.writeAtomicMetadata(relative, data); err != nil {
		return err
	}
	s.projectQuotaRegistry.projectRegistryMu.Lock()
	defer s.projectQuotaRegistry.projectRegistryMu.Unlock()
	s.mu.Lock()
	switch {
	case filepath.Dir(relative) == "." && filepath.Base(relative) != trashDirectoryName:
		s.indexObjectMetadata(meta)
	case filepath.Dir(relative) == trashDirectoryName:
		trashID := filepath.Base(relative)
		s.trashCollector.trashMetadata[trashID] = meta
		s.addMetadataReservations(meta, trashID)
	}
	registryDirty := s.projectQuotaRegistry.projectRegistryDirty
	s.mu.Unlock()
	if registryDirty {
		_ = s.persistProjectReservationsLocked()
	}
	return nil
}

func stableIdentity(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func validIdentity(identity string) bool {
	if len(identity) != 64 {
		return false
	}
	_, err := hex.DecodeString(identity)
	return err == nil
}

func (s *Store) indexObjectMetadata(meta Metadata) {
	s.generationManager.indexObjectMetadata(s, meta)
}

func (s *Store) indexMetadata(meta Metadata) {
	s.generationManager.indexMetadata(s, meta)
}

func (s *Store) markDegraded(identity string, cause error) {
	s.generationManager.markDegraded(s, identity, cause)
}

func (s *Store) clearDegraded(identity string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.generationManager.degraded, identity)
}

func (s *Store) indexDegradedLeaseIDs(identity string, meta Metadata) {
	s.generationManager.indexDegradedLeaseIDs(s, identity, meta)
}

func (s *Store) readObjectMetadata(identity string) (Metadata, error) {
	return s.generationManager.readObjectMetadata(s, identity)
}
