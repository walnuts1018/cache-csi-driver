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

const PressurePolicyUnusedOnly = "UnusedOnly"

const PressurePolicyEvict = "Evict"

const PressurePolicyForceDelete = "ForceDelete"

type GenerationState string

const (
	GenerationStateActive   GenerationState = "Active"
	GenerationStateRetiring GenerationState = "Retiring"
	GenerationStateRetired  GenerationState = "Retired"
)

type PressureVictim struct {
	Lease       Lease
	ForceDelete bool
}

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
	ClassName            string        `json:"className"`
	ClassUID             string        `json:"classUID"`
	SharingPolicy        string        `json:"sharingPolicy,omitempty"`
	DiscardOnLastRelease bool          `json:"discardOnLastRelease,omitempty"`
	NoExec               bool          `json:"noExec"`
	SchemaVersion        string        `json:"schemaVersion"`
	CrashRecoveryReuse   bool          `json:"crashRecoveryReuse"`
	PressurePolicy       string        `json:"pressurePolicy,omitempty"`
	QuotaEnabled         bool          `json:"quotaEnabled"`
	MaxBytes             int64         `json:"maxBytes"`
	Retention            time.Duration `json:"retention"`
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

func (manager *generationManager) indexObjectMetadata(meta Metadata) {
	s := manager.store
	_, recoveringDegraded := s.degraded[meta.Identity]
	if previous, exists := s.metadataByIdentity[meta.Identity]; exists {
		s.removeFallbackReservation(previous)
		s.retiredGenerationCount -= len(previous.Retired)
		for _, lease := range previous.Leases {
			delete(s.leaseIndex, lease.ID)
		}
	} else if recoveringDegraded {
		for leaseID, identity := range s.leaseIndex {
			if identity != meta.Identity || slices.ContainsFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID }) {
				continue
			}
			delete(s.leaseIndex, leaseID)
		}
	}
	s.metadataByIdentity[meta.Identity] = meta
	s.addFallbackReservation(meta)
	s.retiredGenerationCount += len(meta.Retired)
	delete(s.degraded, meta.Identity)
	for _, lease := range meta.Leases {
		s.leaseIndex[lease.ID] = meta.Identity
	}
	for projectID, reservations := range s.projectReservations {
		previousLength := len(reservations)
		kept := slices.DeleteFunc(reservations, func(reservation projectReservation) bool {
			return reservation.Identity == meta.Identity && reservation.TrashID == "" && !metadataHasProjectReservation(meta, projectID, reservation.Generation)
		})
		if len(kept) != previousLength {
			s.projectRegistryDirty = true
		}
		if len(kept) == 0 {
			delete(s.projectReservations, projectID)
		} else {
			s.projectReservations[projectID] = kept
		}
		if len(kept) != previousLength {
			s.syncProjectReservationIndexes(projectID)
		}
	}
	for _, retired := range meta.Retired {
		if retired.ProjectID == 0 {
			continue
		}
		s.addProjectReservation(retired.ProjectID, projectReservation{Identity: meta.Identity, Generation: retired.Generation})
	}
	if meta.ProjectID != 0 {
		s.addProjectReservation(meta.ProjectID, projectReservation{Identity: meta.Identity, Generation: meta.Generation})
	}
}

func (manager *generationManager) indexMetadata(meta Metadata) {
	s := manager.store
	s.projectRegistryMu.Lock()
	defer s.projectRegistryMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexObjectMetadata(meta)
}

func (manager *generationManager) markDegraded(identity string, cause error) {
	s := manager.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markDegradedLocked(identity, cause)
}

func (manager *generationManager) markDegradedLocked(identity string, cause error) {
	s := manager.store
	if previous, exists := s.metadataByIdentity[identity]; exists {
		s.removeFallbackReservation(previous)
		s.retiredGenerationCount -= len(previous.Retired)
	}
	delete(s.metadataByIdentity, identity)
	s.degraded[identity] = fmt.Errorf("%w: %v", ErrDegradedMetadata, cause)
}

func (manager *generationManager) addFallbackReservation(meta Metadata) {
	s := manager.store
	if meta.Policy.DiscardOnLastRelease && len(meta.Leases) > 0 && meta.Policy.MaxBytes > 0 {
		s.fallbackReservedBytes += meta.Policy.MaxBytes
	}
}

func (manager *generationManager) removeFallbackReservation(meta Metadata) {
	s := manager.store
	if meta.Policy.DiscardOnLastRelease && len(meta.Leases) > 0 && meta.Policy.MaxBytes > 0 {
		s.fallbackReservedBytes -= meta.Policy.MaxBytes
	}
}

func (manager *generationManager) indexDegradedLeaseIDs(identity string, meta Metadata) {
	s := manager.store
	if meta.Identity != identity {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, lease := range meta.Leases {
		if lease.ID == "" || len(lease.ID) > 1024 || strings.ContainsRune(lease.ID, '\x00') {
			continue
		}
		s.leaseIndex[lease.ID] = identity
	}
}

func validateMetadata(identity string, meta Metadata) error {
	if meta.FormatVersion != 0 && meta.FormatVersion != storeFormatVersion {
		return fmt.Errorf("unsupported cache metadata format version %d", meta.FormatVersion)
	}
	if meta.Identity != identity || meta.Generation == "" {
		return errors.New("cache metadata identity or generation is inconsistent")
	}
	if meta.GenerationState != "" && meta.GenerationState != GenerationStateActive {
		return errors.New("cache metadata has an unsupported active generation state")
	}
	if meta.PolicyHash != "" {
		policyHash, err := generationPolicyHash(meta.Policy)
		if err != nil {
			return fmt.Errorf("hash cache metadata policy: %w", err)
		}
		if meta.PolicyHash != policyHash {
			return errors.New("cache metadata policy hash does not match its policy snapshot")
		}
	}
	if meta.Policy.Retention < 0 {
		return errors.New("cache metadata has a negative retention")
	}
	if !validSharingPolicy(meta.Policy.SharingPolicy) {
		return errors.New("cache metadata has an unsupported sharing policy")
	}
	if !validPressurePolicy(meta.Policy.PressurePolicy) {
		return errors.New("cache metadata has an unsupported pressure policy")
	}
	for _, retired := range meta.Retired {
		if retired.Generation == "" || retired.State != "" && retired.State != GenerationStateRetiring && retired.State != GenerationStateRetired || retired.Policy.Retention < 0 || !validSharingPolicy(retired.Policy.SharingPolicy) || !validPressurePolicy(retired.Policy.PressurePolicy) {
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
	if meta.FormatVersion != storeFormatVersion || meta.GenerationState == "" || meta.PolicyHash == "" {
		return true
	}
	return slices.ContainsFunc(meta.Retired, func(retired RetiredGeneration) bool { return retired.State == "" })
}

func generationPolicyHash(policy Policy) (string, error) {
	data, err := json.Marshal(policy)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func validPressurePolicy(policy string) bool {
	return policy == "" || policy == PressurePolicyUnusedOnly || policy == PressurePolicyEvict || policy == PressurePolicyForceDelete
}

func validSharingPolicy(policy string) bool {
	return policy == "" || policy == SharingPolicyShared || policy == SharingPolicyExclusive
}

func (manager *generationManager) readObjectMetadata(identity string) (Metadata, error) {
	s := manager.store
	s.mu.Lock()
	if err := s.degraded[identity]; err != nil {
		s.mu.Unlock()
		return Metadata{}, err
	}
	s.mu.Unlock()
	meta, err := s.readMetadata(filepath.Join(s.root, identity))
	if err != nil {
		s.markDegraded(identity, err)
		s.mu.Lock()
		degradedErr := s.degraded[identity]
		s.mu.Unlock()
		return Metadata{}, fmt.Errorf("read cache metadata: %w", degradedErr)
	}
	if err := validateMetadata(identity, meta); err != nil {
		s.markDegraded(identity, err)
		s.mu.Lock()
		degradedErr := s.degraded[identity]
		s.mu.Unlock()
		return Metadata{}, degradedErr
	}
	s.indexMetadata(meta)
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
	relative, err := s.relative(entry)
	if err != nil {
		return err
	}
	if filepath.Dir(relative) == "." && (filepath.Base(relative) != meta.Identity || meta.Generation == "") {
		return errors.New("cache metadata identity or generation is inconsistent")
	}
	if err := s.writeAtomicMetadata(relative, data); err != nil {
		return err
	}
	s.projectRegistryMu.Lock()
	defer s.projectRegistryMu.Unlock()
	s.mu.Lock()
	switch {
	case filepath.Dir(relative) == "." && filepath.Base(relative) != trashDirectoryName:
		s.indexObjectMetadata(meta)
	case filepath.Dir(relative) == trashDirectoryName:
		trashID := filepath.Base(relative)
		s.trashMetadata[trashID] = meta
		s.addMetadataReservations(meta, trashID)
	}
	registryDirty := s.projectRegistryDirty
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
