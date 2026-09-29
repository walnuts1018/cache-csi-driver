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
	"time"
	"uuid"
)

const metadataName = ".cache-csi.json"

const SharingPolicyShared = "Shared"

const SharingPolicyExclusive = "Exclusive"

type Lease struct {
	ID         string `json:"id"`
	Target     string `json:"target"`
	Generation string `json:"generation,omitempty"`
	Namespace  string `json:"namespace"`
	PodName    string `json:"podName"`
	PodUID     string `json:"podUID"`
	ReadOnly   bool   `json:"readOnly,omitempty"`
	NoExec     bool   `json:"noExec,omitempty"`
}

type RetiredGeneration struct {
	Generation      string `json:"generation"`
	ProjectID       uint32 `json:"projectID,omitempty"`
	ProjectAssigned bool   `json:"projectAssigned,omitempty"`
	QuotaBytes      int64  `json:"quotaBytes,omitempty"`
	Policy          Policy `json:"policy"`
}

type Policy struct {
	ClassName            string        `json:"className"`
	ClassUID             string        `json:"classUID"`
	SharingPolicy        string        `json:"sharingPolicy,omitempty"`
	DiscardOnLastRelease bool          `json:"discardOnLastRelease,omitempty"`
	NoExec               bool          `json:"noExec"`
	SchemaVersion        string        `json:"schemaVersion"`
	CrashRecoveryReuse   bool          `json:"crashRecoveryReuse"`
	EvictRunning         bool          `json:"evictRunning"`
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
	Identity        string              `json:"identity"`
	Generation      string              `json:"generation"`
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

func (s *Store) indexObjectMetadata(meta Metadata) {
	if previous, exists := s.metadataByIdentity[meta.Identity]; exists {
		for _, lease := range previous.Leases {
			delete(s.leaseIndex, lease.ID)
		}
	}
	s.metadataByIdentity[meta.Identity] = meta
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

func (s *Store) markDegraded(identity string, cause error) {
	if meta, exists := s.metadataByIdentity[identity]; exists {
		for _, lease := range meta.Leases {
			delete(s.leaseIndex, lease.ID)
		}
	}
	delete(s.metadataByIdentity, identity)
	s.degraded[identity] = fmt.Errorf("%w: %v", ErrDegradedMetadata, cause)
}

func validateMetadata(identity string, meta Metadata) error {
	if meta.Identity != identity || meta.Generation == "" {
		return errors.New("cache metadata identity or generation is inconsistent")
	}
	if meta.Policy.Retention < 0 {
		return errors.New("cache metadata has a negative retention")
	}
	if !validSharingPolicy(meta.Policy.SharingPolicy) {
		return errors.New("cache metadata has an unsupported sharing policy")
	}
	for _, retired := range meta.Retired {
		if retired.Generation == "" || retired.Policy.Retention < 0 || !validSharingPolicy(retired.Policy.SharingPolicy) {
			return errors.New("cache metadata has an invalid retired generation policy")
		}
	}
	return nil
}

func validSharingPolicy(policy string) bool {
	return policy == "" || policy == SharingPolicyShared || policy == SharingPolicyExclusive
}

func (s *Store) readObjectMetadata(identity string) (Metadata, error) {
	if err := s.degraded[identity]; err != nil {
		return Metadata{}, err
	}
	meta, err := s.readMetadata(filepath.Join(s.root, identity))
	if err != nil {
		s.markDegraded(identity, err)
		return Metadata{}, fmt.Errorf("read cache metadata: %w", s.degraded[identity])
	}
	if err := validateMetadata(identity, meta); err != nil {
		s.markDegraded(identity, err)
		return Metadata{}, s.degraded[identity]
	}
	s.indexObjectMetadata(meta)
	return meta, nil
}

func (s *Store) readMetadata(entry string) (Metadata, error) {
	relative, err := s.relative(entry)
	if err != nil {
		return Metadata{}, err
	}
	data, err := s.rootFS.ReadFile(filepath.Join(relative, metadataName))
	if err != nil {
		return Metadata{}, err
	}
	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return Metadata{}, err
	}
	return meta, nil
}

func (s *Store) writeMetadata(entry string, meta Metadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	relative, err := s.relative(entry)
	if err != nil {
		return err
	}
	temp := filepath.Join(relative, ".metadata-"+uuid.NewV7().String())
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
	if err := s.rootFS.Rename(temp, filepath.Join(relative, metadataName)); err != nil {
		_ = s.rootFS.Remove(temp)
		return err
	}
	dir, err := s.rootFS.Open(relative)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	switch {
	case filepath.Dir(relative) == "." && filepath.Base(relative) != trashDirectoryName:
		if filepath.Base(relative) != meta.Identity || meta.Generation == "" {
			return errors.New("cache metadata identity or generation is inconsistent")
		}
		s.indexObjectMetadata(meta)
	case filepath.Dir(relative) == trashDirectoryName:
		trashID := filepath.Base(relative)
		s.trashMetadata[trashID] = meta
		s.addMetadataReservations(meta, trashID)
	}
	if err := s.persistProjectReservations(); err != nil {
		return fmt.Errorf("persist project ID registry after metadata update: %w", err)
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
