package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"uuid"

	"golang.org/x/sys/unix"
)

const metadataName = ".cache-csi.json"
const trashDirectoryName = ".trash"
const projectRegistryName = ".project-ids.json"
const trashBatchSize = 16

var ErrQuotaPolicyConflict = errors.New("active cache generation cannot change its effective quota")
var ErrExclusivePolicyConflict = errors.New("exclusive cache identity already has an active lease")
var ErrDegradedMetadata = errors.New("cache metadata is degraded")
var ErrPressureReclaimIncomplete = errors.New("cache pressure reclaim is incomplete")

const SharingPolicyShared = "Shared"
const SharingPolicyExclusive = "Exclusive"

type PressureConfig struct {
	HighFreePercent      int
	LowFreePercent       int
	HighInodeFreePercent int
	LowInodeFreePercent  int
}

type StoreOptions struct {
	Pressure       PressureConfig
	ProjectIDStart uint32
	ProjectIDCount uint32
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
}

type RetiredGeneration struct {
	Generation      string `json:"generation"`
	ProjectID       uint32 `json:"projectID,omitempty"`
	ProjectAssigned bool   `json:"projectAssigned,omitempty"`
	QuotaBytes      int64  `json:"quotaBytes,omitempty"`
	Policy          Policy `json:"policy"`
}

type Policy struct {
	ClassName          string        `json:"className"`
	ClassUID           string        `json:"classUID"`
	SharingPolicy      string        `json:"sharingPolicy,omitempty"`
	NoExec             bool          `json:"noExec"`
	SchemaVersion      string        `json:"schemaVersion"`
	CrashRecoveryReuse bool          `json:"crashRecoveryReuse"`
	EvictRunning       bool          `json:"evictRunning"`
	QuotaEnabled       bool          `json:"quotaEnabled"`
	MaxBytes           int64         `json:"maxBytes"`
	Retention          time.Duration `json:"retention"`
}

type projectReservation struct {
	Identity   string `json:"identity"`
	Generation string `json:"generation"`
	TrashID    string `json:"trashID,omitempty"`
}

type projectReservationDocument struct {
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

type AcquireOptions struct {
	Identity string
	Lease    Lease
	Policy   Policy
}

type Store struct {
	root                       string
	rootFS                     *os.Root
	pressure                   PressureConfig
	projectIDStart             uint32
	projectIDCount             uint32
	projectRegistryDamaged     bool
	pressureActive             bool
	pressureDetachFailed       map[string]struct{}
	metadataByIdentity         map[string]Metadata
	leaseIndex                 map[string]string
	degraded                   map[string]error
	projectReservations        map[uint32][]projectReservation
	unknownProjectReservations map[string]string
	trashMetadata              map[string]Metadata
	trashCursor                string
	removeTrashEntry           func(string) error
	mu                         sync.Mutex
	stopTrash                  chan struct{}
	trashDone                  chan struct{}
	trashRequests              chan chan error
	closeOnce                  sync.Once
	closeErr                   error
}

func (s *Store) Root() string { return s.root }

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.stopTrash)
		<-s.trashDone
		s.closeErr = s.rootFS.Close()
	})
	return s.closeErr
}

func (s *Store) relative(path string) (string, error) {
	relative, err := filepath.Rel(s.root, filepath.Clean(path))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", fmt.Errorf("path %q is outside cache root", path)
	}
	return relative, nil
}

func (s *Store) readDir(path string) ([]os.DirEntry, error) {
	relative, err := s.relative(path)
	if err != nil {
		return nil, err
	}
	return fs.ReadDir(s.rootFS.FS(), relative)
}

func (s *Store) stat(path string) error {
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	_, err = s.rootFS.Stat(relative)
	return err
}

func (s *Store) ensureDirectory(path string) error {
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	if err := s.rootFS.MkdirAll(relative, 0o700); err != nil {
		return err
	}
	info, err := s.rootFS.Lstat(relative)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q is not a real directory", path)
	}
	return nil
}

func (s *Store) rename(oldPath, newPath string) error {
	oldRelative, err := s.relative(oldPath)
	if err != nil {
		return err
	}
	newRelative, err := s.relative(newPath)
	if err != nil {
		return err
	}
	return s.rootFS.Rename(oldRelative, newRelative)
}

func (s *Store) removeAll(path string) error {
	relative, err := s.relative(path)
	if err != nil {
		return err
	}
	return s.rootFS.RemoveAll(relative)
}

func NewStore(root string, options StoreOptions) (*Store, error) {
	if !filepath.IsAbs(root) {
		return nil, errors.New("cache root must be absolute")
	}
	root = filepath.Clean(root)
	if err := ensureDirectory(root); err != nil {
		return nil, fmt.Errorf("create cache root: %w", err)
	}
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("open cache root: %w", err)
	}
	if err := validatePressure(options.Pressure); err != nil {
		_ = rootFS.Close()
		return nil, err
	}
	if options.ProjectIDStart == 0 && options.ProjectIDCount == 0 {
		options.ProjectIDStart = 2_000_000_000
		options.ProjectIDCount = 1_000_000
	}
	if options.ProjectIDStart == 0 || options.ProjectIDCount == 0 || uint64(options.ProjectIDStart)+uint64(options.ProjectIDCount)-1 > uint64(^uint32(0)) {
		_ = rootFS.Close()
		return nil, errors.New("project ID range must be positive and fit within uint32")
	}
	if err := rootFS.MkdirAll(trashDirectoryName, 0o700); err != nil {
		_ = rootFS.Close()
		return nil, fmt.Errorf("create cache trash directory: %w", err)
	}
	store := &Store{
		root:                       root,
		rootFS:                     rootFS,
		pressure:                   options.Pressure,
		projectIDStart:             options.ProjectIDStart,
		projectIDCount:             options.ProjectIDCount,
		pressureDetachFailed:       make(map[string]struct{}),
		metadataByIdentity:         make(map[string]Metadata),
		leaseIndex:                 make(map[string]string),
		degraded:                   make(map[string]error),
		projectReservations:        make(map[uint32][]projectReservation),
		unknownProjectReservations: make(map[string]string),
		trashMetadata:              make(map[string]Metadata),
		stopTrash:                  make(chan struct{}),
		trashDone:                  make(chan struct{}),
		trashRequests:              make(chan chan error),
	}
	if err := store.rebuildIndexes(); err != nil {
		_ = rootFS.Close()
		return nil, fmt.Errorf("rebuild cache indexes: %w", err)
	}
	go store.runTrashCollector()
	return store, nil
}

func (s *Store) rebuildIndexes() error {
	registryMissing := s.loadProjectRegistry()

	entries, err := s.readDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == trashDirectoryName {
			continue
		}
		path := filepath.Join(s.root, entry.Name())
		meta, err := s.readMetadata(path)
		if errors.Is(err, os.ErrNotExist) {
			generations, generationErr := s.readDir(filepath.Join(path, "generations"))
			if generationErr == nil && hasGenerationDirectory(generations) {
				s.markDegraded(entry.Name(), errors.New("cache metadata is missing while generations remain"))
			} else if generationErr != nil && !errors.Is(generationErr, os.ErrNotExist) {
				s.markDegraded(entry.Name(), generationErr)
			}
			continue
		}
		if err != nil {
			s.markDegraded(entry.Name(), err)
			continue
		}
		if err := validateMetadata(entry.Name(), meta); err != nil {
			s.markDegraded(entry.Name(), err)
			continue
		}
		s.indexObjectMetadata(meta)
	}

	trashEntries, err := s.readDir(filepath.Join(s.root, trashDirectoryName))
	if err != nil {
		return err
	}
	for _, entry := range trashEntries {
		if !entry.IsDir() {
			continue
		}
		meta, err := s.readMetadata(filepath.Join(s.root, trashDirectoryName, entry.Name()))
		if err != nil {
			s.markDegraded(filepath.Join(trashDirectoryName, entry.Name()), err)
			continue
		}
		s.trashMetadata[entry.Name()] = meta
		s.addMetadataReservations(meta, entry.Name())
	}
	if registryMissing && len(s.degraded) > 0 {
		for identity := range s.degraded {
			if filepath.Dir(identity) == "." {
				s.unknownProjectReservations[identity] = ""
			}
		}
	}
	if err := s.reconcileProjectReservations(entries, trashEntries); err != nil {
		return err
	}
	return nil
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
	return false
}

func hasGenerationDirectory(entries []os.DirEntry) bool {
	return slices.ContainsFunc(entries, func(entry os.DirEntry) bool { return entry.IsDir() })
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
			reservation, retain := s.reconcileProjectReservation(projectID, reservation, objects, trash)
			if retain {
				kept = append(kept, reservation)
			}
		}
		if len(kept) == 0 {
			delete(s.projectReservations, projectID)
		} else {
			s.projectReservations[projectID] = kept
		}
	}
	for identity, trashID := range s.unknownProjectReservations {
		trashID, retain := s.reconcileUnknownProjectReservation(identity, trashID, objects, trash)
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
	for index, existing := range s.projectReservations[projectID] {
		if existing.Identity == reservation.Identity && existing.Generation == reservation.Generation {
			s.projectReservations[projectID][index] = reservation
			return
		}
	}
	s.projectReservations[projectID] = append(s.projectReservations[projectID], reservation)
}

func (s *Store) persistProjectReservations() error {
	if s.projectRegistryDamaged {
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
	data, err := json.Marshal(projectReservationDocument{Reservations: reservations, UnknownReservations: unknownReservations})
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
	return closeErr
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
		kept := slices.DeleteFunc(reservations, func(reservation projectReservation) bool {
			return reservation.Identity == meta.Identity && reservation.TrashID == "" && !metadataHasProjectReservation(meta, projectID, reservation.Generation)
		})
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
	if !validSharingPolicy(meta.Policy.SharingPolicy) {
		return errors.New("cache metadata has an unsupported sharing policy")
	}
	for _, retired := range meta.Retired {
		if retired.Generation == "" || !validSharingPolicy(retired.Policy.SharingPolicy) {
			return errors.New("cache metadata has an invalid retired generation policy")
		}
	}
	return nil
}

func validSharingPolicy(policy string) bool {
	return policy == "" || policy == SharingPolicyShared || policy == SharingPolicyExclusive
}

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

func (s *Store) runTrashCollector() {
	defer close(s.trashDone)
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
		for projectID, reservations := range s.projectReservations {
			kept := make([]projectReservation, 0, len(reservations))
			for _, reservation := range reservations {
				if reservation.TrashID != trashID {
					kept = append(kept, reservation)
					continue
				}
				if meta, exists := s.metadataByIdentity[reservation.Identity]; exists && metadataHasProjectReservation(meta, projectID, reservation.Generation) {
					reservation.TrashID = ""
					kept = append(kept, reservation)
				}
			}
			if len(kept) == 0 {
				delete(s.projectReservations, projectID)
			} else {
				s.projectReservations[projectID] = kept
			}
		}
		delete(s.trashMetadata, trashID)
		delete(s.degraded, filepath.Join(trashDirectoryName, trashID))
		for identity, reservedTrashID := range s.unknownProjectReservations {
			if reservedTrashID == trashID {
				delete(s.unknownProjectReservations, identity)
			}
		}
		if err := s.persistProjectReservations(); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("persist project ID registry after trash cleanup: %w", err))
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

func (s *Store) reserveObjectForTrash(identity, trashID string, meta Metadata) error {
	for projectID, reservations := range s.projectReservations {
		for index, reservation := range reservations {
			if reservation.Identity == identity {
				reservation.TrashID = trashID
				reservations[index] = reservation
			}
		}
		s.projectReservations[projectID] = reservations
	}
	if _, damaged := s.degraded[identity]; damaged || meta.Identity != identity {
		s.unknownProjectReservations[identity] = trashID
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
			}
		}
		s.projectReservations[projectID] = reservations
	}
	if s.unknownProjectReservations[identity] == trashID {
		s.unknownProjectReservations[identity] = ""
	}
	if err := s.persistProjectReservations(); err != nil {
		return fmt.Errorf("restore project ID reservation after failed cache quarantine: %w", err)
	}
	return nil
}

func (s *Store) indexObjectInTrash(identity, trashID string, meta Metadata) {
	delete(s.metadataByIdentity, identity)
	for _, lease := range meta.Leases {
		delete(s.leaseIndex, lease.ID)
	}
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
		} else {
			reservations[index] = previous
			s.projectReservations[projectID] = reservations
		}
	} else if index >= 0 {
		reservations = slices.Delete(reservations, index, index+1)
		if len(reservations) == 0 {
			delete(s.projectReservations, projectID)
		} else {
			s.projectReservations[projectID] = reservations
		}
	}
	if err := s.persistProjectReservations(); err != nil {
		return fmt.Errorf("restore project ID reservation after failed retired generation detach: %w", err)
	}
	return nil
}

func Identity(namespaceUID, cacheClass, cacheClassUID, cacheKey, schema string) (string, error) {
	if namespaceUID == "" || cacheClass == "" || cacheClassUID == "" || cacheKey == "" {
		return "", errors.New("namespace UID, cacheClass, cacheClass UID, and cacheKey are required")
	}
	if strings.ContainsRune(cacheKey, '\x00') {
		return "", errors.New("cacheKey contains an invalid character")
	}
	return stableIdentity(namespaceUID + "\x00" + cacheClass + "\x00" + cacheClassUID + "\x00" + cacheKey + "\x00" + schema), nil
}

func (s *Store) Acquire(options AcquireOptions) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateAcquireOptions(options); err != nil {
		return "", false, err
	}
	if err := s.degraded[options.Identity]; err != nil {
		return "", false, err
	}
	if path, found, err := s.existingLeasePath(options); found || err != nil {
		return path, false, err
	}
	entry := filepath.Join(s.root, options.Identity)
	if err := s.ensureDirectory(entry); err != nil {
		return "", false, fmt.Errorf("create cache entry: %w", err)
	}
	meta, err := s.readMetadata(entry)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.markDegraded(options.Identity, err)
		return "", false, fmt.Errorf("read cache metadata: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		if err := s.discardUntrackedGenerations(entry); err != nil {
			return "", false, fmt.Errorf("discard incomplete cache generations: %w", err)
		}
		meta = Metadata{Identity: options.Identity, Generation: uuid.NewV7().String(), CreatedAt: time.Now().UTC(), Policy: options.Policy}
	} else {
		if err := validateMetadata(options.Identity, meta); err != nil {
			s.markDegraded(options.Identity, err)
			return "", false, err
		}
		for _, lease := range meta.Leases {
			_, existingPolicy, err := leaseGenerationAndPolicy(meta, lease)
			if err != nil {
				s.markDegraded(options.Identity, err)
				return "", false, err
			}
			if options.Policy.SharingPolicy == "Exclusive" || existingPolicy.SharingPolicy == "Exclusive" {
				return "", false, ErrExclusivePolicyConflict
			}
		}
		if s.activeLeaseCount(meta) > 0 && (meta.Policy.QuotaEnabled != options.Policy.QuotaEnabled || meta.Policy.QuotaEnabled && meta.Policy.MaxBytes != options.Policy.MaxBytes) {
			return "", false, ErrQuotaPolicyConflict
		}
		if len(meta.Retired) == 0 && s.activeLeaseCount(meta) == 0 && (meta.Policy.QuotaEnabled != options.Policy.QuotaEnabled || meta.Dirty && !meta.Policy.CrashRecoveryReuse) {
			if err := s.detachToTrash(entry); err != nil {
				return "", false, fmt.Errorf("discard cache before generation transition: %w", err)
			}
			if err := s.ensureDirectory(entry); err != nil {
				return "", false, fmt.Errorf("create cache entry after generation transition: %w", err)
			}
			meta = Metadata{Identity: options.Identity, Generation: uuid.NewV7().String(), CreatedAt: time.Now().UTC(), Policy: options.Policy}
		} else {
			return s.acquireExisting(entry, options, meta)
		}
	}
	return s.createLease(entry, options, meta)
}

func validateAcquireOptions(options AcquireOptions) error {
	if !validIdentity(options.Identity) || options.Lease.ID == "" || len(options.Lease.ID) > 1024 || strings.ContainsRune(options.Lease.ID, '\x00') || !filepath.IsAbs(options.Lease.Target) {
		return errors.New("invalid cache identity, lease ID, or target path")
	}
	if !validSharingPolicy(options.Policy.SharingPolicy) {
		return errors.New("unsupported cache sharing policy")
	}
	return nil
}

func (s *Store) existingLeasePath(options AcquireOptions) (string, bool, error) {
	existingIdentity, existing, found, err := s.findLease(options.Lease.ID)
	if err != nil || !found {
		return "", found, err
	}
	if existingIdentity != options.Identity {
		return "", true, errors.New("cache lease ID is already in use")
	}
	leaseIndex := slices.IndexFunc(existing.Leases, func(lease Lease) bool { return lease.ID == options.Lease.ID })
	if leaseIndex < 0 || existing.Leases[leaseIndex].Target != options.Lease.Target {
		return "", true, errors.New("cache lease ID is already in use")
	}
	generation, _, err := leaseGenerationAndPolicy(existing, existing.Leases[leaseIndex])
	if err != nil {
		return "", true, err
	}
	return filepath.Join(s.root, options.Identity, "generations", generation), true, nil
}

func leaseGenerationAndPolicy(meta Metadata, lease Lease) (string, Policy, error) {
	generation := lease.Generation
	if generation == "" {
		generation = meta.Generation
	}
	if generation == meta.Generation {
		return generation, meta.Policy, nil
	}
	for _, retired := range meta.Retired {
		if retired.Generation == generation {
			return generation, retired.Policy, nil
		}
	}
	return "", Policy{}, errors.New("cache lease references an unknown generation")
}

func (s *Store) acquireExisting(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	generationPath := filepath.Join(entry, "generations", meta.Generation)
	if statErr := s.stat(generationPath); errors.Is(statErr, os.ErrNotExist) {
		meta.Leases = slices.DeleteFunc(meta.Leases, func(lease Lease) bool {
			return lease.Generation == "" || lease.Generation == meta.Generation
		})
		meta.Generation = uuid.NewV7().String()
		meta.CreatedAt = time.Now().UTC()
		meta.ProjectAssigned = false
		meta.QuotaBytes = 0
		meta.Leases = nil
		meta.Dirty = false
	} else if statErr != nil {
		return "", false, fmt.Errorf("inspect cache generation: %w", statErr)
	}
	for _, lease := range meta.Leases {
		if lease.ID == options.Lease.ID && (lease.Generation == "" || lease.Generation == meta.Generation) {
			if lease.Target != options.Lease.Target {
				return "", false, errors.New("cache lease already exists for a different target")
			}
			meta.Policy = options.Policy
			index := slices.IndexFunc(meta.Leases, func(existing Lease) bool { return existing.ID == options.Lease.ID })
			meta.Leases[index] = options.Lease
			if err := s.writeMetadata(entry, meta); err != nil {
				return "", false, err
			}
			return filepath.Join(entry, "generations", meta.Generation), false, nil
		}
	}
	meta.Policy = options.Policy
	return s.createLease(entry, options, meta)
}

func (s *Store) activeLeaseCount(meta Metadata) int {
	count := 0
	for _, lease := range meta.Leases {
		if lease.Generation == "" || lease.Generation == meta.Generation {
			count++
		}
	}
	return count
}

func (s *Store) createLease(entry string, options AcquireOptions, meta Metadata) (string, bool, error) {
	generationPath := filepath.Join(entry, "generations", meta.Generation)
	if err := s.ensureDirectory(generationPath); err != nil {
		return "", false, fmt.Errorf("create cache generation: %w", err)
	}
	if len(meta.Leases) == 0 {
		relative, err := s.relative(generationPath)
		if err != nil {
			return "", false, err
		}
		if err := s.rootFS.Chmod(relative, 0o700); err != nil {
			return "", false, fmt.Errorf("restrict cache generation before preparation: %w", err)
		}
	}
	meta.LastUsed = time.Now().UTC()
	options.Lease.Generation = meta.Generation
	meta.Leases = append(meta.Leases, options.Lease)
	meta.Dirty = true
	if err := s.writeMetadata(entry, meta); err != nil {
		return "", false, fmt.Errorf("persist cache lease: %w", err)
	}
	return generationPath, true, nil
}

func (s *Store) discardUntrackedGenerations(entry string) error {
	generations := filepath.Join(entry, "generations")
	entries, err := s.readDir(generations)
	if errors.Is(err, os.ErrNotExist) {
		entries = nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.markDegraded(filepath.Base(entry), err)
		return err
	}
	if hasGenerationDirectory(entries) {
		cause := errors.New("cache metadata is missing while generations remain")
		s.markDegraded(filepath.Base(entry), cause)
		return fmt.Errorf("%w: %v", ErrDegradedMetadata, cause)
	}
	entryContents, err := s.readDir(entry)
	if err != nil {
		return err
	}
	for _, item := range entryContents {
		if strings.HasPrefix(item.Name(), ".metadata-") {
			if err := s.detachToTrash(filepath.Join(entry, item.Name())); err != nil {
				return err
			}
		}
	}
	for _, generation := range entries {
		if generation.IsDir() {
			continue
		}
		if err := s.detachToTrash(filepath.Join(generations, generation.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) LeaseDetails(leaseID string) (string, Lease, string, Policy, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, meta, found, err := s.findLease(leaseID)
	if err != nil || !found {
		return "", Lease{}, "", Policy{}, found, err
	}
	for _, lease := range meta.Leases {
		if lease.ID != leaseID {
			continue
		}
		generation, policy, err := leaseGenerationAndPolicy(meta, lease)
		if err != nil {
			s.markDegraded(identity, err)
			return "", Lease{}, "", Policy{}, false, err
		}
		return identity, lease, filepath.Join(s.root, identity, "generations", generation), policy, true, nil
	}
	return "", Lease{}, "", Policy{}, false, nil
}

func (s *Store) QuotaState(identity string, maxBytes int64) (uint32, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validIdentity(identity) || maxBytes <= 0 {
		return 0, false, false, errors.New("valid cache identity and positive quota limit are required")
	}
	entry := filepath.Join(s.root, identity)
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return 0, false, false, err
		}
		return 0, false, false, err
	}
	if meta.ProjectID == 0 {
		projectID, err := s.projectIDLocked(identity, meta.Generation)
		if err != nil {
			return 0, false, false, err
		}
		meta.ProjectID = projectID
	} else {
		s.addProjectReservation(meta.ProjectID, projectReservation{Identity: identity, Generation: meta.Generation})
	}
	if err := s.writeMetadata(entry, meta); err != nil {
		return 0, false, false, err
	}
	return meta.ProjectID, !meta.ProjectAssigned, meta.QuotaBytes != maxBytes, nil
}

func (s *Store) Expose(identity string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validIdentity(identity) {
		return "", errors.New("invalid cache identity")
	}
	entry := filepath.Join(s.root, identity)
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		return "", err
	}
	generationPath := filepath.Join(entry, "generations", meta.Generation)
	relative, err := s.relative(generationPath)
	if err != nil {
		return "", err
	}
	info, err := s.rootFS.Lstat(relative)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("cache generation is not a real directory")
	}
	if err := s.rootFS.Chmod(relative, 0o777); err != nil {
		return "", fmt.Errorf("expose cache generation: %w", err)
	}
	return generationPath, nil
}

func (s *Store) MarkQuotaApplied(identity string, maxBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := filepath.Join(s.root, identity)
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		return err
	}
	meta.QuotaBytes = maxBytes
	return s.writeMetadata(entry, meta)
}

func (s *Store) MarkProjectAssigned(identity string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := filepath.Join(s.root, identity)
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		return err
	}
	meta.ProjectAssigned = true
	return s.writeMetadata(entry, meta)
}

func (s *Store) projectIDLocked(identity, generation string) (uint32, error) {
	if s.projectRegistryDamaged {
		return 0, errors.New("project ID allocation is disabled because the reservation registry is damaged")
	}
	if len(s.unknownProjectReservations) > 0 {
		return 0, errors.New("project ID allocation is unavailable while damaged cache projects are quarantined")
	}
	hash, _ := hex.DecodeString(identity[:8])
	hashValue := uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
	offset := hashValue % s.projectIDCount
	projectID := s.projectIDStart + offset
	for range s.projectIDCount {
		if _, exists := s.projectReservations[projectID]; !exists {
			s.addProjectReservation(projectID, projectReservation{Identity: identity, Generation: generation})
			if err := s.persistProjectReservations(); err != nil {
				s.projectReservations[projectID] = slices.DeleteFunc(s.projectReservations[projectID], func(reservation projectReservation) bool {
					return reservation.Identity == identity && reservation.Generation == generation
				})
				if len(s.projectReservations[projectID]) == 0 {
					delete(s.projectReservations, projectID)
				}
				return 0, fmt.Errorf("reserve XFS project ID: %w", err)
			}
			return projectID, nil
		}
		offset = (offset + 1) % s.projectIDCount
		projectID = s.projectIDStart + offset
	}
	return 0, errors.New("no XFS project IDs are available")
}

func ensureDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q is not a real directory", path)
	}
	return nil
}

func (s *Store) Release(leaseID, target string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, meta, found, err := s.findLease(leaseID)
	if err != nil || !found {
		return err
	}
	if target != "" && !slices.ContainsFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID && lease.Target == target }) {
		return errors.New("cache lease target does not match")
	}
	var generation string
	for _, lease := range meta.Leases {
		if lease.ID == leaseID {
			generation = lease.Generation
			if generation == "" {
				generation = meta.Generation
			}
			break
		}
	}
	meta.Leases = slices.DeleteFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID })
	meta.LastUsed = time.Now().UTC()
	meta.Dirty = s.activeLeaseCount(meta) > 0
	entry := filepath.Join(s.root, identity)
	retiredIndex := slices.IndexFunc(meta.Retired, func(retired RetiredGeneration) bool { return retired.Generation == generation })
	if retiredIndex >= 0 && !s.hasGenerationLeases(meta, generation) {
		retired := meta.Retired[retiredIndex]
		generationPath := filepath.Join(entry, "generations", generation)
		if err := s.detachGenerationToTrash(generationPath, identity, retired); err != nil {
			return fmt.Errorf("detach released cache generation: %w", err)
		}
		meta.Retired = slices.Delete(meta.Retired, retiredIndex, retiredIndex+1)
	}
	return s.writeMetadata(entry, meta)
}

func (s *Store) hasGenerationLeases(meta Metadata, generation string) bool {
	return slices.ContainsFunc(meta.Leases, func(lease Lease) bool {
		return lease.Generation == generation || lease.Generation == "" && generation == meta.Generation
	})
}

func (s *Store) findLease(leaseID string) (string, Metadata, bool, error) {
	identity, exists := s.leaseIndex[leaseID]
	if !exists {
		return "", Metadata{}, false, nil
	}
	meta, err := s.readObjectMetadata(identity)
	if err != nil {
		return "", Metadata{}, false, fmt.Errorf("read cache metadata while finding lease: %w", err)
	}
	if slices.ContainsFunc(meta.Leases, func(lease Lease) bool { return lease.ID == leaseID }) {
		return identity, meta, true, nil
	}
	return "", Metadata{}, false, nil
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

func (s *Store) ReleaseTarget(leaseID string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, meta, found, err := s.findLease(leaseID)
	if err != nil || !found {
		return "", found, err
	}
	for _, lease := range meta.Leases {
		if lease.ID == leaseID {
			return lease.Target, true, nil
		}
	}
	return "", false, nil
}

func (s *Store) FindDegradedGenerationForTarget(target string, sameSource func(source, target string) (bool, error)) (string, string, bool, error) {
	if target == "" || !filepath.IsAbs(target) || sameSource == nil {
		return "", "", false, errors.New("absolute target path and source matcher are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var matchedIdentity, matchedSource string
	for identity := range s.degraded {
		if !validIdentity(identity) {
			continue
		}
		generations, err := s.readDir(filepath.Join(s.root, identity, "generations"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", "", false, fmt.Errorf("inspect degraded cache generations: %w", err)
		}
		for _, generation := range generations {
			if !generation.IsDir() {
				continue
			}
			source := filepath.Join(s.root, identity, "generations", generation.Name())
			matches, err := sameSource(source, target)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return "", "", false, fmt.Errorf("match degraded cache generation: %w", err)
			}
			if !matches {
				continue
			}
			if matchedIdentity != "" {
				return "", "", false, errors.New("target matches multiple degraded cache generations")
			}
			matchedIdentity = identity
			matchedSource = source
		}
	}
	return matchedIdentity, matchedSource, matchedIdentity != "", nil
}

func (s *Store) CleanupDegradedObject(identity string, sourceMounted func(source string) (bool, error)) error {
	if !validIdentity(identity) || sourceMounted == nil {
		return errors.New("valid degraded cache identity and mount inspector are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, degraded := s.degraded[identity]; !degraded {
		return nil
	}
	objectPath := filepath.Join(s.root, identity)
	generations, err := s.readDir(filepath.Join(objectPath, "generations"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect degraded cache generations before cleanup: %w", err)
	}
	for _, generation := range generations {
		if !generation.IsDir() {
			continue
		}
		source := filepath.Join(objectPath, "generations", generation.Name())
		mounted, err := sourceMounted(source)
		if err != nil {
			return fmt.Errorf("verify degraded cache generation before cleanup: %w", err)
		}
		if mounted {
			return nil
		}
	}
	if err := s.detachToTrash(objectPath); err != nil {
		return fmt.Errorf("quarantine unmounted degraded cache: %w", err)
	}
	return nil
}

func (s *Store) RecoverLeases(verifyMount func(source string, lease Lease, policy Policy) (bool, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if verifyMount == nil {
		return errors.New("mount verifier is not configured")
	}
	entries, err := s.readDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != trashDirectoryName {
			if err := s.recoverObjectLeases(filepath.Join(s.root, entry.Name()), entry.Name(), verifyMount); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) recoverObjectLeases(path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) error {
	meta, err := s.readMetadata(path)
	if errors.Is(err, os.ErrNotExist) {
		return s.recoverMissingMetadataObject(path, identity, verifyMount)
	}
	if err == nil {
		err = validateMetadata(identity, meta)
	}
	if err != nil {
		s.markDegraded(identity, err)
		s.quarantineDegradedObject(path, identity, verifyMount)
		return nil
	}
	return s.recoverValidObjectLeases(path, identity, meta, verifyMount)
}

func (s *Store) recoverMissingMetadataObject(path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) error {
	generations, err := s.readDir(filepath.Join(path, "generations"))
	if errors.Is(err, os.ErrNotExist) || err == nil && !hasGenerationDirectory(generations) {
		if err := s.discardUntrackedGenerations(path); err != nil {
			s.markDegraded(identity, err)
		}
		return nil
	}
	if err != nil {
		s.markDegraded(identity, err)
		return nil
	}
	s.markDegraded(identity, errors.New("cache metadata is missing while generations remain"))
	s.quarantineDegradedObject(path, identity, verifyMount)
	return nil
}

func (s *Store) recoverValidObjectLeases(path, identity string, meta Metadata, verifyMount func(string, Lease, Policy) (bool, error)) error {
	wasDirty := meta.Dirty
	active, uncertain := s.verifyRecoveredLeases(path, identity, meta, verifyMount)
	if uncertain {
		return nil
	}
	allLeasesActive := len(active) == len(meta.Leases)
	if allLeasesActive && (len(active) > 0 || !wasDirty || meta.Policy.CrashRecoveryReuse) {
		s.indexObjectMetadata(meta)
		return nil
	}
	if len(active) == 0 && wasDirty && !meta.Policy.CrashRecoveryReuse {
		return s.recoverDirtyObject(path, identity, meta)
	}
	meta.Leases = active
	if s.recoverRetiredGenerations(path, identity, &meta, active) {
		return nil
	}
	generationPath := filepath.Join(path, "generations", meta.Generation)
	if statErr := s.stat(generationPath); errors.Is(statErr, os.ErrNotExist) {
		meta.Leases = slices.DeleteFunc(meta.Leases, func(lease Lease) bool { return lease.Generation == "" || lease.Generation == meta.Generation })
		meta.Generation = uuid.NewV7().String()
		meta.CreatedAt = time.Now().UTC()
		meta.ProjectAssigned = false
		meta.QuotaBytes = 0
	} else if statErr != nil {
		s.markDegraded(identity, statErr)
		return nil
	}
	meta.Dirty = len(active) > 0
	if err := s.writeMetadata(path, meta); err != nil {
		s.markDegraded(identity, err)
	}
	return nil
}

func (s *Store) verifyRecoveredLeases(path, identity string, meta Metadata, verifyMount func(string, Lease, Policy) (bool, error)) ([]Lease, bool) {
	active := make([]Lease, 0, len(meta.Leases))
	for _, lease := range meta.Leases {
		generation, policy, err := leaseGenerationAndPolicy(meta, lease)
		if err != nil {
			s.markDegraded(identity, err)
			return nil, true
		}
		mounted, err := verifyMount(filepath.Join(path, "generations", generation), lease, policy)
		if err != nil {
			s.markDegraded(identity, err)
			return nil, true
		}
		if mounted {
			active = append(active, lease)
		}
	}
	return active, false
}

func (s *Store) recoverDirtyObject(path, identity string, meta Metadata) error {
	if err := s.detachToTrash(path); err != nil {
		s.markDegraded(identity, err)
		return nil
	}
	if err := s.ensureDirectory(path); err != nil {
		return fmt.Errorf("create cache object after recovery: %w", err)
	}
	meta = Metadata{
		Identity:   identity,
		Generation: uuid.NewV7().String(),
		CreatedAt:  time.Now().UTC(),
		LastUsed:   time.Now().UTC(),
		Policy:     meta.Policy,
	}
	if err := s.writeMetadata(path, meta); err != nil {
		s.markDegraded(identity, err)
	}
	return nil
}

func (s *Store) recoverRetiredGenerations(path, identity string, meta *Metadata, active []Lease) bool {
	for index := 0; index < len(meta.Retired); {
		retired := meta.Retired[index]
		if slices.ContainsFunc(active, func(lease Lease) bool { return lease.Generation == retired.Generation }) {
			index++
			continue
		}
		if err := s.detachGenerationToTrash(filepath.Join(path, "generations", retired.Generation), identity, retired); err != nil {
			s.markDegraded(identity, err)
			return true
		}
		meta.Retired = slices.Delete(meta.Retired, index, index+1)
	}
	return false
}

func (s *Store) quarantineDegradedObject(path, identity string, verifyMount func(string, Lease, Policy) (bool, error)) {
	generationsPath := filepath.Join(path, "generations")
	generations, err := s.readDir(generationsPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.markDegraded(identity, err)
		return
	}
	for _, generation := range generations {
		if !generation.IsDir() {
			continue
		}
		source := filepath.Join(generationsPath, generation.Name())
		mounted, err := verifyMount(source, Lease{}, Policy{})
		if err != nil {
			s.markDegraded(identity, err)
			return
		}
		if mounted {
			s.markDegraded(identity, errors.New("cache generation remains mounted with unreadable metadata"))
			return
		}
	}
	if err := s.detachToTrash(path); err != nil {
		s.markDegraded(identity, err)
	}
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
		if meta.Policy.EvictRunning && s.activeLeaseCount(meta) > 0 {
			candidates = append(candidates, candidate{identity: identity, meta: meta, path: filepath.Join(s.root, identity)})
		}
	}
	slices.SortFunc(candidates, func(left, right candidate) int { return left.meta.LastUsed.Compare(right.meta.LastUsed) })
	if underPressure {
		for _, candidate := range candidates {
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
				leases := make([]Lease, 0)
				for _, lease := range meta.Leases {
					if lease.Generation == retired.Generation {
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
