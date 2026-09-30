package cache

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
)

func (s *Store) QuarantineFallbackVolume(volumeID string, sourceMounted func(string) (bool, error)) error {
	identity, err := FallbackIdentity(volumeID)
	if err != nil || sourceMounted == nil {
		return errors.New("valid fallback volume ID and mount inspector are required")
	}
	unlockLease := s.lockLease(volumeID)
	defer unlockLease()
	unlockIdentity := s.lockIdentity(identity)
	defer unlockIdentity()
	s.leaseManager.fallbackMu.Lock()
	defer s.leaseManager.fallbackMu.Unlock()

	objectPath := filepath.Join(s.metadataRepository.root, identity)
	if err := s.metadataRepository.stat(objectPath); errors.Is(err, os.ErrNotExist) {
		s.forgetFallbackVolume(identity, volumeID)
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect fallback cache object before quarantine: %w", err)
	}
	generations, err := s.metadataRepository.readDir(filepath.Join(objectPath, "generations"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect fallback cache generations before quarantine: %w", err)
	}
	for _, generation := range generations {
		if !generation.IsDir() {
			continue
		}
		if err := s.ensureFallbackSourceUnmounted(filepath.Join(objectPath, "generations", generation.Name()), sourceMounted); err != nil {
			return err
		}
	}
	if !hasGenerationDirectory(generations) {
		if err := s.ensureFallbackSourceUnmounted(objectPath, sourceMounted); err != nil {
			return err
		}
	}
	if err := s.detachToTrash(objectPath); err != nil {
		return fmt.Errorf("quarantine fallback cache object: %w", err)
	}
	return nil
}

func (s *Store) ensureFallbackSourceUnmounted(source string, sourceMounted func(string) (bool, error)) error {
	mounted, err := sourceMounted(source)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("verify fallback cache source before quarantine: %w", err)
	}
	if mounted {
		return ErrFallbackObjectMounted
	}
	return nil
}

func (s *Store) forgetFallbackVolume(identity, volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if meta, exists := s.generationManager.metadataByIdentity[identity]; exists {
		s.generationManager.removeFallbackReservation(meta)
		s.generationManager.retiredGenerationCount -= len(meta.Retired)
	}
	delete(s.generationManager.metadataByIdentity, identity)
	maps.DeleteFunc(s.generationManager.leaseIndex, func(leaseID, leaseIdentity string) bool {
		return leaseID == volumeID || leaseIdentity == identity
	})
	delete(s.generationManager.degraded, identity)
}
