package cache

import "fmt"

func (s *Store) ProjectRegistryError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.projectQuotaRegistry.projectRegistryDamaged {
		return fmt.Errorf("%w: project ID reservation registry is damaged", ErrDegradedMetadata)
	}
	if s.projectQuotaRegistry.projectRegistryDirty {
		return fmt.Errorf("%w: project ID reservation registry has unpersisted changes", ErrDegradedMetadata)
	}
	return nil
}
