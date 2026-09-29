package cache

import "fmt"

func (s *Store) ProjectRegistryError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.projectRegistryDamaged {
		return fmt.Errorf("%w: project ID reservation registry is damaged", ErrDegradedMetadata)
	}
	if s.projectRegistryDirty {
		return fmt.Errorf("%w: project ID reservation registry has unpersisted changes", ErrDegradedMetadata)
	}
	return nil
}
