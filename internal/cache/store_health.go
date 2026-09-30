package cache

import (
	"errors"
	"fmt"
	"os"
	"uuid"
)

func (store *Store) CheckFilesystem() error {
	name := ".cache-csi-health-" + uuid.NewV7().String()
	renamed := name + ".renamed"
	defer func() {
		_ = store.metadataRepository.rootFS.Remove(name)
		_ = store.metadataRepository.rootFS.Remove(renamed)
	}()

	file, err := store.metadataRepository.rootFS.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create cache filesystem health probe: %w", err)
	}
	if _, err := file.Write([]byte{0}); err != nil {
		_ = file.Close()
		return fmt.Errorf("write cache filesystem health probe: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync cache filesystem health probe: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close cache filesystem health probe: %w", err)
	}
	if err := store.metadataRepository.rootFS.Rename(name, renamed); err != nil {
		return fmt.Errorf("rename cache filesystem health probe: %w", err)
	}
	directory, err := store.metadataRepository.rootFS.Open(".")
	if err != nil {
		return fmt.Errorf("open cache root for health probe: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync cache root after health probe rename: %w", err)
	}
	if err := store.metadataRepository.rootFS.Remove(renamed); err != nil {
		return fmt.Errorf("remove cache filesystem health probe: %w", err)
	}
	directory, err = store.metadataRepository.rootFS.Open(".")
	if err != nil {
		return fmt.Errorf("reopen cache root for health probe: %w", err)
	}
	syncErr = directory.Sync()
	closeErr = directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync cache root after health probe cleanup: %w", err)
	}
	return nil
}
