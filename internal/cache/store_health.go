package cache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func (store *Store) CheckStoreOperations() error {
	if err := store.CheckFilesystem(); err != nil {
		return err
	}
	entries, err := store.metadataRepository.readDir(store.metadataRepository.root)
	if err != nil {
		return fmt.Errorf("list cache store for operation probe: %w", err)
	}
	directories := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != trashDirectoryName {
			directories = append(directories, entry.Name())
		}
	}
	directories = append(directories, trashDirectoryName)
	for _, directory := range directories {
		if err := store.probeStoreDirectory(directory); err != nil {
			return fmt.Errorf("probe cache store directory %q: %w", directory, err)
		}
	}
	return nil
}

func (store *Store) probeStoreDirectory(directory string) (resultErr error) {
	name := filepath.Join(directory, ".cache-csi-health-"+uuid.NewV7().String())
	renamed := name + ".renamed"
	defer func() {
		for _, candidate := range []string{name, renamed} {
			if err := store.metadataRepository.rootFS.Remove(candidate); err != nil && !errors.Is(err, os.ErrNotExist) {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}()
	file, err := store.metadataRepository.rootFS.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write([]byte{0}); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := store.metadataRepository.rootFS.Rename(name, renamed); err != nil {
		return err
	}
	if err := store.syncHealthProbeDirectory(directory); err != nil {
		return err
	}
	if err := store.metadataRepository.rootFS.Remove(renamed); err != nil {
		return err
	}
	return store.syncHealthProbeDirectory(directory)
}

func (store *Store) syncHealthProbeDirectory(directory string) error {
	handle, err := store.metadataRepository.rootFS.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(handle.Sync(), handle.Close())
}
