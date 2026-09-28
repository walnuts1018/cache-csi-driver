package driver

import "sync"

type lockEntry struct {
	mutex sync.Mutex
	users int
}

type operationLocks struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}

func (locks *operationLocks) Lock(key string) func() {
	locks.mu.Lock()
	if locks.entries == nil {
		locks.entries = make(map[string]*lockEntry)
	}
	entry := locks.entries[key]
	if entry == nil {
		entry = &lockEntry{}
		locks.entries[key] = entry
	}
	entry.users++
	locks.mu.Unlock()
	entry.mutex.Lock()
	return func() {
		entry.mutex.Unlock()
		locks.mu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(locks.entries, key)
		}
		locks.mu.Unlock()
	}
}
