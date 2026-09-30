package cache

import (
	"errors"
	"fmt"
)

var ErrQuarantineQueueFull = errors.New("degraded cache quarantine queue is full")

type quarantineRequest struct {
	identity      string
	sourceMounted func(string) (bool, error)
}

func (s *Store) ScheduleQuarantine(identity string, sourceMounted func(string) (bool, error)) error {
	if !validIdentity(identity) || sourceMounted == nil {
		return errors.New("valid degraded cache identity and mount inspector are required")
	}
	s.trashCollector.collectorMu.Lock()
	collectorAvailable := s.trashCollector.collectorStarted && !s.trashCollector.collectorFinished
	s.trashCollector.collectorMu.Unlock()
	if !collectorAvailable {
		return errors.New("cache trash collector is unavailable")
	}
	request := quarantineRequest{identity: identity, sourceMounted: sourceMounted}
	select {
	case <-s.trashCollector.stopTrash:
		return errors.New("cache store is closed")
	case s.trashCollector.quarantineRequests <- request:
		return nil
	default:
		return fmt.Errorf("schedule degraded cache quarantine: %w", ErrQuarantineQueueFull)
	}
}
