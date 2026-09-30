package driver

import (
	"context"
	"errors"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/walnuts1018/cache-csi-driver/internal/cache"
	"github.com/walnuts1018/cache-csi-driver/internal/kube"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fallbackCause struct {
	class  string
	reason string
}

const (
	fallbackClassExpected   = "expected"
	fallbackClassUnexpected = "unexpected"
	publishOutcomeHit       = "hit"
	publishOutcomeMiss      = "miss"
	publishOutcomeFallback  = "fallback"
)

func expectedFallbackCause(reason string) fallbackCause {
	return fallbackCause{class: fallbackClassExpected, reason: reason}
}

func unexpectedFallbackCause(reason string) fallbackCause {
	return fallbackCause{class: fallbackClassUnexpected, reason: reason}
}

func fallbackCauseForError(err error, backendReason string) (fallbackCause, bool) {
	switch {
	case errors.Is(err, cache.ErrDegradedMetadata):
		return expectedFallbackCause("metadata_degraded"), true
	case errors.Is(err, cache.ErrExclusivePolicyConflict):
		return expectedFallbackCause("exclusive_conflict"), true
	case errors.Is(err, cache.ErrQuotaPolicyConflict):
		return expectedFallbackCause("quota_conflict"), true
	case errors.Is(err, cache.ErrLeaseGenerationRetired):
		return expectedFallbackCause("generation_retired"), true
	case errors.Is(err, cache.ErrPressureActive):
		return expectedFallbackCause("pressure_active"), true
	case status.Code(err) == codes.Unknown || status.Code(err) == codes.Internal:
		return unexpectedFallbackCause(backendReason), true
	default:
		return fallbackCause{}, false
	}
}

func fallbackCauseForResolution(err error) fallbackCause {
	if errors.Is(err, kube.ErrResolverNotSynced) {
		return expectedFallbackCause("resolver_not_synced")
	}
	return expectedFallbackCause("api_unavailable")
}

func (s *Server) useFallback(ctx context.Context, req *csi.NodePublishVolumeRequest, cause fallbackCause, trigger error) error {
	if cause.class == fallbackClassUnexpected {
		s.markUnexpectedBackendFailure(ctx, cause, trigger)
	}
	err := s.publishFallback(ctx, req)
	if err == nil && s.metrics != nil {
		s.metrics.RecordPublish(publishOutcomeFallback)
		s.metrics.RecordFallback(cause.class, cause.reason)
	}
	return err
}

func (s *Server) markUnexpectedBackendFailure(ctx context.Context, cause fallbackCause, trigger error) {
	s.backendDegraded.Store(true)
	if s.metrics != nil {
		s.metrics.RecordBackendFailure(cause.reason)
	}
	s.logger.WarnContext(ctx, "primary cache backend operation failed", "stage", cause.reason, "error", trigger)
}

func (s *Server) recordNormalPublish(result string) {
	if s.metrics != nil {
		s.metrics.RecordPublish(result)
	}
}

func (s *Server) recordPublishOutcome(outcome string) {
	if s.metrics != nil {
		s.metrics.RecordPublish(outcome)
	}
}

func (s *Server) recordFallbackReuse() {
	s.recordPublishOutcome(publishOutcomeFallback)
}
