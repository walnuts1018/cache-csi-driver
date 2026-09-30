package driver

import (
	"context"

	"github.com/walnuts1018/cache-csi-driver/internal/nodehealth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	publishOutcomeHit  = "hit"
	publishOutcomeMiss = "miss"
)

func (server *Server) recordNormalPublish(result string) {
	if server.metrics != nil {
		server.metrics.RecordPublish(result)
	}
}

func (server *Server) markNodeUnavailable(ctx context.Context, reason string, err error, evict bool) error {
	server.options.Health.Set(nodehealth.PhaseUnavailable, reason, evict)
	if err != nil {
		server.logger.ErrorContext(ctx, "cache node cannot provide a requested cache", "reason", reason, "evict", evict, "error", err)
		return status.Errorf(codes.Unavailable, "cache node is unavailable: %s: %v", reason, err)
	}
	return status.Errorf(codes.Unavailable, "cache node is unavailable: %s", reason)
}

func (server *Server) markNodeDegraded(ctx context.Context, reason string, err error) {
	server.options.Health.Set(nodehealth.PhaseDegraded, reason, false)
	if err != nil {
		server.logger.WarnContext(ctx, "cache node is degraded but can provide new cache generations", "reason", reason, "error", err)
	}
}
