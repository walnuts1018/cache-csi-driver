package driver

import (
	"context"
	"errors"
	"fmt"
	"time"

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

func (server *Server) fail(ctx context.Context, failure nodehealth.Failure, err error) error {
	if failure.Scope == nodehealth.FailureScopeNode {
		server.options.Health.RecordFailure(failure)
	}
	if err != nil {
		server.logger.ErrorContext(ctx, "cache operation failed", "reason", failure.Reason, "scope", failure.Scope, "impact", failure.Impact, "error", err)
		if failure.Scope == nodehealth.FailureScopeNode {
			return status.Errorf(codes.Unavailable, "cache node is unavailable: %s: %v", failure.Reason, err)
		}
		return status.Errorf(codes.Unavailable, "cache operation failed: %s: %v", failure.Reason, err)
	}
	if failure.Scope == nodehealth.FailureScopeNode {
		return status.Errorf(codes.Unavailable, "cache node is unavailable: %s", failure.Reason)
	}
	return status.Errorf(codes.Unavailable, "cache operation failed: %s", failure.Reason)
}

func storeFailure(reason nodehealth.FailureReason) nodehealth.Failure {
	return nodehealth.Failure{
		Scope:        nodehealth.FailureScopeNode,
		Subsystem:    nodehealth.SubsystemStoreOperations,
		Reason:       reason,
		Impact:       nodehealth.NodeImpactNoSchedule,
		Retryability: nodehealth.PermanentUntilProbe,
	}
}

func objectFailure(reason nodehealth.FailureReason) nodehealth.Failure {
	return nodehealth.Failure{
		Scope:        nodehealth.FailureScopeObject,
		Reason:       reason,
		Retryability: nodehealth.Retryable,
	}
}

func (server *Server) mountOperationFailure(ctx context.Context, err error) error {
	requestFailure := nodehealth.Failure{
		Scope:        nodehealth.FailureScopeRequest,
		Reason:       nodehealth.ReasonRequestMount,
		Retryability: nodehealth.Retryable,
	}
	if server.options.MountProbe == nil {
		return server.fail(ctx, requestFailure, err)
	}
	probeContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	probeErr := server.options.MountProbe(probeContext)
	cancel()
	if probeErr == nil {
		return server.fail(ctx, requestFailure, err)
	}
	return server.fail(ctx, nodehealth.Failure{
		Scope:        nodehealth.FailureScopeNode,
		Subsystem:    nodehealth.SubsystemMount,
		Reason:       nodehealth.ReasonMountUnavailable,
		Impact:       nodehealth.NodeImpactNoSchedule,
		Retryability: nodehealth.PermanentUntilProbe,
	}, errors.Join(err, fmt.Errorf("mount capability probe failed: %w", probeErr)))
}
