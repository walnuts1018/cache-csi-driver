package nodehealth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	LeaseComponentLabel = "app.kubernetes.io/component"
	LeaseComponentValue = "node-health"
	LeaseNodeNameKey    = "cache.csi.walnuts.dev/node-name"
	LeasePhaseKey       = "cache.csi.walnuts.dev/state"
	LeaseReasonKey      = "cache.csi.walnuts.dev/reason"
	LeaseEvictKey       = "cache.csi.walnuts.dev/evict"
	LeaseDuration       = 20 * time.Second
	LeaseRenewInterval  = 5 * time.Second
)

const leaseDurationSeconds = int32(LeaseDuration / time.Second)

type Reporter struct {
	client    kubernetes.Interface
	namespace string
	nodeName  string
	tracker   *Tracker
	logger    *slog.Logger
}

func NewReporter(client kubernetes.Interface, namespace, nodeName string, tracker *Tracker, logger *slog.Logger) (*Reporter, error) {
	if client == nil || namespace == "" || nodeName == "" || tracker == nil {
		return nil, errors.New("Kubernetes client, namespace, node name, and health tracker are required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Reporter{client: client, namespace: namespace, nodeName: nodeName, tracker: tracker, logger: logger}, nil
}

func (reporter *Reporter) Run(ctx context.Context) {
	reporter.publish(ctx)
	ticker := time.NewTicker(LeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reporter.publish(ctx)
		}
	}
}

func (reporter *Reporter) publish(ctx context.Context) {
	requestContext, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	client := reporter.client.CoordinationV1().Leases(reporter.namespace)
	name := LeaseName(reporter.nodeName)
	lease, err := client.Get(requestContext, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		lease = &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: reporter.namespace}}
	} else if err != nil {
		reporter.logger.WarnContext(ctx, "read cache node health Lease failed", "lease", name, "error", err)
		return
	} else if lease.Annotations[LeaseNodeNameKey] != reporter.nodeName {
		reporter.logger.ErrorContext(ctx, "cache node health Lease name is owned by a different Node", "lease", name, "node", reporter.nodeName)
		return
	}

	snapshot := reporter.tracker.Current()
	if lease.Labels == nil {
		lease.Labels = make(map[string]string)
	}
	if lease.Annotations == nil {
		lease.Annotations = make(map[string]string)
	}
	lease.Labels["app.kubernetes.io/name"] = "cache-csi-driver"
	lease.Labels[LeaseComponentLabel] = LeaseComponentValue
	lease.Annotations[LeaseNodeNameKey] = reporter.nodeName
	lease.Annotations[LeasePhaseKey] = string(snapshot.Phase)
	lease.Annotations[LeaseReasonKey] = snapshot.Reason
	lease.Annotations[LeaseEvictKey] = fmt.Sprint(snapshot.Evict)
	now := metav1.NewMicroTime(time.Now())
	lease.Spec.HolderIdentity = &reporter.nodeName
	duration := leaseDurationSeconds
	lease.Spec.LeaseDurationSeconds = &duration
	lease.Spec.RenewTime = &now

	if lease.ResourceVersion == "" {
		_, err = client.Create(requestContext, lease, metav1.CreateOptions{})
	} else {
		_, err = client.Update(requestContext, lease, metav1.UpdateOptions{})
	}
	if err != nil && !apierrors.IsConflict(err) && !apierrors.IsAlreadyExists(err) {
		reporter.logger.WarnContext(ctx, "publish cache node health Lease failed", "lease", name, "phase", snapshot.Phase, "error", err)
	}
}

func LeaseName(nodeName string) string {
	digest := sha256.Sum256([]byte(nodeName))
	return "cache-csi-node-" + hex.EncodeToString(digest[:12])
}

func LeaseRenewTime(lease *coordinationv1.Lease) time.Time {
	if lease == nil || lease.Spec.RenewTime == nil {
		return time.Time{}
	}
	return lease.Spec.RenewTime.Time
}

func IsCurrentLease(lease *coordinationv1.Lease, now time.Time) bool {
	renewedAt := LeaseRenewTime(lease)
	if renewedAt.IsZero() || renewedAt.After(now.Add(time.Second)) {
		return false
	}
	duration := LeaseDuration
	if lease.Spec.LeaseDurationSeconds != nil && *lease.Spec.LeaseDurationSeconds > 0 {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	return now.Sub(renewedAt) <= duration
}

func IsSchedulableLease(lease *coordinationv1.Lease, now time.Time) bool {
	if !IsCurrentLease(lease, now) {
		return false
	}
	phase := Phase(lease.Annotations[LeasePhaseKey])
	return phase == PhaseReady || phase == PhaseDegraded
}

func IsEvictingLease(lease *coordinationv1.Lease, now time.Time) bool {
	return IsCurrentLease(lease, now) && lease.Annotations[LeasePhaseKey] == string(PhaseUnavailable) && lease.Annotations[LeaseEvictKey] == "true"
}
