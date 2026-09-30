package nodehealth

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	ReadyLabel              = "cache.csi.walnuts.dev/ready"
	CSIPluginName           = "cache.csi.walnuts.dev"
	ControllerInterval      = 5 * time.Second
	acceptedEvictionGap     = 30 * time.Second
	staleLeaseEvictionGrace = 30 * time.Second
)

type evictionKey struct {
	namespace string
	uid       types.UID
}

type Controller struct {
	client     kubernetes.Interface
	namespace  string
	logger     *slog.Logger
	nextEvict  map[evictionKey]time.Time
	staleSince map[string]time.Time
}

func NewController(client kubernetes.Interface, namespace string, logger *slog.Logger) (*Controller, error) {
	if client == nil || namespace == "" {
		return nil, errors.New("Kubernetes client and controller namespace are required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Controller{
		client:     client,
		namespace:  namespace,
		logger:     logger,
		nextEvict:  make(map[evictionKey]time.Time),
		staleSince: make(map[string]time.Time),
	}, nil
}

func (controller *Controller) Run(ctx context.Context) {
	controller.Reconcile(ctx)
	ticker := time.NewTicker(ControllerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			controller.Reconcile(ctx)
		}
	}
}

func (controller *Controller) Reconcile(ctx context.Context) {
	leases, err := controller.client.CoordinationV1().Leases(controller.namespace).List(ctx, metav1.ListOptions{LabelSelector: LeaseComponentLabel + "=" + LeaseComponentValue})
	if err != nil {
		controller.logger.ErrorContext(ctx, "list cache node health leases", "error", err)
		return
	}
	now := time.Now()
	leaseByNode := make(map[string]*coordinationv1.Lease, len(leases.Items))
	for index := range leases.Items {
		lease := &leases.Items[index]
		nodeName := lease.Annotations[LeaseNodeNameKey]
		if nodeName == "" || LeaseName(nodeName) != lease.Name {
			continue
		}
		leaseByNode[nodeName] = lease
	}

	nodes, err := controller.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		controller.logger.ErrorContext(ctx, "list Nodes while reconciling cache health", "error", err)
		return
	}
	activeEvictions := make(map[evictionKey]struct{})
	presentNodes := make(map[string]struct{}, len(nodes.Items))
	for index := range nodes.Items {
		node := &nodes.Items[index]
		presentNodes[node.Name] = struct{}{}
		lease := leaseByNode[node.Name]
		ready := IsSchedulableLease(lease, now)
		if err := controller.setNodeReady(ctx, node, ready); err != nil {
			controller.logger.ErrorContext(ctx, "update cache scheduling label", "node", node.Name, "error", err)
		}
		leaseCurrent := IsCurrentLease(lease, now)
		if leaseCurrent {
			delete(controller.staleSince, node.Name)
		} else if _, found := controller.staleSince[node.Name]; !found {
			controller.staleSince[node.Name] = now
		}
		staleExpired := !leaseCurrent && now.Sub(controller.staleSince[node.Name]) >= staleLeaseEvictionGrace
		if IsEvictingLease(lease, now) || staleExpired {
			if err := controller.evictCachePods(ctx, node.Name, activeEvictions); err != nil {
				controller.logger.ErrorContext(ctx, "evict cache Pods from unavailable Node", "node", node.Name, "error", err)
			}
		}
	}
	for nodeName := range controller.staleSince {
		if _, exists := presentNodes[nodeName]; !exists {
			delete(controller.staleSince, nodeName)
		}
	}
	for key := range controller.nextEvict {
		if _, ok := activeEvictions[key]; !ok {
			delete(controller.nextEvict, key)
		}
	}
}

func (controller *Controller) setNodeReady(ctx context.Context, node *corev1.Node, ready bool) error {
	current, exists := node.Labels[ReadyLabel]
	if ready && exists && current == "true" || !ready && !exists {
		return nil
	}
	var value any
	if ready {
		value = "true"
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"labels": map[string]any{ReadyLabel: value}}})
	if err != nil {
		return fmt.Errorf("encode Node label update: %w", err)
	}
	_, err = controller.client.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patch Node %s cache readiness: %w", node.Name, err)
	}
	return nil
}

func (controller *Controller) evictCachePods(ctx context.Context, nodeName string, active map[evictionKey]struct{}) error {
	pods, err := controller.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", nodeName).String()})
	if err != nil {
		return fmt.Errorf("list Pods on Node %s: %w", nodeName, err)
	}
	now := time.Now()
	for index := range pods.Items {
		pod := &pods.Items[index]
		if pod.DeletionTimestamp != nil || !podHasCacheCSIVolume(pod) {
			continue
		}
		key := evictionKey{namespace: pod.Namespace, uid: pod.UID}
		active[key] = struct{}{}
		if retryAt := controller.nextEvict[key]; now.Before(retryAt) {
			continue
		}
		uid := pod.UID
		eviction := &policyv1.Eviction{
			TypeMeta:   metav1.TypeMeta{APIVersion: "policy/v1", Kind: "Eviction"},
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
			DeleteOptions: &metav1.DeleteOptions{
				Preconditions: &metav1.Preconditions{UID: &uid},
			},
		}
		err := controller.client.PolicyV1().Evictions(pod.Namespace).Evict(ctx, eviction)
		if apierrors.IsNotFound(err) {
			delete(controller.nextEvict, key)
			continue
		}
		if apierrors.IsTooManyRequests(err) {
			controller.nextEvict[key] = now.Add(ControllerInterval)
			controller.logger.WarnContext(ctx, "cache Pod eviction is blocked, possibly by a PodDisruptionBudget", "namespace", pod.Namespace, "pod", pod.Name, "podUID", pod.UID)
			continue
		}
		if err != nil {
			controller.nextEvict[key] = now.Add(ControllerInterval)
			controller.logger.WarnContext(ctx, "cache Pod eviction request failed", "namespace", pod.Namespace, "pod", pod.Name, "podUID", pod.UID, "error", err)
			continue
		}
		controller.nextEvict[key] = now.Add(acceptedEvictionGap)
		controller.logger.InfoContext(ctx, "requested cache Pod eviction from unavailable Node", "namespace", pod.Namespace, "pod", pod.Name, "podUID", pod.UID)
	}
	return nil
}

func podHasCacheCSIVolume(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Spec.Volumes, func(volume corev1.Volume) bool {
		return volume.CSI != nil && volume.CSI.Driver == CSIPluginName
	})
}
