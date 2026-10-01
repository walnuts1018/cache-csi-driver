package nodehealth

import (
	"cmp"
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	ReadyLabel            = "cache.csi.walnuts.dev/ready"
	CSIPluginName         = "cache.csi.walnuts.dev"
	ControllerInterval    = 5 * time.Second
	probeTimeout          = 2 * time.Second
	probeConcurrency      = 16
	pluginHealthPort      = 9807
	acceptedEvictionGap   = 30 * time.Second
	healthReasonMaxLength = 64
	readyLabelValue       = "true"
	jsonPatchPathField    = "path"
	jsonPatchValueField   = "value"
)

type evictionKey struct {
	namespace string
	uid       types.UID
}

type nodeIdentity struct {
	name string
	uid  types.UID
}

type probeCandidate struct {
	node *corev1.Node
	pod  *corev1.Pod
}

type probeResult struct {
	candidate probeCandidate
	status    HealthStatus
	err       error
}

type Controller struct {
	client     kubernetes.Interface
	apiCache   *APICache
	namespace  string
	logger     *slog.Logger
	http       *http.Client
	healthPort int
	nextEvict  map[evictionKey]time.Time
	notified   map[evictionKey]struct{}
}

func NewController(client kubernetes.Interface, apiCache *APICache, namespace string, logger *slog.Logger) (*Controller, error) {
	if client == nil || apiCache == nil || namespace == "" || apiCache.namespace != namespace {
		return nil, errors.New("kubernetes client, matching API cache, and controller namespace are required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Controller{
		client:     client,
		apiCache:   apiCache,
		namespace:  namespace,
		logger:     logger,
		http:       &http.Client{Timeout: probeTimeout},
		healthPort: pluginHealthPort,
		nextEvict:  make(map[evictionKey]time.Time),
		notified:   make(map[evictionKey]struct{}),
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
	nodes, err := controller.apiCache.Nodes()
	if err != nil {
		controller.logger.ErrorContext(ctx, "read Node informer cache while reconciling cache health", "error", err)
		return
	}
	pluginPods, err := controller.apiCache.PluginPods()
	if err != nil {
		controller.logger.ErrorContext(ctx, "read node-plugin Pod informer cache while reconciling cache health", "error", err)
		return
	}
	candidates := controller.probeCandidates(nodes, pluginPods)
	statusByNode := controller.probeNodeStatuses(ctx, candidates)
	activeEvictions, activeNotifications := controller.reconcileNodes(ctx, nodes, statusByNode, time.Now())
	controller.clearInactiveEvictions(activeEvictions)
	controller.clearInactiveNotifications(activeNotifications)
}

func (controller *Controller) probeCandidates(nodes []*corev1.Node, pluginPods []*corev1.Pod) []probeCandidate {
	podsByNode := make(map[string][]*corev1.Pod, len(nodes))
	for _, pod := range pluginPods {
		if pod.Spec.NodeName != "" && pod.DeletionTimestamp == nil {
			podsByNode[pod.Spec.NodeName] = append(podsByNode[pod.Spec.NodeName], pod)
		}
	}
	candidates := make([]probeCandidate, 0, len(nodes))
	for _, node := range nodes {
		currentPods := make([]*corev1.Pod, 0, len(podsByNode[node.Name]))
		for _, pod := range podsByNode[node.Name] {
			if !pod.CreationTimestamp.Before(&node.CreationTimestamp) {
				currentPods = append(currentPods, pod)
			}
		}
		if pod := selectPluginPod(currentPods); pod != nil {
			candidates = append(candidates, probeCandidate{node: node, pod: pod})
		}
	}
	return candidates
}

func (controller *Controller) probeNodeStatuses(ctx context.Context, candidates []probeCandidate) map[nodeIdentity]HealthStatus {
	results := controller.probeNodes(ctx, candidates)
	statusByNode := make(map[nodeIdentity]HealthStatus, len(results))
	for _, result := range results {
		if !controller.candidateIsCurrent(result.candidate) {
			continue
		}
		status := result.status
		if result.err != nil {
			controller.logger.WarnContext(ctx, "probe cache node plugin health endpoint failed", "node", result.candidate.node.Name, "nodeUID", result.candidate.node.UID, "pod", result.candidate.pod.Name, "podUID", result.candidate.pod.UID, "error", result.err)
			status = HealthStatus{Phase: PhaseUnavailable, Reason: "NodePluginUnavailable"}
		}
		statusByNode[nodeIdentity{name: result.candidate.node.Name, uid: result.candidate.node.UID}] = status
	}
	return statusByNode
}

func (controller *Controller) reconcileNodes(ctx context.Context, nodes []*corev1.Node, statusByNode map[nodeIdentity]HealthStatus, now time.Time) (map[evictionKey]struct{}, map[evictionKey]struct{}) {
	activeEvictions := make(map[evictionKey]struct{})
	activeNotifications := make(map[evictionKey]struct{})
	for _, node := range nodes {
		status := statusForNode(node, statusByNode)
		controller.reconcileNode(ctx, node, status, activeEvictions, activeNotifications, now)
	}
	return activeEvictions, activeNotifications
}

func statusForNode(node *corev1.Node, statuses map[nodeIdentity]HealthStatus) HealthStatus {
	status, found := statuses[nodeIdentity{name: node.Name, uid: node.UID}]
	if found {
		return status
	}
	return HealthStatus{Phase: PhaseUnavailable, Reason: "NodePluginUnavailable"}
}

func (controller *Controller) reconcileNode(ctx context.Context, node *corev1.Node, status HealthStatus, activeEvictions, activeNotifications map[evictionKey]struct{}, now time.Time) {
	schedulable := status.Phase == PhaseReady || status.Phase == PhaseDegraded
	if err := controller.setNodeReady(ctx, node, schedulable); err != nil {
		controller.logger.ErrorContext(ctx, "update cache scheduling label", "node", node.Name, "nodeUID", node.UID, "error", err)
	}
	if schedulable {
		return
	}
	pods, err := controller.apiCache.PodsOnNode(node.Name)
	if err != nil {
		controller.logger.ErrorContext(ctx, "read Pod informer cache for unavailable cache Node", "node", node.Name, "error", err)
		return
	}
	for _, pod := range pods {
		controller.reconcilePod(ctx, pod, status, activeEvictions, activeNotifications, now)
	}
}

func (controller *Controller) reconcilePod(ctx context.Context, pod *corev1.Pod, status HealthStatus, activeEvictions, activeNotifications map[evictionKey]struct{}, now time.Time) {
	if pod.DeletionTimestamp != nil || !podHasCacheCSIVolume(pod) {
		return
	}
	if isDaemonSetPod(pod) || !podHasReplacementController(pod) {
		if status.Evict && !isDaemonSetPod(pod) {
			controller.warnUnmanagedPod(ctx, pod, activeNotifications)
		}
		return
	}
	if !status.Evict && isEstablishedPod(pod) {
		return
	}
	key := evictionKey{namespace: pod.Namespace, uid: pod.UID}
	activeEvictions[key] = struct{}{}
	controller.requestEviction(ctx, pod, key, now)
}

func (controller *Controller) clearInactiveEvictions(active map[evictionKey]struct{}) {
	for key := range controller.nextEvict {
		if _, exists := active[key]; !exists {
			delete(controller.nextEvict, key)
		}
	}
}

func (controller *Controller) clearInactiveNotifications(active map[evictionKey]struct{}) {
	for key := range controller.notified {
		if _, exists := active[key]; !exists {
			delete(controller.notified, key)
		}
	}
}

func (controller *Controller) probeNodes(ctx context.Context, candidates []probeCandidate) []probeResult {
	results := make([]probeResult, len(candidates))
	if len(candidates) == 0 {
		return results
	}
	workerCount := min(probeConcurrency, len(candidates))
	jobs := make(chan int, len(candidates))
	for index := range candidates {
		jobs <- index
	}
	close(jobs)
	var waitGroup sync.WaitGroup
	for range workerCount {
		waitGroup.Go(func() {
			for index := range jobs {
				if err := ctx.Err(); err != nil {
					results[index] = probeResult{candidate: candidates[index], err: err}
					continue
				}
				results[index] = controller.probeNode(ctx, candidates[index])
			}
		})
	}
	waitGroup.Wait()
	return results
}

func (controller *Controller) probeNode(ctx context.Context, candidate probeCandidate) probeResult {
	result := probeResult{candidate: candidate}
	address, err := netip.ParseAddr(candidate.pod.Status.PodIP)
	if err != nil || address.IsUnspecified() {
		result.err = errors.New("node-plugin Pod has no valid Pod IP")
		return result
	}
	requestContext, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	url := "http://" + net.JoinHostPort(address.String(), strconv.Itoa(controller.healthPort)) + "/health"
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, url, nil)
	if err != nil {
		result.err = err
		return result
	}
	response, err := controller.http.Do(request)
	if err != nil {
		result.err = err
		return result
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		result.err = fmt.Errorf("node-plugin health endpoint returned HTTP %d", response.StatusCode)
		return result
	}
	if err := jsonv2.UnmarshalRead(response.Body, &result.status); err != nil {
		result.err = fmt.Errorf("decode node-plugin health response: %w", err)
		return result
	}
	if !validHealthStatus(result.status) {
		result.err = errors.New("node-plugin health response contains an invalid phase or reason")
	}
	return result
}

func validHealthStatus(status HealthStatus) bool {
	if len(status.Reason) == 0 || len(status.Reason) > healthReasonMaxLength {
		return false
	}
	switch status.Phase {
	case PhaseStarting, PhaseRecovering, PhaseReady, PhaseDegraded, PhaseUnavailable:
		return true
	default:
		return false
	}
}

func selectPluginPod(pods []*corev1.Pod) *corev1.Pod {
	if len(pods) == 0 {
		return nil
	}
	slices.SortFunc(pods, func(left, right *corev1.Pod) int {
		leftReady := left.Status.Phase == corev1.PodRunning && podReady(left)
		rightReady := right.Status.Phase == corev1.PodRunning && podReady(right)
		if leftReady != rightReady {
			if leftReady {
				return -1
			}
			return 1
		}
		return cmp.Compare(right.CreationTimestamp.UnixNano(), left.CreationTimestamp.UnixNano())
	})
	return pods[0]
}

func (controller *Controller) candidateIsCurrent(candidate probeCandidate) bool {
	node, err := controller.apiCache.CurrentNode(candidate.node.Name)
	if err != nil || node.UID != candidate.node.UID {
		return false
	}
	pod, err := controller.apiCache.CurrentPluginPod(candidate.pod.Namespace, candidate.pod.Name)
	return err == nil && pod.UID == candidate.pod.UID && pod.Spec.NodeName == node.Name && pod.DeletionTimestamp == nil
}

func (controller *Controller) setNodeReady(ctx context.Context, node *corev1.Node, ready bool) error {
	current, exists := node.Labels[ReadyLabel]
	if ready && exists && current == readyLabelValue || !ready && !exists {
		return nil
	}
	operations := []map[string]any{{"op": "test", jsonPatchPathField: "/metadata/uid", jsonPatchValueField: string(node.UID)}}
	if ready {
		if node.Labels == nil {
			operations = append(operations, map[string]any{"op": "add", jsonPatchPathField: "/metadata/labels", jsonPatchValueField: map[string]string{ReadyLabel: readyLabelValue}})
		} else {
			operations = append(operations, map[string]any{"op": "add", jsonPatchPathField: "/metadata/labels/" + escapeJSONPointer(ReadyLabel), jsonPatchValueField: readyLabelValue})
		}
	} else {
		operations = append(operations, map[string]any{"op": "remove", jsonPatchPathField: "/metadata/labels/" + escapeJSONPointer(ReadyLabel)})
	}
	patch, err := jsonv2.Marshal(operations)
	if err != nil {
		return fmt.Errorf("encode Node label update: %w", err)
	}
	_, err = controller.client.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patch Node %s cache readiness: %w", node.Name, err)
	}
	return nil
}

func escapeJSONPointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

func (controller *Controller) requestEviction(ctx context.Context, pod *corev1.Pod, key evictionKey, now time.Time) {
	if retryAt := controller.nextEvict[key]; now.Before(retryAt) {
		return
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
		return
	}
	if apierrors.IsTooManyRequests(err) {
		controller.nextEvict[key] = now.Add(ControllerInterval)
		controller.logger.WarnContext(ctx, "cache Pod eviction is blocked by a PodDisruptionBudget", "namespace", pod.Namespace, "pod", pod.Name, "podUID", pod.UID)
		return
	}
	if err != nil {
		controller.nextEvict[key] = now.Add(ControllerInterval)
		controller.logger.WarnContext(ctx, "cache Pod eviction request failed", "namespace", pod.Namespace, "pod", pod.Name, "podUID", pod.UID, "error", err)
		return
	}
	controller.nextEvict[key] = now.Add(acceptedEvictionGap)
	controller.logger.InfoContext(ctx, "requested cache Pod eviction from unavailable Node", "namespace", pod.Namespace, "pod", pod.Name, "podUID", pod.UID)
}

func (controller *Controller) warnUnmanagedPod(ctx context.Context, pod *corev1.Pod, active map[evictionKey]struct{}) {
	key := evictionKey{namespace: pod.Namespace, uid: pod.UID}
	active[key] = struct{}{}
	if _, exists := controller.notified[key]; exists {
		return
	}
	controller.notified[key] = struct{}{}
	message := "Node cache backend is unavailable but this Pod has no replacement controller"
	controller.logger.WarnContext(ctx, message, "namespace", pod.Namespace, "pod", pod.Name, "podUID", pod.UID)
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{GenerateName: pod.Name + ".cache-health-", Namespace: pod.Namespace},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID,
		},
		Reason: "CacheNodeUnavailable", Message: message, Type: corev1.EventTypeWarning,
		Source:         corev1.EventSource{Component: "cache-csi-health-controller"},
		FirstTimestamp: metav1.Now(), LastTimestamp: metav1.Now(), Count: 1,
	}
	if _, err := controller.client.CoreV1().Events(pod.Namespace).Create(ctx, event, metav1.CreateOptions{}); err != nil && !apierrors.IsForbidden(err) {
		controller.logger.WarnContext(ctx, "create cache Node unavailable Event failed", "namespace", pod.Namespace, "pod", pod.Name, "error", err)
	}
}

func podHasCacheCSIVolume(pod *corev1.Pod) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.CSI != nil && volume.CSI.Driver == CSIPluginName {
			return true
		}
	}
	return false
}

func podHasReplacementController(pod *corev1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		switch owner.APIVersion {
		case "apps/v1":
			if owner.Kind == "ReplicaSet" || owner.Kind == "StatefulSet" {
				return true
			}
		case "batch/v1":
			if owner.Kind == "Job" {
				return true
			}
		case "v1":
			if owner.Kind == "ReplicationController" {
				return true
			}
		}
	}
	return false
}

func isDaemonSetPod(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.OwnerReferences, func(owner metav1.OwnerReference) bool {
		return owner.APIVersion == "apps/v1" && owner.Kind == "DaemonSet" && owner.Controller != nil && *owner.Controller
	})
}

func podReady(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Status.Conditions, func(condition corev1.PodCondition) bool {
		return condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue
	})
}

func isEstablishedPod(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning && podReady(pod)
}
