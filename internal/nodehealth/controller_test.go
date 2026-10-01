package nodehealth

import (
	"bytes"
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const (
	testPluginSelector         = "app.kubernetes.io/component=node"
	testReasonCacheReady       = "CacheReady"
	testReasonCacheRootFailure = "CacheRootUnavailable"
)

func TestControllerHealthSchedulingAndEviction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                string
		status              HealthStatus
		wantReady           bool
		wantEvictions       int
		wantEvent           bool
		wantRunningEviction bool
	}{
		{name: "ready", status: HealthStatus{Phase: PhaseReady, Reason: testReasonCacheReady}, wantReady: true},
		{name: "unavailable without eviction", status: HealthStatus{Phase: PhaseUnavailable, Reason: "CachePressure"}, wantEvictions: 1},
		{name: "unavailable with eviction", status: HealthStatus{Phase: PhaseUnavailable, Reason: testReasonCacheRootFailure, Evict: true}, wantEvictions: 2, wantEvent: true, wantRunningEviction: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newControllerFixture(t, test.status)
			var evictions []string
			fixture.client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "eviction" {
					return false, nil, nil
				}
				evictions = append(evictions, action.GetNamespace()+"/"+action.(ktesting.CreateAction).GetObject().(*policyv1.Eviction).Name)
				return true, nil, nil
			})
			fixture.controller.Reconcile(t.Context())

			updatedNode, err := fixture.client.CoreV1().Nodes().Get(t.Context(), "worker-a", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got := updatedNode.Labels[ReadyLabel] == readyLabelValue; got != test.wantReady {
				t.Fatalf("Node ready label = %t, want %t", got, test.wantReady)
			}
			if len(evictions) != test.wantEvictions {
				t.Fatalf("evictions = %v, want %d eviction requests", evictions, test.wantEvictions)
			}
			if got := slices.Contains(evictions, "workloads/running"); got != test.wantRunningEviction {
				t.Fatalf("Running cache Pod eviction = %t, want %t", got, test.wantRunningEviction)
			}
			if slices.Contains(evictions, "workloads/daemonset-owned") {
				t.Fatal("evicted a DaemonSet Pod")
			}
			if slices.Contains(evictions, "workloads/bare") {
				t.Fatal("evicted a bare Pod")
			}
			events, err := fixture.client.CoreV1().Events("workloads").List(t.Context(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if (len(events.Items) > 0) != test.wantEvent {
				t.Fatalf("warning Events = %d, want event=%t", len(events.Items), test.wantEvent)
			}
		})
	}
}

func TestHealthEndpointReturnsTypedStatus(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.SetCondition(SubsystemFilesystem, PhaseUnavailable, testReasonCacheRootFailure, true)
	response := httptest.NewRecorder()
	tracker.HealthHandler(response, httptest.NewRequest(http.MethodGet, "/health", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("health endpoint status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("health endpoint Content-Type = %q, want application/json", got)
	}
	var status HealthStatus
	if err := jsonv2.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	want := HealthStatus{Phase: PhaseUnavailable, Reason: testReasonCacheRootFailure, Evict: true}
	if status != want {
		t.Fatalf("health status = %+v, want %+v", status, want)
	}
}

func TestControllerIgnoresOldPluginPodUIDAndRemovesReadyLabelOnProbeFailure(t *testing.T) {
	t.Parallel()
	fixture := newControllerFixture(t, HealthStatus{Phase: PhaseReady, Reason: testReasonCacheReady})
	oldPod, err := fixture.client.CoreV1().Pods("system").Get(t.Context(), "cache-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oldPod.OwnerReferences[0].UID = "old-daemonset-uid"
	if _, err := fixture.client.CoreV1().Pods("system").Update(t.Context(), oldPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	fixture.transport.unavailable.Store(true)
	fixture.controller.Reconcile(t.Context())
	updatedNode, err := fixture.client.CoreV1().Nodes().Get(t.Context(), "worker-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := updatedNode.Labels[ReadyLabel]; exists {
		t.Fatalf("stale plugin Pod kept Node ready label: %v", updatedNode.Labels)
	}
}

func TestControllerRejectsHealthForReplacedPodOrNodeUID(t *testing.T) {
	t.Parallel()
	fixture := newControllerFixture(t, HealthStatus{Phase: PhaseReady, Reason: testReasonCacheReady})
	nodes, err := fixture.controller.apiCache.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	pluginPods, err := fixture.controller.apiCache.PluginPods()
	if err != nil {
		t.Fatal(err)
	}
	candidate := probeCandidate{node: nodes[0], pod: pluginPods[0]}
	updatedNode := candidate.node.DeepCopy()
	updatedNode.UID = "replacement-node-uid"
	if _, err := fixture.client.CoreV1().Nodes().Update(t.Context(), updatedNode, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, func() bool { return !fixture.controller.candidateIsCurrent(candidate) }) {
		t.Fatal("health observation for a replaced Node UID was accepted")
	}

	currentNode, err := fixture.controller.apiCache.CurrentNode(candidate.node.Name)
	if err != nil {
		t.Fatal(err)
	}
	currentPod, err := fixture.controller.apiCache.CurrentPluginPod(candidate.pod.Namespace, candidate.pod.Name)
	if err != nil {
		t.Fatal(err)
	}
	candidate.node = currentNode
	candidate.pod = currentPod
	replacementPod := currentPod.DeepCopy()
	replacementPod.UID = "replacement-plugin-pod-uid"
	if _, err := fixture.client.CoreV1().Pods(currentPod.Namespace).Update(t.Context(), replacementPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, func() bool { return !fixture.controller.candidateIsCurrent(candidate) }) {
		t.Fatal("health observation for a replaced node-plugin Pod UID was accepted")
	}
}

func TestControllerDoesNotPairOldPluginPodWithRecreatedNode(t *testing.T) {
	t.Parallel()
	fixture := newControllerFixture(t, HealthStatus{Phase: PhaseReady, Reason: testReasonCacheReady})
	node, err := fixture.client.CoreV1().Nodes().Get(t.Context(), "worker-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node.UID = "replacement-node-uid"
	node.CreationTimestamp = metav1.NewTime(time.Now().Add(time.Second))
	if _, err := fixture.client.CoreV1().Nodes().Update(t.Context(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, func() bool {
		current, err := fixture.controller.apiCache.CurrentNode(node.Name)
		return err == nil && current.UID == node.UID
	}) {
		t.Fatal("Node informer did not observe replacement Node UID")
	}
	fixture.controller.Reconcile(t.Context())
	updatedNode, err := fixture.client.CoreV1().Nodes().Get(t.Context(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := updatedNode.Labels[ReadyLabel]; exists {
		t.Fatalf("old plugin Pod made recreated Node ready: %v", updatedNode.Labels)
	}
}

func TestControllerRetriesPDBBlockedEviction(t *testing.T) {
	t.Parallel()
	fixture := newControllerFixture(t, HealthStatus{Phase: PhaseUnavailable, Reason: testReasonCacheRootFailure, Evict: true})
	attempts := 0
	fixture.client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		attempts++
		if attempts == 1 {
			return true, nil, apierrors.NewTooManyRequests("blocked by PDB", 1)
		}
		return true, nil, nil
	})
	fixture.controller.Reconcile(t.Context())
	if attempts != 2 {
		t.Fatalf("first reconcile eviction attempts = %d, want two eligible replacement Pods", attempts)
	}
	fixture.controller.Reconcile(t.Context())
	if attempts != 2 {
		t.Fatalf("immediate retry attempts = %d, want retry backoff", attempts)
	}
	for key := range fixture.controller.nextEvict {
		fixture.controller.nextEvict[key] = time.Now().Add(-time.Second)
	}
	fixture.controller.Reconcile(t.Context())
	if attempts != 4 {
		t.Fatalf("retry attempts = %d, want two retried eviction requests", attempts)
	}
}

func newControllerFixture(t *testing.T, status HealthStatus) *controllerFixture {
	t.Helper()
	transport := &testHealthTransport{status: status}
	trueValue := true
	controllerOwner := func(kind string, uid types.UID) metav1.OwnerReference {
		return metav1.OwnerReference{APIVersion: "apps/v1", Kind: kind, UID: uid, Controller: &trueValue}
	}
	cacheVolume := corev1.Volume{Name: "cache", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{Driver: CSIPluginName}}}
	readyCondition := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", UID: "node-uid", Labels: map[string]string{ReadyLabel: readyLabelValue}}}
	daemonSet := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "cache-node", Namespace: "system", UID: "daemonset-uid"}}
	pluginPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cache-node", Namespace: "system", UID: "plugin-pod-uid", Labels: map[string]string{"app.kubernetes.io/component": "node"}, OwnerReferences: []metav1.OwnerReference{controllerOwner("DaemonSet", daemonSet.UID)}},
		Spec:       corev1.PodSpec{NodeName: node.Name},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "127.0.0.1", Conditions: []corev1.PodCondition{readyCondition}},
	}
	workload := func(name string, phase corev1.PodPhase, ready bool, owner metav1.OwnerReference) *corev1.Pod {
		conditions := []corev1.PodCondition(nil)
		if ready {
			conditions = []corev1.PodCondition{readyCondition}
		}
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "workloads", UID: types.UID(name + "-uid"), OwnerReferences: []metav1.OwnerReference{owner}}, Spec: corev1.PodSpec{NodeName: node.Name, Volumes: []corev1.Volume{cacheVolume}}, Status: corev1.PodStatus{Phase: phase, Conditions: conditions}}
	}
	objects := []runtime.Object{
		node,
		daemonSet,
		pluginPod,
		workload("running", corev1.PodRunning, true, controllerOwner("ReplicaSet", "rs-uid")),
		workload("pending", corev1.PodPending, false, controllerOwner("ReplicaSet", "rs-uid")),
		workload("bare", corev1.PodPending, false, metav1.OwnerReference{}),
		workload("daemonset-owned", corev1.PodPending, false, controllerOwner("DaemonSet", "workload-daemonset-uid")),
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ordinary", Namespace: "workloads", UID: "ordinary-uid", OwnerReferences: []metav1.OwnerReference{controllerOwner("ReplicaSet", "rs-uid")}}, Spec: corev1.PodSpec{NodeName: node.Name}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
	}
	client := fake.NewClientset(objects...)
	apiCache, err := NewAPICache(client, "system", daemonSet.Name, testPluginSelector)
	if err != nil {
		t.Fatal(err)
	}
	informerContext, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	if err := apiCache.Start(informerContext); err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(client, apiCache, "system", nil)
	if err != nil {
		t.Fatal(err)
	}
	controller.http = &http.Client{Transport: transport}
	return &controllerFixture{client: client, controller: controller, transport: transport}
}

type controllerFixture struct {
	client     *fake.Clientset
	controller *Controller
	transport  *testHealthTransport
}

type testHealthTransport struct {
	status      HealthStatus
	unavailable atomic.Bool
}

func (transport *testHealthTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport.unavailable.Load() {
		return nil, errors.New("test health endpoint is unavailable")
	}
	body, err := jsonv2.Marshal(transport.status)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    request,
	}, nil
}

func waitFor(t *testing.T, condition func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return condition()
}
