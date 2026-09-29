package kube

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	k8stesting "k8s.io/client-go/testing"
	toolscache "k8s.io/client-go/tools/cache"
)

const testCacheClassesResource = "cacheclasses"

const testCacheClassUID = "class-uid"

const testNamespaceName = "build"

const testNamespaceUID = "namespace-uid"

func TestIsTemporaryAPIError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "service unavailable", err: apierrors.NewServiceUnavailable("temporarily unavailable"), want: true},
		{name: "request timeout", err: apierrors.NewTimeoutError("request timed out", 0), want: true},
		{name: "wrapped too many requests", err: errors.Join(errors.New("resolver request failed"), apierrors.NewTooManyRequests("rate limited", 1)), want: true},
		{name: "network timeout", err: &net.DNSError{IsTimeout: true, Err: "resolver timeout"}, want: true},
		{name: "API connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, want: true},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: true},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF, want: true},
		{name: "not found", err: apierrors.NewNotFound(schema.GroupResource{Resource: testCacheClassesResource}, "missing"), want: false},
		{name: "forbidden", err: apierrors.NewForbidden(schema.GroupResource{Resource: testCacheClassesResource}, "restricted", errors.New("access denied")), want: false},
		{name: "request canceled", err: context.Canceled, want: false},
		{name: "DNS name not found", err: &net.DNSError{IsNotFound: true, Err: "no such host"}, want: false},
		{name: "unknown API status", err: apierrors.NewGenericServerResponse(499, "get", schema.GroupResource{Resource: testCacheClassesResource}, "", "client closed request", 0, false), want: false},
		{name: "unrelated Kubernetes status", err: apierrors.NewBadRequest("invalid request"), want: false},
		{name: "ordinary error", err: errors.New("invalid CacheClass configuration"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := IsTemporaryAPIError(tt.err); got != tt.want {
				t.Fatalf("IsTemporaryAPIError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestResolverGetsNamespaceAndServiceAccountOnDemand(t *testing.T) {
	t.Parallel()

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespaceName, UID: testNamespaceUID}}
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "builder", Namespace: namespace.Name, UID: "service-account-uid"}}
	kubernetesClient := kubefake.NewClientset(namespace, serviceAccount)
	resolver, ctx, cancel := newSyncedTestResolver(t, kubernetesClient, testCacheClass("ServiceAccount"))
	defer cancel()

	resolvedNamespaceUID, resolvedServiceAccountUID, resolvedClass, err := resolver.Resolve(ctx, namespace.Name, "compiler", serviceAccount.Name)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedNamespaceUID != string(namespace.UID) || resolvedServiceAccountUID != string(serviceAccount.UID) || resolvedClass.UID != testCacheClassUID || resolvedClass.Object.Spec.Backend != cachev1alpha1.BackendDirectory {
		t.Fatalf("resolved cache identity inputs = namespace UID %q, service account UID %q, class %+v", resolvedNamespaceUID, resolvedServiceAccountUID, resolvedClass)
	}

	updatedNamespace := namespace.DeepCopy()
	updatedNamespace.UID = "replacement-namespace-uid"
	if _, err := kubernetesClient.CoreV1().Namespaces().Update(ctx, updatedNamespace, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	updatedServiceAccount := serviceAccount.DeepCopy()
	updatedServiceAccount.UID = "replacement-service-account-uid"
	if _, err := kubernetesClient.CoreV1().ServiceAccounts(namespace.Name).Update(ctx, updatedServiceAccount, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	resolvedNamespaceUID, resolvedServiceAccountUID, _, err = resolver.Resolve(ctx, namespace.Name, "compiler", serviceAccount.Name)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedNamespaceUID != string(updatedNamespace.UID) || resolvedServiceAccountUID != string(updatedServiceAccount.UID) {
		t.Fatalf("resolve reused stale identity: got namespace UID %q and service account UID %q", resolvedNamespaceUID, resolvedServiceAccountUID)
	}

	getCounts := map[string]int{}
	for _, action := range kubernetesClient.Actions() {
		if action.GetVerb() == "get" {
			getCounts[action.GetResource().Resource]++
		}
		if action.GetResource().Resource == "namespaces" || action.GetResource().Resource == "serviceaccounts" {
			if action.GetVerb() == "list" || action.GetVerb() == "watch" {
				t.Fatalf("resolver watched cluster-wide identity resources: %#v", action)
			}
		}
	}
	if getCounts["namespaces"] != 2 || getCounts["serviceaccounts"] != 2 {
		t.Fatalf("on-demand GET counts = %#v, want two GETs per identity object", getCounts)
	}
}

func TestResolverUsesOnlyCacheClassInformerForSynchronization(t *testing.T) {
	t.Parallel()

	kubernetesClient := kubefake.NewClientset()
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), testCacheClass("Namespace"))
	resolver := newResolver(kubernetesClient, dynamicClient)
	if resolver.HasSynced() {
		t.Fatal("resolver reported synchronized before the CacheClass informer started")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	resolver.Start(ctx)
	if !toolscache.WaitForCacheSync(ctx.Done(), resolver.classInformer.HasSynced) {
		t.Fatal("CacheClass informer did not synchronize")
	}
	if !resolver.HasSynced() {
		t.Fatal("resolver remained unsynchronized after the CacheClass informer synced")
	}
	for _, action := range kubernetesClient.Actions() {
		if action.GetVerb() == "list" || action.GetVerb() == "watch" {
			t.Fatalf("resolver started a Kubernetes core informer: %#v", action)
		}
	}
}

func TestResolverDoesNotUseStaleUIDWhenIdentityGETFails(t *testing.T) {
	t.Parallel()

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespaceName, UID: testNamespaceUID}}
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "builder", Namespace: namespace.Name, UID: "service-account-uid"}}
	kubernetesClient := kubefake.NewClientset(namespace, serviceAccount)
	var failNamespace atomic.Bool
	var failServiceAccount atomic.Bool
	kubernetesClient.PrependReactor("get", "namespaces", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if failNamespace.Load() {
			return true, nil, apierrors.NewServiceUnavailable("namespace lookup unavailable")
		}
		return false, nil, nil
	})
	kubernetesClient.PrependReactor("get", "serviceaccounts", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if failServiceAccount.Load() {
			return true, nil, apierrors.NewServiceUnavailable("service account lookup unavailable")
		}
		return false, nil, nil
	})
	resolver, ctx, cancel := newSyncedTestResolver(t, kubernetesClient, testCacheClass("ServiceAccount"))
	defer cancel()
	if _, _, _, err := resolver.Resolve(ctx, namespace.Name, "compiler", serviceAccount.Name); err != nil {
		t.Fatal(err)
	}

	failNamespace.Store(true)
	namespaceUID, serviceAccountUID, _, err := resolver.Resolve(ctx, namespace.Name, "compiler", serviceAccount.Name)
	if !IsTemporaryAPIError(err) || namespaceUID != "" || serviceAccountUID != "" {
		t.Fatalf("resolve with unavailable Namespace GET = %q, %q, %v; want no UID and temporary error", namespaceUID, serviceAccountUID, err)
	}

	failNamespace.Store(false)
	failServiceAccount.Store(true)
	namespaceUID, serviceAccountUID, _, err = resolver.Resolve(ctx, namespace.Name, "compiler", serviceAccount.Name)
	if !IsTemporaryAPIError(err) || namespaceUID != "" || serviceAccountUID != "" {
		t.Fatalf("resolve with unavailable ServiceAccount GET = %q, %q, %v; want no UID and temporary error", namespaceUID, serviceAccountUID, err)
	}
}

func TestResolverCoalescesGETAndKeepsItAliveForOtherWaiters(t *testing.T) {
	t.Parallel()

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespaceName, UID: testNamespaceUID}}
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRequest) }) }
	t.Cleanup(release)
	var getCount atomic.Int32
	baseClient := kubefake.NewClientset(namespace)
	namespaceClient := blockingNamespaceClient{
		NamespaceInterface: baseClient.CoreV1().Namespaces(),
		object:             namespace,
		calls:              &getCount,
		started:            requestStarted,
		release:            releaseRequest,
	}
	coreAPI := resolverTestCoreV1Client{CoreV1Interface: baseClient.CoreV1(), namespaces: &namespaceClient}
	kubernetesClient := resolverTestKubernetesClient{Interface: baseClient, core: coreAPI}
	resolver := newResolver(kubernetesClient, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()))
	firstContext, cancelFirst := context.WithCancel(t.Context())
	firstResult := make(chan error, 1)
	go func() {
		_, err := resolver.getNamespace(firstContext, namespace.Name)
		firstResult <- err
	}()
	select {
	case <-requestStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Namespace GET did not start")
	}

	cancelFirst()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first waiter error = %v, want context cancellation", err)
	}
	secondResult := make(chan struct {
		namespace *corev1.Namespace
		err       error
	}, 1)
	secondContext := &resolverTestObservedContext{done: make(chan struct{}), doneCalled: make(chan struct{})}
	go func() {
		resolved, err := resolver.getNamespace(secondContext, namespace.Name)
		secondResult <- struct {
			namespace *corev1.Namespace
			err       error
		}{namespace: resolved, err: err}
	}()
	select {
	case <-secondContext.doneCalled:
	case <-t.Context().Done():
		t.Fatal("test context ended before the second waiter joined")
	}
	release()
	result := <-secondResult
	if result.err != nil || result.namespace == nil || result.namespace.UID != namespace.UID {
		t.Fatalf("second waiter result = %#v, want the completed Namespace GET", result)
	}
	if got := getCount.Load(); got != 1 {
		t.Fatalf("Namespace GET count = %d, want one coalesced GET", got)
	}
}

type resolverTestKubernetesClient struct {
	kubernetes.Interface
	core coreclient.CoreV1Interface
}

func (c resolverTestKubernetesClient) CoreV1() coreclient.CoreV1Interface {
	return c.core
}

type resolverTestCoreV1Client struct {
	coreclient.CoreV1Interface
	namespaces coreclient.NamespaceInterface
}

func (c resolverTestCoreV1Client) Namespaces() coreclient.NamespaceInterface {
	return c.namespaces
}

type blockingNamespaceClient struct {
	coreclient.NamespaceInterface
	object  *corev1.Namespace
	calls   *atomic.Int32
	started chan struct{}
	release <-chan struct{}
}

type resolverTestObservedContext struct {
	done       chan struct{}
	doneCalled chan struct{}
	once       sync.Once
}

func (*resolverTestObservedContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c *resolverTestObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneCalled) })
	return c.done
}

func (c *resolverTestObservedContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func (*resolverTestObservedContext) Value(any) any {
	return nil
}

func (c *blockingNamespaceClient) Get(ctx context.Context, _ string, _ metav1.GetOptions) (*corev1.Namespace, error) {
	if c.calls.Add(1) == 1 {
		close(c.started)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return c.object.DeepCopy(), nil
	}
}

func TestResolverReportsMissingServiceAccount(t *testing.T) {
	t.Parallel()

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespaceName, UID: testNamespaceUID}}
	kubernetesClient := kubefake.NewClientset(namespace)
	resolver, ctx, cancel := newSyncedTestResolver(t, kubernetesClient, testCacheClass("ServiceAccount"))
	defer cancel()

	_, _, _, err := resolver.Resolve(ctx, namespace.Name, "compiler", "newly-created")
	if !errors.Is(err, ErrServiceAccountNotFound) || !apierrors.IsNotFound(err) {
		t.Fatalf("resolve missing ServiceAccount error = %v, want distinct unresolved sentinel wrapping NotFound", err)
	}
}

func TestResolverNamespaceScopeDoesNotReadServiceAccount(t *testing.T) {
	t.Parallel()

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespaceName, UID: testNamespaceUID}}
	kubernetesClient := kubefake.NewClientset(namespace)
	resolver, ctx, cancel := newSyncedTestResolver(t, kubernetesClient, testCacheClass("Namespace"))
	defer cancel()

	namespaceUID, serviceAccountUID, _, err := resolver.Resolve(ctx, namespace.Name, "compiler", "builder")
	if err != nil {
		t.Fatal(err)
	}
	if namespaceUID != string(namespace.UID) || serviceAccountUID != "" {
		t.Fatalf("resolved identity = namespace UID %q, service account UID %q", namespaceUID, serviceAccountUID)
	}
	for _, action := range kubernetesClient.Actions() {
		if action.GetResource().Resource == "serviceaccounts" {
			t.Fatalf("Namespace-scoped CacheClass caused a ServiceAccount API request: %#v", action)
		}
	}
}

func newSyncedTestResolver(t *testing.T, kubernetesClient *kubefake.Clientset, cacheClass *unstructured.Unstructured) (*Resolver, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	resolver := newResolver(kubernetesClient, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cacheClass))
	resolver.Start(ctx)
	if !toolscache.WaitForCacheSync(ctx.Done(), resolver.classInformer.HasSynced) {
		cancel()
		t.Fatal("CacheClass informer did not synchronize")
	}
	return resolver, ctx, cancel
}

func testCacheClass(scope string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": cachev1alpha1.GroupVersion.String(),
		"kind":       "CacheClass",
		"metadata": map[string]any{
			"name": "compiler",
			"uid":  testCacheClassUID,
		},
		"spec": map[string]any{"backend": "directory", "scope": scope},
	}}
}
