package kube

import (
	"context"
	"errors"
	"io"
	"net"
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
	kubefake "k8s.io/client-go/kubernetes/fake"
	toolscache "k8s.io/client-go/tools/cache"
)

const testCacheClassesResource = "cacheclasses"

const testCacheClassUID = "class-uid"

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

func TestResolverUsesInformerCachesAfterStartupSync(t *testing.T) {
	t.Parallel()

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "build", UID: "namespace-uid"}}
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "builder", Namespace: namespace.Name, UID: "service-account-uid"}}
	cacheClass := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": cachev1alpha1.GroupVersion.String(),
		"kind":       "CacheClass",
		"metadata": map[string]any{
			"name": "compiler",
			"uid":  testCacheClassUID,
		},
		"spec": map[string]any{"backend": "directory"},
	}}
	kubernetesClient := kubefake.NewClientset(namespace, serviceAccount)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cacheClass)
	resolver := newResolver(kubernetesClient, dynamicClient)
	if _, _, _, err := resolver.Resolve(t.Context(), namespace.Name, "compiler", serviceAccount.Name); !errors.Is(err, ErrResolverNotSynced) {
		t.Fatalf("resolve before cache sync error = %v, want ErrResolverNotSynced", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	resolver.Start(ctx)
	if !toolscache.WaitForCacheSync(ctx.Done(), resolver.namespaceInformer.HasSynced, resolver.serviceAccountInformer.HasSynced, resolver.classInformer.HasSynced) {
		t.Fatal("resolver informer caches did not synchronize")
	}

	resolvedNamespaceUID, resolvedServiceAccountUID, resolvedClass, err := resolver.Resolve(ctx, namespace.Name, "compiler", serviceAccount.Name)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedNamespaceUID != string(namespace.UID) || resolvedServiceAccountUID != string(serviceAccount.UID) || resolvedClass.UID != testCacheClassUID || resolvedClass.Object.Spec.Backend != cachev1alpha1.BackendDirectory {
		t.Fatalf("resolved cache identity inputs = namespace UID %q, service account UID %q, class %+v", resolvedNamespaceUID, resolvedServiceAccountUID, resolvedClass)
	}

	cancel()
	if _, _, _, err := resolver.Resolve(t.Context(), namespace.Name, "compiler", serviceAccount.Name); err != nil {
		t.Fatalf("resolve from the synchronized cache after API informer shutdown: %v", err)
	}
	for _, action := range append(kubernetesClient.Actions(), dynamicClient.Actions()...) {
		if action.GetVerb() == "get" {
			t.Fatalf("resolver issued a per-request API GET: %#v", action)
		}
		if action.GetResource().Resource == "pods" {
			t.Fatalf("resolver queried Pods instead of using podInfoOnMount context: %#v", action)
		}
	}
}

func TestResolverReportsServiceAccountCacheMissSeparately(t *testing.T) {
	t.Parallel()

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "build", UID: "namespace-uid"}}
	cacheClass := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": cachev1alpha1.GroupVersion.String(),
		"kind":       "CacheClass",
		"metadata": map[string]any{
			"name": "compiler",
			"uid":  testCacheClassUID,
		},
		"spec": map[string]any{"backend": "directory"},
	}}
	kubernetesClient := kubefake.NewClientset(namespace)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cacheClass)
	resolver := newResolver(kubernetesClient, dynamicClient)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	resolver.Start(ctx)
	if !toolscache.WaitForCacheSync(ctx.Done(), resolver.namespaceInformer.HasSynced, resolver.serviceAccountInformer.HasSynced, resolver.classInformer.HasSynced) {
		t.Fatal("resolver informer caches did not synchronize")
	}

	_, _, _, err := resolver.Resolve(ctx, namespace.Name, "compiler", "newly-created")
	if !errors.Is(err, ErrServiceAccountNotCached) || !apierrors.IsNotFound(err) {
		t.Fatalf("resolve missing ServiceAccount error = %v, want distinct cache-miss sentinel wrapping NotFound", err)
	}
}
