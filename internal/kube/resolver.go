package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var cacheClassGVR = schema.GroupVersionResource{Group: cachev1alpha1.GroupVersion.Group, Version: cachev1alpha1.GroupVersion.Version, Resource: "cacheclasses"}

var ErrAPIResolverUnavailable = errors.New("Kubernetes API resolver is unavailable")

func IsTemporaryAPIError(err error) bool {
	if err == nil {
		return false
	}
	switch apierrors.ReasonForError(err) {
	case metav1.StatusReasonTimeout, metav1.StatusReasonServerTimeout, metav1.StatusReasonServiceUnavailable, metav1.StatusReasonTooManyRequests, metav1.StatusReasonInternalError:
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

type Resolver struct {
	kubernetes kubernetes.Interface
	dynamic    dynamic.Interface
}

type ResolvedClass struct {
	Object cachev1alpha1.CacheClass
	UID    string
}

func NewResolver(config *rest.Config) (*Resolver, error) {
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create dynamic Kubernetes client: %w", err)
	}
	return &Resolver{kubernetes: kubeClient, dynamic: dynamicClient}, nil
}

func (r *Resolver) Resolve(ctx context.Context, namespace, className string) (string, ResolvedClass, error) {
	namespaceObject, err := r.kubernetes.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		return "", ResolvedClass{}, fmt.Errorf("get pod namespace: %w", err)
	}
	object, err := r.dynamic.Resource(cacheClassGVR).Get(ctx, className, metav1.GetOptions{})
	if err != nil {
		return "", ResolvedClass{}, fmt.Errorf("get CacheClass %q: %w", className, err)
	}
	class := cachev1alpha1.CacheClass{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &class); err != nil {
		return "", ResolvedClass{}, fmt.Errorf("decode CacheClass %q: %w", className, err)
	}
	if err := class.Spec.Validate(); err != nil {
		return "", ResolvedClass{}, fmt.Errorf("CacheClass %q is invalid: %w", className, err)
	}
	class.UID = object.GetUID()
	return string(namespaceObject.UID), ResolvedClass{Object: class, UID: string(object.GetUID())}, nil
}

func Unstructured(class *cachev1alpha1.CacheClass) (*unstructured.Unstructured, error) {
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(class)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: object}, nil
}
