package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"

	cachev1alpha1 "github.com/walnuts1018/cache-csi-driver/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
)

var cacheClassGVR = schema.GroupVersionResource{Group: cachev1alpha1.GroupVersion.Group, Version: cachev1alpha1.GroupVersion.Version, Resource: "cacheclasses"}

var ErrAPIResolverUnavailable = errors.New("the Kubernetes API resolver is unavailable")

var ErrResolverNotSynced = errors.New("the Kubernetes API informer cache is not synchronized")

var ErrServiceAccountNotCached = errors.New("the Kubernetes API informer cache does not contain the ServiceAccount")

func IsTemporaryAPIError(err error) bool {
	if err == nil {
		return false
	}
	reason := apierrors.ReasonForError(err)
	if reason == metav1.StatusReasonTimeout || reason == metav1.StatusReasonServerTimeout || reason == metav1.StatusReasonServiceUnavailable || reason == metav1.StatusReasonTooManyRequests || reason == metav1.StatusReasonInternalError {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH)
}

type Resolver struct {
	namespaceLister        corelisters.NamespaceLister
	serviceAccountLister   corelisters.ServiceAccountLister
	classLister            toolscache.GenericLister
	namespaceInformer      toolscache.SharedIndexInformer
	serviceAccountInformer toolscache.SharedIndexInformer
	classInformer          toolscache.SharedIndexInformer
	kubernetesFactory      informers.SharedInformerFactory
	dynamicFactory         dynamicinformer.DynamicSharedInformerFactory
	startOnce              sync.Once
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
	return newResolver(kubeClient, dynamicClient), nil
}

func newResolver(kubernetesClient kubernetes.Interface, dynamicClient dynamic.Interface) *Resolver {
	kubernetesFactory := informers.NewSharedInformerFactory(kubernetesClient, 0)
	namespaceInformer := kubernetesFactory.Core().V1().Namespaces()
	serviceAccountInformer := kubernetesFactory.Core().V1().ServiceAccounts()
	dynamicFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)
	classInformer := dynamicFactory.ForResource(cacheClassGVR)
	return &Resolver{
		namespaceLister:        namespaceInformer.Lister(),
		serviceAccountLister:   serviceAccountInformer.Lister(),
		classLister:            classInformer.Lister(),
		namespaceInformer:      namespaceInformer.Informer(),
		serviceAccountInformer: serviceAccountInformer.Informer(),
		classInformer:          classInformer.Informer(),
		kubernetesFactory:      kubernetesFactory,
		dynamicFactory:         dynamicFactory,
	}
}

func (r *Resolver) Start(ctx context.Context) {
	r.startOnce.Do(func() {
		r.kubernetesFactory.StartWithContext(ctx)
		r.dynamicFactory.Start(ctx.Done())
	})
}

func (r *Resolver) HasSynced() bool {
	return r.namespaceInformer.HasSynced() && r.serviceAccountInformer.HasSynced() && r.classInformer.HasSynced()
}

func (r *Resolver) Resolve(ctx context.Context, namespace, className, serviceAccountName string) (string, string, ResolvedClass, error) {
	if err := ctx.Err(); err != nil {
		return "", "", ResolvedClass{}, err
	}
	if !r.HasSynced() {
		return "", "", ResolvedClass{}, ErrResolverNotSynced
	}
	namespaceObject, err := r.namespaceLister.Get(namespace)
	if err != nil {
		return "", "", ResolvedClass{}, fmt.Errorf("get pod namespace: %w", err)
	}
	object, err := r.classLister.Get(className)
	if err != nil {
		return "", "", ResolvedClass{}, fmt.Errorf("get CacheClass %q: %w", className, err)
	}
	unstructuredClass, ok := object.(*unstructured.Unstructured)
	if !ok {
		return "", "", ResolvedClass{}, fmt.Errorf("CacheClass %q informer object has unexpected type %T", className, object)
	}
	class := cachev1alpha1.CacheClass{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredClass.Object, &class); err != nil {
		return "", "", ResolvedClass{}, fmt.Errorf("decode CacheClass %q: %w", className, err)
	}
	if err := class.Spec.Validate(); err != nil {
		return "", "", ResolvedClass{}, fmt.Errorf("CacheClass %q is invalid: %w", className, err)
	}
	class.UID = unstructuredClass.GetUID()
	serviceAccountUID := ""
	scope := class.Spec.Scope
	if scope == "" {
		scope = cachev1alpha1.ScopeServiceAccount
	}
	if scope == cachev1alpha1.ScopeServiceAccount {
		if serviceAccountName == "" {
			serviceAccountName = "default"
		}
		serviceAccount, err := r.serviceAccountLister.ServiceAccounts(namespace).Get(serviceAccountName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				err = fmt.Errorf("%w: ServiceAccount %s/%s: %w", ErrServiceAccountNotCached, namespace, serviceAccountName, err)
			}
			return "", "", ResolvedClass{}, fmt.Errorf("get ServiceAccount: %w", err)
		}
		if serviceAccount.UID == "" {
			return "", "", ResolvedClass{}, fmt.Errorf("ServiceAccount %s/%s has no UID", namespace, serviceAccountName)
		}
		serviceAccountUID = string(serviceAccount.UID)
	}
	return string(namespaceObject.UID), serviceAccountUID, ResolvedClass{Object: class, UID: string(unstructuredClass.GetUID())}, nil
}

func Unstructured(class *cachev1alpha1.CacheClass) (*unstructured.Unstructured, error) {
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(class)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: object}, nil
}
