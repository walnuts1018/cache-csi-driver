package nodehealth

import (
	"context"
	"errors"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	coordinationlisters "k8s.io/client-go/listers/coordination/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

type APICache struct {
	namespace     string
	leaseFactory  informers.SharedInformerFactory
	nodeFactory   informers.SharedInformerFactory
	leaseLister   coordinationlisters.LeaseLister
	nodeLister    corelisters.NodeLister
	leaseInformer toolscache.SharedIndexInformer
	nodeInformer  toolscache.SharedIndexInformer
}

func NewAPICache(client kubernetes.Interface, namespace string) (*APICache, error) {
	if client == nil || namespace == "" {
		return nil, errors.New("kubernetes client and health namespace are required")
	}
	leaseFactory := informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = LeaseComponentLabel + "=" + LeaseComponentValue
		}),
	)
	nodeFactory := informers.NewSharedInformerFactory(client, 0)
	leaseInformer := leaseFactory.Coordination().V1().Leases()
	nodeInformer := nodeFactory.Core().V1().Nodes()
	return &APICache{
		namespace:     namespace,
		leaseFactory:  leaseFactory,
		nodeFactory:   nodeFactory,
		leaseLister:   leaseInformer.Lister(),
		nodeLister:    nodeInformer.Lister(),
		leaseInformer: leaseInformer.Informer(),
		nodeInformer:  nodeInformer.Informer(),
	}, nil
}

func (apiCache *APICache) Start(ctx context.Context) error {
	apiCache.leaseFactory.Start(ctx.Done())
	apiCache.nodeFactory.Start(ctx.Done())
	if !toolscache.WaitForCacheSync(ctx.Done(), apiCache.leaseInformer.HasSynced, apiCache.nodeInformer.HasSynced) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("kubernetes lease and node informer caches did not synchronize")
	}
	return nil
}

func (apiCache *APICache) Synced() bool {
	return apiCache.leaseInformer.HasSynced() && apiCache.nodeInformer.HasSynced()
}

func (apiCache *APICache) Leases() ([]*coordinationv1.Lease, error) {
	if !apiCache.leaseInformer.HasSynced() {
		return nil, errors.New("kubernetes lease informer cache is not synchronized")
	}
	return apiCache.leaseLister.Leases(apiCache.namespace).List(labels.Set{LeaseComponentLabel: LeaseComponentValue}.AsSelector())
}

func (apiCache *APICache) Nodes() ([]*corev1.Node, error) {
	if !apiCache.nodeInformer.HasSynced() {
		return nil, errors.New("kubernetes node informer cache is not synchronized")
	}
	return apiCache.nodeLister.List(labels.Everything())
}
