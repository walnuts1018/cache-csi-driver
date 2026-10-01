package nodehealth

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

const podNodeNameIndex = "spec.nodeName"

type APICache struct {
	namespace         string
	pluginSelector    labels.Selector
	pluginDaemonSet   string
	factory           informers.SharedInformerFactory
	daemonSetFactory  informers.SharedInformerFactory
	nodeLister        corelisters.NodeLister
	podLister         corelisters.PodLister
	daemonSetLister   appslisters.DaemonSetLister
	nodeInformer      toolscache.SharedIndexInformer
	podInformer       toolscache.SharedIndexInformer
	daemonSetInformer toolscache.SharedIndexInformer
}

func NewAPICache(client kubernetes.Interface, namespace, pluginDaemonSet, pluginPodLabelSelector string) (*APICache, error) {
	if client == nil || namespace == "" || pluginDaemonSet == "" || pluginPodLabelSelector == "" {
		return nil, errors.New("kubernetes client, namespace, node-plugin DaemonSet, and Pod selector are required")
	}
	selector, err := labels.Parse(pluginPodLabelSelector)
	if err != nil {
		return nil, errors.New("node-plugin Pod selector is invalid")
	}
	factory := informers.NewSharedInformerFactory(client, 0)
	daemonSetFactory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace(namespace))
	nodeInformer := factory.Core().V1().Nodes().Informer()
	podInformer := factory.Core().V1().Pods().Informer()
	daemonSetInformer := daemonSetFactory.Apps().V1().DaemonSets().Informer()
	if err := podInformer.AddIndexers(toolscache.Indexers{
		podNodeNameIndex: func(object any) ([]string, error) {
			pod, ok := object.(*corev1.Pod)
			if !ok {
				return nil, errors.New("pod informer contains an unexpected object")
			}
			if pod.Spec.NodeName == "" {
				return nil, nil
			}
			return []string{pod.Spec.NodeName}, nil
		},
	}); err != nil {
		return nil, err
	}
	return &APICache{
		namespace:         namespace,
		pluginSelector:    selector,
		pluginDaemonSet:   pluginDaemonSet,
		factory:           factory,
		daemonSetFactory:  daemonSetFactory,
		nodeLister:        factory.Core().V1().Nodes().Lister(),
		podLister:         factory.Core().V1().Pods().Lister(),
		daemonSetLister:   daemonSetFactory.Apps().V1().DaemonSets().Lister(),
		nodeInformer:      nodeInformer,
		podInformer:       podInformer,
		daemonSetInformer: daemonSetInformer,
	}, nil
}

func (apiCache *APICache) Start(ctx context.Context) error {
	apiCache.factory.Start(ctx.Done())
	apiCache.daemonSetFactory.Start(ctx.Done())
	if !toolscache.WaitForCacheSync(ctx.Done(), apiCache.nodeInformer.HasSynced, apiCache.podInformer.HasSynced, apiCache.daemonSetInformer.HasSynced) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("kubernetes node, Pod, and DaemonSet informer caches did not synchronize")
	}
	return nil
}

func (apiCache *APICache) Synced() bool {
	return apiCache.nodeInformer.HasSynced() && apiCache.podInformer.HasSynced() && apiCache.daemonSetInformer.HasSynced()
}

func (apiCache *APICache) Nodes() ([]*corev1.Node, error) {
	if !apiCache.nodeInformer.HasSynced() {
		return nil, errors.New("kubernetes node informer cache is not synchronized")
	}
	return apiCache.nodeLister.List(labels.Everything())
}

func (apiCache *APICache) PluginPods() ([]*corev1.Pod, error) {
	if !apiCache.podInformer.HasSynced() {
		return nil, errors.New("kubernetes Pod informer cache is not synchronized")
	}
	pods, err := apiCache.podLister.Pods(apiCache.namespace).List(apiCache.pluginSelector)
	if err != nil {
		return nil, err
	}
	daemonSet, err := apiCache.daemonSetLister.DaemonSets(apiCache.namespace).Get(apiCache.pluginDaemonSet)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return []*corev1.Pod{}, nil
		}
		return nil, err
	}
	current := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if controlledByUID(pod, "DaemonSet", daemonSet.UID) {
			current = append(current, pod)
		}
	}
	return current, nil
}

func (apiCache *APICache) PodsOnNode(nodeName string) ([]*corev1.Pod, error) {
	if !apiCache.podInformer.HasSynced() {
		return nil, errors.New("kubernetes Pod informer cache is not synchronized")
	}
	objects, err := apiCache.podInformer.GetIndexer().ByIndex(podNodeNameIndex, nodeName)
	if err != nil {
		return nil, err
	}
	pods := make([]*corev1.Pod, 0, len(objects))
	for _, object := range objects {
		pod, ok := object.(*corev1.Pod)
		if !ok {
			return nil, errors.New("pod informer contains an unexpected object")
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

func (apiCache *APICache) CurrentPluginPod(namespace, name string) (*corev1.Pod, error) {
	pod, err := apiCache.podLister.Pods(namespace).Get(name)
	if err != nil {
		return nil, err
	}
	if !apiCache.pluginSelector.Matches(labels.Set(pod.Labels)) {
		return nil, errors.New("pod is not a cache CSI node plugin")
	}
	daemonSet, err := apiCache.daemonSetLister.DaemonSets(apiCache.namespace).Get(apiCache.pluginDaemonSet)
	if err != nil {
		return nil, err
	}
	if !controlledByUID(pod, "DaemonSet", daemonSet.UID) {
		return nil, errors.New("pod is controlled by an obsolete node-plugin DaemonSet")
	}
	return pod, nil
}

func controlledByUID(pod *corev1.Pod, kind string, uid types.UID) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && owner.Kind == kind && owner.UID == uid {
			return true
		}
	}
	return false
}

func (apiCache *APICache) CurrentNode(name string) (*corev1.Node, error) {
	return apiCache.nodeLister.Get(name)
}
