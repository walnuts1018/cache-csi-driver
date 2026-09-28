package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "cache.storage.walnuts.dev", Version: "v1alpha1"}

func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &CacheClass{}, &CacheClassList{})
	return nil
}

func (in *CacheClass) DeepCopy() *CacheClass {
	if in == nil {
		return nil
	}
	out := *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec.MaxBytes = in.Spec.MaxBytes.DeepCopy()
	out.Spec.Quota.DefaultMaxBytes = in.Spec.Quota.DefaultMaxBytes.DeepCopy()
	out.Spec.Retention = in.Spec.Retention
	return &out
}

func (in *CacheClass) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *CacheClassList) DeepCopy() *CacheClassList {
	if in == nil {
		return nil
	}
	out := *in
	out.ListMeta = *in.ListMeta.DeepCopy()
	out.Items = make([]CacheClass, len(in.Items))
	for i := range in.Items {
		out.Items[i] = *(in.Items[i].DeepCopy())
	}
	return &out
}

func (in *CacheClassList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
