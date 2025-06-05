package recorder

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/loft-sh/magpie/pkg/eventstores/idempotent"
	"github.com/loft-sh/magpie/types"
	errors2 "github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	cmNameFormat = "magpie-%s-%s-%s"
)

var _ reconcile.TypedReconciler[ctrl.Request]

type Target struct {
	GVK     schema.GroupVersionKind
	Fields  [][]string
	updates bool
	creates bool
	deletes bool

	memory map[string]struct{}
}

type Reconciler struct {
	Client      client.Client
	configMapNS string
	target      Target
	store       types.EventStore[string]
}

func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	obj := unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.target.GVK)
	err := r.Client.Get(ctx, client.ObjectKey{Name: request.Name, Namespace: request.Namespace}, &obj)
	if err != nil {
		if errors.IsNotFound(err) {
			err = r.SyncTargetDelete(ctx, obj)
			if err != nil {
				return reconcile.Result{}, errors2.Wrap(err, "failed to sync delete event")
			}
		}
		return reconcile.Result{}, err
	}

	err = r.SyncTargetAdd(ctx, obj)
	if err != nil {
		return reconcile.Result{}, err
	}

	fmt.Print(r.store.List(getResourceIDFromUnstructured(obj)))
	return ctrl.Result{}, nil
}

func (r *Reconciler) SyncTargetAdd(ctx context.Context, obj unstructured.Unstructured) error {
	cm, err := r.getGVKConfigMap(ctx, r.target.GVK)
	if err != nil {
		return errors2.Wrapf(err, "get configmap for gvk [%s]", r.target.GVK.String())
	}

	if _, ok := cm.Data[getResourceIDFromUnstructured(obj)]; ok {
		return nil
	}

	cm.Data[getResourceIDFromUnstructured(obj)] = obj.GetCreationTimestamp().String()

	err = r.Client.Update(ctx, cm)
	if err != nil {
		return errors2.Wrapf(err, "update configmap for gvk [%s]", r.target.GVK.String())
	}

	r.store.Add(getResourceIDFromUnstructured(obj), types.Event{Obj: obj.Object, EventType: types.Create})
	return nil
}

func (r *Reconciler) SyncTargetDelete(ctx context.Context, key string) error {
	cm, err := r.getGVKConfigMap(ctx, r.target.GVK)
	if err != nil {
		return errors2.Wrapf(err, "get configmap for gvk [%s]", r.target.GVK.String())
	}

	if _, ok := cm.Data[key]; !ok {
		return nil
	}
	delete(cm.Data, key)
	err = r.Client.Update(ctx, cm)
	if err != nil {
		return errors2.Wrapf(err, "update configmap for gvk [%s]", r.target.GVK.String())
	}

	r.store.Add(key, types.Event{EventType: types.Delete})
	return nil
}

func (r *Reconciler) createEventFromTarget(obj map[string]interface{}) map[string]interface{} {
	filteredObj := make(map[string]interface{})
	for _, field := range r.target.Fields {
		val := getNested(obj, field...)
		setNested(filteredObj, val, field...)
	}
	return filteredObj
}

func SetupWithManagerForTargets(ctx context.Context, mgr ctrl.Manager, targets []Target, cmNS string) error {
	eventStore, err := idempotent.NewStore()
	if err != nil {
		return errors2.Wrap(err, "failed to create event store")
	}

	for _, target := range targets {
		r := &Reconciler{
			Client: mgr.GetClient(),
			target: target,
			store:  eventStore,
		}

		err = ctrl.NewControllerManagedBy(mgr).
			// Uncomment the following line adding a pointer to an instance of the controlled resource as an argument
			For(&unstructured.Unstructured{Object: map[string]interface{}{"kind": target.GVK.Kind, "apiVersion": target.GVK.GroupVersion().String()}}).
			Named("cluster").
			Complete(r)
		if err != nil {
			return err
		}

		go func() {
			mgr.GetCache().WaitForCacheSync(ctx)

			err = r.ensureConfigMap(ctx, mgr, target.GVK, cmNS)
			if err != nil {
				klog.V(1).ErrorS(err, fmt.Sprintf("failed to list ConfigMaps for gvr [%s]", target.GVK))
				<-ctx.Done()
				return
			}

		}()
	}
	return nil
}

func (r *Reconciler) ensureConfigMap(ctx context.Context, mgr ctrl.Manager, gvk schema.GroupVersionKind, ns string) error {
	cmName := fmt.Sprintf(cmNameFormat, gvk.Group, gvk.Version, gvk.Kind)
	cm, err := r.getGVKConfigMap(ctx, gvk)
	if err != nil && !errors.IsNotFound(err) {
		return errors2.Wrap(err, fmt.Sprintf("failed to get magpie ConfigMap [%s]", cmName))
	}
	if errors.IsNotFound(err) {
		cm.Namespace = ns
		err = mgr.GetClient().Create(ctx, cm)
		if err != nil {
			return errors2.Wrap(err, "failed to create ConfigMap")
		}
	}

	list := unstructured.UnstructuredList{Items: []unstructured.Unstructured{}}

	list.SetGroupVersionKind(gvk)
	err = mgr.GetClient().List(ctx, &list, nil)
	if err != nil {
		return errors2.Wrap(err, fmt.Sprintf("failed to list gvk [%s]", gvk.String()))
	}

	currentGVKids := make([]string, len(list.Items))
	for index, item := range list.Items {
		currentGVKids[index] = getResourceIDFromUnstructured(item)
	}

	cmNeedsUpdate := false
	for key := range cm.Data {
		if !slices.Contains(currentGVKids, key) {
			cmNeedsUpdate = true
			r.store.Add(key, types.Event{EventType: types.InferredDelete})
			delete(cm.Data, key)
		}
	}
	for index, key := range currentGVKids {
		_, ok := cm.Data[key]
		if ok {
			continue
		}
		cmNeedsUpdate = true
		cm.Data[key] = time.Now().String()
		r.store.Add(key, types.Event{Obj: list.Items[index].Object, EventType: types.InferredCreate})
	}
	if cmNeedsUpdate {
		err = mgr.GetClient().Update(ctx, cm)
		if err != nil {
			return errors2.Wrap(err, "failed to update ")
		}
	}
	return nil
}

func (r *Reconciler) getGVKConfigMap(ctx context.Context, gvk schema.GroupVersionKind) (*v1.ConfigMap, error) {
	cm := &v1.ConfigMap{}
	cmName := fmt.Sprintf(cmNameFormat, gvk.Group, gvk.Version, gvk.Kind)
	err := r.Client.Get(ctx, ktypes.NamespacedName{Name: cmName, Namespace: r.configMapNS}, cm)
	if err != nil {
		return nil, err
	}
	return cm, nil
}

func getResourceID(ns, name, uid string) string {
	return fmt.Sprintf(resourceIDFormat, ns, name, uid)
}
func getResourceIDFromUnstructured(obj unstructured.Unstructured) string {
	return getResourceID(obj.GetNamespace(), obj.GetName(), string(obj.GetUID()))
}
func setNested(obj map[string]interface{}, val any, keys ...string) {
	if obj == nil || len(keys) == 0 {
		return
	}

	if len(keys) == 1 {
		obj[keys[0]] = val
		return
	}

	obj[keys[0]] = make(map[string]interface{})
	nestedObj := obj[keys[0]].(map[string]interface{})
	setNested(nestedObj, val, keys[1:]...)
}

func getNested(obj map[string]interface{}, keys ...string) any {
	var t any
	if obj == nil || len(keys) == 0 {
		return t
	}

	if len(keys) == 1 {
		return obj[keys[0]]
	}

	switch v := obj[keys[0]].(type) {
	case map[string]interface{}:
		if len(keys) > 1 {
			return getNested(v, keys[1:]...)
		}
	}

	return t
}
