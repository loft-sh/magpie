package recorder

import (
	"context"
	"fmt"
	"strings"

	"github.com/loft-sh/magpie/pkg/eventstores/idempotent"
	"github.com/loft-sh/magpie/types"
	errors2 "github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ reconcile.TypedReconciler[ctrl.Request]

type Target struct {
	GVK         schema.GroupVersionKind
	Fields      [][]string
	TrackCreate bool
	TrackDelete bool
}

type Reconciler struct {
	Client      client.Client
	configMapNS string
	target      Target
	store       types.EventStore
}

func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	obj := unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.target.GVK)
	err := r.Client.Get(ctx, client.ObjectKey{Name: request.Name, Namespace: request.Namespace}, &obj)
	if err != nil {
		if errors.IsNotFound(err) {
			err = r.SyncTargetDelete(ctx, r.store.GetResourceKeyFromUnstructured(obj))
			if err != nil {
				return reconcile.Result{}, errors2.Wrap(err, "failed to sync delete event")
			}
		}
		return reconcile.Result{}, err
	}

	err = r.SyncTargetCreate(ctx, obj)
	if err != nil {
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *Reconciler) SyncTargetCreate(ctx context.Context, obj unstructured.Unstructured) error {
	if !r.target.TrackCreate {
		return nil
	}

	err := r.store.Add(
		ctx,
		types.KeyedEvent{
			Key: types.ResourceKey{
				NamespacedName: ktypes.NamespacedName{
					Namespace: obj.GetNamespace(),
					Name:      obj.GetName()},
				UID:              obj.GetUID(),
				GroupVersionKind: r.target.GVK},
			Event: types.Event{Obj: r.createEventFromTarget(obj.Object), EventType: types.Create},
		})
	if err != nil {
		return fmt.Errorf("failed to add event to store: %w", err)
	}

	return nil
}

func (r *Reconciler) SyncTargetDelete(ctx context.Context, key types.ResourceKey) error {
	if !r.target.TrackDelete {
		return nil
	}

	err := r.store.Add(ctx, types.KeyedEvent{Key: key, Event: types.Event{EventType: types.Delete}})
	if err != nil {
		return errors2.Wrapf(err, "failed to add event to store")
	}

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
	for _, target := range targets {
		eventStore, err := idempotent.NewStore(cmNS, target.GVK, mgr.GetClient())
		if err != nil {
			return errors2.Wrap(err, "failed to create event store")
		}

		r := &Reconciler{
			Client:      mgr.GetClient(),
			target:      target,
			store:       eventStore,
			configMapNS: cmNS,
		}
		err = eventStore.InitializeForGVK(ctx, cmNS)
		if err != nil {
			return errors2.Wrapf(err, "failed to initialize event store for gvk [%s]", target.GVK.String())
		}

		err = ctrl.NewControllerManagedBy(mgr).
			// Uncomment the following line adding a pointer to an instance of the controlled resource as an argument
			For(&unstructured.Unstructured{Object: map[string]interface{}{"kind": target.GVK.Kind, "apiVersion": target.GVK.GroupVersion().String()}}).
			Named("cluster").
			Complete(r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) parseResourceKeyFromString(key string) (types.ResourceKey, error) {
	parts := strings.Split(key, "_")
	if len(parts) != 3 {
		return types.ResourceKey{}, fmt.Errorf("invalid resource key string [%s], should be of format \"Namespace-Name-UID\"", key)
	}
	return types.ResourceKey{NamespacedName: ktypes.NamespacedName{Namespace: parts[0], Name: parts[1]}, UID: ktypes.UID(parts[2]), GroupVersionKind: r.target.GVK}, nil
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
