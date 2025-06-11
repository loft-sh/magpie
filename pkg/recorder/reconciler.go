package recorder

import (
	"context"
	"fmt"

	"github.com/loft-sh/magpie/pkg/client/filtered"
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

var (
	requiredFields = [][]string{
		{
			"metadata",
			"namespace",
		},
		{
			"metadata",
			"name",
		},
		{
			"metadata",
			"uid",
		},
	}
)

type Target struct {
	GVK         schema.GroupVersionKind
	Fields      [][]string
	TrackCreate bool
	TrackDelete bool
}

type Reconciler struct {
	Client         client.Client
	filteredReader filtered.Reader
	configMapNS    string
	target         Target
	store          types.EventStore
}

func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	obj, err := r.filteredReader.Get(ctx, client.ObjectKey{Name: request.Name, Namespace: request.Namespace})
	if err != nil {
		if errors.IsNotFound(err) {
			err = r.SyncTargetDelete(ctx, types.ResourceKey{NamespacedName: request.NamespacedName})
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
				UID: obj.GetUID(),
			},
			Event: types.Event{Obj: obj.Object, EventType: types.Create},
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

func SetupWithManagerForTargets(ctx context.Context, mgr ctrl.Manager, targets []Target, cmNS string) error {
	for _, target := range targets {
		filteredReader := filtered.NewReader(mgr.GetClient(), target.GVK, append(target.Fields, requiredFields...))
		eventStore, err := idempotent.NewStore(cmNS, target.GVK, mgr.GetClient(), filteredReader)
		if err != nil {
			return errors2.Wrap(err, "failed to create event store")
		}

		r := &Reconciler{
			Client:         mgr.GetClient(),
			filteredReader: filteredReader,
			target:         target,
			store:          eventStore,
			configMapNS:    cmNS,
		}

		list, err := r.filteredReader.List(ctx, &client.ListOptions{})
		if err != nil {
			return errors2.Wrap(err, fmt.Sprintf("failed to list e.gvk [%s]", target.GVK.String()))
		}

		err = eventStore.InitializeForGVK(ctx, cmNS, list)
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
