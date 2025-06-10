package idempotent

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/loft-sh/magpie/types"
	errors2 "github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ types.EventStore = (*Store)(nil)

const (
	cmNameFormat = "magpie-%s-%s-%s"
)

type Store struct {
	sync.RWMutex
	client      client.Client
	configMapNS string
	gvk         schema.GroupVersionKind
}

func NewStore(ns string, gvk schema.GroupVersionKind, client client.Client) (*Store, error) {
	return &Store{configMapNS: ns, gvk: gvk, client: client}, nil
}

func (e *Store) List(ctx context.Context, key types.ResourceKey) ([]types.Event, error) {
	e.RLock()
	defer e.RUnlock()

	cm, err := e.getGVKConfigMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap: %w", err)
	}

	events, err := e.listEventsFromConfigMap(cm, key)
	if err != nil {
		return nil, fmt.Errorf("failed to list events: %w", err)
	}

	return events, nil
}

func (e *Store) Add(ctx context.Context, eventsToAdd ...types.KeyedEvent) error {
	if len(eventsToAdd) == 0 {
		return nil
	}

	e.Lock()
	defer e.Unlock()

	eventTimestamp := time.Now()

	cm, err := e.getGVKConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get configmap: %w", err)
	}

	updated := false
	for _, keyedEvent := range eventsToAdd {
		currentEvents, err := e.listEventsFromConfigMap(cm, keyedEvent.Key)
		if err != nil {
			return fmt.Errorf("failed to list events: %w", err)
		}
		if currentEvents == nil {
			currentEvents = make([]types.Event, 0, 1)
		}

		if len(currentEvents) > 0 && currentEvents[len(currentEvents)-1].EventType == keyedEvent.EventType {
			continue
		}

		updated = true
		keyedEvent.Time = eventTimestamp
		currentEvents = append(currentEvents, keyedEvent.Event)

		err = e.addEventsToConfigMap(cm, keyedEvent.Key, currentEvents)
		if err != nil {
			return fmt.Errorf("failed to add events to configmap: %w", err)
		}
	}

	if updated {
		err = e.client.Update(ctx, cm)
		if err != nil {
			return fmt.Errorf("failed to update configmap: %w", err)
		}
	}
	return nil
}

func (e *Store) InitializeForGVK(ctx context.Context, ns string) error {
	cm, err := e.getGVKConfigMap(ctx)
	if err != nil && !errors.IsNotFound(err) {
		return errors2.Wrap(err, fmt.Sprintf("failed to get magpie ConfigMap [%s]", e.getCMName()))
	}
	if errors.IsNotFound(err) {
		cm = &v1.ConfigMap{}
		cm.Namespace = ns
		cm.Name = e.getCMName()

		err = e.client.Create(ctx, cm)
		if err != nil {
			return errors2.Wrap(err, "failed to create ConfigMap")
		}
	}

	list := unstructured.UnstructuredList{Items: []unstructured.Unstructured{}}

	list.SetGroupVersionKind(e.gvk)
	err = e.client.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		return errors2.Wrap(err, fmt.Sprintf("failed to list e.gvk [%s]", e.gvk.String()))
	}

	currentGVKids := make([]string, len(list.Items))
	for index, item := range list.Items {
		currentGVKids[index] = e.GetResourceKeyFromUnstructured(item).String()
	}

	eventsToAdd := make([]types.KeyedEvent, 0)
	for keyString := range cm.Data {
		if !slices.Contains(currentGVKids, keyString) {
			key, err := e.parseResourceKeyFromString(keyString)
			if err != nil {
				return errors2.Wrapf(err, "failed to parse resource key [%s]", keyString)
			}

			eventsToAdd = append(eventsToAdd, types.KeyedEvent{Key: key, Event: types.Event{EventType: types.InferredDelete}})
		}
	}

	for index, keyString := range currentGVKids {
		updated := e.addObjToConfigMap(list.Items[index], cm)
		key, err := e.parseResourceKeyFromString(keyString)
		if err != nil {
			return errors2.Wrapf(err, "failed to parse resource key [%s]", keyString)
		}
		if updated {
			eventsToAdd = append(eventsToAdd, types.KeyedEvent{Key: key, Event: types.Event{Obj: list.Items[index].Object, EventType: types.InferredCreate}}) // TODO: make InferredCreate idempotent with create
		}
	}

	if len(eventsToAdd) > 0 {
		err = e.Add(ctx, eventsToAdd...)
		if err != nil {
			return errors2.Wrapf(err, "failed to add event to store")
		}
	}

	return nil
}

func (e *Store) GetResourceKeyFromUnstructured(obj unstructured.Unstructured) types.ResourceKey {
	return e.getResourceKey(obj.GetNamespace(), obj.GetName(), obj.GetUID())
}

func (e *Store) getResourceKey(ns, name string, uid ktypes.UID) types.ResourceKey {
	return types.ResourceKey{
		NamespacedName: ktypes.NamespacedName{
			Namespace: ns,
			Name:      name,
		},
		UID:              uid,
		GroupVersionKind: e.gvk,
	}
}

func (e *Store) addObjToConfigMap(obj unstructured.Unstructured, cm *v1.ConfigMap) bool {
	if _, ok := cm.Data[e.GetResourceKeyFromUnstructured(obj).String()]; ok {
		return false
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[e.GetResourceKeyFromUnstructured(obj).String()] = obj.GetCreationTimestamp().String()
	return true
}

func (e *Store) parseResourceKeyFromString(key string) (types.ResourceKey, error) {
	parts := strings.Split(key, "_")
	if len(parts) != 3 {
		return types.ResourceKey{}, fmt.Errorf("invalid resource key string [%s], should be of format \"Namespace-Name-UID\"", key)
	}
	return types.ResourceKey{NamespacedName: ktypes.NamespacedName{Namespace: parts[0], Name: parts[1]}, UID: ktypes.UID(parts[2]), GroupVersionKind: e.gvk}, nil
}

func (e *Store) getGVKConfigMap(ctx context.Context) (*v1.ConfigMap, error) {
	cm := &v1.ConfigMap{}
	err := e.client.Get(ctx, ktypes.NamespacedName{Name: e.getCMName(), Namespace: e.configMapNS}, cm)
	if err != nil {
		return nil, err
	}
	return cm, nil
}

func (e *Store) getCMName() string {
	cmName := strings.ToLower(fmt.Sprintf(cmNameFormat, e.gvk.Group, e.gvk.Version, e.gvk.Kind))
	if e.gvk.Group == "" {
		cmName = strings.ToLower(fmt.Sprintf("%s-%s", e.gvk.Kind, e.gvk.Version))
	}
	return strings.ToLower(cmName)
}

func (e *Store) listEventsFromConfigMap(cm *v1.ConfigMap, key types.ResourceKey) ([]types.Event, error) {
	if cm.Data == nil {
		return nil, nil
	}

	v, ok := cm.Data[key.String()]
	if !ok {
		return nil, nil
	}

	var events []types.Event
	if err := json.Unmarshal([]byte(v), &events); err != nil {
		return nil, fmt.Errorf("failed to unmarshal events: %w", err)
	}
	return events, nil
}

func (e *Store) addEventsToConfigMap(cm *v1.ConfigMap, key types.ResourceKey, events []types.Event) error {
	eventsJSON, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("failed to marshal events: %w", err)
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string, 1)
	}

	cm.Data[key.String()] = string(eventsJSON)
	return nil
}
