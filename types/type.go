package types

import (
	"fmt"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"time"
)

const (
	Create eventType = iota
	InferredCreate
	Delete
	InferredDelete
)

type eventType int

type Event struct {
	Obj       map[string]interface{} `json:"obj,omitempty"`
	EventType eventType              `json:"event_type,omitempty"`
	Time      time.Time              `json:"time,omitempty"`
}

type KeyedEvent struct {
	Event
	Key ResourceKey
}

type ResourceKey struct {
	types.NamespacedName
	UID types.UID
	schema.GroupVersionKind
}

func (r ResourceKey) String() string {
	if r.Namespace == "" {
		return fmt.Sprintf("%s_%s", r.Name, r.UID)
	}
	return fmt.Sprintf("%s_%s_%s", r.Namespace, r.Name, r.UID)
}
