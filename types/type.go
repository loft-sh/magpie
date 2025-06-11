package types

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

const (
	_ eventType = iota
	Create
	InferredCreate
	Delete
	InferredDelete
)

type eventType int

type Event struct {
	Obj       map[string]interface{} `json:"obj,omitempty"`
	EventType eventType              `json:"event_type,omitempty"`
	UID       types.UID              `json:"uid,omitempty"`
	Time      time.Time              `json:"time,omitempty"`
}

type KeyedEvent struct {
	Event
	Key ResourceKey
}

type ResourceKey struct {
	types.NamespacedName
	types.UID
}

func (r ResourceKey) String() string {
	return fmt.Sprintf("%s_%s_%s", r.Namespace, r.Name, r.UID)
}
