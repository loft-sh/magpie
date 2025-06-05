package types

import (
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
	Obj       map[string]interface{}
	EventType eventType
	Time      time.Time
}

type ResourceKey struct {
	types.NamespacedName
	schema.GroupVersionKind
}
