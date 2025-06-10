package types

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type EventStore interface {
	List(ctx context.Context, key ResourceKey) ([]Event, error)
	Add(ctx context.Context, event ...KeyedEvent) error
	GetResourceKeyFromUnstructured(obj unstructured.Unstructured) ResourceKey
}
