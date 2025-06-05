package types

type EventStore[key any] interface {
	List(ResourceKey) []Event
	Add(ResourceKey, Event) bool
}
