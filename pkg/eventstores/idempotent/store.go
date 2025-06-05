package idempotent

import (
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/loft-sh/magpie/types"
	ktypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var _ types.EventStore[string] = (*Store)(nil)

const (
	defaultSize      = 100
	resourceIDFormat = "%s/%s/%s"
)

type Store struct {
	sync.RWMutex
	cache *lru.Cache[types.ResourceKey, []types.Event]
}

func NewStore() (*Store, error) {
	cache, err := lru.New[types.ResourceKey, []types.Event](defaultSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cache: %w", err)
	}
	return &Store{cache: cache}, nil
}

func (e *Store) List(key types.ResourceKey) []types.Event {
	e.RLock()
	defer e.RUnlock()

	v, _ := e.cache.Get(key)
	return v
}

func (e *Store) Add(key types.ResourceKey, event types.Event) bool {
	e.Lock()
	defer e.Unlock()

	event.Time = time.Now()
	v, _ := e.cache.Get(key)
	if v[len(v)-1].EventType == event.EventType {
		return false
	}
	v = append(v, event)
	evicted := e.cache.Add(key, v)
	if evicted {
		log.Log.Info(fmt.Sprintf("eviction occured to make room for gvk [%s] events", key))
	}
	return true
}

func getResourceID(id ktypes.NamespacedName, uid string) string {
	return fmt.Sprintf(resourceIDFormat, id.Namespace, id.Name, uid)
}
