package cache_test

import (
	"testing"
	"time"

	"github.com/metacubex/sing/common/cache"
)

func TestEvictionCallbackCanReadCache(t *testing.T) {
	var c *cache.LruCache[int, int]
	callbackRead := make(chan struct{})
	c = cache.New(cache.WithEvict[int, int](func(int, int) {
		go func() {
			c.Exist(1)
			close(callbackRead)
		}()
		select {
		case <-callbackRead:
		case <-time.After(time.Second):
			t.Error("eviction callback ran while the cache was locked")
		}
	}))
	c.Store(1, 1)
	c.Delete(1)
	<-callbackRead
}

func TestDeleteFuncPreservesReplacement(t *testing.T) {
	oldValue, newValue := new(int), new(int)
	var evicted []*int
	c := cache.New(cache.WithEvict[int, *int](func(_ int, value *int) { evicted = append(evicted, value) }))
	c.Store(1, newValue)
	if c.DeleteFunc(1, func(value *int) bool { return value == oldValue }) {
		t.Fatal("stale owner removed a replacement")
	}
	if value, ok := c.Load(1); !ok || value != newValue || len(evicted) != 0 {
		t.Fatal("failed predicate changed the cache")
	}
	if !c.DeleteFunc(1, func(value *int) bool { return value == newValue }) || len(evicted) != 1 || evicted[0] != newValue {
		t.Fatal("matching owner did not evict its value exactly once")
	}
}

func TestEvictionCallbacksObserveCommittedCache(t *testing.T) {
	operations := map[string]func(*cache.LruCache[int, int]){
		"delete":      func(c *cache.LruCache[int, int]) { c.Delete(1) },
		"delete_if":   func(c *cache.LruCache[int, int]) { c.DeleteFunc(1, func(int) bool { return true }) },
		"clear":       func(c *cache.LruCache[int, int]) { c.Clear() },
		"capacity":    func(c *cache.LruCache[int, int]) { c.Store(2, 2) },
		"expiration":  func(c *cache.LruCache[int, int]) { c.StoreWithExpire(1, 1, time.Unix(1, 0)) },
		"health_load": func(c *cache.LruCache[int, int]) { c.Load(1) },
		"health_store": func(c *cache.LruCache[int, int]) {
			c.LoadOrStore(1, func() int { return 2 })
		},
		"health_store_age": func(c *cache.LruCache[int, int]) {
			c.LoadOrStoreWithAge(1, 60, func() int { return 2 })
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			var c *cache.LruCache[int, int]
			callbacks := 0
			c = cache.New(cache.WithSize[int, int](1), cache.WithAge[int, int](60),
				cache.WithHealthCheck[int, int](func(_ int, value int) bool { return value != 1 }),
				cache.WithEvict[int, int](func(key, value int) {
					callbacks++
					if key != 1 || value != 1 {
						t.Errorf("unexpected eviction %d=%d", key, value)
					}
					if current, ok := c.Load(key); ok && current == value {
						t.Error("callback observed its removed value")
					}
				}))
			c.Store(1, 1)
			done := make(chan struct{})
			go func() { operation(c); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("eviction callback could not reenter cache")
			}
			if callbacks != 1 {
				t.Fatalf("eviction callbacks = %d, want 1", callbacks)
			}
		})
	}
}
