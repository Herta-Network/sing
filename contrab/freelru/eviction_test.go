package freelru

import (
	"testing"
	"time"
)

func TestSynchronizedEvictionCallbacksCanReenter(t *testing.T) {
	constructors := map[string]func() Cache[int, int]{
		"synced": func() Cache[int, int] {
			c, e := NewSynced[int, int](8, func(k int) uint32 { return uint32(k) })
			if e != nil {
				t.Fatal(e)
			}
			return c
		},
		"sharded": func() Cache[int, int] {
			c, e := NewSharded[int, int](8, func(k int) uint32 { return uint32(k) })
			if e != nil {
				t.Fatal(e)
			}
			return c
		},
	}
	for name, newCache := range constructors {
		t.Run(name, func(t *testing.T) {
			for _, operation := range []string{"remove", "purge", "expired", "capacity"} {
				t.Run(operation, func(t *testing.T) {
					cache := newCache()
					var called int
					cache.SetOnEvict(func(int, int) { _ = cache.Len(); called++ })
					cache.Add(1, 1)
					done := make(chan struct{})
					go func() {
						defer close(done)
						switch operation {
						case "remove":
							cache.Remove(1)
						case "purge":
							cache.Purge()
						case "expired":
							cache.AddWithLifetime(2, 2, -time.Second)
							cache.PurgeExpired()
						case "capacity":
							for i := 2; i < 20; i++ {
								cache.Add(i, i)
							}
						}
					}()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Fatal("eviction callback deadlocked")
					}
					if called == 0 {
						t.Fatal("eviction was not exercised")
					}
				})
			}
		})
	}
}
