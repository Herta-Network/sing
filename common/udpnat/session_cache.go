package udpnat

import (
	"runtime"

	"github.com/metacubex/sing/common/cache"
	"github.com/metacubex/sing/contrab/maphash"
)

const maxSessionShards = 256

type sessionCache[K comparable] struct {
	shards []*cache.LruCache[K, *conn]
	hasher maphash.Hasher[K]
	mask   uint32
}

func newSessionCache[K comparable](maxAge int64, maxSessions int) *sessionCache[K] {
	if maxSessions < 1 {
		maxSessions = 1
	}
	shardCount := sessionShardCount(maxSessions)
	shards := make([]*cache.LruCache[K, *conn], shardCount)
	baseSize := maxSessions / shardCount
	remainder := maxSessions % shardCount
	for index := range shards {
		shardSize := baseSize
		if index < remainder {
			shardSize++
		}
		shards[index] = cache.New(
			cache.WithAge[K, *conn](maxAge),
			cache.WithSize[K, *conn](shardSize),
			cache.WithUpdateAgeOnGet[K, *conn](),
			cache.WithEvict[K, *conn](func(_ K, conn *conn) {
				conn.Close()
			}),
		)
	}
	return &sessionCache[K]{
		shards: shards,
		hasher: maphash.NewHasher[K](),
		mask:   uint32(shardCount - 1),
	}
}

func sessionShardCount(maxSessions int) int {
	if maxSessions <= 1 {
		return 1
	}
	target := runtime.GOMAXPROCS(0) * 4
	if target > maxSessionShards {
		target = maxSessionShards
	}
	shardCount := 1
	for shardCount < target {
		shardCount <<= 1
	}
	for shardCount > maxSessions {
		shardCount >>= 1
	}
	return shardCount
}

func (c *sessionCache[K]) shard(key K) *cache.LruCache[K, *conn] {
	return c.shards[c.hasher.Hash32(key)&c.mask]
}

func (c *sessionCache[K]) LoadOrStore(key K, constructor func() *conn) (*conn, bool) {
	return c.shard(key).LoadOrStore(key, constructor)
}

func (c *sessionCache[K]) Delete(key K, expected *conn) bool {
	return c.shard(key).DeleteFunc(key, func(value *conn) bool {
		return value == expected
	})
}

func (c *sessionCache[K]) Clear() {
	for _, shard := range c.shards {
		shard.Clear()
	}
}

func (c *sessionCache[K]) Len() int {
	var length int
	for _, shard := range c.shards {
		length += shard.Len()
	}
	return length
}
