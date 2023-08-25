package EvoSwap

import (
	"context"
)

type CacheResp int

const (
	Fine CacheResp = iota
	SetWithEvict
	GetWithEvict
	GetNotExist
)

const Mib = 1024 * 1024
const (
	DefaultCapacity = 128 * Mib
)

type Cache[K comparable, V any] interface {
	Set(ctx context.Context, key K, val V, siz int64) CacheResp
	Get(ctx context.Context, key K) (V, CacheResp)
	Capacity() int64
	Used() int64
	Available() int64
	Flush() int64
}

type Config[K comparable, V any] struct {
	Lru2Conf *LRU2Config[K, V]
}

func NewCache[K comparable, V any](c *Config[K, V]) Cache[K, V] {
	if c.Lru2Conf != nil {
		return NewLRU2[K, V](c.Lru2Conf)
	}
	return nil
}
