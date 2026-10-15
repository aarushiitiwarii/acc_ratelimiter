package main

import (
	"container/list"
	"sync"
)

// LRU is a bounded, sharded, concurrency-safe LRU cache keyed by string.
// Sharding keeps lock contention low at high request rates. onEvict (optional)
// is invoked OUTSIDE the shard lock, so it may take other locks safely.
type LRU[V any] struct {
	shards  []*lruShard[V]
	onEvict func(key string, v V)
}

type lruShard[V any] struct {
	mu  sync.Mutex
	ll  *list.List
	m   map[string]*list.Element
	cap int
}

type lruEntry[V any] struct {
	key string
	val V
}

func NewLRU[V any](capacity, shards int, onEvict func(string, V)) *LRU[V] {
	if shards < 1 {
		shards = 1
	}
	per := capacity / shards
	if per < 1 {
		per = 1
	}
	c := &LRU[V]{onEvict: onEvict}
	for i := 0; i < shards; i++ {
		c.shards = append(c.shards, &lruShard[V]{ll: list.New(), m: make(map[string]*list.Element), cap: per})
	}
	return c
}

func (c *LRU[V]) shard(key string) *lruShard[V] {
	h := uint32(2166136261) // FNV-1a
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return c.shards[h%uint32(len(c.shards))]
}

// Get returns the value and marks it most-recently-used.
func (c *LRU[V]) Get(key string) (V, bool) {
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[key]; ok {
		s.ll.MoveToFront(e)
		return e.Value.(*lruEntry[V]).val, true
	}
	var zero V
	return zero, false
}

// Put inserts or replaces a value, evicting the LRU entry if the shard is full.
func (c *LRU[V]) Put(key string, v V) {
	c.GetOrCreate(key, func() V { return v }, true)
}

// GetOrCreate returns the existing value or creates one with mk. If replace is
// true an existing value is overwritten.
func (c *LRU[V]) GetOrCreate(key string, mk func() V, replace bool) V {
	s := c.shard(key)
	s.mu.Lock()
	if e, ok := s.m[key]; ok {
		s.ll.MoveToFront(e)
		ent := e.Value.(*lruEntry[V])
		if replace {
			ent.val = mk()
		}
		v := ent.val
		s.mu.Unlock()
		return v
	}
	v := mk()
	s.m[key] = s.ll.PushFront(&lruEntry[V]{key: key, val: v})
	var evicted *lruEntry[V]
	if s.ll.Len() > s.cap {
		last := s.ll.Back()
		evicted = last.Value.(*lruEntry[V])
		s.ll.Remove(last)
		delete(s.m, evicted.key)
	}
	s.mu.Unlock()
	if evicted != nil {
		evictions.Add(1)
		if c.onEvict != nil {
			c.onEvict(evicted.key, evicted.val)
		}
	}
	return v
}

// Range visits every entry. The callback runs under the shard lock and must not
// call back into the LRU.
func (c *LRU[V]) Range(f func(key string, v V)) {
	for _, s := range c.shards {
		s.mu.Lock()
		for k, e := range s.m {
			f(k, e.Value.(*lruEntry[V]).val)
		}
		s.mu.Unlock()
	}
}

func (c *LRU[V]) Len() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		n += s.ll.Len()
		s.mu.Unlock()
	}
	return n
}
