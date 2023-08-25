package EvoSwap

import (
	"container/list"
	"context"
	"sync"
)

type FromWhat int

const (
	FromGet FromWhat = iota
	FromSet
)

type IdxVal struct {
	lis  *list.List
	node *list.Element
}

type LRU2Config[K comparable, V any] struct {
	Capacity  int64
	PostSet   func(key K, val V)
	PostGet   func(key K, val V)
	PostEvict func(key K, val V)
}

type LRU2[K comparable, V any] struct {
	a, b struct {
		siz, cap int64
		lis      list.List
	}

	sync.RWMutex
	idx map[K]*IdxVal

	postSet   func(key K, value V)
	postGet   func(key K, value V)
	postEvict func(key K, value V)
}

func (l *LRU2[K, V]) moveToAList(s *IdxVal, item *Item[K, V]) {
	s.lis = &l.a.lis
	l.b.lis.Remove(s.node)
	s.node = l.a.lis.PushFront(item)
	l.a.siz += item.siz
	l.b.siz -= item.siz
}

func (l *LRU2[K, V]) Set(ctx context.Context, key K, val V, siz int64) CacheResp {
	l.Lock()
	defer l.Unlock()

	var item *Item[K, V]
	if s, ok := l.idx[key]; ok {
		// exist already, reset its val and siz
		item = s.node.Value.(*Item[K, V])
		// do something before dropping the old val
		if l.postEvict != nil {
			l.postEvict(item.key, item.val)
		}

		item.val = val
		item.siz = siz

		if s.lis != &l.a.lis {
			// visit again, move it to the front of a.lis
			l.moveToAList(s, item)
		} else {
			// move to list front
			s.lis.MoveToFront(s.node)
		}
	} else {
		// not exist before, create one, and put it to the front of b.lis
		item = &Item[K, V]{
			key: key,
			val: val,
			siz: siz,
		}

		l.idx[key] = &IdxVal{
			lis:  &l.b.lis,
			node: l.b.lis.PushFront(item),
		}
		l.b.siz += siz
	}

	if l.postSet != nil {
		l.postSet(key, val)
	}

	return l.tryEvict(FromSet)
}

func (l *LRU2[K, V]) Get(ctx context.Context, key K) (V, CacheResp) {
	l.Lock()
	defer l.Unlock()

	var item *Item[K, V]
	if s, ok := l.idx[key]; ok {
		// exist
		item = s.node.Value.(*Item[K, V])
		if l.postGet != nil {
			l.postGet(key, item.val)
		}

		if s.lis != &l.a.lis {
			// visit again, move it to the front of a.lis
			l.moveToAList(s, item)
			return item.val, l.tryEvict(FromGet)
		} else {
			// move to list front
			s.lis.MoveToFront(s.node)
			return item.val, Fine
		}
	}

	var nullVal V
	return nullVal, GetNotExist
}

func (l *LRU2[K, V]) tryEvict(what FromWhat) CacheResp {
	evict := false
	if l.a.siz > l.a.cap {
		// take the tail of a.lis and move it to the head of b.lis
		evict = true

		item := l.a.lis.Remove(l.a.lis.Back()).(*Item[K, V])
		l.a.siz -= item.siz

		l.idx[item.key].node = l.b.lis.PushFront(item)
		l.idx[item.key].lis = &l.b.lis

		l.b.siz += item.siz
	}

	for l.b.siz > l.b.cap {
		// there could be more than one item need to be evicted
		evict = true
		item := l.b.lis.Remove(l.b.lis.Back()).(*Item[K, V])
		l.b.siz -= item.siz
		delete(l.idx, item.key)
		if l.postEvict != nil {
			l.postEvict(item.key, item.val)
		}
	}

	if evict {
		if what == FromSet {
			return GetWithEvict
		}
		return SetWithEvict
	}
	return Fine
}

type Item[K comparable, V any] struct {
	key K
	val V
	vis int
	siz int64
}

func fillConfig[K comparable, V any](c *LRU2Config[K, V]) {
	if c == nil {
		c = new(LRU2Config[K, V])
	}
	if c.Capacity == 0 {
		c.Capacity = DefaultCapacity
	}

}

func NewLRU2[K comparable, V any](c *LRU2Config[K, V]) *LRU2[K, V] {

	fillConfig[K, V](c)
	lru := &LRU2[K, V]{}
	lru.a.cap = c.Capacity * 1 / 3
	lru.b.cap = c.Capacity - lru.a.cap
	lru.idx = make(map[K]*IdxVal)

	lru.postSet = c.PostSet
	lru.postGet = c.PostGet
	lru.postEvict = c.PostEvict

	return lru
}

func (l *LRU2[K, V]) Capacity() int64 {
	l.Lock()
	defer l.Unlock()

	return l.a.cap + l.b.cap
}

func (l *LRU2[K, V]) Used() int64 {
	l.Lock()
	defer l.Unlock()

	return l.a.siz + l.b.siz
}

func (l *LRU2[K, V]) Available() int64 {
	l.Lock()
	defer l.Unlock()

	return l.a.cap + l.b.cap - (l.a.siz + l.b.siz)
}

func (l *LRU2[K, V]) Flush() int64 {
	// TODO
	return 0
}
