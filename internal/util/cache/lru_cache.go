// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
)

type LRU struct {
	ctx       context.Context
	cancel    context.CancelFunc
	evictList *list.List
	items     map[interface{}]*list.Element
	capacity  int
	onEvicted func(k Key, v Value)
	m         sync.RWMutex
	evictedCh chan *entry
	stats     *Stats
}

type Stats struct {
	hitCount     float32
	evictedCount float32
	readCount    float32
	writeCount   float32
}

func (s *Stats) String() string {
	var hitRatio float32
	var evictedRatio float32
	if s.readCount != 0 {
		hitRatio = s.hitCount / s.readCount
		evictedRatio = s.evictedCount / s.writeCount
	}

	return fmt.Sprintf("lru cache hit ratio = %f, evictedRatio = %f", hitRatio, evictedRatio)
}

type Key interface {
}
type Value interface {
}

type entry struct {
	key   Key
	value Value
}

func NewLRU(capacity int, onEvicted func(k Key, v Value)) (*LRU, error) {
	if capacity <= 0 {
		return nil, errors.New("cache size must be positive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &LRU{
		ctx:       ctx,
		cancel:    cancel,
		capacity:  capacity,
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element),
		onEvicted: onEvicted,
		evictedCh: make(chan *entry, 16),
		stats:     &Stats{},
	}
	go c.evictedWorker()
	return c, nil
}

func (c *LRU) evictedWorker() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case e, ok := <-c.evictedCh:
			if ok {
				if c.onEvicted != nil {
					c.onEvicted(e.key, e.value)
				}
			}
		}
	}
}

func (c *LRU) Add(key, value Value) {
	c.m.Lock()
	defer c.m.Unlock()
	c.stats.writeCount++
	if e, ok := c.items[key]; ok {
		c.evictList.MoveToFront(e)
		e.Value.(*entry).value = value
		return
	}
	e := &entry{key: key, value: value}
	listE := c.evictList.PushFront(e)
	c.items[key] = listE

	if c.evictList.Len() > c.capacity {
		c.stats.evictedCount++
		oldestE := c.evictList.Back()
		if oldestE != nil {
			c.evictList.Remove(oldestE)
			kv := oldestE.Value.(*entry)
			delete(c.items, kv.key)
			if c.onEvicted != nil {
				c.evictedCh <- kv
			}
		}
	}
}

func (c *LRU) Get(key Key) (value Value, ok bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	c.stats.readCount++
	if e, ok := c.items[key]; ok {
		c.stats.hitCount++
		c.evictList.MoveToFront(e)
		kv := e.Value.(*entry)
		return kv.value, true
	}
	return nil, false
}

func (c *LRU) Remove(key Key) {
	c.m.Lock()
	defer c.m.Unlock()
	if e, ok := c.items[key]; ok {
		c.evictList.Remove(e)
		kv := e.Value.(*entry)
		delete(c.items, kv.key)
		if c.onEvicted != nil {
			c.evictedCh <- kv
		}
	}
}

func (c *LRU) Contains(key Key) bool {
	c.m.RLock()
	defer c.m.RUnlock()
	_, ok := c.items[key]
	return ok
}

func (c *LRU) Keys() []Key {
	c.m.RLock()
	defer c.m.RUnlock()
	keys := make([]Key, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*entry).key
		i++
	}
	return keys
}

func (c *LRU) Len() int {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.evictList.Len()
}

func (c *LRU) Capacity() int {
	return c.capacity
}

func (c *LRU) Purge() {
	c.m.Lock()
	defer c.m.Unlock()
	for k, v := range c.items {
		if c.onEvicted != nil {
			c.evictedCh <- v.Value.(*entry)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

func (c *LRU) Resize(capacity int) int {
	c.m.Lock()
	defer c.m.Unlock()
	c.capacity = capacity
	if capacity >= c.evictList.Len() {
		return 0
	}
	diff := c.evictList.Len() - c.capacity
	for i := 0; i < diff; i++ {
		oldestE := c.evictList.Back()
		if oldestE != nil {
			c.evictList.Remove(oldestE)
			kv := oldestE.Value.(*entry)
			delete(c.items, kv.key)
			if c.onEvicted != nil {
				c.evictedCh <- kv
			}
		}
	}
	return diff
}

func (c *LRU) GetOldest() (Key, Value, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*entry)
		return kv.key, kv.value, true
	}
	return nil, nil, false
}

func (c *LRU) Close() {
	c.Purge()
	c.cancel()
	remain := len(c.evictedCh)
	for i := 0; i < remain; i++ {
		e, ok := <-c.evictedCh
		if ok {
			c.onEvicted(e.key, e.value)
		}
	}
	close(c.evictedCh)
}

func (c *LRU) Stats() *Stats {
	return c.stats
}
