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
	ctx         context.Context
	cancel      context.CancelFunc
	evictList   *list.List
	items       map[interface{}]*list.Element
	size        uint64
	capacity    uint64
	storeHelper *StoreHelper
	m           sync.RWMutex
	evictedCh   chan *Entry
	stats       *Stats
}

type Stats struct {
	hitCount     float32
	evictedCount float32
	readCount    float32
	writeCount   float32
	memoryUsage  uint64
}

func (s *Stats) String() string {
	var hitRatio float32
	var evictedRatio float32
	if s.readCount != 0 {
		hitRatio = s.hitCount / s.readCount
		evictedRatio = s.evictedCount / s.writeCount
	}

	return fmt.Sprintf("lru cache hit ratio = %f, evictedRatio = %f. memoryUsage = %d", hitRatio, evictedRatio, s.memoryUsage)
}

type StoreHelper struct {
	Store       func(Key, Value) *Entry
	Load        func(entry *Entry) (Value, bool)
	MeasureSize func(Value) int
	OnEvicted   func(k Key, v Value)
}

func defaultStoreHelper() *StoreHelper {
	return &StoreHelper{
		Store: func(key Key, value Value) *Entry {
			return &Entry{Key: key, Value: value}
		},
		Load: func(entry *Entry) (Value, bool) {
			return entry.Value, true
		},
		MeasureSize: func(value Value) int {
			return len(value.([]byte))
		},
	}
}

type Option func(lru *LRU)

func SetStoreHelper(helper *StoreHelper) Option {
	return func(lru *LRU) {
		lru.storeHelper = helper
	}
}

func NewLRU(capacity uint64, opts ...Option) (*LRU, error) {
	if capacity == 0 {
		return nil, errors.New("cache Size must be positive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &LRU{
		ctx:         ctx,
		cancel:      cancel,
		capacity:    capacity,
		evictList:   list.New(),
		items:       make(map[interface{}]*list.Element),
		evictedCh:   make(chan *Entry, 16),
		stats:       &Stats{},
		storeHelper: defaultStoreHelper(),
	}
	for _, opt := range opts {
		opt(c)
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
				if c.storeHelper.OnEvicted != nil {
					c.storeHelper.OnEvicted(e.Key, e.Value)
				}
			}
		}
	}
}

func (c *LRU) Add(key Key, value Value) {
	c.m.Lock()
	defer c.m.Unlock()
	c.stats.writeCount++
	if e, ok := c.items[key]; ok {
		c.evictList.MoveToFront(e)
		e.Value.(*Entry).Value = value
		return
	}
	e := c.storeHelper.Store(key, value)
	e.Size = c.storeHelper.MeasureSize(value)
	listE := c.evictList.PushFront(e)
	c.items[key] = listE
	c.size += uint64(e.Size)

	for c.size > c.capacity {
		c.stats.evictedCount++
		oldestE := c.evictList.Back()
		if oldestE != nil {
			c.evictList.Remove(oldestE)
			kv := oldestE.Value.(*Entry)
			c.size -= uint64(kv.Size)
			delete(c.items, kv.Key)
			if c.storeHelper.OnEvicted != nil {
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
		kv := e.Value.(*Entry)
		return c.storeHelper.Load(kv)
	}
	return nil, false
}

func (c *LRU) Remove(key Key) {
	c.m.Lock()
	defer c.m.Unlock()
	if e, ok := c.items[key]; ok {
		c.evictList.Remove(e)
		kv := e.Value.(*Entry)
		delete(c.items, kv.Key)
		c.size -= uint64(kv.Size)
		if c.storeHelper.OnEvicted != nil {
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
		keys[i] = ent.Value.(*Entry).Key
		i++
	}
	return keys
}

func (c *LRU) Size() uint64 {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.size
}

func (c *LRU) Capacity() uint64 {
	return c.capacity
}

func (c *LRU) Purge() {
	c.m.Lock()
	defer c.m.Unlock()
	for k, v := range c.items {
		if c.storeHelper.OnEvicted != nil {
			c.storeHelper.OnEvicted(v.Value.(*Entry).Key, v.Value.(*Entry).Value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
	c.size = 0
}

func (c *LRU) Resize(capacity uint64) uint64 {
	c.m.Lock()
	defer c.m.Unlock()
	c.capacity = capacity
	if capacity >= c.size {
		return 0
	}
	diff := c.size - capacity
	for c.size > capacity {
		oldestE := c.evictList.Back()
		if oldestE != nil {
			c.evictList.Remove(oldestE)
			kv := oldestE.Value.(*Entry)
			delete(c.items, kv.Key)
			c.size -= uint64(kv.Size)
			if c.storeHelper.OnEvicted != nil {
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
		kv := ent.Value.(*Entry)
		return kv.Key, kv.Value, true
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
			c.storeHelper.OnEvicted(e.Key, e.Value)
		}
	}
	close(c.evictedCh)
}

func (c *LRU) Stats() *Stats {
	c.stats.memoryUsage = c.size
	return c.stats
}
