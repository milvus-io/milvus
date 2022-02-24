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

package storage

import (
	"container/list"
	"errors"
)

type LRU struct {
	evictList *list.List
	items     map[interface{}]*list.Element
	size      int
	onEvicted func(k, v interface{})
}

type entry struct {
	key   interface{}
	value interface{}
}

func NewLRU(size int, onEvicted func(k, v interface{})) (*LRU, error) {
	if size <= 0 {
		return nil, errors.New("cache size must be positive")
	}
	c := &LRU{
		size:      size,
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element),
		onEvicted: onEvicted,
	}
	return c, nil
}

func (c *LRU) Add(key, value interface{}) {
	if e, ok := c.items[key]; ok {
		c.evictList.MoveToFront(e)
		e.Value.(*entry).value = value
		return
	}
	e := &entry{key: key, value: value}
	listE := c.evictList.PushFront(e)
	c.items[key] = listE

	if c.evictList.Len() > c.size {
		c.RemoveOldest()
	}
}

func (c *LRU) Get(key interface{}) (value interface{}, ok bool) {
	if e, ok := c.items[key]; ok {
		c.evictList.MoveToFront(e)
		kv := e.Value.(*entry)
		return kv.value, true
	}
	return nil, false
}

func (c *LRU) Remove(key interface{}) {
	if e, ok := c.items[key]; ok {
		c.evictList.Remove(e)
		kv := e.Value.(*entry)
		delete(c.items, kv.key)
		if c.onEvicted != nil {
			c.onEvicted(kv.key, kv.value)
		}
	}
}

func (c *LRU) Contains(key interface{}) bool {
	_, ok := c.items[key]
	return ok
}

// Peek get value but not update the recently-used list.
func (c *LRU) Peek(key interface{}) (value interface{}, ok bool) {
	if e, ok := c.items[key]; ok {
		kv := e.Value.(*entry)
		return kv.value, true
	}
	return nil, false
}

func (c *LRU) Keys() []interface{} {
	keys := make([]interface{}, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*entry).key
		i++
	}
	return keys
}

func (c *LRU) Len() int {
	return c.evictList.Len()
}

func (c *LRU) Purge() {
	for k, v := range c.items {
		if c.onEvicted != nil {
			c.onEvicted(k, v.Value.(*entry).value)
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

func (c *LRU) Resize(size int) int {
	c.size = size
	if size >= c.evictList.Len() {
		return 0
	}
	diff := c.evictList.Len() - c.size
	for i := 0; i < diff; i++ {
		c.RemoveOldest()
	}
	return diff
}

func (c *LRU) GetOldest() (interface{}, interface{}, bool) {
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*entry)
		return kv.key, kv.value, true
	}
	return nil, nil, false
}

func (c *LRU) RemoveOldest() {
	e := c.evictList.Back()
	if e != nil {
		c.evictList.Remove(e)
		kv := e.Value.(*entry)
		delete(c.items, kv.key)
		if c.onEvicted != nil {
			c.onEvicted(kv.key, kv.value)
		}
	}
}
