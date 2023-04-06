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

package typeutil

import (
	"fmt"
	"math/rand"
	"sync"

	"golang.org/x/exp/constraints"
)

const (
	defaultSkipListMaxLevel = 16
	defaultSkipListSkip     = 4
)

// SkipList generic skip list.
// with extra truncate and traverse-after  feature.
type SkipList[K constraints.Ordered, V any] struct {
	// mutex for read/write ops
	mut sync.RWMutex

	// head of skip list
	head *skipListNode[K, V]

	// maxLevel for skip list
	maxLevel int
	// skip rate
	skip int
	// current level, speed up lookup
	level int
	// current skip list length
	length int
}

// randomLevel generate rand level based on skip setup.
// each level has 1/skip probability to generate.
func (sl *SkipList[K, V]) randomLevel() int {
	var i int
	for i = 1; i < sl.maxLevel; i++ {
		if rand.Int()%sl.skip != 0 {
			break
		}
	}
	return i
}

func (sl *SkipList[K, V]) search(key K) (element *skipListNode[K, V], path []*skipListNode[K, V], found bool) {
	current := sl.head
	path = make([]*skipListNode[K, V], sl.maxLevel)
	// fill not used level with head
	for i := sl.level; i < sl.maxLevel; i++ {
		path[i] = sl.head
	}
	// begin search with current used level
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		path[i] = current
	}
	current = current.next[0]
	if current != nil && current.key == key {
		element = current
		found = true
	}
	return
}

// Get searches skip list with provided key.
func (sl *SkipList[K, V]) Get(key K) (value V, ok bool) {
	sl.mut.RLock()
	defer sl.mut.RUnlock()

	var ele *skipListNode[K, V]
	ele, _, ok = sl.search(key)
	if ok {
		value = ele.value
	}
	return
}

// Insert inserts an element into skip list.
func (sl *SkipList[K, V]) Upsert(key K, value V) {
	sl.mut.Lock()
	defer sl.mut.Unlock()

	element, path, found := sl.search(key)
	// same key exists, overwrite value
	if found {
		element.value = value
		return
	}

	level := sl.randomLevel()
	if level > sl.level {
		sl.level = level
	}

	// not found, create a new node
	element = &skipListNode[K, V]{
		key:   key,
		value: value,
		next:  make([]*skipListNode[K, V], level),
	}

	// update next for prevous node stored in path
	for i := 0; i < level; i++ {
		element.next[i] = path[i].next[i]
		path[i].next[i] = element
	}
	sl.length++
}

// Delete removes node with provided key.
// returns value if found.
func (sl *SkipList[K, V]) Delete(key K) (value V, ok bool) {
	sl.mut.Lock()
	defer sl.mut.Unlock()

	element, path, found := sl.search(key)
	// not found, return empty value.
	if !found {
		return
	}

	value = element.value
	ok = true

	// remove element from next list, only update node levels.
	for i := range element.next {
		path[i].next[i] = element.next[i]
	}

	// update level after delete
	sl.refreshLevel()
	return
}

// ListAfter returns values after provided key.
// include indicates value related to key is returned or not.
func (sl *SkipList[K, V]) ListAfter(key K, include bool) []V {
	sl.mut.RLock()
	defer sl.mut.RUnlock()

	element, path, ok := sl.search(key)

	// key does not exist, use path[0]
	if !ok {
		element = path[0]
	}

	var values []V
	// include target && target exists
	if include && ok {
		values = append(values, element.value)
	}

	for element.next[0] != nil {
		element = element.next[0]
		values = append(values, element.value)
	}
	return values
}

// TruncateBefore truncates elements before key.
func (sl *SkipList[K, V]) TruncateBefore(key K) {
	sl.mut.Lock()
	defer sl.mut.Unlock()

	_, path, _ := sl.search(key)

	// use path as head.next
	for i, node := range path {
		sl.head.next[i] = node.next[i]
	}

	// update level after truncate
	sl.refreshLevel()
}

func (sl *SkipList[K, V]) refreshLevel() {
	var i int
	for i = 0; i < sl.maxLevel; i++ {
		// level i empty
		if sl.head.next[i] == nil {
			break
		}
	}
	sl.level = i
}

// sanityCheck performs sanity check for skip list.
// USED FOR unit test.
func (sl *SkipList[K, V]) sanityCheck() bool {
	// ordered check for each level
	for i := 0; i < sl.maxLevel; i++ {
		// head does not contains data, skip it
		if sl.head.next[i] != nil {
			current := sl.head.next[i]
			for current.next[i] != nil {
				if current.key >= current.next[i].key {
					return false
				}
				current = current.next[i]
			}
		}
	}
	return true
}

// skipListNode is the SkipList node struct.
type skipListNode[K comparable, V any] struct {
	key   K
	value V
	next  []*skipListNode[K, V]
}

// skipListOpt skip list option struct.
type skipListOpt struct {
	maxLevel int
	skip     int
}

// SkipListOption setup function alias.
type SkipListOption func(opt *skipListOpt)

// WithMaxLevel returns SkipListOption to setup max level.
func WithMaxLevel(maxLevel int) SkipListOption {
	return func(opt *skipListOpt) {
		opt.maxLevel = maxLevel
	}
}

// WithSkip returns SkipListOption to setup skip.
func WithSkip(skip int) SkipListOption {
	return func(opt *skipListOpt) {
		opt.skip = skip
	}
}

// NewSkipList creates a new SkipList with provided option.
func NewSkipList[K constraints.Ordered, V any](opts ...SkipListOption) (*SkipList[K, V], error) {
	option := &skipListOpt{
		maxLevel: defaultSkipListMaxLevel,
		skip:     defaultSkipListSkip,
	}

	for _, opt := range opts {
		opt(option)
	}
	if option.maxLevel < 1 {
		return nil, fmt.Errorf("invalid maxlevel %d", option.maxLevel)
	}
	if option.skip < 1 {
		return nil, fmt.Errorf("invalid skip %d", option.skip)
	}

	return &SkipList[K, V]{
		maxLevel: option.maxLevel,
		skip:     option.skip,

		head: &skipListNode[K, V]{
			next: make([]*skipListNode[K, V], option.maxLevel),
		},
	}, nil
}
