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

package deletebuffer

import (
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
)

var errBufferFull = errors.New("buffer full")

type timed interface {
	Timestamp() uint64
	Size() int64
}

// DeleteBuffer is the interface for delete buffer.
type DeleteBuffer[T timed] interface {
	Put(T)
	ListAfter(uint64) []T
	SafeTs() uint64
}

func NewDoubleCacheDeleteBuffer[T timed](startTs uint64, maxSize int64) DeleteBuffer[T] {
	return &doubleCacheBuffer[T]{
		head:    newDoubleCacheItem[T](startTs, maxSize),
		maxSize: maxSize,
		ts:      startTs,
	}
}

// doubleCacheBuffer implements DeleteBuffer with fixed sized double cache.
type doubleCacheBuffer[T timed] struct {
	mut        sync.RWMutex
	head, tail *doubleCacheItem[T]
	maxSize    int64
	ts         uint64
}

func (c *doubleCacheBuffer[T]) SafeTs() uint64 {
	return c.ts
}

// Put implements DeleteBuffer.
func (c *doubleCacheBuffer[T]) Put(entry T) {
	c.mut.Lock()
	defer c.mut.Unlock()

	err := c.head.Put(entry)
	if errors.Is(err, errBufferFull) {
		c.evict(entry.Timestamp())
		c.head.Put(entry)
	}
}

// ListAfter implements DeleteBuffer.
func (c *doubleCacheBuffer[T]) ListAfter(ts uint64) []T {
	c.mut.RLock()
	defer c.mut.RUnlock()
	var result []T
	if c.tail != nil {
		result = append(result, c.tail.ListAfter(ts)...)
	}
	if c.head != nil {
		result = append(result, c.head.ListAfter(ts)...)
	}
	return result
}

// evict sets head as tail and evicts tail.
func (c *doubleCacheBuffer[T]) evict(newTs uint64) {
	c.tail = c.head
	c.head = newDoubleCacheItem[T](newTs, c.maxSize/2)
	c.ts = c.tail.headTs
}

func newDoubleCacheItem[T timed](ts uint64, maxSize int64) *doubleCacheItem[T] {
	return &doubleCacheItem[T]{
		headTs:  ts,
		maxSize: maxSize,
	}
}

type doubleCacheItem[T timed] struct {
	mut     sync.RWMutex
	headTs  uint64
	size    int64
	maxSize int64

	data []T
}

// Cache adds entry into cache item.
// returns error if item is full
func (c *doubleCacheItem[T]) Put(entry T) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.size+entry.Size() > c.maxSize {
		return errBufferFull
	}

	c.data = append(c.data, entry)
	c.size += entry.Size()
	return nil
}

// ListAfter returns entries of which ts after provided value.
func (c *doubleCacheItem[T]) ListAfter(ts uint64) []T {
	c.mut.RLock()
	defer c.mut.RUnlock()
	idx := sort.Search(len(c.data), func(idx int) bool {
		return c.data[idx].Timestamp() >= ts
	})
	// not found
	if idx == len(c.data) {
		return nil
	}
	return c.data[idx:]
}
