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
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
)

var errBufferFull = errors.New("buffer full")

type timed interface {
	Timestamp() uint64
	Size() int64
	EntryNum() int64
}

// DeleteBuffer is the interface for delete buffer.
type DeleteBuffer[T timed] interface {
	Put(T)
	ListAfter(uint64) []T
	SafeTs() uint64
	TryDiscard(uint64)
	// Size returns current size information of delete buffer: entryNum and memory
	Size() (entryNum, memorySize int64)

	// Register L0 segment
	RegisterL0(segments ...segments.Segment)
	// ListAll L0
	ListL0() []segments.Segment
	// Clean delete data, include l0 segment and delete buffer
	UnRegister(ts uint64)

	// clean up delete buffer
	Clear()
}

func NewDoubleCacheDeleteBuffer[T timed](startTs uint64, maxSize int64) DeleteBuffer[T] {
	return &doubleCacheBuffer[T]{
		head:       newCacheBlock[T](startTs, maxSize),
		maxSize:    maxSize,
		ts:         startTs,
		l0Segments: make([]segments.Segment, 0),
	}
}

// doubleCacheBuffer implements DeleteBuffer with fixed sized double cache.
type doubleCacheBuffer[T timed] struct {
	mut        sync.RWMutex
	head, tail *cacheBlock[T]
	maxSize    int64
	ts         uint64

	// maintain l0 segment list
	l0Segments []segments.Segment
}

func (c *doubleCacheBuffer[T]) RegisterL0(segmentList ...segments.Segment) {
	c.mut.Lock()
	defer c.mut.Unlock()

	// Filter out nil segments
	for _, seg := range segmentList {
		if seg != nil {
			c.l0Segments = append(c.l0Segments, seg)
		}
	}
}

func (c *doubleCacheBuffer[T]) ListL0() []segments.Segment {
	c.mut.RLock()
	defer c.mut.RUnlock()
	return c.l0Segments
}

func (c *doubleCacheBuffer[T]) UnRegister(ts uint64) {
	c.mut.Lock()
	defer c.mut.Unlock()
	var newSegments []segments.Segment

	for _, s := range c.l0Segments {
		if s.StartPosition().GetTimestamp() < ts {
			s.Release(context.TODO())
			continue
		}
		newSegments = append(newSegments, s)
	}
	c.l0Segments = newSegments
}

func (c *doubleCacheBuffer[T]) Clear() {
	c.mut.Lock()
	defer c.mut.Unlock()

	for _, s := range c.l0Segments {
		s.Release(context.TODO())
	}
	c.l0Segments = nil
	// reset cache block
	c.tail = c.head
	c.head = newCacheBlock[T](c.ts, c.maxSize)
}

func (c *doubleCacheBuffer[T]) SafeTs() uint64 {
	return c.ts
}

func (c *doubleCacheBuffer[T]) TryDiscard(_ uint64) {
}

// Put implements DeleteBuffer.
func (c *doubleCacheBuffer[T]) Put(entry T) {
	c.mut.Lock()
	defer c.mut.Unlock()

	err := c.head.Put(entry)
	if errors.Is(err, errBufferFull) {
		c.evict(entry.Timestamp(), entry)
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

func (c *doubleCacheBuffer[T]) Size() (entryNum int64, memorySize int64) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	if c.head != nil {
		blockNum, blockSize := c.head.Size()
		entryNum += blockNum
		memorySize += blockSize
	}

	if c.tail != nil {
		blockNum, blockSize := c.tail.Size()
		entryNum += blockNum
		memorySize += blockSize
	}

	return entryNum, memorySize
}

// evict sets head as tail and evicts tail.
func (c *doubleCacheBuffer[T]) evict(newTs uint64, entry T) {
	c.tail = c.head
	c.head = &cacheBlock[T]{
		headTs:   newTs,
		maxSize:  c.maxSize / 2,
		size:     entry.Size(),
		entryNum: entry.EntryNum(),
		data:     []T{entry},
	}
	c.ts = c.tail.headTs
}

func newCacheBlock[T timed](ts uint64, maxSize int64, elements ...T) *cacheBlock[T] {
	var entryNum, memorySize int64
	for _, element := range elements {
		entryNum += element.EntryNum()
		memorySize += element.Size()
	}
	return &cacheBlock[T]{
		headTs:   ts,
		maxSize:  maxSize,
		data:     elements,
		entryNum: entryNum,
		size:     memorySize,
	}
}

type cacheBlock[T timed] struct {
	mut      sync.RWMutex
	headTs   uint64
	entryNum int64
	size     int64
	maxSize  int64

	data []T
}

// Cache adds entry into cache item.
// returns error if item is full
func (c *cacheBlock[T]) Put(entry T) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.size+entry.Size() > c.maxSize {
		return errBufferFull
	}

	c.data = append(c.data, entry)
	c.size += entry.Size()
	c.entryNum += entry.EntryNum()
	return nil
}

// ListAfter returns entries of which ts after provided value.
func (c *cacheBlock[T]) ListAfter(ts uint64) []T {
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

func (c *cacheBlock[T]) Size() (entryNum, memorySize int64) {
	return c.entryNum, c.size
}
