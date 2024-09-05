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
	"sync"

	"github.com/cockroachdb/errors"
)

func NewListDeleteBuffer[T timed](startTs uint64, sizePerBlock int64) DeleteBuffer[T] {
	return &listDeleteBuffer[T]{
		safeTs:       startTs,
		sizePerBlock: sizePerBlock,
		list:         []*cacheBlock[T]{newCacheBlock[T](startTs, sizePerBlock)},
	}
}

// listDeleteBuffer implements DeleteBuffer with a list.
// head points to the earliest block.
// tail points to the latest block which shall be written into.
type listDeleteBuffer[T timed] struct {
	mut sync.RWMutex

	list []*cacheBlock[T]

	safeTs       uint64
	sizePerBlock int64
}

func (b *listDeleteBuffer[T]) Put(entry T) {
	b.mut.Lock()
	defer b.mut.Unlock()

	tail := b.list[len(b.list)-1]
	err := tail.Put(entry)
	if errors.Is(err, errBufferFull) {
		b.list = append(b.list, newCacheBlock[T](entry.Timestamp(), b.sizePerBlock, entry))
	}
}

func (b *listDeleteBuffer[T]) ListAfter(ts uint64) []T {
	b.mut.RLock()
	defer b.mut.RUnlock()

	var result []T
	for _, block := range b.list {
		result = append(result, block.ListAfter(ts)...)
	}
	return result
}

func (b *listDeleteBuffer[T]) SafeTs() uint64 {
	b.mut.RLock()
	defer b.mut.RUnlock()
	return b.safeTs
}

func (b *listDeleteBuffer[T]) TryDiscard(ts uint64) {
	b.mut.Lock()
	defer b.mut.Unlock()
	if len(b.list) == 1 {
		return
	}
	var nextHead int
	for idx := len(b.list) - 1; idx >= 0; idx-- {
		block := b.list[idx]
		if block.headTs <= ts {
			nextHead = idx
			break
		}
	}

	if nextHead > 0 {
		for idx := 0; idx < nextHead; idx++ {
			b.list[idx] = nil
		}
		b.list = b.list[nextHead:]
	}
}

func (b *listDeleteBuffer[T]) Size() (entryNum, memorySize int64) {
	b.mut.RLock()
	defer b.mut.RUnlock()

	for _, block := range b.list {
		blockNum, blockSize := block.Size()
		entryNum += blockNum
		memorySize += blockSize
	}
	return entryNum, memorySize
}
