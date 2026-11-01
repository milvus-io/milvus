// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"iter"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// VarSizeBlockBuffer is a reusable, non-thread-safe buffer with variable-sized blocks.
// Appending does not move existing elements. Block reuse reduces allocations, while variable block sizes balance batch writes and space utilization.
// With only Append, all blocks have size threshold, except that a maxCountHint in (0, threshold) makes the first block smaller.
// maxCountHint only hints at the initial block capacity; it does not limit appends.
// Reserve prepares space for upcoming appends, allocating larger blocks if needed.
// Reset clears the logical contents while retaining blocks for reuse.
type VarSizeBlockBuffer[T any] struct {
	blocks    [][]T // Allocated blocks retained across resets.
	idx       int   // Index of the current write block.
	offset    int   // Number of used elements in the current block.
	count     int64 // Number of elements currently stored in the buffer.
	threshold int64 // Default capacity of newly allocated blocks.
}

// NewVarSizeBlockBuffer creates a buffer with threshold as the default block capacity.
// If maxCountHint is provided, positive, and less than threshold, the first block is allocated with maxCountHint elements to avoid wasting space for small inputs.
// maxCountHint only guides the initial allocation; it does not limit appends.
// It returns an error if threshold is non-positive or more than one hint is provided.
func NewVarSizeBlockBuffer[T any](threshold int64, maxCountHint ...int64) (*VarSizeBlockBuffer[T], error) {
	if threshold <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("threshold must be positive")
	}
	if len(maxCountHint) > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("at most one maxCountHint is allowed")
	}
	if len(maxCountHint) == 1 && maxCountHint[0] < 0 {
		return nil, merr.WrapErrParameterInvalidMsg("maxCountHint must be non-negative")
	}

	b := &VarSizeBlockBuffer[T]{
		threshold: threshold,
		idx:       -1,
	}
	if len(maxCountHint) == 1 && maxCountHint[0] > 0 && maxCountHint[0] < threshold {
		b.blocks = [][]T{make([]T, maxCountHint[0])}
		b.idx = 0
	}
	return b, nil
}

// Reserve ensures that the next additional Appends do not allocate.
// It counts free space in the current block and retained blocks first; if that is not enough,
// it appends one block with capacity max(threshold, shortfall).
// It does not change Len or the write position. It returns an error if additional is negative.
func (b *VarSizeBlockBuffer[T]) Reserve(additional int64) error {
	if additional < 0 {
		return merr.WrapErrParameterInvalidMsg("additional must be non-negative")
	}
	remaining := int64(b.remainingCapacity())
	if remaining >= additional {
		return nil
	}
	b.grow(additional - remaining)
	return nil
}

// remainingCapacity returns the free capacity from the write position to the end of all retained blocks.
func (b *VarSizeBlockBuffer[T]) remainingCapacity() int {
	remaining := 0
	i := b.idx
	if i < 0 {
		i = 0
	}
	for ; i < len(b.blocks); i++ {
		remaining += len(b.blocks[i])
	}
	return remaining - b.offset
}

// Append appends one element, allocating a threshold-sized block if needed.
func (b *VarSizeBlockBuffer[T]) Append(val T) {
	if b.idx < 0 || b.offset >= len(b.blocks[b.idx]) {
		b.advanceBlock()
	}
	b.blocks[b.idx][b.offset] = val
	b.offset++
	b.count++
}

// advanceBlock moves the write position to the next block, allocating one if needed.
// The caller must ensure the current block is full or no current block exists.
func (b *VarSizeBlockBuffer[T]) advanceBlock() {
	if b.idx+1 >= len(b.blocks) { // no next block to reuse
		b.grow()
	}
	b.idx++
	b.offset = 0
}

// grow appends a new block with capacity max(threshold, count[0]).
// If count is omitted, the block capacity is threshold.
// It does not change the write position or element count.
func (b *VarSizeBlockBuffer[T]) grow(count ...int64) {
	s := b.threshold
	if len(count) > 0 && count[0] > s {
		s = count[0]
	}
	next := make([]T, s)
	b.blocks = append(b.blocks, next)
}

// Len returns the number of elements currently stored in the buffer.
func (b *VarSizeBlockBuffer[T]) Len() int64 {
	return b.count
}

// Reset resets Len and the write position while retaining allocated blocks for reuse.
func (b *VarSizeBlockBuffer[T]) Reset() {
	b.count = 0
	b.idx = -1
	b.offset = 0
}

// Blocks returns an iterator over the stored elements in append order.
// The buffer must not be modified during iteration.
// Yielded slices must not be retained across buffer modifications.
func (b *VarSizeBlockBuffer[T]) Blocks() iter.Seq[[]T] {
	return func(yield func([]T) bool) {
		if b.count == 0 {
			return
		}
		for i := 0; i <= b.idx; i++ {
			block := b.blocks[i]
			if i == b.idx {
				block = block[:b.offset]
			}
			if !yield(block) {
				return
			}
		}
	}
}
