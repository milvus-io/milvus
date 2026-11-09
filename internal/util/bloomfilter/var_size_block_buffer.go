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

package bloomfilter

import (
	"iter"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// varSizeBlockBuffer is an append-only, non-thread-safe buffer with variable-sized blocks.
// Blocks are retained for the lifetime of the buffer; resetting for reuse is not currently exposed.
type varSizeBlockBuffer[T any] struct {
	blocks    [][]T
	idx       int   // Index of the current write block.
	offset    int   // Number of used elements in the current block.
	count     int64 // Number of elements currently stored in the buffer.
	threshold int64 // Default capacity of newly allocated blocks.
}

// newVarSizeBlockBuffer creates a buffer with threshold as the default block capacity.
// If maxCountHint is positive and less than threshold, the first block is allocated with maxCountHint elements to avoid wasting space for small inputs.
// maxCountHint only guides the initial allocation; it does not limit appends. Zero disables the hint.
// It returns an error if threshold is non-positive or maxCountHint is negative.
func newVarSizeBlockBuffer[T any](threshold int64, maxCountHint int64) (*varSizeBlockBuffer[T], error) {
	if threshold <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("threshold must be positive")
	}
	if maxCountHint < 0 {
		return nil, merr.WrapErrParameterInvalidMsg("maxCountHint must be non-negative")
	}

	b := &varSizeBlockBuffer[T]{
		threshold: threshold,
		idx:       -1,
	}
	if maxCountHint > 0 && maxCountHint < threshold {
		b.blocks = [][]T{make([]T, maxCountHint)}
		b.idx = 0
	}
	return b, nil
}

// Reserve ensures that the next additional Appends do not allocate.
// It counts free space in the current block and reserved blocks first; if that is not enough,
// it appends one block with capacity max(threshold, shortfall).
// It does not change Len or the write position. It returns an error if additional is negative.
func (b *varSizeBlockBuffer[T]) Reserve(additional int64) error {
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

// remainingCapacity returns the free capacity from the write position to the end of all reserved blocks.
func (b *varSizeBlockBuffer[T]) remainingCapacity() int {
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
func (b *varSizeBlockBuffer[T]) Append(val T) {
	if b.idx < 0 || b.offset >= len(b.blocks[b.idx]) {
		b.advanceBlock()
	}
	b.blocks[b.idx][b.offset] = val
	b.offset++
	b.count++
}

// advanceBlock moves the write position to the next block, allocating one if needed.
// The caller must ensure the current block is full or no current block exists.
func (b *varSizeBlockBuffer[T]) advanceBlock() {
	if b.idx+1 >= len(b.blocks) { // no reserved next block
		b.grow(0)
	}
	b.idx++
	b.offset = 0
}

// grow appends a new block with capacity max(threshold, count).
// If count is zero, the block capacity is threshold.
// It does not change the write position or element count.
func (b *varSizeBlockBuffer[T]) grow(count int64) {
	s := b.threshold
	if count > s {
		s = count
	}
	next := make([]T, s)
	b.blocks = append(b.blocks, next)
}

// Len returns the number of elements currently stored in the buffer.
func (b *varSizeBlockBuffer[T]) Len() int64 {
	return b.count
}

// Blocks returns an iterator over the stored elements in append order.
// The buffer must not be modified during iteration.
// Yielded slices must not be retained across buffer modifications.
func (b *varSizeBlockBuffer[T]) Blocks() iter.Seq[[]T] {
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
