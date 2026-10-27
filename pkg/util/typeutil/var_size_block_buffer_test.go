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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestNewVarSizeBlockBuffer(t *testing.T) {
	t.Run("without hint", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](65536)
		require.NoError(t, err)
		require.NotNil(t, buf)
		require.Nil(t, buf.blocks)
		require.Equal(t, -1, buf.idx)
		require.Equal(t, int64(65536), buf.threshold)
		require.Zero(t, buf.count)
		require.Zero(t, buf.offset)
	})

	t.Run("big positive hint", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](12, 20)
		require.NoError(t, err)
		require.NotNil(t, buf)
		require.Nil(t, buf.blocks)
		require.Equal(t, -1, buf.idx)
		require.Equal(t, int64(12), buf.threshold)
		require.Zero(t, buf.count)
		require.Zero(t, buf.offset)
	})

	t.Run("zero hint", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](12, 0)
		require.Nil(t, err)
		require.NotNil(t, buf)
		require.Nil(t, buf.blocks)
		require.Equal(t, -1, buf.idx)
		require.Equal(t, int64(12), buf.threshold)
		require.Zero(t, buf.count)
		require.Zero(t, buf.offset)
	})

	t.Run("hint = threshold", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](12, 12)
		require.Nil(t, err)
		require.NotNil(t, buf)
		require.Nil(t, buf.blocks)
		require.Equal(t, -1, buf.idx)
		require.Equal(t, int64(12), buf.threshold)
		require.Zero(t, buf.count)
		require.Zero(t, buf.offset)
	})

	t.Run("small positive hint", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](65536, 12)
		require.NoError(t, err)
		require.NotNil(t, buf)
		require.Len(t, buf.blocks, 1)
		require.Len(t, buf.blocks[0], 12)
		require.Equal(t, 0, buf.idx)
		require.Equal(t, int64(65536), buf.threshold)
		require.Zero(t, buf.count)
		require.Zero(t, buf.offset)
	})

	t.Run("invalid more than one hints", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](65536, 12, 12)
		require.Nil(t, buf)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "maxCountHint")
	})

	t.Run("invalid negative hint", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](65536, -12)
		require.Nil(t, buf)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "maxCountHint")
	})

	// invalid threshold
	for _, threshold := range []int64{0, -1} {
		t.Run(fmt.Sprintf("invalid threshold %d", threshold), func(t *testing.T) {
			buf, err := NewVarSizeBlockBuffer[uint64](threshold)
			require.Nil(t, buf)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, "threshold")
		})
	}
}

func TestAppend(t *testing.T) {
	for _, tc := range []struct {
		name       string
		hint       []int64
		values     []uint64
		wantBlocks [][]uint64
		wantIdx    int
		wantOffset int
	}{
		{
			name:       "first append",
			values:     []uint64{1},
			wantBlocks: [][]uint64{{1, 0, 0, 0}},
			wantIdx:    0, wantOffset: 1,
		},
		{
			name:       "append within current block",
			values:     []uint64{1, 2, 3},
			wantBlocks: [][]uint64{{1, 2, 3, 0}},
			wantIdx:    0, wantOffset: 3,
		},
		{
			name:       "fill current block without growing",
			values:     []uint64{1, 2, 3, 4},
			wantBlocks: [][]uint64{{1, 2, 3, 4}},
			wantIdx:    0, wantOffset: 4,
		},
		{
			name:       "append after current block is full",
			values:     []uint64{1, 2, 3, 4, 5},
			wantBlocks: [][]uint64{{1, 2, 3, 4}, {5, 0, 0, 0}},
			wantIdx:    1, wantOffset: 1,
		},
		{
			name:       "append across multiple blocks",
			values:     []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
			wantBlocks: [][]uint64{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 0, 0, 0}},
			wantIdx:    2, wantOffset: 1,
		},
		{
			name:       "append beyond small hint",
			hint:       []int64{2},
			values:     []uint64{1, 2, 3},
			wantBlocks: [][]uint64{{1, 2}, {3, 0, 0, 0}},
			wantIdx:    1, wantOffset: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := NewVarSizeBlockBuffer[uint64](4, tc.hint...)
			require.NoError(t, err)

			for i, val := range tc.values {
				buf.Append(val)
				require.Equal(t, int64(i+1), buf.count)
			}

			require.Equal(t, tc.wantBlocks, buf.blocks)
			require.Equal(t, tc.wantIdx, buf.idx)
			require.Equal(t, tc.wantOffset, buf.offset)
		})
	}

	t.Run("reuse existing next block", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		firstBlock := []uint64{1, 2, 3, 4}
		nextBlock := make([]uint64, 4)
		buf.blocks = [][]uint64{firstBlock, nextBlock}
		buf.idx = 0
		buf.offset = len(firstBlock)
		buf.count = int64(len(firstBlock))

		buf.Append(5)

		require.Len(t, buf.blocks, 2)
		require.Same(t, &firstBlock[0], &buf.blocks[0][0])
		require.Same(t, &nextBlock[0], &buf.blocks[1][0])
		require.Equal(t, []uint64{1, 2, 3, 4}, buf.blocks[0])
		require.Equal(t, []uint64{5, 0, 0, 0}, buf.blocks[1])
		require.Equal(t, 1, buf.idx)
		require.Equal(t, 1, buf.offset)
		require.Equal(t, int64(5), buf.count)
	})
}

func TestGrow(t *testing.T) {
	t.Run("grow without count", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		require.Nil(t, buf.blocks)

		buf.grow()

		require.Len(t, buf.blocks, 1)
		require.Len(t, buf.blocks[0], 4)
	})

	for _, tc := range []struct {
		name     string
		count    int64
		wantSize int
	}{
		{name: "zero count", count: 0, wantSize: 4},
		{name: "count below threshold", count: 2, wantSize: 4},
		{name: "count equals threshold", count: 4, wantSize: 4},
		{name: "count above threshold", count: 7, wantSize: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := NewVarSizeBlockBuffer[uint64](4)
			require.NoError(t, err)

			buf.grow(tc.count)

			require.Len(t, buf.blocks, 1)
			require.Len(t, buf.blocks[0], tc.wantSize)
			require.Equal(t, -1, buf.idx)
			require.Zero(t, buf.offset)
			require.Zero(t, buf.count)
		})
	}

	t.Run("preserve existing block and write position", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4, 2)
		require.NoError(t, err)
		firstBlock := buf.blocks[0]
		firstBlock[0] = 42
		buf.offset = 1
		buf.count = 1

		buf.grow(7)

		require.Len(t, buf.blocks, 2)
		require.Len(t, buf.blocks[0], 2)
		require.Same(t, &firstBlock[0], &buf.blocks[0][0])
		require.Equal(t, uint64(42), buf.blocks[0][0])
		require.Len(t, buf.blocks[1], 7)
		require.Equal(t, 0, buf.idx)
		require.Equal(t, 1, buf.offset)
		require.Equal(t, int64(1), buf.count)
	})
}

func TestAdvanceBlock(t *testing.T) {
	t.Run("init first block", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		require.Nil(t, buf.blocks)
		require.Equal(t, -1, buf.idx)

		buf.advanceBlock()

		require.Len(t, buf.blocks, 1)
		require.Len(t, buf.blocks[0], 4)
		require.Equal(t, 0, buf.idx)
		require.Equal(t, 0, buf.offset)
	})

	t.Run("reuse existing next block", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		firstBlock := []uint64{1, 2, 3, 4}
		nextBlock := make([]uint64, 4)
		buf.blocks = [][]uint64{firstBlock, nextBlock}
		buf.idx = 0
		buf.offset = len(firstBlock)
		buf.count = int64(len(firstBlock))

		buf.advanceBlock()

		require.Len(t, buf.blocks, 2)
		require.Equal(t, 1, buf.idx)
		require.Zero(t, buf.offset)
		require.Equal(t, int64(4), buf.count)
		require.Same(t, &nextBlock[0], &buf.blocks[buf.idx][0])
		require.Equal(t, []uint64{1, 2, 3, 4}, buf.blocks[0])
	})
}

func TestLen(t *testing.T) {
	buf, err := NewVarSizeBlockBuffer[uint64](4)
	require.NoError(t, err)
	require.Zero(t, buf.Len())

	buf.Append(1)
	buf.Append(2)
	require.Equal(t, int64(2), buf.Len())
}

func TestReset(t *testing.T) {
	t.Run("clear current contents", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		require.Zero(t, buf.Len())

		buf.Append(1)
		buf.Reset()

		require.Len(t, buf.blocks, 1)
		require.Len(t, buf.blocks[0], 4)
		require.Equal(t, int64(0), buf.count)
		require.Equal(t, -1, buf.idx)
		require.Equal(t, 0, buf.offset)
		require.Equal(t, int64(0), buf.Len())
	})

	t.Run("append after resetting empty buffer", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)

		buf.Reset()

		require.Empty(t, buf.blocks)
		require.Equal(t, -1, buf.idx)
		require.Zero(t, buf.Len())
		require.Zero(t, buf.offset)
		require.NotPanics(t, func() {
			buf.Append(42)
		})
		require.Equal(t, int64(1), buf.Len())
		require.Equal(t, [][]uint64{{42, 0, 0, 0}}, buf.blocks)
		require.Equal(t, 0, buf.idx)
		require.Equal(t, 1, buf.offset)
	})

	t.Run("reuse multiple blocks after reset", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		for _, val := range []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9} {
			buf.Append(val)
		}
		require.Len(t, buf.blocks, 3)
		oldBlocks := append([][]uint64(nil), buf.blocks...)

		buf.Reset()

		require.Zero(t, buf.Len())
		require.Equal(t, -1, buf.idx)
		require.Zero(t, buf.offset)
		require.Len(t, buf.blocks, 3)
		for _, val := range []uint64{11, 12, 13, 14, 15} {
			buf.Append(val)
		}

		require.Len(t, buf.blocks, 3)
		for i, block := range oldBlocks {
			require.Same(t, &block[0], &buf.blocks[i][0])
		}
		require.Equal(t, []uint64{11, 12, 13, 14}, buf.blocks[0])
		require.Equal(t, uint64(15), buf.blocks[1][0])
		require.Equal(t, int64(5), buf.Len())
		require.Equal(t, 1, buf.idx)
		require.Equal(t, 1, buf.offset)
	})

	t.Run("repeated reset", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		buf.Append(1)
		firstBlock := buf.blocks[0]

		buf.Reset()
		buf.Reset()

		require.Zero(t, buf.Len())
		require.Equal(t, -1, buf.idx)
		require.Zero(t, buf.offset)
		require.Len(t, buf.blocks, 1)
		require.Same(t, &firstBlock[0], &buf.blocks[0][0])

		buf.Append(2)

		require.Len(t, buf.blocks, 1)
		require.Equal(t, uint64(2), firstBlock[0])
		require.Equal(t, int64(1), buf.Len())
		require.Equal(t, 0, buf.idx)
		require.Equal(t, 1, buf.offset)
	})
}

func TestReserve(t *testing.T) {
	t.Run("invalid negative additional", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)

		err = buf.Reserve(-1)

		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "additional")
	})

	// All cases use threshold 4.
	for _, tc := range []struct {
		name          string
		hint          []int64
		appends       int
		reset         bool
		additional    int64
		wantBlockLens []int
		wantIdx       int
		wantOffset    int
	}{
		// remaining is enough: no allocation.
		{name: "zero on empty buffer", additional: 0, wantIdx: -1},
		{name: "zero on partial block", appends: 1, additional: 0, wantBlockLens: []int{4}, wantIdx: 0, wantOffset: 1},
		{name: "already enough", appends: 1, additional: 2, wantBlockLens: []int{4}, wantIdx: 0, wantOffset: 1},
		{name: "equal to remaining", appends: 1, additional: 3, wantBlockLens: []int{4}, wantIdx: 0, wantOffset: 1},
		{name: "small hint block is enough", hint: []int64{2}, additional: 2, wantBlockLens: []int{2}, wantIdx: 0},

		// no blocks: remaining is 0, so additional-remaining equals additional.
		{name: "empty buffer below threshold", additional: 2, wantBlockLens: []int{4}, wantIdx: -1},
		{name: "empty buffer equal to threshold", additional: 4, wantBlockLens: []int{4}, wantIdx: -1},
		{name: "empty buffer above threshold", additional: 10, wantBlockLens: []int{10}, wantIdx: -1},

		// partial current block: the shortfall, not additional, decides the new block size.
		{name: "partial block, shortfall below threshold", appends: 1, additional: 6, wantBlockLens: []int{4, 4}, wantIdx: 0, wantOffset: 1},
		{name: "partial block, shortfall equal to threshold", appends: 1, additional: 7, wantBlockLens: []int{4, 4}, wantIdx: 0, wantOffset: 1},
		{name: "partial block, shortfall above threshold", appends: 1, additional: 10, wantBlockLens: []int{4, 7}, wantIdx: 0, wantOffset: 1},

		// current block is full but idx is not -1.
		{name: "full current block", appends: 4, additional: 2, wantBlockLens: []int{4, 4}, wantIdx: 0, wantOffset: 4},

		// remaining comes from a small hint block.
		{name: "small hint block is not enough", hint: []int64{2}, additional: 5, wantBlockLens: []int{2, 4}, wantIdx: 0},

		// remaining comes from blocks retained across Reset.
		{name: "retained blocks are enough", appends: 5, reset: true, additional: 8, wantBlockLens: []int{4, 4}, wantIdx: -1},
		{name: "retained blocks, shortfall below threshold", appends: 5, reset: true, additional: 10, wantBlockLens: []int{4, 4, 4}, wantIdx: -1},
		{name: "retained blocks, shortfall above threshold", appends: 5, reset: true, additional: 15, wantBlockLens: []int{4, 4, 7}, wantIdx: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := NewVarSizeBlockBuffer[uint64](4, tc.hint...)
			require.NoError(t, err)
			for i := 0; i < tc.appends; i++ {
				buf.Append(uint64(i))
			}
			if tc.reset {
				buf.Reset()
			}
			wantLen := buf.Len()

			err = buf.Reserve(tc.additional)

			require.NoError(t, err)
			var blockLens []int
			for _, block := range buf.blocks {
				blockLens = append(blockLens, len(block))
			}
			require.Equal(t, tc.wantBlockLens, blockLens)
			require.Equal(t, tc.wantIdx, buf.idx)
			require.Equal(t, tc.wantOffset, buf.offset)
			require.Equal(t, wantLen, buf.Len())
		})
	}

	t.Run("repeated reserve does not allocate again", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)

		require.NoError(t, buf.Reserve(10))
		require.NoError(t, buf.Reserve(10))

		require.Len(t, buf.blocks, 1)
		require.Len(t, buf.blocks[0], 10)
	})

	t.Run("appends after reserve do not allocate", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		buf.Append(0)
		require.NoError(t, buf.Reserve(10))
		blocksAfterReserve := len(buf.blocks)

		for i := uint64(1); i <= 10; i++ {
			buf.Append(i)
		}

		require.Len(t, buf.blocks, blocksAfterReserve)
		var got []uint64
		for block := range buf.Blocks() {
			got = append(got, block...)
		}
		require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, got)
		require.Equal(t, int64(11), buf.Len())
	})
}

func TestRemainingCapacity(t *testing.T) {
	t.Run("empty buffer", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)

		require.Equal(t, 0, buf.remainingCapacity())
	})

	t.Run("after reset", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4, 2)
		require.NoError(t, err)
		for _, val := range []uint64{1, 2, 3} {
			buf.Append(val)
		}
		require.Len(t, buf.blocks, 2)

		buf.Reset()

		// Both retained blocks are fully available again: 2 + 4.
		require.Equal(t, 6, buf.remainingCapacity())
	})

	for _, tc := range []struct {
		name       string
		blockSizes []int
		idx        int
		offset     int
		want       int
	}{
		{name: "single unused block", blockSizes: []int{4}, idx: 0, offset: 0, want: 4},
		{name: "single partially used block", blockSizes: []int{4}, idx: 0, offset: 1, want: 3},
		{name: "single full block", blockSizes: []int{4}, idx: 0, offset: 4, want: 0},
		{name: "multiple blocks with remaining capacity", blockSizes: []int{2, 4, 6}, idx: 1, offset: 1, want: 9},
		{name: "current block full with next block available", blockSizes: []int{2, 4, 6}, idx: 1, offset: 4, want: 6},
		{name: "last block partially used", blockSizes: []int{2, 4, 6}, idx: 2, offset: 2, want: 4},
		{name: "all blocks full", blockSizes: []int{2, 4, 6}, idx: 2, offset: 6, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := NewVarSizeBlockBuffer[uint64](4)
			require.NoError(t, err)
			for i, size := range tc.blockSizes {
				buf.blocks = append(buf.blocks, make([]uint64, size))
				if i < tc.idx {
					buf.count += int64(size)
				}
			}
			buf.idx = tc.idx
			buf.offset = tc.offset
			buf.count += int64(tc.offset)

			require.Equal(t, tc.want, buf.remainingCapacity())
		})
	}
}

func TestBlocks(t *testing.T) {
	t.Run("single element", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		buf.Append(42)

		var blocks [][]uint64
		for block := range buf.Blocks() {
			blocks = append(blocks, block)
		}

		require.Equal(t, [][]uint64{{42}}, blocks)
	})

	for _, tc := range []struct {
		name   string
		hint   []int64
		values []uint64
		want   [][]uint64
	}{
		{name: "empty buffer"},
		{name: "empty preallocated buffer", hint: []int64{2}},
		{name: "full block", values: []uint64{1, 2, 3, 4}, want: [][]uint64{{1, 2, 3, 4}}},
		{name: "multiple blocks", values: []uint64{1, 2, 3, 4, 5}, want: [][]uint64{{1, 2, 3, 4}, {5}}},
		{name: "small first block", hint: []int64{2}, values: []uint64{1, 2, 3}, want: [][]uint64{{1, 2}, {3}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := NewVarSizeBlockBuffer[uint64](4, tc.hint...)
			require.NoError(t, err)
			for _, val := range tc.values {
				buf.Append(val)
			}

			var blocks [][]uint64
			for block := range buf.Blocks() {
				blocks = append(blocks, block)
			}

			require.Equal(t, tc.want, blocks)
		})
	}

	t.Run("exclude old contents after reset", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		for _, val := range []uint64{1, 2, 3, 4, 5} {
			buf.Append(val)
		}
		buf.Reset()

		for range buf.Blocks() {
			t.Fatal("reset buffer must not yield any blocks")
		}

		buf.Append(42)
		var blocks [][]uint64
		for block := range buf.Blocks() {
			blocks = append(blocks, block)
		}
		require.Equal(t, [][]uint64{{42}}, blocks)
	})

	t.Run("stop iteration early", func(t *testing.T) {
		buf, err := NewVarSizeBlockBuffer[uint64](4)
		require.NoError(t, err)
		for _, val := range []uint64{1, 2, 3, 4, 5} {
			buf.Append(val)
		}

		var blocks [][]uint64
		for block := range buf.Blocks() {
			blocks = append(blocks, block)
			break
		}

		require.Equal(t, [][]uint64{{1, 2, 3, 4}}, blocks)
	})
}
