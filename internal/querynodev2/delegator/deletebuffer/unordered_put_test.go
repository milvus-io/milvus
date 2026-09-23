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
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storage"
)

func itemAt(ts uint64) *Item {
	return &Item{Ts: ts, Data: []BufferItem{{
		PartitionID: 1,
		DeleteData: storage.DeleteData{
			Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(int64(ts))},
			Tss:      []uint64{ts},
			RowCount: 1,
		},
	}}}
}

func tsOf(items []*Item) []uint64 {
	return lo.Map(items, func(item *Item, _ int) uint64 { return item.Ts })
}

// Two split children forward their deletes to the source from different WALs,
// so the source's buffer receives them out of timestamp order. ListAfter binary
// searches every block, so an unsorted block answers wrongly: [1010, 990, 1020]
// asked for >= 1001 returns only 1020.
func TestListDeleteBufferKeepsOrderUnderUnorderedPut(t *testing.T) {
	buffer := NewListDeleteBuffer[*Item](100, 1<<20, []string{"1", "dml-1"})
	for _, ts := range []uint64{1010, 990, 1020} {
		buffer.Put(itemAt(ts))
	}
	assert.Equal(t, []uint64{1010, 1020}, tsOf(buffer.ListAfter(1001)))
	assert.Equal(t, []uint64{990, 1010, 1020}, tsOf(buffer.ListAfter(0)))
	rows, _ := buffer.Size()
	assert.EqualValues(t, 3, rows)
}

// An out-of-order item goes into the block its timestamp falls in, even a full
// one, so the list stays sorted across blocks and a block dropped by
// tryCleanDelete never holds an entry newer than the clean timestamp.
func TestListDeleteBufferInsertsLateItemIntoItsBlock(t *testing.T) {
	size := itemAt(1).Size()
	// two items per block.
	buffer := NewListDeleteBuffer[*Item](100, 2*size, []string{"1", "dml-1"})
	ldb := buffer.(*listDeleteBuffer[*Item])
	for _, ts := range []uint64{200, 210, 300, 310, 400} {
		buffer.Put(itemAt(ts))
	}
	assert.Len(t, ldb.list, 3)

	buffer.Put(itemAt(305)) // into the full second block
	buffer.Put(itemAt(205)) // into the full first block
	buffer.Put(itemAt(150)) // older than every item: the first block
	assert.Len(t, ldb.list, 3, "a late item must not open a new block")
	assert.Equal(t, []uint64{150, 200, 205, 210, 300, 305, 310, 400}, tsOf(buffer.ListAfter(100)))
	assert.Equal(t, []uint64{300, 305, 310, 400}, tsOf(buffer.ListAfter(211)))
	assert.LessOrEqual(t, ldb.list[0].headTs, uint64(150))
	buffer.Put(itemAt(50)) // older than the first block's head: it becomes the head
	assert.EqualValues(t, 50, ldb.list[0].headTs)

	// cleaning up to 350 drops the first block only: every item in it is older.
	buffer.TryDiscard(350)
	assert.Equal(t, []uint64{300, 305, 310, 400}, tsOf(buffer.ListAfter(0)))

	// an in-order item still appends.
	buffer.Put(itemAt(500))
	assert.Equal(t, []uint64{400, 500}, tsOf(buffer.ListAfter(400)))
}

func TestCacheBlockPutKeepsOrder(t *testing.T) {
	size := itemAt(1).Size()
	block := newCacheBlock[*Item](0, 2*size)
	assert.NoError(t, block.Put(itemAt(20)))
	assert.NoError(t, block.Put(itemAt(30)))
	assert.ErrorIs(t, block.Put(itemAt(40)), errBufferFull)
	// an older item is inserted, not refused, even when the block is full.
	assert.NoError(t, block.Put(itemAt(25)))
	assert.NoError(t, block.Put(itemAt(25)))
	assert.Equal(t, []uint64{25, 25, 30}, tsOf(block.ListAfter(21)))
	num, _ := block.Size()
	assert.EqualValues(t, 4, num)
	last, ok := block.lastTs()
	assert.True(t, ok)
	assert.EqualValues(t, 30, last)
	_, ok = newCacheBlock[*Item](0, size).lastTs()
	assert.False(t, ok)
}
