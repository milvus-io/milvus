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

package compactor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Compaction writers stamp FieldNullCounts (zero included, one entry per
// present field) on the insert binlogs they return;
// buildCompactionOutputStats must surface them on Statistics.NullCounts so
// DataCoord's verbatim copy satisfies the NullCounts presence contract.
func TestBuildCompactionOutputStats_NullCounts(t *testing.T) {
	t.Run("packed writer shape", func(t *testing.T) {
		insertLogs := []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{100, 101}, Binlogs: []*datapb.Binlog{
				{EntriesNum: 10, MemorySize: 64, FieldNullCounts: map[int64]int64{100: 3, 101: 0}},
				{EntriesNum: 10, MemorySize: 64, FieldNullCounts: map[int64]int64{100: 1, 101: 0}},
			}},
		}
		s := buildCompactionOutputStats(insertLogs, nil, 0)
		assert.Equal(t, map[int64]int64{100: 4, 101: 0}, s.GetNullCounts())
	})
	t.Run("l0 delta-only output has no entries", func(t *testing.T) {
		deltalogs := []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{EntriesNum: 5, MemorySize: 32}}},
		}
		s := buildCompactionOutputStats(nil, deltalogs, 0)
		assert.Nil(t, s.GetNullCounts())
	})
}

func TestBuildMaterializationStatsDelta(t *testing.T) {
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 102, Fields: []int64{102}},
		{GroupID: 103, Fields: []int64{103}},
	}
	memorySizes := map[int64]int{102: 4096, 103: 2048}
	nullCounts := map[int64]int64{102: 0, 103: 7}

	got := buildMaterializationStatsDelta(columnGroups, memorySizes, nullCounts, 512)

	assert.EqualValues(t, 6144, got.GetInsertBinlogSize())
	assert.EqualValues(t, 2, got.GetInsertBinlogCount())
	assert.EqualValues(t, 512, got.GetStatsBinlogSize())
	assert.Equal(t, map[int64]int64{102: 0, 103: 7}, got.GetNullCounts())

	// Fields materialization does not change must be absent from the increment,
	// so the receiver's accumulation leaves the segment's values untouched.
	assert.EqualValues(t, 0, got.GetDeltaBinlogSize())
	assert.EqualValues(t, 0, got.GetDeltaBinlogCount())
	assert.EqualValues(t, 0, got.GetDeleteNumRows())
	assert.EqualValues(t, 0, got.GetTimestampFrom())
	assert.EqualValues(t, 0, got.GetTimestampTo())
	assert.EqualValues(t, 0, got.GetDeltaTimestampFrom())
	assert.EqualValues(t, 0, got.GetDeltaTimestampTo())
	assert.Empty(t, got.GetTimestampQuantiles())
}

// TestBuildMaterializationStatsDeltaMissingMemorySize pins that a column group
// with no recorded memory size contributes zero bytes but still counts as a
// binlog and still gets a null_counts entry — the presence contract in
// storage.BuildStatsFromFieldBinlogs requires an entry for every field
// physically present in the segment.
func TestBuildMaterializationStatsDeltaMissingMemorySize(t *testing.T) {
	columnGroups := []storagecommon.ColumnGroup{{GroupID: 104, Fields: []int64{104}}}

	got := buildMaterializationStatsDelta(columnGroups, map[int64]int{}, map[int64]int64{104: 0}, 0)

	assert.EqualValues(t, 0, got.GetInsertBinlogSize())
	assert.EqualValues(t, 1, got.GetInsertBinlogCount())
	assert.Equal(t, map[int64]int64{104: 0}, got.GetNullCounts())
}
