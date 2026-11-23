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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Fold disjoint contributions from real output rows. Source 0 has an external
// and an inline value: inheriting it must not also charge its inline bytes.
// Source 1 is only partially retained and has no snapshot, but its isolated
// inline contribution to output 10 is known, even though another output got
// its external reference.
func TestReusedTextSizeLineage(t *testing.T) {
	const id = int64(100)
	field := &schemapb.FieldSchema{FieldID: id, DataType: schemapb.DataType_Text, Nullable: true}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	lob := compaction.NewLOBCompactionContext()
	lob.ComputeStrategies([]int64{id}, 0.3)
	source := &datapb.CompactionSegmentBinlogs{FieldLogicalSizes: &datapb.FieldLogicalSizes{
		Version: 1, RowCount: 2,
		Fields: map[int64]*datapb.FieldLogicalSize{id: {PayloadBytes: 70001, ValidityBits: 2}},
	}}
	l := newTextSizeLineage(schema, lob, []*datapb.CompactionSegmentBinlogs{source, {}})
	require.NotNil(t, l)
	l.sources[0].read, l.sources[1].read = 2, 3
	l.sources[0].present = map[int64]struct{}{id: {}}
	ext := make([]byte, 24)
	ext[0] = 1
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.Binary}}, nil))
	defer builder.Release()
	col := builder.Field(0).(*array.BinaryBuilder)
	col.AppendValues([][]byte{ext, {0, 'a'}, append([]byte{0}, []byte("猫")...), ext}, nil)
	r := storage.NewSimpleArrowRecord(builder.NewRecord(), map[int64]int{id: 0})
	defer r.Release()
	l.recordRange(10, 0, r, 0, 2)
	l.recordRange(10, 1, r, 2, 3)
	l.recordRange(11, 1, r, 3, 4)
	makeOutput := func(segment, rows int64) *datapb.CompactionSegment {
		return &datapb.CompactionSegment{SegmentID: segment, NumOfRows: rows, Stats: &datapb.Statistics{
			FieldLogicalSizes: &datapb.FieldLogicalSizes{Version: 1, RowCount: rows, Fields: map[int64]*datapb.FieldLogicalSize{101: {}}},
		}}
	}
	out, partial := makeOutput(10, 3), makeOutput(11, 1)
	l.finish([]*datapb.CompactionSegment{out, partial})
	require.Equal(t, &datapb.FieldLogicalSize{PayloadBytes: 70004, ValidityBits: 3}, out.Stats.FieldLogicalSizes.Fields[id])
	require.NotContains(t, partial.Stats.FieldLogicalSizes.Fields, id)
	require.Contains(t, partial.Stats.FieldLogicalSizes.Fields, int64(101))
	require.EqualValues(t, 70001, source.FieldLogicalSizes.Fields[id].PayloadBytes)
	// Metadata says two, but three physical rows were scanned. Two routed
	// rows are NOT a whole source, even if its stale snapshot says otherwise.
	l.sources[0].read = 3
	out = makeOutput(10, 3)
	l.finish([]*datapb.CompactionSegment{out})
	require.NotContains(t, out.Stats.FieldLogicalSizes.Fields, id)
	l.sources[0].read = 2
	l.sources[0].present = nil // this field was materialized, not copied
	l.finish([]*datapb.CompactionSegment{out})
	require.NotContains(t, out.Stats.FieldLogicalSizes.Fields, id)
}

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
		s := buildCompactionOutputStats(insertLogs, nil, nil, 0)
		assert.Equal(t, map[int64]int64{100: 4, 101: 0}, s.GetNullCounts())
	})
	t.Run("l0 delta-only output has no entries", func(t *testing.T) {
		deltalogs := []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{EntriesNum: 5, MemorySize: 32}}},
		}
		s := buildCompactionOutputStats(nil, deltalogs, nil, 0)
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

	got := buildMaterializationStatsDelta(columnGroups, memorySizes, nullCounts, nil, 512, nil)

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

	got := buildMaterializationStatsDelta(columnGroups, map[int64]int{}, map[int64]int64{104: 0}, nil, 0, nil)

	assert.EqualValues(t, 0, got.GetInsertBinlogSize())
	assert.EqualValues(t, 1, got.GetInsertBinlogCount())
	assert.Equal(t, map[int64]int64{104: 0}, got.GetNullCounts())
}

func TestBuildMaterializationStatsDeltaValidElementCounts(t *testing.T) {
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 102, Fields: []int64{102}},
		{GroupID: 103, Fields: []int64{103}},
	}
	got := buildMaterializationStatsDelta(columnGroups, map[int64]int{102: 10, 103: 10},
		map[int64]int64{102: 1, 103: 0}, map[int64]int64{103: 0}, 0, nil)
	// Only the VectorArray output (103) carries a count, zero included.
	assert.Equal(t, map[int64]int64{103: 0}, got.GetValidElementCounts())

	got = buildMaterializationStatsDelta(columnGroups, map[int64]int{}, map[int64]int64{}, nil, 0, nil)
	assert.Nil(t, got.GetValidElementCounts())
}
