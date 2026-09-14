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

package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	testMiB = int64(1) << 20
	testGiB = int64(1) << 30
)

func testResourceSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
			{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}}},
		},
	}
}

func TestTaskResource_Formulas(t *testing.T) {
	paramtable.Init()
	defaultCPU := Params.DataCoordCfg.TaskResourceDefaultCPU.GetAsInt64()
	minMem := Params.DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize()

	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 2 * testGiB}, indexTaskResource(testGiB, true))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, indexTaskResource(testGiB, false))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, statsTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, l0CompactionTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: minMem}, lightweightTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: minMem}, defaultTaskResource())

	// Mix compaction streams: its input, bounded by one output segment; an
	// unknown input is priced at the bound.
	maxSegment := Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * testMiB
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 300 * testMiB}, mixCompactionTaskResource(300*testMiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: maxSegment}, mixCompactionTaskResource(5*testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: maxSegment}, mixCompactionTaskResource(0))

	// Clustering: its input; the worker applies its buffer share.
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 10 * testGiB}, clusteringCompactionTaskResource(10*testGiB))

	// Analyze: raw vectors times the factor; the worker applies its train share.
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 2 * testGiB}, analyzeTaskResource(testGiB))

	// Import: one buffer per file times the factor; the worker applies its
	// allocator share.
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 3 * 2 * 100 * testMiB}, importTaskResource(3, 100*testMiB))
	// Pre-import: one buffer per file, nothing in flight, no allocator.
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 3 * 100 * testMiB}, preImportTaskResource(3, 100*testMiB))
	// A task with no files listed yet is still charged one buffer.
	assert.Equal(t, importTaskResource(1, 100*testMiB), importTaskResource(0, 100*testMiB))
	assert.Equal(t, preImportTaskResource(1, 100*testMiB), preImportTaskResource(0, 100*testMiB))

	// Nothing is ever priced below the floor: a 0-byte input still costs minTaskMemory.
	assert.Equal(t, minMem, indexTaskResource(0, true).Memory)
	assert.Equal(t, minMem, statsTaskResource(0).Memory)
	assert.Equal(t, minMem, l0CompactionTaskResource(0).Memory)
	assert.Equal(t, minMem, analyzeTaskResource(0).Memory)
	assert.Equal(t, minMem, clusteringCompactionTaskResource(0).Memory)
	assert.Equal(t, minMem, importTaskResource(0, 0).Memory)
	assert.Equal(t, minMem, preImportTaskResource(0, 0).Memory)
}

func TestImportFileBufferSize(t *testing.T) {
	paramtable.Init()
	base := Params.DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3}}}

	// Base buffer per (vchannel, partition) pair, exactly as the worker sizes it,
	// with no largest-file cap because the worker never applies one.
	assert.Equal(t, base*2*3, importFileBufferSize(job))

	// L0 import: the worker uses the base buffer regardless of shards.
	l0Job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3},
		Options: []*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}},
	}}
	assert.Equal(t, base, importFileBufferSize(l0Job))
}

func TestStatsTargetFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
			{FieldID: 102, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
				{Key: "max_length", Value: "64"}, {Key: "enable_match", Value: "true"}, {Key: "enable_analyzer", Value: "true"},
			}},
			{FieldID: 103, Name: "plain", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}}},
			{FieldID: 104, Name: "json", DataType: schemapb.DataType_JSON},
			{FieldID: 105, Name: "json2", DataType: schemapb.DataType_JSON},
			{FieldID: 106, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{102}, OutputFieldIds: []int64{106},
		}},
	}
	assert.Equal(t, []int64{102}, statsTargetFields(schema, indexpb.StatsSubJob_TextIndexJob))
	// Every JSON field gets json key stats, so both are read.
	assert.Equal(t, []int64{104, 105}, statsTargetFields(schema, indexpb.StatsSubJob_JsonKeyIndexJob))
	assert.Equal(t, []int64{106}, statsTargetFields(schema, indexpb.StatsSubJob_BM25Job))
	// A sort reads everything; a nil schema names nothing.
	assert.Nil(t, statsTargetFields(schema, indexpb.StatsSubJob_Sort))
	assert.Nil(t, statsTargetFields(nil, indexpb.StatsSubJob_TextIndexJob))
	// A sub job whose fields are not in this schema reads nothing specific.
	noMatch := &schemapb.CollectionSchema{Fields: schema.Fields[:2]}
	assert.Nil(t, statsTargetFields(noMatch, indexpb.StatsSubJob_TextIndexJob))

	// statsInputSize: the targeted fields' bytes when known, else the segment.
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 102, Binlogs: []*datapb.Binlog{{MemorySize: 7000}}},
			{FieldID: 104, Binlogs: []*datapb.Binlog{{MemorySize: 9000}}},
			{FieldID: 105, Binlogs: []*datapb.Binlog{{MemorySize: 1000}}},
		},
		Stats: &datapb.Statistics{InsertBinlogSize: 100000},
	}}
	assert.Equal(t, int64(7000), statsInputSize(segment, schema, indexpb.StatsSubJob_TextIndexJob))
	assert.Equal(t, int64(10000), statsInputSize(segment, schema, indexpb.StatsSubJob_JsonKeyIndexJob))
	assert.Equal(t, int64(100000), statsInputSize(segment, schema, indexpb.StatsSubJob_Sort))
	assert.Equal(t, int64(100000), statsInputSize(segment, nil, indexpb.StatsSubJob_TextIndexJob))
	assert.Equal(t, int64(100000), statsInputSize(segment, noMatch, indexpb.StatsSubJob_TextIndexJob))
}

func TestTaskResource_ConfigOverride(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(Params.DataCoordCfg.TaskResourceVectorIndexCPU.Key, "4")
	pt.Save(Params.DataCoordCfg.TaskResourceIndexMemoryFactor.Key, "3")
	defer pt.Reset(Params.DataCoordCfg.TaskResourceVectorIndexCPU.Key)
	defer pt.Reset(Params.DataCoordCfg.TaskResourceIndexMemoryFactor.Key)

	assert.Equal(t, taskcommon.Resource{CPU: 4, Memory: 3 * testGiB}, indexTaskResource(testGiB, true))
}

// TestTaskResource_ConfigFloors pins the "never zero" invariant against a
// configuration that asks for zero: CPU is floored at one whole core.
func TestTaskResource_ConfigFloors(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	for _, key := range []string{
		Params.DataCoordCfg.TaskResourceDefaultCPU.Key,
		Params.DataCoordCfg.TaskResourceVectorIndexCPU.Key,
		Params.DataCoordCfg.TaskResourceAnalyzeCPU.Key,
		Params.DataCoordCfg.TaskResourceClusteringCompactionCPU.Key,
	} {
		pt.Save(key, "0")
		defer pt.Reset(key)
	}

	assert.Equal(t, int64(1), defaultTaskResource().CPU)
	assert.Equal(t, int64(1), indexTaskResource(testGiB, true).CPU)
	assert.Equal(t, int64(1), analyzeTaskResource(testGiB).CPU)
	assert.Equal(t, int64(1), clusteringCompactionTaskResource(testGiB).CPU)
}

func TestEstimateSegmentSize(t *testing.T) {
	paramtable.Init()
	schema := testResourceSchema()

	// Stats present (every storage version persists it): use it verbatim.
	withStats := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: 1000, StorageVersion: 3,
		Stats: &datapb.Statistics{InsertBinlogSize: 700, StatsBinlogSize: 200, DeltaBinlogSize: 100},
	}}
	assert.Equal(t, int64(1000), estimateSegmentSize(withStats, schema))

	// V1 without Stats but with binlogs: EnsureStats rebuilds from the arrays.
	fromBinlogs := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 512000, EntriesNum: 1000}}}},
	}}
	assert.Equal(t, int64(512000), estimateSegmentSize(fromBinlogs, schema))

	// External-collection shape: no Stats, no binlogs, rows known -> rows x per-record estimate.
	external := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 1000, ManifestPath: "m"}}
	perRecord, err := typeutilEstimateSizePerRecord(schema)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000)*perRecord, estimateSegmentSize(external, schema))

	// Nothing to go on at all.
	assert.Equal(t, int64(0), estimateSegmentSize(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 4}}, schema))
	assert.Equal(t, int64(0), estimateSegmentSize(external, nil))
	assert.Equal(t, int64(0), estimateSegmentSize(nil, schema))
	assert.Equal(t, int64(0), estimateSegmentSize(&SegmentInfo{}, schema))

	// A schema the estimator rejects (varchar without max_length) is not guessed at.
	badSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar},
	}}
	assert.Equal(t, int64(0), estimateSegmentSize(external, badSchema))
}

func TestEstimateFieldSize(t *testing.T) {
	paramtable.Init()
	schema := testResourceSchema()

	// Binlog bytes for the field exist: use them.
	v1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 512000}}},
			{FieldID: 102, Binlogs: []*datapb.Binlog{{MemorySize: 64000}}},
		},
	}}
	assert.Equal(t, int64(512000), estimateFieldSize(v1, schema, 101))
	assert.Equal(t, int64(64000), estimateFieldSize(v1, schema, 102))

	// A struct-array parent binlog carries its children's bytes.
	withChildren := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 5, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 200, ChildFields: []int64{101}, Binlogs: []*datapb.Binlog{{MemorySize: 4096}}},
		},
	}}
	assert.Equal(t, int64(4096), estimateFieldSize(withChildren, schema, 101))

	// V3 after a DataCoord restart: Binlogs empty, Stats present. The vector
	// field and the pk are exact (rows x width); the varchar, the only
	// variable-width field, gets whatever the segment holds beyond them and the
	// two system fields -- here its true 40 bytes per row, not the 64 its
	// max_length would suggest.
	strBytes := fieldBytesPerRow(typeutil.GetFieldByID(schema, 102))
	assert.Greater(t, strBytes, int64(0))

	total := int64(1000) * (systemFieldsWidth + 8 + 128*4 + 40)
	v3 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, NumOfRows: 1000, StorageVersion: 3, ManifestPath: "m",
		Stats: &datapb.Statistics{InsertBinlogSize: total},
	}}
	assert.Equal(t, int64(1000*128*4), estimateFieldSize(v3, schema, 101))
	assert.Equal(t, int64(1000*8), estimateFieldSize(v3, schema, 100))
	assert.Equal(t, int64(1000*40), estimateFieldSize(v3, schema, 102))

	// External collection: no Stats either -> rows x per-field bytes.
	external := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 1000, ManifestPath: "m"}}
	assert.Equal(t, int64(1000*128*4), estimateFieldSize(external, schema, 101))
	assert.Equal(t, int64(1000)*strBytes, estimateFieldSize(external, schema, 102))

	// Unknown field / nil schema: fall back to the whole segment size (conservative).
	assert.Equal(t, total, estimateFieldSize(v3, schema, 999))
	assert.Equal(t, int64(0), estimateFieldSize(external, nil, 101))
	assert.Equal(t, int64(0), estimateFieldSize(nil, schema, 101))
	assert.Equal(t, int64(0), estimateFieldSize(&SegmentInfo{}, schema, 101))

	// A vector field whose dim cannot be resolved is neither fixed-width nor
	// estimable, so it falls back to the whole segment; with no size known at
	// all there is nothing to fall back to.
	dimless := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
	}}
	assert.Equal(t, int64(0), estimateFieldSize(external, dimless, 101))

	// A field the estimator rejects outright (varchar without max_length) has no
	// per-row size, so apportioning is abandoned for the whole-segment fallback.
	unsizable := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar},
	}}
	assert.Equal(t, int64(0), fieldBytesPerRow(unsizable.Fields[0]))
	assert.Equal(t, int64(0), estimateFieldSize(external, unsizable, 102))
}

func TestApportionFieldSize(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "16"}}},
			{FieldID: 102, Name: "short", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "40"}}},
			{FieldID: 103, Name: "long", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "160"}}},
			{FieldID: 104, Name: "flag", DataType: schemapb.DataType_Bool},
		},
	}
	field := func(id int64) *schemapb.FieldSchema { return typeutil.GetFieldByID(schema, id) }
	const rows = int64(1000)
	// Exact part: 16 system + 8 pk + 32 vec + 1 bool = 57 bytes per row.
	const exactPerRow = systemFieldsWidth + 8 + 16*2 + 1
	// The two varchars really hold 200 bytes per row between them.
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: rows, StorageVersion: 3,
		Stats: &datapb.Statistics{InsertBinlogSize: rows * (exactPerRow + 200)},
	}}

	// Fixed-width fields are exact whatever the residual.
	assert.Equal(t, rows*8, apportionFieldSize(segment, schema, field(100)))
	assert.Equal(t, rows*32, apportionFieldSize(segment, schema, field(101)))
	assert.Equal(t, rows*1, apportionFieldSize(segment, schema, field(104)))

	// The residual (200 bytes per row) is split by the schema's per-row
	// estimate of each variable-width field: 40 : 160. Together they account
	// for exactly the residual, never more.
	short := apportionFieldSize(segment, schema, field(102))
	long := apportionFieldSize(segment, schema, field(103))
	assert.Equal(t, rows*40, short)
	assert.Equal(t, rows*160, long)
	assert.Equal(t, rows*200, short+long)

	// A schema that already lists the system fields is not charged for them twice.
	withSystem := &schemapb.CollectionSchema{Fields: append([]*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "RowID", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
	}, schema.Fields...)}
	assert.Equal(t, rows*40, apportionFieldSize(segment, withSystem, field(102)))

	// Insert size smaller than the exact part (inconsistent stats): the
	// variable-width field falls back to rows x its per-row estimate.
	inconsistent := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, NumOfRows: rows, Stats: &datapb.Statistics{InsertBinlogSize: rows * 10},
	}}
	assert.Equal(t, rows*fieldBytesPerRow(field(102)), apportionFieldSize(inconsistent, schema, field(102)))
	// So does a segment with no insert size at all.
	unsized := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: rows}}
	assert.Equal(t, rows*fieldBytesPerRow(field(103)), apportionFieldSize(unsized, schema, field(103)))
	// Fixed-width fields stay exact without an insert size.
	assert.Equal(t, rows*32, apportionFieldSize(unsized, schema, field(101)))

	// No rows, nothing to apportion; an unsizable field (varchar without
	// max_length) has no share to claim.
	assert.Equal(t, int64(0), apportionFieldSize(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 4}}, schema, field(102)))
	assert.Equal(t, int64(0), apportionFieldSize(segment, schema, &schemapb.FieldSchema{FieldID: 105, DataType: schemapb.DataType_VarChar}))

	// A struct-array child field counts among the variable-width fields.
	nested := &schemapb.CollectionSchema{
		Fields: schema.Fields[:2],
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 200, Fields: []*schemapb.FieldSchema{
			{
				FieldID: 201, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "8"}, {Key: "max_capacity", Value: "4"}},
			},
		}}},
	}
	nestedSeg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 5, NumOfRows: rows, Stats: &datapb.Statistics{InsertBinlogSize: rows * (systemFieldsWidth + 8 + 32 + 20)},
	}}
	assert.Equal(t, rows*20, apportionFieldSize(nestedSeg, nested, nested.StructArrayFields[0].Fields[0]))
}

func TestFixedFieldWidth(t *testing.T) {
	dim := []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}
	for dt, want := range map[schemapb.DataType]int64{
		schemapb.DataType_Bool: 1, schemapb.DataType_Int8: 1, schemapb.DataType_Int16: 2,
		schemapb.DataType_Int32: 4, schemapb.DataType_Float: 4,
		schemapb.DataType_Int64: 8, schemapb.DataType_Double: 8, schemapb.DataType_Timestamptz: 8,
		schemapb.DataType_VarChar: 0, schemapb.DataType_JSON: 0, schemapb.DataType_Array: 0,
		schemapb.DataType_SparseFloatVector: 0, schemapb.DataType_ArrayOfVector: 0,
	} {
		assert.Equal(t, want, fixedFieldWidth(&schemapb.FieldSchema{DataType: dt, TypeParams: dim}), dt.String())
	}
	for dt, want := range map[schemapb.DataType]int64{
		schemapb.DataType_FloatVector: 32, schemapb.DataType_Float16Vector: 16, schemapb.DataType_BFloat16Vector: 16,
		schemapb.DataType_BinaryVector: 1, schemapb.DataType_Int8Vector: 8,
	} {
		assert.Equal(t, want, fixedFieldWidth(&schemapb.FieldSchema{DataType: dt, TypeParams: dim}), dt.String())
	}
	// A dense vector without a dim is not sizeable.
	assert.Equal(t, int64(0), fixedFieldWidth(&schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}))
	// vectorFieldBytes refuses sparse vectors: they have no dim to multiply.
	assert.Equal(t, int64(0), vectorFieldBytes(&schemapb.FieldSchema{DataType: schemapb.DataType_SparseFloatVector, TypeParams: dim}, 10))
}

func TestResourceCache(t *testing.T) {
	var c resourceCache
	calls := 0
	compute := func(ok bool) func() (taskcommon.Resource, bool) {
		return func() (taskcommon.Resource, bool) {
			calls++
			return taskcommon.Resource{CPU: int64(calls), Memory: 1}, ok
		}
	}
	// Not ok: value is returned but not cached, so the next call recomputes.
	assert.Equal(t, int64(1), c.get(compute(false)).CPU)
	assert.Equal(t, int64(2), c.get(compute(false)).CPU)
	// Ok: cached; subsequent calls do not recompute.
	assert.Equal(t, int64(3), c.get(compute(true)).CPU)
	assert.Equal(t, int64(3), c.get(compute(true)).CPU)
	assert.Equal(t, 3, calls)
}

func typeutilEstimateSizePerRecord(s *schemapb.CollectionSchema) (int64, error) {
	n, err := typeutil.EstimateSizePerRecord(s)
	return int64(n), err
}
