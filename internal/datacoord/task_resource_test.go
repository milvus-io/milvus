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

// taskPrice and taskPriceResolved unpack the two-value GetTaskResource in
// assertions.
func taskPrice(res taskcommon.Resource, _ bool) taskcommon.Resource { return res }

func taskPriceResolved(_ taskcommon.Resource, ok bool) bool { return ok }

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

	// Clustering: its input, bounded by nothing else.
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 10 * testGiB}, clusteringCompactionTaskResource(10*testGiB))

	// Analyze: raw vectors times the factor.
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 2 * testGiB}, analyzeTaskResource(testGiB))

	// Import: what its read buffers hold, times the factor.
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * 300 * testMiB}, importTaskResource(300*testMiB))
	// Pre-import: the same buffers, nothing in flight.
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 300 * testMiB}, preImportTaskResource(300*testMiB))

	// Nothing is ever priced below the floor: a 0-byte input still costs minTaskMemory.
	assert.Equal(t, minMem, indexTaskResource(0, true).Memory)
	assert.Equal(t, minMem, statsTaskResource(0).Memory)
	assert.Equal(t, minMem, l0CompactionTaskResource(0).Memory)
	assert.Equal(t, minMem, analyzeTaskResource(0).Memory)
	assert.Equal(t, minMem, clusteringCompactionTaskResource(0).Memory)
	assert.Equal(t, minMem, importTaskResource(0).Memory)
	assert.Equal(t, minMem, preImportTaskResource(0).Memory)
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

// TestImportBufferedBytes: a buffer never fills beyond the file it reads, so a
// task of many small files holds their bytes, not one whole buffer each.
func TestImportBufferedBytes(t *testing.T) {
	paramtable.Init()
	const buffer = 16 * testMiB

	// No files listed yet: one buffer.
	assert.Equal(t, buffer, importBufferedBytes(nil, buffer))

	// 100 files of 1MiB each: 100MiB, not 100 buffers.
	small := make([]*datapb.ImportFileStats, 0, 100)
	for i := 0; i < 100; i++ {
		small = append(small, &datapb.ImportFileStats{TotalMemorySize: testMiB})
	}
	assert.Equal(t, 100*testMiB, importBufferedBytes(small, buffer))

	// Files larger than the buffer are charged one buffer each; a file whose
	// size is unknown is charged a whole buffer too.
	big := []*datapb.ImportFileStats{{TotalMemorySize: buffer * 10}, {TotalMemorySize: 0}, {TotalMemorySize: testMiB}}
	assert.Equal(t, 2*buffer+testMiB, importBufferedBytes(big, buffer))
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
	schema := testResourceSchema() // pk int64 100, vec float32 dim 128 101, str varchar(64) 102
	const rows = int64(1000)
	varCharBound := rows * (64 + 4)

	// storage v1: one binlog per field, bounded by the schema.
	v1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: rows,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 512000}}},
			{FieldID: 102, Binlogs: []*datapb.Binlog{{MemorySize: 64000}}},
		},
	}}
	assert.Equal(t, int64(512000), estimateFieldSize(v1, schema, 101))
	assert.Equal(t, int64(64000), estimateFieldSize(v1, schema, 102))

	// storage v2/v3 with binlogs: the pk and the varchar share a 5MB short column
	// group. Each is the smaller of its schema size and the group, not the group.
	v2 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 6, NumOfRows: rows, StorageVersion: 2,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{100, 102}, Binlogs: []*datapb.Binlog{{MemorySize: 5 * testMiB}}},
			{FieldID: 101, ChildFields: []int64{101}, Binlogs: []*datapb.Binlog{{MemorySize: 4096}}},
		},
	}}
	assert.Equal(t, rows*8, estimateFieldSize(v2, schema, 100))
	assert.Equal(t, varCharBound, estimateFieldSize(v2, schema, 102))
	// A group smaller than the schema size bounds the field.
	assert.Equal(t, int64(4096), estimateFieldSize(v2, schema, 101))

	// V3 after a DataCoord restart: no binlogs, Stats present. Fixed-width
	// fields are exact; the varchar is bounded by what the segment holds beyond
	// the fixed-width and system fields, 40 bytes a row here, below max_length.
	total := rows * (taskcommon.SystemFieldsBytesPerRow + 8 + 128*4 + 40)
	v3 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, NumOfRows: rows, StorageVersion: 3, ManifestPath: "m",
		Stats: &datapb.Statistics{InsertBinlogSize: total},
	}}
	assert.Equal(t, rows*128*4, estimateFieldSize(v3, schema, 101))
	assert.Equal(t, rows*8, estimateFieldSize(v3, schema, 100))
	assert.Equal(t, rows*40, estimateFieldSize(v3, schema, 102))

	// External collection: no Stats either, so the schema bound alone.
	external := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: rows, ManifestPath: "m"}}
	assert.Equal(t, rows*128*4, estimateFieldSize(external, schema, 101))
	assert.Equal(t, varCharBound, estimateFieldSize(external, schema, 102))

	// Unknown field: its group, else the whole segment. Nil schema or segment.
	assert.Equal(t, 5*testMiB, estimateFieldSize(v2, &schemapb.CollectionSchema{}, 100))
	assert.Equal(t, total, estimateFieldSize(v3, schema, 999))
	assert.Equal(t, int64(0), estimateFieldSize(external, nil, 101))
	assert.Equal(t, int64(0), estimateFieldSize(nil, schema, 101))
	assert.Equal(t, int64(0), estimateFieldSize(&SegmentInfo{}, schema, 101))

	// No bound and no container: the per-row estimate, else the segment size.
	jsonSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 103, Name: "meta", DataType: schemapb.DataType_JSON},
	}}
	jsonPerRow := fieldBytesPerRow(jsonSchema.Fields[0])
	assert.Greater(t, jsonPerRow, int64(0))
	assert.Equal(t, rows*jsonPerRow, estimateFieldSize(external, jsonSchema, 103))
	dimless := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
	}}
	assert.Equal(t, int64(0), estimateFieldSize(external, dimless, 101))
	unsizable := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar},
	}}
	assert.Equal(t, int64(0), fieldBytesPerRow(unsizable.Fields[0]))
	assert.Equal(t, int64(0), estimateFieldSize(external, unsizable, 102))

	// vectorFieldBytes refuses sparse vectors: they have no dim to multiply.
	dim := []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}
	assert.Equal(t, int64(0), vectorFieldBytes(&schemapb.FieldSchema{DataType: schemapb.DataType_SparseFloatVector, TypeParams: dim}, 10))
	assert.Equal(t, int64(320), vectorFieldBytes(&schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector, TypeParams: dim}, 10))
}

func TestVariableWidthResidual(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "16"}}},
			{FieldID: 102, Name: "short", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "40"}}},
			{FieldID: 103, Name: "meta", DataType: schemapb.DataType_JSON},
			{FieldID: 104, Name: "flag", DataType: schemapb.DataType_Bool},
		},
	}
	const rows = int64(1000)
	// Fixed part: 16 system + 8 pk + 32 vec + 1 bool = 57 bytes a row; the
	// varchar and the json hold 300 bytes a row between them.
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: rows, StorageVersion: 3,
		Stats: &datapb.Statistics{InsertBinlogSize: rows * (taskcommon.SystemFieldsBytesPerRow + 8 + 32 + 1 + 300)},
	}}
	assert.Equal(t, rows*300, variableWidthResidual(segment, schema))

	// The varchar is its max_length bound, below the residual; the unbounded
	// json is the whole residual, which is conservative.
	assert.Equal(t, rows*(40+4), estimateFieldSize(segment, schema, 102))
	assert.Equal(t, rows*300, estimateFieldSize(segment, schema, 103))
	// Fixed-width fields never take the residual as their container.
	assert.Equal(t, rows*8, estimateFieldSize(segment, schema, 100))
	assert.Equal(t, rows*1, estimateFieldSize(segment, schema, 104))

	// A schema that already lists the system fields is not charged twice.
	withSystem := &schemapb.CollectionSchema{Fields: append([]*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "RowID", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
	}, schema.Fields...)}
	assert.Equal(t, rows*300, variableWidthResidual(segment, withSystem))

	// Inconsistent or missing statistics give no residual.
	inconsistent := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, NumOfRows: rows, Stats: &datapb.Statistics{InsertBinlogSize: rows * 10},
	}}
	assert.Equal(t, int64(0), variableWidthResidual(inconsistent, schema))
	assert.Equal(t, int64(0), variableWidthResidual(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: rows}}, schema))
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
	// Not ok: the value is returned, reported as unresolved, and not cached,
	// so the next call recomputes.
	res, ok := c.get(compute(false))
	assert.Equal(t, int64(1), res.CPU)
	assert.False(t, ok)
	res, ok = c.get(compute(false))
	assert.Equal(t, int64(2), res.CPU)
	assert.False(t, ok)
	// Ok: cached and reported as resolved; subsequent calls do not recompute.
	res, ok = c.get(compute(true))
	assert.Equal(t, int64(3), res.CPU)
	assert.True(t, ok)
	res, ok = c.get(compute(true))
	assert.Equal(t, int64(3), res.CPU)
	assert.True(t, ok)
	assert.Equal(t, 3, calls)
}

func typeutilEstimateSizePerRecord(s *schemapb.CollectionSchema) (int64, error) {
	n, err := typeutil.EstimateSizePerRecord(s)
	return int64(n), err
}
