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

package taskresource

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
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

func TestCompactionReaderMemory(t *testing.T) {
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		Stats: &datapb.Statistics{InsertBinlogSize: 1024 * testMiB},
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{
				{MemorySize: 16 * testMiB, LogSize: 4 * testMiB},
				{MemorySize: 32 * testMiB, LogSize: 8 * testMiB},
			}},
			{FieldID: 101, Binlogs: []*datapb.Binlog{
				{MemorySize: 64 * testMiB, LogSize: 16 * testMiB},
			}},
		},
	}}
	// One chunk per field, rather than the sum of every chunk in the segment.
	assert.EqualValues(t, 120*testMiB, CompactionReaderMemory(segment, nil))
	segment.Binlogs[0].Binlogs[0].MemorySize = 0
	assert.EqualValues(t, 2048*testMiB, CompactionReaderMemory(segment, nil))
	segment.Binlogs[0].Binlogs[0].MemorySize = 16 * testMiB
	segment.StorageVersion = storage.StorageV2
	assert.EqualValues(t, 2*packed.DefaultReadBufferSize, CompactionReaderMemory(segment, nil))
	segment.StorageVersion = storage.StorageV1
	segment.ManifestPath = "manifest"
	assert.EqualValues(t, 2*packed.DefaultReadBufferSize, CompactionReaderMemory(segment, nil))
	segment.ManifestPath = ""
	segment.Binlogs[0].Binlogs[0].LogSize = 0
	assert.EqualValues(t, 2048*testMiB, CompactionReaderMemory(segment, nil))
	segment.Binlogs[0].Binlogs = nil
	assert.EqualValues(t, 2048*testMiB, CompactionReaderMemory(segment, nil))
	segment.Binlogs = nil
	assert.EqualValues(t, 2048*testMiB, CompactionReaderMemory(segment, nil))
}

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
	defaultCPU := paramtable.Get().DataCoordCfg.TaskResourceDefaultCPU.GetAsInt64()
	minMem := paramtable.Get().DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize()

	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 2 * testGiB}, Index(testGiB, true))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, Index(testGiB, false))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, Stats(testGiB))
	// Sort compaction is the stats formula over the whole segment, same factor.
	assert.Equal(t, Stats(testGiB), SortCompaction(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, L0Compaction(testGiB, 0))
	// The targets' stats logs are loaded in full on top of the scaled deltas.
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2*testGiB + 100*testMiB}, L0Compaction(testGiB, 100*testMiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: minMem}, Lightweight())
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 1 << 30}, ExternalRefresh())
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: minMem}, Default())

	// Mix compaction streams: its input, bounded by one output segment; an
	// unknown input is priced at the bound.
	maxSegment := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * testMiB
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 300 * testMiB}, MixCompaction(300*testMiB, 0, 0))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: maxSegment}, MixCompaction(5*testGiB, 0, 0))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: maxSegment}, MixCompaction(0, 0, 0))
	// Delete maps are not streamed: they come on top of the streaming cap.
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: maxSegment + 2*testGiB}, MixCompaction(5*testGiB, 0, 2*testGiB))
	// Merge-sort's simultaneous reader windows are not bounded by one output
	// segment, and its delete maps are retained on top of both budgets.
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: maxSegment + 3*testGiB + 128*testMiB},
		MixCompaction(10*testGiB, 3*testGiB, 128*testMiB))

	// Clustering: its input, bounded by nothing else.
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 10 * testGiB}, ClusteringCompaction(10*testGiB))

	// Analyze: raw vectors times the factor.
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 2 * testGiB}, Analyze(testGiB))

	// Import: what its read buffers hold, times the factor; fixed reservations
	// such as delete budgets are added once, not scaled.
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * 300 * testMiB}, Import(300*testMiB, 0))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2*300*testMiB + 128*testMiB}, Import(300*testMiB, 128*testMiB))
	// Pre-import: the same buffers, nothing in flight.
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 300 * testMiB}, PreImport(300*testMiB))

	// Nothing is ever priced below the floor: a 0-byte input still costs minTaskMemory.
	assert.Equal(t, minMem, Index(0, true).Memory)
	assert.Equal(t, minMem, Stats(0).Memory)
	assert.Equal(t, minMem, L0Compaction(0, 0).Memory)
	assert.Equal(t, minMem, Analyze(0).Memory)
	assert.Equal(t, minMem, ClusteringCompaction(0).Memory)
	assert.Equal(t, minMem, Import(0, 0).Memory)
	assert.Equal(t, minMem, PreImport(0).Memory)
}

// TestImportBufferedBytes: a buffer never fills beyond the file it reads, so a
// task of many small files holds their bytes, not one whole buffer each.
func TestImportBufferedBytes(t *testing.T) {
	paramtable.Init()
	const buffer = 16 * testMiB

	// No files listed yet: one buffer.
	assert.Equal(t, buffer, ImportBufferedBytes(nil, buffer))

	// 100 files of 1MiB each: 100MiB, not 100 buffers.
	small := make([]*datapb.ImportFileStats, 0, 100)
	for i := 0; i < 100; i++ {
		small = append(small, &datapb.ImportFileStats{TotalMemorySize: testMiB})
	}
	assert.Equal(t, 100*testMiB, ImportBufferedBytes(small, buffer))

	// Files larger than the buffer are charged one buffer each; a file whose
	// size is unknown is charged a whole buffer too.
	big := []*datapb.ImportFileStats{{TotalMemorySize: buffer * 10}, {TotalMemorySize: 0}, {TotalMemorySize: testMiB}}
	assert.Equal(t, 2*buffer+testMiB, ImportBufferedBytes(big, buffer))
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
	assert.Equal(t, []int64{102}, StatsTargetFields(schema, indexpb.StatsSubJob_TextIndexJob))
	// Every JSON field gets json key stats, so both are read.
	assert.Equal(t, []int64{104, 105}, StatsTargetFields(schema, indexpb.StatsSubJob_JsonKeyIndexJob))
	assert.Equal(t, []int64{106}, StatsTargetFields(schema, indexpb.StatsSubJob_BM25Job))
	// A sort reads everything; a nil schema names nothing.
	assert.Nil(t, StatsTargetFields(schema, indexpb.StatsSubJob_Sort))
	assert.Nil(t, StatsTargetFields(nil, indexpb.StatsSubJob_TextIndexJob))
	// A sub job whose fields are not in this schema reads nothing specific.
	noMatch := &schemapb.CollectionSchema{Fields: schema.Fields[:2]}
	assert.Nil(t, StatsTargetFields(noMatch, indexpb.StatsSubJob_TextIndexJob))

	// statsInputSize: the targeted fields' bytes when known, else the segment.
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 102, Binlogs: []*datapb.Binlog{{MemorySize: 7000}}},
			{FieldID: 104, Binlogs: []*datapb.Binlog{{MemorySize: 9000}}},
			{FieldID: 105, Binlogs: []*datapb.Binlog{{MemorySize: 1000}}},
		},
		Stats: &datapb.Statistics{InsertBinlogSize: 100000},
	}}
	assert.Equal(t, int64(7000), StatsInputSize(segment, schema, indexpb.StatsSubJob_TextIndexJob))
	assert.Equal(t, int64(10000), StatsInputSize(segment, schema, indexpb.StatsSubJob_JsonKeyIndexJob))
	assert.Equal(t, int64(100000), StatsInputSize(segment, schema, indexpb.StatsSubJob_Sort))
	assert.Equal(t, int64(100000), StatsInputSize(segment, nil, indexpb.StatsSubJob_TextIndexJob))
	assert.Equal(t, int64(100000), StatsInputSize(segment, noMatch, indexpb.StatsSubJob_TextIndexJob))
}

func TestTaskResource_ConfigOverride(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(paramtable.Get().DataCoordCfg.TaskResourceVectorIndexCPU.Key, "4")
	pt.Save(paramtable.Get().DataCoordCfg.TaskResourceIndexMemoryFactor.Key, "3")
	defer pt.Reset(paramtable.Get().DataCoordCfg.TaskResourceVectorIndexCPU.Key)
	defer pt.Reset(paramtable.Get().DataCoordCfg.TaskResourceIndexMemoryFactor.Key)

	assert.Equal(t, taskcommon.Resource{CPU: 4, Memory: 3 * testGiB}, Index(testGiB, true))
}

// TestTaskResource_ConfigFloors pins the "never zero" invariant against a
// configuration that asks for zero: CPU and memory must both remain positive.
func TestTaskResource_ConfigFloors(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	for _, key := range []string{
		paramtable.Get().DataCoordCfg.TaskResourceDefaultCPU.Key,
		paramtable.Get().DataCoordCfg.TaskResourceVectorIndexCPU.Key,
		paramtable.Get().DataCoordCfg.TaskResourceAnalyzeCPU.Key,
		pt.DataCoordCfg.TaskResourceMinTaskMemory.Key,
		paramtable.Get().DataCoordCfg.TaskResourceClusteringCompactionCPU.Key,
	} {
		pt.Save(key, "0")
		defer pt.Reset(key)
	}

	assert.Equal(t, int64(1), Default().Memory)
	assert.Equal(t, int64(1), PreImport(0).Memory)
	assert.Equal(t, int64(32), PreImport(32).Memory)
	assert.Equal(t, int64(1), Default().CPU)
	assert.Equal(t, int64(1), Index(testGiB, true).CPU)
	assert.Equal(t, int64(1), Analyze(testGiB).CPU)
	assert.Equal(t, int64(1), ClusteringCompaction(testGiB).CPU)
}

func TestEstimateSegmentSize(t *testing.T) {
	paramtable.Init()
	schema := testResourceSchema()

	// Stats present (every storage version persists it): use it verbatim.
	withStats := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: 1000, StorageVersion: 3,
		Stats: &datapb.Statistics{InsertBinlogSize: 700, StatsBinlogSize: 200, DeltaBinlogSize: 100},
	}}
	assert.Equal(t, int64(1000), EstimateSegmentSize(withStats, schema))

	// V1 without Stats but with binlogs: EnsureStats rebuilds from the arrays.
	fromBinlogs := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 512000, EntriesNum: 1000}}}},
	}}
	assert.Equal(t, int64(512000), EstimateSegmentSize(fromBinlogs, schema))

	// External-collection shape: no Stats, no binlogs, rows known -> rows x per-record estimate.
	external := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 1000, ManifestPath: "m"}}
	perRecord, err := typeutilEstimateSizePerRecord(schema)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000)*perRecord, EstimateSegmentSize(external, schema))

	// Nothing to go on at all.
	assert.Equal(t, int64(0), EstimateSegmentSize(&meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 4}}, schema))
	assert.Equal(t, int64(0), EstimateSegmentSize(external, nil))
	assert.Equal(t, int64(0), EstimateSegmentSize(&meta.SegmentInfo{}, schema))

	// A schema the estimator rejects (varchar without max_length) is not guessed at.
	badSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar},
	}}
	assert.Equal(t, int64(0), EstimateSegmentSize(external, badSchema))
}

func TestEstimateVarCharFieldSizeUsesSmallerBound(t *testing.T) {
	const rows = int64(1000)
	for _, source := range []string{"v1", "v2-files", "v2-manifest", "v3-stats"} {
		t.Run(source, func(t *testing.T) {
			field := &schemapb.FieldSchema{
				FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}},
			}
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
			for _, recorded := range []int64{32000, 68125, 5 * testMiB} {
				segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
					NumOfRows: rows, Binlogs: []*datapb.FieldBinlog{{
						FieldID: 102,
						Binlogs: []*datapb.Binlog{{MemorySize: recorded}},
					}},
				}}
				switch source {
				case "v2-files", "v2-manifest":
					segment.StorageVersion = storage.StorageV2
					segment.Binlogs[0].FieldID = 0
					segment.Binlogs[0].ChildFields = []int64{101, 102}
					if source == "v2-manifest" {
						segment.ManifestPath = "manifest"
					}
				case "v3-stats":
					segment.StorageVersion = storage.StorageV3
					segment.ManifestPath = "manifest"
					segment.Binlogs = nil
					segment.Stats = &datapb.Statistics{LoadResource: &datapb.LoadResourceStatistics{
						ColumnGroups: []*datapb.ColumnGroupStatistics{{GroupId: 0, FieldIds: []int64{101, 102}, MemorySize: recorded}},
					}}
				}
				// The schema includes four-byte offsets and the nullable bitmap.
				assert.Equal(t, min(recorded, rows*(64+4)+(rows+7)/8), EstimateFieldSize(segment, schema, 102))
				// Missing schema information cannot cap a measured size at zero.
				field.TypeParams = nil
				assert.Equal(t, recorded, EstimateFieldSize(segment, schema, 102))
				field.TypeParams = []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}}
			}
		})
	}
}

func TestEstimateNullableVectorFieldSize(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID: 101, DataType: schemapb.DataType_BinaryVector, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}},
	}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		NumOfRows: 9, StorageVersion: storage.StorageV3, ManifestPath: "manifest",
		Stats: &datapb.Statistics{InsertBinlogSize: 1000},
	}}
	// Nine one-byte payloads, ten int32 offsets, two validity bytes.
	assert.Equal(t, int64(51), EstimateFieldSize(segment, schema, 101))
	segment.Stats.LoadResource = &datapb.LoadResourceStatistics{
		ColumnGroups: []*datapb.ColumnGroupStatistics{{FieldIds: []int64{101}, MemorySize: 100}},
	}
	assert.Equal(t, int64(100), EstimateFieldSize(segment, schema, 101))
}

func TestEstimateFieldSize(t *testing.T) {
	paramtable.Init()
	schema := testResourceSchema() // pk int64 100, vec float32 dim 128 101, str varchar(64) 102
	const rows = int64(1000)
	varCharBound := rows * (64 + 4)

	// Step 1 -- recorded field bytes below the schema bound are retained.
	// Storage v1 has one binlog per field.
	v1 := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: rows,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 512000}}},
			{FieldID: 102, Binlogs: []*datapb.Binlog{{MemorySize: 64000}}},
		},
	}}
	assert.Equal(t, int64(512000), EstimateFieldSize(v1, schema, 101))
	assert.Equal(t, int64(64000), EstimateFieldSize(v1, schema, 102))

	// Shared groups are capped at the varchar schema bound. Fixed-width fields
	// are capped when the reader projects them through the manifest.
	groups := []*datapb.FieldBinlog{
		{FieldID: 0, ChildFields: []int64{100, 102}, Binlogs: []*datapb.Binlog{{MemorySize: 5 * testMiB}}},
		{FieldID: 101, ChildFields: []int64{101}, Binlogs: []*datapb.Binlog{{MemorySize: 4096}}},
	}
	v2 := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 6, NumOfRows: rows, StorageVersion: 2, ManifestPath: "m", Binlogs: groups,
	}}
	assert.Equal(t, rows*8, EstimateFieldSize(v2, schema, 100))
	assert.Equal(t, varCharBound, EstimateFieldSize(v2, schema, 102))
	// A recorded size below the exact width is kept: the cap only ever
	// lowers the price.
	assert.Equal(t, int64(4096), EstimateFieldSize(v2, schema, 101))

	// Without a manifest the worker reads the insert files and decodes the
	// whole group before it takes the column out. Fixed-width fields retain
	// that group size; varchar estimates its field input using the schema cap.
	v2Files := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 8, NumOfRows: rows, StorageVersion: 2, Binlogs: groups,
	}}
	assert.Equal(t, 5*testMiB, EstimateFieldSize(v2Files, schema, 100))
	assert.Equal(t, varCharBound, EstimateFieldSize(v2Files, schema, 102))
	assert.Equal(t, int64(4096), EstimateFieldSize(v2Files, schema, 101))
	const wideRows = int64(1_000_000)
	wide := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 9, NumOfRows: wideRows, StorageVersion: 2,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{100, 102}, Binlogs: []*datapb.Binlog{{MemorySize: 2 << 30}}},
		},
	}}
	assert.Equal(t, int64(2<<30), EstimateFieldSize(wide, schema, 100))
	wide.ManifestPath = "m"
	assert.Equal(t, wideRows*8, EstimateFieldSize(wide, schema, 100))

	// A binlog size that is not positive is "not in meta", not "free".
	zeroBinlog := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 7, NumOfRows: rows,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 0}}}},
	}}
	assert.Equal(t, rows*8, EstimateFieldSize(zeroBinlog, schema, 100))

	// Step 2 -- storage v3 after a DataCoord restart: no binlogs in meta, so
	// the schema bound, capped at the segment.
	total := rows * (16 + 8 + 128*4 + 40)
	v3 := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, NumOfRows: rows, StorageVersion: 3, ManifestPath: "m",
		Stats: &datapb.Statistics{InsertBinlogSize: total},
	}}
	assert.Equal(t, rows*128*4, EstimateFieldSize(v3, schema, 101))
	assert.Equal(t, rows*8, EstimateFieldSize(v3, schema, 100))
	assert.Equal(t, varCharBound, EstimateFieldSize(v3, schema, 102))

	// Step 3 -- unknown field, or no schema: the whole segment. Nil inputs: 0.
	assert.Equal(t, 5*testMiB, EstimateFieldSize(v2, &schemapb.CollectionSchema{}, 100))
	assert.Equal(t, total, EstimateFieldSize(v3, schema, 999))
	assert.Equal(t, total, EstimateFieldSize(v3, nil, 101))
	assert.Equal(t, int64(0), EstimateFieldSize(&meta.SegmentInfo{}, schema, 101))

	// A type the write path does not bound falls back to the whole segment.
	dimless := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
	}}
	assert.Equal(t, total, EstimateFieldSize(v3, dimless, 101))
	unsizable := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar},
	}}
	assert.Equal(t, total, EstimateFieldSize(v3, unsizable, 102))
}

func TestEstimateFieldSizeLogicalSizes(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name   string
		change func(*meta.SegmentInfo, *schemapb.CollectionSchema)
		want   int64
	}{
		{name: "payload exceeds encoded group and segment", want: 1000},
		{name: "known zero", change: func(segment *meta.SegmentInfo, _ *schemapb.CollectionSchema) {
			segment.Stats.FieldLogicalSizes.Fields[102].PayloadBytes = 0
		}, want: 0},
		{name: "unsigned payload saturates the resource budget", change: func(segment *meta.SegmentInfo, _ *schemapb.CollectionSchema) {
			segment.Stats.FieldLogicalSizes.Fields[102].PayloadBytes = math.MaxUint64
		}, want: math.MaxInt64},
		{name: "nullable bitmap", change: func(segment *meta.SegmentInfo, schema *schemapb.CollectionSchema) {
			schema.Fields[0].Nullable = true
			segment.Stats.FieldLogicalSizes.Fields[102].ValidityBits = 9
		}, want: 1002},
		{name: "unmeasured field", change: func(segment *meta.SegmentInfo, _ *schemapb.CollectionSchema) {
			delete(segment.Stats.FieldLogicalSizes.Fields, 102)
		}, want: 64},
		{name: "legacy segment", change: func(segment *meta.SegmentInfo, _ *schemapb.CollectionSchema) {
			segment.Stats.FieldLogicalSizes = nil
			segment.FieldLogicalSizesManifest = ""
		}, want: 64},
		{name: "replaced manifest", change: func(segment *meta.SegmentInfo, _ *schemapb.CollectionSchema) {
			segment.ManifestPath = packed.MarshalManifestPath("files/insert_log/1/2/3", 2)
		}, want: 64},
		{name: "changed row count", change: func(segment *meta.SegmentInfo, _ *schemapb.CollectionSchema) {
			segment.NumOfRows++
		}, want: 64},
		{name: "unknown statistics version", change: func(segment *meta.SegmentInfo, _ *schemapb.CollectionSchema) {
			segment.Stats.FieldLogicalSizes.Version++
		}, want: 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := packed.MarshalManifestPath("files/insert_log/1/2/3", 1)
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 3, NumOfRows: 9, StorageVersion: storage.StorageV3, Level: datapb.SegmentLevel_L1,
				ManifestPath: manifest, FieldLogicalSizesManifest: manifest,
				Stats: &datapb.Statistics{
					InsertBinlogSize: 256,
					LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
						{GroupId: 0, FieldIds: []int64{102, 103}, MemorySize: 64},
					}},
					FieldLogicalSizes: &datapb.FieldLogicalSizes{
						Version: storage.FieldLogicalSizeVersion, RowCount: 9,
						Fields: map[int64]*datapb.FieldLogicalSize{102: {PayloadBytes: 1000}},
					},
				},
			}}
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 102, DataType: schemapb.DataType_Text},
				{FieldID: 103, DataType: schemapb.DataType_Text},
			}}
			if tc.change != nil {
				tc.change(segment, schema)
			}
			size := EstimateFieldSize(segment, schema, 102)
			assert.Equal(t, tc.want, size)
			if tc.want == math.MaxInt64 {
				assert.EqualValues(t, math.MaxInt64, Index(size, false).Memory)
			}
		})
	}
}

func TestStatsInputSizeLogicalSizes(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name   string
		fields map[int64]*datapb.FieldLogicalSize
		want   int64
	}{
		// Round the selected fields' validity bits after aggregation: six bits
		// need one byte. Neither the group nor segment size caps known payloads.
		{"known fields exceed reference group", map[int64]*datapb.FieldLogicalSize{
			102: {PayloadBytes: 1000, ValidityBits: 3}, 103: {PayloadBytes: 2000, ValidityBits: 3},
		}, 3001},
		{"known and unmeasured fields share a group", map[int64]*datapb.FieldLogicalSize{
			102: {PayloadBytes: 1000, ValidityBits: 3},
		}, 1065},
		{"known nulls", map[int64]*datapb.FieldLogicalSize{
			102: {ValidityBits: 3}, 103: {ValidityBits: 3},
		}, 1},
		{"unmeasured group charged once", nil, 64},
		{"unsigned payload and unmeasured field saturate the resource budget", map[int64]*datapb.FieldLogicalSize{
			102: {PayloadBytes: math.MaxUint64, ValidityBits: 3},
		}, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := packed.MarshalManifestPath("files/insert_log/1/2/3", 1)
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 3, NumOfRows: 3, StorageVersion: storage.StorageV3, Level: datapb.SegmentLevel_L1,
				ManifestPath: manifest, FieldLogicalSizesManifest: manifest,
				Stats: &datapb.Statistics{
					InsertBinlogSize: 256,
					LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
						{GroupId: 0, FieldIds: []int64{102, 103}, MemorySize: 64},
					}},
					FieldLogicalSizes: &datapb.FieldLogicalSizes{
						Version: storage.FieldLogicalSizeVersion, RowCount: 3, Fields: tc.fields,
					},
				},
			}}
			schema := &schemapb.CollectionSchema{}
			for _, id := range []int64{102, 103} {
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
					FieldID: id, DataType: schemapb.DataType_Text, Nullable: true,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "enable_match", Value: "true"}, {Key: "enable_analyzer", Value: "true"},
					},
				})
			}
			size := StatsInputSize(segment, schema, indexpb.StatsSubJob_TextIndexJob)
			assert.Equal(t, tc.want, size)
			if tc.want == math.MaxInt64 {
				assert.EqualValues(t, math.MaxInt64, Stats(size).Memory)
			}
		})
	}
}

// The schema bound of a varchar(65535) or a JSON field is rows x the per-value
// limit -- orders of magnitude above the data. Uncapped, a task priced on it
// exceeds every worker, is dispatched as oversized, and the worker books the
// whole of it: that worker then reports no free memory until the task ends.
// A field is capped at its segment, whatever the collection kind.
func TestEstimateFieldSizeIsCappedAtTheSegment(t *testing.T) {
	paramtable.Init()
	const rows = int64(1_000_000)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "doc", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}}},
		{FieldID: 102, Name: "meta", DataType: schemapb.DataType_JSON},
	}}
	varCharBound := rows * (65535 + 4)
	jsonBound := rows * (paramtable.Get().CommonCfg.JSONMaxLength.GetAsInt64() + 4)

	// A 2GiB segment, no binlogs in meta (v3 after a restart, internal or
	// external alike).
	const segmentSize = int64(2) << 30
	seg := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, NumOfRows: rows, StorageVersion: 3, ManifestPath: "m",
		Stats: &datapb.Statistics{InsertBinlogSize: segmentSize},
	}}
	require.Greater(t, varCharBound, segmentSize, "setup: the bound must exceed the segment")
	require.Greater(t, jsonBound, segmentSize, "setup: the bound must exceed the segment")
	assert.Equal(t, segmentSize, EstimateFieldSize(seg, schema, 101))
	assert.Equal(t, segmentSize, EstimateFieldSize(seg, schema, 102))

	// A segment with neither binlogs nor Stats is capped at the segment estimate
	// from the schema -- still nowhere near the per-value limit.
	bare := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: rows, StorageVersion: 3, ManifestPath: "m"}}
	capped := EstimateFieldSize(bare, schema, 102)
	assert.Positive(t, capped)
	assert.Equal(t, EstimateSegmentSize(bare, schema), capped)
	assert.Less(t, capped, jsonBound)
	assert.Less(t, EstimateFieldSize(bare, schema, 101), varCharBound)
}

func TestResourceCache(t *testing.T) {
	var c Cache
	calls := 0
	compute := func(ok bool) func() (taskcommon.Resource, bool) {
		return func() (taskcommon.Resource, bool) {
			calls++
			return taskcommon.Resource{CPU: int64(calls), Memory: 1}, ok
		}
	}
	// Not ok: the value is returned without caching,
	// so the next call recomputes.
	res := c.Get(compute(false))
	assert.Equal(t, int64(1), res.CPU)
	res = c.Get(compute(false))
	assert.Equal(t, int64(2), res.CPU)
	// Ok: cached; subsequent calls do not recompute.
	res = c.Get(compute(true))
	assert.Equal(t, int64(3), res.CPU)
	res = c.Get(compute(true))
	assert.Equal(t, int64(3), res.CPU)
	assert.Equal(t, 3, calls)
}

func typeutilEstimateSizePerRecord(s *schemapb.CollectionSchema) (int64, error) {
	n, err := typeutil.EstimateSizePerRecord(s)
	return int64(n), err
}

func TestDeleteMapBytes(t *testing.T) {
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, Stats: &datapb.Statistics{
		DeleteNumRows: 1000, DeltaBinlogSize: 50 * testMiB,
	}}}
	// Int64 key: the per-entry map cost only.
	assert.Equal(t, int64(1000*deleteMapEntryBytes), DeleteMapBytes(segment, schemapb.DataType_Int64))
	// VarChar key: the map also keeps the strings, taken from the deltalog bytes.
	assert.Equal(t, int64(1000*deleteMapEntryBytes)+50*testMiB, DeleteMapBytes(segment, schemapb.DataType_VarChar))
	assert.Zero(t, DeleteMapBytes(&meta.SegmentInfo{}, schemapb.DataType_Int64))
}

// TestImportNodeMemoryShare: the share the import allocator leaves to all
// import readers on a node.
func TestImportNodeMemoryShare(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().DataNodeCfg.ImportMemoryLimitPercentage.Key
	defer paramtable.Get().Reset(key)

	assert.InDelta(t, 0.1, ImportNodeMemoryShare(), 1e-9, "default 10%")
	paramtable.Get().Save(key, "25")
	assert.InDelta(t, 0.25, ImportNodeMemoryShare(), 1e-9)
	// An invalid setting falls back to the allocator's own default.
	paramtable.Get().Save(key, "120")
	assert.InDelta(t, 0.1, ImportNodeMemoryShare(), 1e-9)

	// An import task also holds the hashed copies waiting for sync, so its
	// bound carries the same factor its estimate applies to the buffers.
	factorKey := paramtable.Get().DataCoordCfg.TaskResourceImportMemoryFactor.Key
	defer paramtable.Get().Reset(factorKey)
	assert.InDelta(t, 0.2, ImportTaskNodeMemoryShare(), 1e-9, "10% x default factor 2")
	paramtable.Get().Save(factorKey, "3")
	assert.InDelta(t, 0.3, ImportTaskNodeMemoryShare(), 1e-9)
}

func TestCompactionReaderMemoryStreamingBatches(t *testing.T) {
	for _, version := range []int64{storage.StorageV2, storage.StorageV3} {
		segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			StorageVersion: version, NumOfRows: 1_000_000, Stats: &datapb.Statistics{InsertBinlogSize: 512 * testMiB},
		}}
		if version == storage.StorageV3 {
			segment.ManifestPath = "manifest"
		}
		assert.EqualValues(t, 2*packed.DefaultReadBufferSize, CompactionReaderMemory(segment, nil))
		segment.Stats.InsertBinlogSize = testMiB
		assert.EqualValues(t, 2*testMiB, CompactionReaderMemory(segment, nil))
	}
	// Missing size statistics still reserve the nominal reader budget.
	unknown := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		StorageVersion: storage.StorageV3, ManifestPath: "manifest",
	}}
	assert.EqualValues(t, 2*packed.DefaultReadBufferSize, CompactionReaderMemory(unknown, nil))
}

func TestStatsInputSizeDeduplicatesColumnGroups(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Text, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}}},
		{FieldID: 101, DataType: schemapb.DataType_Text, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}}},
		{FieldID: 102, DataType: schemapb.DataType_Text, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}}},
	}}
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		StorageVersion: storage.StorageV2, NumOfRows: 100,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{100, 101}, Binlogs: []*datapb.Binlog{{MemorySize: 1000}}},
			{FieldID: 1, ChildFields: []int64{102}, Binlogs: []*datapb.Binlog{{MemorySize: 2000}}},
		},
	}}
	assert.EqualValues(t, 3000, StatsInputSize(segment, schema, indexpb.StatsSubJob_TextIndexJob))
	// Catalog-reloaded V3 segments have only persistent group statistics.
	segment.StorageVersion = storage.StorageV3
	segment.ManifestPath = "manifest"
	segment.Binlogs = nil
	segment.Stats = &datapb.Statistics{LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
		{GroupId: 0, FieldIds: []int64{100, 101}, MemorySize: 1000},
		{GroupId: 102, MemorySize: 2000},
	}}}
	assert.EqualValues(t, 3000, StatsInputSize(segment, schema, indexpb.StatsSubJob_TextIndexJob))
	// Unknown group sizes leave field-level estimation in charge.
	segment.Stats.LoadResource.ColumnGroups[0].MemorySize = 0
	segment.Stats.InsertBinlogSize = 5000
	assert.EqualValues(t, 12000, StatsInputSize(segment, schema, indexpb.StatsSubJob_TextIndexJob))
}
