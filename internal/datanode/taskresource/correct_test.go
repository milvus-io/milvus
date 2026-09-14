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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
)

func TestMain(m *testing.M) {
	paramtable.Init()
	os.Exit(m.Run())
}

// estimate is a DataCoord estimate whose memory is deliberately far from any
// corrected value, so a test can tell "corrected" from "left alone".
var estimate = taskcommon.Resource{CPU: 3, Memory: 777 * mib}

func minMemory() int64 {
	return paramtable.Get().DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize()
}

func binlogs(fieldID int64, sizes ...int64) *datapb.FieldBinlog {
	fb := &datapb.FieldBinlog{FieldID: fieldID}
	for _, size := range sizes {
		fb.Binlogs = append(fb.Binlogs, &datapb.Binlog{MemorySize: size})
	}
	return fb
}

func kv(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

// A zero estimate comes from a coordinator that predates estimates: every
// family must leave it at zero, whatever the request says.
func TestZeroEstimateIsNeverCorrected(t *testing.T) {
	zero := taskcommon.Resource{}
	assert.Equal(t, zero, CorrectIndex(&workerpb.CreateJobRequest{
		FieldType: schemapb.DataType_FloatVector, Dim: 8, NumRows: 1000,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "HNSW")},
	}, zero))
	assert.Equal(t, zero, CorrectStats(&workerpb.CreateStatsRequest{
		SubJobType: indexpb.StatsSubJob_Sort,
		InsertLogs: []*datapb.FieldBinlog{binlogs(100, gib)},
	}, zero))
	assert.Equal(t, zero, CorrectAnalyze(&workerpb.AnalyzeRequest{
		FieldType: schemapb.DataType_FloatVector, Dim: 8,
		SegmentStats: map[int64]*indexpb.SegmentStats{1: {NumRows: 1000}},
	}, zero))
	assert.Equal(t, zero, CorrectCompaction(&datapb.CompactionPlan{
		Type:           datapb.CompactionType_MixCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{FieldBinlogs: []*datapb.FieldBinlog{binlogs(100, gib)}}},
	}, zero))
	assert.Equal(t, zero, CorrectImport(&datapb.ImportRequest{Vchannels: []string{"a"}, PartitionIDs: []int64{1}}, zero))
	assert.Equal(t, zero, CorrectPreImport(&datapb.PreImportRequest{}, zero))
}

func TestCorrectIndex_VectorFieldIsExact(t *testing.T) {
	// 1M x 128-dim float32 is exactly 512MB whatever the binlogs say, so a
	// build DataCoord priced on an apportioned guess is corrected to its model.
	const rows, dim = int64(1_000_000), int64(128)
	raw := rows * dim * 4
	req := &workerpb.CreateJobRequest{
		Field:       &schemapb.FieldSchema{FieldID: 101, DataType: schemapb.DataType_FloatVector},
		NumRows:     rows,
		Dim:         dim,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "HNSW"), kv("M", "16")},
		InsertLogs:  []*datapb.FieldBinlog{binlogs(101, 3*gib)}, // ignored for a dense vector
	}
	perElement := int64((3*16+2)*4 + hnswLabelBytes + hnswPointerBytes + hnswLockBytes)
	want := taskcommon.Resource{CPU: estimate.CPU, Memory: 2*raw + rows*perElement}
	assert.Equal(t, want, CorrectIndex(req, estimate))

	// The field type from FieldType is used when the request has no schema.
	legacy := &workerpb.CreateJobRequest{
		FieldType: schemapb.DataType_FloatVector, FieldID: 101, NumRows: rows, Dim: dim,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "HNSW"), kv("M", "16")},
	}
	assert.Equal(t, want, CorrectIndex(legacy, estimate))
}

func TestCorrectIndex_ScalarFieldFromBinlogs(t *testing.T) {
	// An Int64 in a 200MB short column group: the field is the smaller of its
	// schema size (10M x 8 = 80MB) and the group, not the whole group.
	const rows = int64(10_000_000)
	req := &workerpb.CreateJobRequest{
		Field:       &schemapb.FieldSchema{FieldID: 102, DataType: schemapb.DataType_Int64},
		NumRows:     rows,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "STL_SORT")},
		InsertLogs: []*datapb.FieldBinlog{
			binlogs(101, gib),
			{FieldID: 1, ChildFields: []int64{102, 103}, Binlogs: []*datapb.Binlog{{MemorySize: 100 * mib}, {MemorySize: 100 * mib}}},
		},
	}
	raw := rows * 8
	sortMemory := 2*raw + rows*4 + rows/8 + 1
	assert.Equal(t, sortMemory, CorrectIndex(req, estimate).Memory)

	// A group smaller than the schema size bounds the field instead.
	small := proto.Clone(req).(*workerpb.CreateJobRequest)
	small.InsertLogs[1].Binlogs = []*datapb.Binlog{{MemorySize: 50 * mib}}
	assert.Equal(t, 2*50*mib+rows*4+rows/8+1, CorrectIndex(small, estimate).Memory)

	// Optional scalar fields the build loads are added, sized the same way: an
	// untyped one by its binlogs, a typed fixed-width one by its schema size.
	req.OptionalScalarFields = []*indexpb.OptionalFieldInfo{{FieldID: 101}}
	assert.Equal(t, sortMemory+gib, CorrectIndex(req, estimate).Memory)
	req.OptionalScalarFields = []*indexpb.OptionalFieldInfo{{FieldID: 101, FieldType: int32(schemapb.DataType_Int32)}}
	assert.Equal(t, sortMemory+rows*4, CorrectIndex(req, estimate).Memory)

	// A varchar is bounded by max_length when its group is larger.
	varChar := &workerpb.CreateJobRequest{
		Field: &schemapb.FieldSchema{
			FieldID: 104, DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{kv("max_length", "16")},
		},
		NumRows:     rows,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "SOME_FUTURE_INDEX")},
		InsertLogs:  []*datapb.FieldBinlog{{FieldID: 1, ChildFields: []int64{104}, Binlogs: []*datapb.Binlog{{MemorySize: 4 * gib}}}},
	}
	factor := paramtable.Get().DataCoordCfg.TaskResourceIndexMemoryFactor.GetAsFloat()
	assert.Equal(t, int64(float64(rows*(16+4))*factor), CorrectIndex(varChar, estimate).Memory)

	// A struct-array child has no schema bound: its parent's binlogs.
	child := &workerpb.CreateJobRequest{
		Field:       &schemapb.FieldSchema{FieldID: 201, DataType: schemapb.DataType_Array},
		NumRows:     1000,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "INVERTED")},
		InsertLogs:  []*datapb.FieldBinlog{{FieldID: 200, ChildFields: []int64{201}, Binlogs: []*datapb.Binlog{{MemorySize: 80 * mib}}}},
	}
	assert.Equal(t, 80*mib+160*mib, CorrectIndex(child, estimate).Memory)

	// Without binlogs a fixed-width field is still exact.
	noLogs := &workerpb.CreateJobRequest{
		Field:       &schemapb.FieldSchema{FieldID: 102, DataType: schemapb.DataType_Int64},
		NumRows:     rows,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "STL_SORT")},
	}
	assert.Equal(t, sortMemory, CorrectIndex(noLogs, estimate).Memory)
}

// The V3-after-restart shape: no binlogs for a scalar field. The request has
// nothing better than DataCoord had, so the estimate stands.
func TestCorrectIndex_NoBetterInputKeepsEstimate(t *testing.T) {
	req := &workerpb.CreateJobRequest{
		Field:       &schemapb.FieldSchema{FieldID: 102, DataType: schemapb.DataType_VarChar},
		NumRows:     1000,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "INVERTED")},
		Manifest:    "manifest-path",
	}
	assert.Equal(t, estimate, CorrectIndex(req, estimate))

	// A dense vector without rows or dim is not exact either.
	assert.Equal(t, estimate, CorrectIndex(&workerpb.CreateJobRequest{
		FieldType: schemapb.DataType_FloatVector, Dim: 0, NumRows: 1000,
	}, estimate))
}

func TestCorrectIndex_UnknownTypeUsesCoordinatorFactor(t *testing.T) {
	req := &workerpb.CreateJobRequest{
		Field:       &schemapb.FieldSchema{FieldID: 101, DataType: schemapb.DataType_FloatVector},
		NumRows:     1000,
		Dim:         256,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "SOME_FUTURE_INDEX")},
	}
	factor := paramtable.Get().DataCoordCfg.TaskResourceIndexMemoryFactor.GetAsFloat()
	assert.Equal(t, max(int64(float64(1000*256*4)*factor), minMemory()), CorrectIndex(req, estimate).Memory)
}

func TestIndexBuildParams_MergesKnowhereDefaults(t *testing.T) {
	req := &workerpb.CreateJobRequest{
		TypeParams:  []*commonpb.KeyValuePair{kv("dim", "8")},
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "DISKANN")},
	}
	params := indexBuildParams(req)
	assert.Equal(t, "8", params["dim"])
	assert.Equal(t, "DISKANN", params["index_type"])
	// knowhere.DISKANN.build.max_degree from this node's configuration.
	assert.Equal(t, "56", params["max_degree"])

	// Without knowhere defaults enabled, only the request's own params.
	pt := paramtable.Get()
	pt.Save(pt.KnowhereConfig.Enable.Key, "false")
	defer pt.Reset(pt.KnowhereConfig.Enable.Key)
	_, ok := indexBuildParams(req)["max_degree"]
	assert.False(t, ok)
}

func TestIndexBuildModels(t *testing.T) {
	const rows, dim = int64(10_000), int64(64)
	raw := rows * dim * 4
	perRow := raw / rows
	in := func(params map[string]string) indexInput {
		return indexInput{raw: raw, rows: rows, dim: dim, dataType: schemapb.DataType_FloatVector, params: params}
	}
	lists := rows*idBytes + 100*perRow
	// vamanaGraph mirrors the per-node graph term at runtime; the slack is a
	// float, so it cannot be folded into an integer constant expression.
	vamanaGraph := func(degree int64) int64 {
		return rows * (int64(float64(degree*4)*vamanaDegreeSlack) + 16)
	}

	cases := []struct {
		name      string
		indexType string
		params    map[string]string
		want      int64
	}{
		{"flat", "FLAT", nil, 2 * raw},
		{"ivf flat", "IVF_FLAT", map[string]string{"nlist": "100"}, 2*raw + lists},
		{"ivf flat without nlist", "IVF_FLAT", nil, 2*raw + rows*idBytes},
		{"ivf sq8", "IVF_SQ8", map[string]string{"nlist": "100"}, raw + rows*dim + lists},
		{"ivf sq6", "IVF_SQ_CC", map[string]string{"nlist": "100", "sq_type": "SQ6"}, raw + rows*((dim*6+7)/8) + lists},
		{"ivf sq4", "IVF_SQ_CC", map[string]string{"nlist": "100", "sq_type": "SQ4U"}, raw + rows*((dim+1)/2) + lists},
		{"ivf sq fp16", "IVF_SQ_CC", map[string]string{"nlist": "100", "sq_type": "FP16"}, raw + rows*dim*2 + lists},
		{"ivf pq", "IVF_PQ", map[string]string{"nlist": "100", "m": "8", "nbits": "8"}, raw + rows*8 + lists + 256*perRow},
		{"ivf pq without m", "IVF_PQ", map[string]string{"nlist": "100"}, raw + rows*perRow + lists + 256*perRow},
		{"ivf rabitq", "IVF_RABITQ", map[string]string{"nlist": "100"}, raw + rows*((dim+7)/8+8) + lists},
		{"ivf rabitq refine", "IVF_RABITQ", map[string]string{"nlist": "100", "refine": "true"}, 2*raw + rows*((dim+7)/8+8) + lists},
		{"scann", "SCANN", map[string]string{"nlist": "100"}, 2*raw + rows*((dim+1)/2) + lists},
		{"scann without raw", "SCANN", map[string]string{"nlist": "100", "with_raw_data": "false"}, raw + rows*((dim+1)/2) + lists},
		{"hnsw default M", "HNSW", nil, 2*raw + rows*((3*defaultHNSWM+2)*4+hnswLabelBytes+hnswPointerBytes+hnswLockBytes)},
		{
			"diskann", "DISKANN",
			map[string]string{"max_degree": "48", "pq_code_budget_gb_ratio": "0.25"},
			raw + raw/4 + vamanaGraph(48),
		},
		{
			"diskann defaults", "DISKANN", nil,
			raw + int64(float64(raw)*defaultPQCodeBudgetRatio) + vamanaGraph(defaultMaxDegree),
		},
		{
			"svs vamana", "SVS_VAMANA",
			map[string]string{"svs_graph_max_degree": "32"},
			raw + int64(float64(raw)*defaultPQCodeBudgetRatio) + vamanaGraph(32),
		},
		{"sparse", "SPARSE_INVERTED_INDEX", nil, 2 * raw},
		{"sort", "STL_SORT", nil, 2*raw + rows*4 + rows/8 + 1},
		{"inverted small", "INVERTED", nil, raw + 2*raw},
		{"bitmap", "BITMAP", map[string]string{"bitmap_cardinality_limit": "50"}, raw + (rows/8+1)*50},
		{"hybrid", "HYBRID", map[string]string{"bitmap_cardinality_limit": "50"}, max(raw+(rows/8+1)*50, 3*raw)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, indexBuildMemory(c.indexType, in(c.params)))
		})
	}

	// A tantivy writer never buffers more than its budget.
	huge := indexInput{raw: 4 * gib, rows: 1}
	assert.Equal(t, 4*gib+tantivyMemoryBudget, indexBuildMemory("INVERTED", huge))

	// BITMAP without the param falls back to the auto-index cardinality limit.
	limit := paramtable.Get().AutoIndexConfig.BitmapCardinalityLimit.GetAsInt64()
	assert.Equal(t, raw+(rows/8+1)*limit, indexBuildMemory("BITMAP", in(nil)))

	// Zero rows: no per-row term, no division by zero.
	assert.Equal(t, int64(0), indexInput{raw: 10}.bytesPerRow())
}

func TestParams(t *testing.T) {
	params := map[string]string{"i": "7", "neg": "-1", "f": "0.5", "b": "false", "junk": "x"}
	assert.Equal(t, int64(7), intParam(params, "i", 3))
	assert.Equal(t, int64(3), intParam(params, "neg", 3))
	assert.Equal(t, int64(3), intParam(params, "junk", 3))
	assert.Equal(t, 0.5, floatParam(params, "f", 0.1))
	assert.Equal(t, 0.1, floatParam(params, "junk", 0.1))
	assert.False(t, boolParam(params, "b", true))
	assert.True(t, boolParam(params, "junk", true))
}

func TestCorrectStats(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
			{FieldID: 101, DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{kv("enable_match", "true"), kv("enable_analyzer", "true")}},
			{FieldID: 102, DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{kv("enable_match", "true"), kv("enable_analyzer", "true")}},
			{FieldID: 103, DataType: schemapb.DataType_JSON},
			{FieldID: 104, DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{104}}},
	}
	logs := []*datapb.FieldBinlog{
		binlogs(100, 8*mib), binlogs(101, 100*mib), binlogs(102, 50*mib), binlogs(103, 300*mib), binlogs(104, 120*mib),
	}

	text := &workerpb.CreateStatsRequest{SubJobType: indexpb.StatsSubJob_TextIndexJob, Schema: schema, InsertLogs: logs}
	assert.Equal(t, taskcommon.Resource{CPU: estimate.CPU, Memory: tantivyBuild(100*mib) + tantivyBuild(50*mib)}, CorrectStats(text, estimate))

	jsonReq := &workerpb.CreateStatsRequest{SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob, Schema: schema, InsertLogs: logs, JsonKeyStatsTantivyMemory: 64 * mib}
	assert.Equal(t, 2*300*mib+64*mib, CorrectStats(jsonReq, estimate).Memory)

	bm25 := &workerpb.CreateStatsRequest{SubJobType: indexpb.StatsSubJob_BM25Job, Schema: schema, InsertLogs: logs}
	assert.Equal(t, 2*120*mib, CorrectStats(bm25, estimate).Memory)

	sortReq := &workerpb.CreateStatsRequest{
		SubJobType: indexpb.StatsSubJob_Sort, Schema: schema, InsertLogs: logs, NumRows: 1000, BinlogMaxSize: uint64(16 * mib),
		DeltaLogs: []*datapb.FieldBinlog{binlogs(0, 10*mib)},
	}
	assert.Equal(t, 578*mib+1000*rowIndexBytes+16*mib+deleteMapExpansion*10*mib, CorrectStats(sortReq, estimate).Memory)

	// A target field without binlog bytes (V3 after a restart, or a field
	// added later): a partial sum would under-price, so the estimate stands.
	partial := &workerpb.CreateStatsRequest{SubJobType: indexpb.StatsSubJob_TextIndexJob, Schema: schema, InsertLogs: logs[:2]}
	assert.Equal(t, estimate, CorrectStats(partial, estimate))
	// A BM25 output field the schema does not list.
	orphanBM25 := &workerpb.CreateStatsRequest{SubJobType: indexpb.StatsSubJob_BM25Job, InsertLogs: logs, Schema: &schemapb.CollectionSchema{
		Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{104}}},
	}}
	assert.Equal(t, estimate, CorrectStats(orphanBM25, estimate))
	// No target field at all.
	noText := &workerpb.CreateStatsRequest{SubJobType: indexpb.StatsSubJob_TextIndexJob, Schema: &schemapb.CollectionSchema{Fields: schema.Fields[:1]}, InsertLogs: logs}
	assert.Equal(t, estimate, CorrectStats(noText, estimate))
	// Sort without any insert bytes.
	assert.Equal(t, estimate, CorrectStats(&workerpb.CreateStatsRequest{SubJobType: indexpb.StatsSubJob_Sort}, estimate))
	// An unknown sub job.
	assert.Equal(t, estimate, CorrectStats(&workerpb.CreateStatsRequest{SubJobType: indexpb.StatsSubJob_None}, estimate))
}

func TestCorrectAnalyze(t *testing.T) {
	factor := paramtable.Get().DataCoordCfg.TaskResourceAnalyzeMemoryFactor.GetAsFloat()
	req := &workerpb.AnalyzeRequest{
		FieldType: schemapb.DataType_FloatVector, Dim: 128,
		SegmentStats: map[int64]*indexpb.SegmentStats{1: {NumRows: 600}, 2: {NumRows: 400}},
	}
	raw := int64(1000 * 128 * 4)
	assert.Equal(t, max(int64(float64(raw)*factor), minMemory()), CorrectAnalyze(req, estimate).Memory)

	// Down-sampled to the train share of this machine.
	big := &workerpb.AnalyzeRequest{
		FieldType: schemapb.DataType_FloatVector, Dim: 1 << 20, MaxTrainSizeRatio: 0.001,
		SegmentStats: map[int64]*indexpb.SegmentStats{1: {NumRows: 1 << 20}},
	}
	train := int64(float64(hardware.GetMemoryCount()) * 0.001)
	assert.Equal(t, max(int64(float64(train)*factor), minMemory()), CorrectAnalyze(big, estimate).Memory)

	// No rows: nothing exact, estimate stands.
	assert.Equal(t, estimate, CorrectAnalyze(&workerpb.AnalyzeRequest{FieldType: schemapb.DataType_FloatVector, Dim: 8}, estimate))
}

func TestCorrectCompaction(t *testing.T) {
	segment := func(level datapb.SegmentLevel, insert, delta, stats int64) *datapb.CompactionSegmentBinlogs {
		s := &datapb.CompactionSegmentBinlogs{Level: level}
		if insert > 0 {
			s.FieldBinlogs = []*datapb.FieldBinlog{binlogs(100, insert)}
		}
		if delta > 0 {
			s.Deltalogs = []*datapb.FieldBinlog{binlogs(0, delta)}
		}
		if stats > 0 {
			s.Field2StatslogPaths = []*datapb.FieldBinlog{binlogs(100, stats)}
		}
		return s
	}

	t.Run("l0 loads deletes and target bloom filters", func(t *testing.T) {
		plan := &datapb.CompactionPlan{Type: datapb.CompactionType_Level0DeleteCompaction, SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			segment(datapb.SegmentLevel_L0, 0, 100*mib, 0),
			segment(datapb.SegmentLevel_L0, 0, 50*mib, 0),
			segment(datapb.SegmentLevel_L1, 0, 0, 30*mib),
		}}
		factor := paramtable.Get().DataCoordCfg.TaskResourceL0CompactionMemoryFactor.GetAsFloat()
		assert.Equal(t, int64(float64(150*mib)*factor)+30*mib, CorrectCompaction(plan, estimate).Memory)
		plan.SegmentBinlogs = plan.SegmentBinlogs[2:]
		assert.Equal(t, estimate, CorrectCompaction(plan, estimate))
	})

	t.Run("sort retains its input", func(t *testing.T) {
		plan := &datapb.CompactionPlan{Type: datapb.CompactionType_SortCompaction, TotalRows: 1000, SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			segment(datapb.SegmentLevel_L1, 300*mib, 10*mib, 0),
		}}
		binlogMaxSize := paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsInt64()
		assert.Equal(t, 300*mib+1000*rowIndexBytes+binlogMaxSize+deleteMapExpansion*10*mib, CorrectCompaction(plan, estimate).Memory)
		plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{segment(datapb.SegmentLevel_L1, 0, 0, 0)}
		assert.Equal(t, estimate, CorrectCompaction(plan, estimate))
	})

	t.Run("mix streams up to the plan max size", func(t *testing.T) {
		plan := &datapb.CompactionPlan{Type: datapb.CompactionType_MixCompaction, MaxSize: 500 * mib, SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			segment(datapb.SegmentLevel_L1, 200*mib, 5*mib, 0),
			segment(datapb.SegmentLevel_L1, 200*mib, 0, 0),
		}}
		assert.Equal(t, 400*mib+deleteMapExpansion*5*mib, CorrectCompaction(plan, estimate).Memory)
		plan.MaxSize = 300 * mib
		assert.Equal(t, 300*mib+deleteMapExpansion*5*mib, CorrectCompaction(plan, estimate).Memory)
		plan.MaxSize = 0
		assert.Equal(t, 400*mib+deleteMapExpansion*5*mib, CorrectCompaction(plan, estimate).Memory)
		plan.Type = datapb.CompactionType_BumpSchemaVersionCompaction
		assert.Equal(t, 400*mib+deleteMapExpansion*5*mib, CorrectCompaction(plan, estimate).Memory)
		plan.SegmentBinlogs = nil
		assert.Equal(t, estimate, CorrectCompaction(plan, estimate))
	})

	t.Run("clustering buffers up to its machine share", func(t *testing.T) {
		ratio := paramtable.Get().DataNodeCfg.ClusteringCompactionMemoryBufferRatio.GetAsFloat()
		share := int64(float64(hardware.GetMemoryCount()) * ratio)
		small := &datapb.CompactionPlan{Type: datapb.CompactionType_ClusteringCompaction, SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			segment(datapb.SegmentLevel_L1, 100*mib, 0, 0),
		}}
		assert.Equal(t, 100*mib, CorrectCompaction(small, estimate).Memory)
		huge := &datapb.CompactionPlan{Type: datapb.CompactionType_ClusteringCompaction, SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			segment(datapb.SegmentLevel_L1, share+gib, 0, 0),
		}}
		assert.Equal(t, share, CorrectCompaction(huge, estimate).Memory)
		huge.SegmentBinlogs = nil
		assert.Equal(t, estimate, CorrectCompaction(huge, estimate))
	})

	t.Run("other plan types keep the estimate", func(t *testing.T) {
		plan := &datapb.CompactionPlan{Type: datapb.CompactionType_UndefinedCompaction, SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			segment(datapb.SegmentLevel_L1, gib, 0, 0),
		}}
		assert.Equal(t, estimate, CorrectCompaction(plan, estimate))
	})
}

func TestCorrectImport(t *testing.T) {
	pt := paramtable.Get()
	base := pt.DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	factor := pt.DataCoordCfg.TaskResourceImportMemoryFactor.GetAsFloat()

	req := &datapb.ImportRequest{
		Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3},
		Files: []*internalpb.ImportFile{{}, {}, {}},
	}
	perFile := importv2.CalculateImportBufferSize(2, 3, 0)
	want := int64(float64(min(3*perFile, importv2.ImportMemoryLimit())) * factor)
	assert.Equal(t, max(want, minMemory()), CorrectImport(req, estimate).Memory)

	// Many files are bounded by this machine's import memory limit.
	pt.Save(pt.DataNodeCfg.ImportMemoryLimitPercentage.Key, "0.0001")
	defer pt.Reset(pt.DataNodeCfg.ImportMemoryLimitPercentage.Key)
	limit := importv2.ImportMemoryLimit()
	assert.Equal(t, max(int64(float64(limit)*factor), minMemory()), CorrectImport(req, estimate).Memory)
	pt.Reset(pt.DataNodeCfg.ImportMemoryLimitPercentage.Key)

	// An L0 import uses the base buffer per file.
	l0 := &datapb.ImportRequest{
		Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3},
		Files:   []*internalpb.ImportFile{{}, {}},
		Options: []*commonpb.KeyValuePair{kv(importutilv2.L0Import, "true")},
	}
	assert.Equal(t, max(int64(float64(min(2*base, importv2.ImportMemoryLimit()))*factor), minMemory()), CorrectImport(l0, estimate).Memory)

	// No files listed yet: still one buffer.
	empty := &datapb.ImportRequest{Vchannels: []string{"a"}, PartitionIDs: []int64{1}}
	assert.Equal(t, max(int64(float64(min(importv2.CalculateImportBufferSize(1, 1, 0), importv2.ImportMemoryLimit()))*factor), minMemory()),
		CorrectImport(empty, estimate).Memory)
}

func TestCorrectPreImport(t *testing.T) {
	base := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	req := &datapb.PreImportRequest{ImportFiles: []*internalpb.ImportFile{{}, {}, {}, {}, {}}}
	assert.Equal(t, taskcommon.Resource{CPU: estimate.CPU, Memory: max(5*base, minMemory())}, CorrectPreImport(req, estimate))
	assert.Equal(t, max(base, minMemory()), CorrectPreImport(&datapb.PreImportRequest{}, estimate).Memory)
}

// The corrected memory is floored like the estimate.
func TestCorrectedIsFloored(t *testing.T) {
	req := &workerpb.CreateJobRequest{
		Field:       &schemapb.FieldSchema{FieldID: 102, DataType: schemapb.DataType_Int8},
		NumRows:     10,
		IndexParams: []*commonpb.KeyValuePair{kv("index_type", "STL_SORT")},
		InsertLogs:  []*datapb.FieldBinlog{binlogs(102, 10)},
	}
	assert.Equal(t, minMemory(), CorrectIndex(req, estimate).Memory)
}
