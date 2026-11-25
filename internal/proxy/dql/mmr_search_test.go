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

package dql

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func mmrTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "mmr_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
			{FieldID: 102, Name: "tag", DataType: schemapb.DataType_Int64},
		},
	}
}

func mmrFunctionScore(params ...*commonpb.KeyValuePair) *schemapb.FunctionScore {
	return &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{
		Name:   "mmr",
		Type:   schemapb.FunctionType_Rerank,
		Params: append([]*commonpb.KeyValuePair{{Key: "reranker", Value: "mmr"}}, params...),
	}}}
}

func TestSearchTask_MMRInitSearchRequest(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	schemaInfo := mustNewSchemaInfo(mmrTestSchema())

	newRequest := func(limit, offset int, extra ...*commonpb.KeyValuePair) *milvuspb.SearchRequest {
		return &milvuspb.SearchRequest{
			CollectionName: "mmr_collection",
			OutputFields:   []string{"tag"},
			SearchParams: append([]*commonpb.KeyValuePair{
				{Key: AnnsFieldKey, Value: "vec"},
				{Key: TopKKey, Value: strconv.Itoa(limit)},
				{Key: OffsetKey, Value: strconv.Itoa(offset)},
				{Key: common.MetricTypeKey, Value: metric.COSINE},
				{Key: ParamsKey, Value: `{"nprobe": 10}`},
			}, extra...),
			SearchInput:   &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: nil},
			FunctionScore: mmrFunctionScore(),
		}
	}
	newTask := func(request *milvuspb.SearchRequest) *SearchTask {
		translated, _, _, _, _, err := translateOutputFields(request.GetOutputFields(), schemaInfo, true)
		require.NoError(t, err)
		outputIDs, err := getOutputFieldIDs(schemaInfo, translated)
		require.NoError(t, err)
		return &SearchTask{
			ctx:            ctx,
			collectionName: request.GetCollectionName(),
			SearchRequest: &internalpb.SearchRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Search},
				CollectionID:   1,
				PartitionIDs:   []int64{1},
				DslType:        commonpb.DslType_BoolExprV1,
				OutputFieldsId: outputIDs,
			},
			request:                request,
			schema:                 schemaInfo,
			translatedOutputFields: translated,
			tr:                     timerecord.NewTimeRecorder("test"),
			queryInfos:             []*planpb.QueryInfo{{}},
		}
	}

	t.Run("window retrieval", func(t *testing.T) {
		task := newTask(newRequest(5, 3))
		require.NoError(t, task.initSearchRequest(ctx))

		// The ANN field becomes the default MMR input.
		assert.Equal(t, []string{"vec"}, task.rerankMeta.GetInputFieldNames())
		require.NotNil(t, task.mmrPage)
		assert.Equal(t, mmrPage{window: 100, limit: 5, offset: 3}, *task.mmrPage)

		// QueryNode returns the window from rank 0; reduce applies no offset.
		assert.Equal(t, int64(100), task.GetTopk())
		assert.Equal(t, int64(0), task.GetOffset())
		plan := &planpb.PlanNode{}
		require.NoError(t, proto.Unmarshal(task.SerializedExprPlan, plan))
		assert.Equal(t, int64(100), plan.GetVectorAnns().GetQueryInfo().GetTopk())

		// Vectors and output fields come from one requery before rerank.
		assert.True(t, task.needRequery)
		assert.Empty(t, plan.GetOutputFieldIds())
		assert.ElementsMatch(t, []string{"tag", "vec"}, task.requeryOutputFields())

		p, err := newBuiltInPipeline(task)
		require.NoError(t, err)
		assert.Equal(t, searchWithRequeryAndRerankByFieldDataPipe.name, p.name)

		op, err := newRerankOperator(task, nil)
		require.NoError(t, err)
		rr := op.(*rerankOperator)
		assert.Equal(t, int64(5), rr.topK)
		assert.Equal(t, int64(3), rr.offset)
		assert.Equal(t, []string{"vec"}, rr.annsFields)
	})

	t.Run("explicit candidate size", func(t *testing.T) {
		request := newRequest(10, 0)
		request.FunctionScore = mmrFunctionScore(&commonpb.KeyValuePair{Key: "candidate_size", Value: "40"})
		task := newTask(request)
		require.NoError(t, task.initSearchRequest(ctx))
		assert.Equal(t, int64(40), task.GetTopk())
	})

	t.Run("expected rows", func(t *testing.T) {
		assert.Equal(t, int64(5), (&mmrPage{window: 20, limit: 5, offset: 0}).expectedRows())
		assert.Equal(t, int64(2), (&mmrPage{window: 20, limit: 5, offset: 18}).expectedRows())
		assert.Equal(t, int64(0), (&mmrPage{window: 20, limit: 5, offset: 30}).expectedRows())
	})

	t.Run("rejections", func(t *testing.T) {
		request := newRequest(10, 0)
		request.FunctionScore = mmrFunctionScore(&commonpb.KeyValuePair{Key: "candidate_size", Value: "5"})
		err := newTask(request).initSearchRequest(ctx)
		assert.ErrorContains(t, err, "must not exceed candidate_size")

		err = newTask(newRequest(5, 0, &commonpb.KeyValuePair{Key: GroupByFieldKey, Value: "tag"})).initSearchRequest(ctx)
		assert.ErrorContains(t, err, "group_by")

		err = newTask(newRequest(5, 0, &commonpb.KeyValuePair{Key: IteratorField, Value: "true"})).initSearchRequest(ctx)
		assert.ErrorContains(t, err, "iterator")

		// The window becomes the QueryNode top-k, so it obeys the top-k limit.
		key := paramtable.Get().QuotaConfig.TopKLimit.Key
		require.NoError(t, paramtable.Get().Save(key, "50"))
		err = newTask(newRequest(5, 0)).initSearchRequest(ctx)
		require.NoError(t, paramtable.Get().Reset(key))
		assert.ErrorContains(t, err, "candidate_size [100] is invalid")

		request = newRequest(5, 0)
		request.FunctionScore = mmrFunctionScore(&commonpb.KeyValuePair{Key: "lambda", Value: "2"})
		err = newTask(request).initSearchRequest(ctx)
		assert.ErrorContains(t, err, "lambda")
	})

	t.Run("function chains window", func(t *testing.T) {
		chainRequest := func(candidateSize int64) *milvuspb.SearchRequest {
			request := newRequest(5, 2)
			request.FunctionScore = nil
			request.FunctionChains = []*schemapb.FunctionChain{l2FunctionChain(&schemapb.FunctionChainOp{
				Op:     "mmr",
				Inputs: []string{"vec"},
				Params: map[string]*schemapb.FunctionParamValue{
					"candidate_size": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: candidateSize}},
				},
			})}
			return request
		}
		task := newTask(chainRequest(30))
		require.NoError(t, task.initSearchRequest(ctx))
		assert.Equal(t, mmrPage{window: 30, limit: 5, offset: 2}, *task.mmrPage)
		assert.Equal(t, int64(30), task.GetTopk())
		op, err := newRerankOperator(task, nil)
		require.NoError(t, err)
		assert.True(t, op.(*rerankOperator).useRequestLimit)

		// The window is bounded before it becomes the QueryNode top-k.
		err = newTask(chainRequest(5_000_000)).initSearchRequest(ctx)
		assert.ErrorContains(t, err, "candidate_size must be in")
		err = newTask(chainRequest(3)).initSearchRequest(ctx)
		assert.ErrorContains(t, err, "must not exceed candidate_size")
	})

	t.Run("other rerankers keep reduce paging", func(t *testing.T) {
		request := newRequest(5, 3)
		request.FunctionScore = &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{
			Type:            schemapb.FunctionType_Rerank,
			InputFieldNames: []string{"tag"},
			Params: []*commonpb.KeyValuePair{
				{Key: "reranker", Value: "decay"}, {Key: "origin", Value: "0"}, {Key: "scale", Value: "10"},
			},
		}}}
		task := newTask(request)
		require.NoError(t, task.initSearchRequest(ctx))
		assert.Nil(t, task.mmrPage)
		assert.Equal(t, int64(8), task.GetTopk())
		assert.Equal(t, int64(3), task.GetOffset())
	})
}

// mmrSearchResult builds one QueryNode result with ids 1..n, scores
// descending.
func mmrSearchResult(n int) *internalpb.SearchResults {
	ids := make([]int64, n)
	scores := make([]float32, n)
	for i := range ids {
		ids[i] = int64(i + 1)
		scores[i] = 0.95 - 0.01*float32(i)
	}
	data := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       int64(n),
		Topks:      []int64{int64(n)},
		Scores:     scores,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}},
	}
	blob, _ := proto.Marshal(data)
	return &internalpb.SearchResults{
		Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		MetricType:     metric.COSINE,
		NumQueries:     1,
		TopK:           int64(n),
		SlicedBlob:     blob,
		SlicedNumCount: 1,
	}
}

// mmrRequeryFields returns pk, vec and tag for ids 1..n: ids 1-5 point one
// way, ids 6-n the other, and tag = 10*id.
func mmrRequeryFields(n int) []*schemapb.FieldData {
	pks := make([]int64, n)
	tags := make([]int64, n)
	vecs := make([]float32, 0, 2*n)
	for i := range pks {
		pks[i] = int64(i + 1)
		tags[i] = int64(10 * (i + 1))
		if i < 5 {
			vecs = append(vecs, 1, 0.01*float32(i))
		} else {
			vecs = append(vecs, 0.01*float32(i), 1)
		}
	}
	long := func(id int64, name string, v []int64) *schemapb.FieldData {
		return &schemapb.FieldData{FieldId: id, FieldName: name, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: v}}},
		}}
	}
	return []*schemapb.FieldData{
		long(100, "pk", pks),
		{FieldId: 101, FieldName: "vec", Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vecs}},
		}}},
		long(102, "tag", tags),
	}
}

func TestSearchWithRequeryAndRerankByFieldDataPipe_MMR(t *testing.T) {
	paramtable.Init()
	schema := mmrTestSchema()
	funcScore := mmrFunctionScore(
		&commonpb.KeyValuePair{Key: "candidate_size", Value: "8"},
		&commonpb.KeyValuePair{Key: "lambda", Value: "0.3"},
	)
	funcScore.Functions[0].InputFieldNames = []string{"vec"}

	mockTest(t, (*requeryOperator).requery, &milvuspb.QueryResults{FieldsData: mmrRequeryFields(10)}, segcore.StorageCost{}, nil)

	run := func(limit, offset int64) *milvuspb.SearchResults {
		task := &SearchTask{
			ctx:            context.Background(),
			collectionName: "mmr_collection",
			SearchRequest: &internalpb.SearchRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Search, Timestamp: uint64(time.Now().UnixNano())},
				MetricType:   metric.COSINE,
				Topk:         8,
				Nq:           1,
				PartitionIDs: []int64{1},
				CollectionID: 1,
				DbID:         1,
				FieldId:      101,
			},
			schema: &schemaInfo{
				CollectionSchema: schema,
				PkField:          schema.GetFields()[0],
			},
			queryInfos:             []*planpb.QueryInfo{{QueryFieldId: 101, Topk: 8, RoundDecimal: -1}},
			translatedOutputFields: []string{"tag"},
			rerankMeta:             mustNewRerankMeta(t, schema, funcScore),
			mmrPage:                &mmrPage{window: 8, limit: limit, offset: offset},
			needRequery:            true,
			request:                &milvuspb.SearchRequest{},
		}
		p, err := newPipeline(searchWithRequeryAndRerankByFieldDataPipe, task)
		require.NoError(t, err)
		require.NoError(t, p.AddNodes(task, endNode))
		span := noop.NewTracerProvider().Tracer("test")
		_, sp := span.Start(context.Background(), "test")
		result, _, err := p.Run(context.Background(), sp, []*internalpb.SearchResults{mmrSearchResult(8)}, segcore.StorageCost{})
		require.NoError(t, err)
		return result
	}
	ids := func(r *milvuspb.SearchResults) []int64 { return r.GetResults().GetIds().GetIntId().GetData() }

	full := run(8, 0)
	order := ids(full)
	require.Len(t, order, 8)
	// The best row of the second direction is promoted to second place.
	assert.Equal(t, []int64{1, 6}, order[:2])
	for _, id := range order {
		assert.LessOrEqual(t, id, int64(8))
	}

	// Output fields are aligned with the MMR order and exclude the vector.
	require.Len(t, full.GetResults().GetFieldsData(), 1)
	tag := full.GetResults().GetFieldsData()[0]
	assert.Equal(t, "tag", tag.GetFieldName())
	for i, id := range order {
		assert.Equal(t, 10*id, tag.GetScalars().GetLongData().GetData()[i])
	}
	// Each row keeps its search score.
	for i, id := range order {
		assert.InDelta(t, 0.95-0.01*float64(id-1), full.GetResults().GetScores()[i], 1e-6)
	}

	assert.Equal(t, order[0:3], ids(run(3, 0)))
	assert.Equal(t, order[3:6], ids(run(3, 3)))
	assert.Equal(t, order[6:8], ids(run(3, 6)))
	assert.Empty(t, ids(run(3, 9)))
}

func TestMMRInputFields(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{
		Name: "mmr_inputs",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}}},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 200, Name: "chunks",
			Fields: []*schemapb.FieldSchema{{
				FieldID: 201, Name: "chunks[emb]", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			}},
		}},
		Functions: []*schemapb.FunctionSchema{{
			Name: "bm25", Type: schemapb.FunctionType_BM25,
			InputFieldIds: []int64{101}, InputFieldNames: []string{"text"},
			OutputFieldIds: []int64{102}, OutputFieldNames: []string{"sparse"},
		}},
	}
	withInput := func(name string) *schemapb.FunctionScore {
		fs := mmrFunctionScore()
		fs.Functions[0].InputFieldNames = []string{name}
		return fs
	}

	// BM25 outputs cannot be read back, so MMR rejects them before searching.
	_, err := newRerankMeta(schema, withInput("sparse"))
	assert.ErrorContains(t, err, "BM25")

	// A struct-array vector sub-field is a valid input and is requeried by its
	// flattened name, which output-field translation accepts.
	meta, err := newRerankMeta(schema, withInput("chunks[emb]"))
	require.NoError(t, err)
	assert.Equal(t, []int64{201}, meta.GetInputFieldIDs())
	translated, _, _, _, _, err := translateOutputFields([]string{"chunks[emb]"}, mustNewSchemaInfo(schema), false)
	require.NoError(t, err)
	assert.Contains(t, translated, "chunks[emb]")

	// Other rerankers still reject vector inputs.
	decay := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{
		Type: schemapb.FunctionType_Rerank, InputFieldNames: []string{"chunks[emb]"},
		Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "decay"}},
	}}}
	_, err = newRerankMeta(schema, decay)
	assert.ErrorContains(t, err, "unsupported field type")
}

// Element-level search can return several hits of one row (one per matched
// element). MMR sees the same stored vector for both and pushes the second
// hit behind the other rows; element indices stay aligned with the hits.
func TestSearchWithRequeryAndRerankByFieldDataPipe_MMRElementLevel(t *testing.T) {
	paramtable.Init()
	schema := mmrTestSchema()
	funcScore := mmrFunctionScore(&commonpb.KeyValuePair{Key: "candidate_size", Value: "8"})
	funcScore.Functions[0].InputFieldNames = []string{"vec"}

	long := func(id int64, name string, v ...int64) *schemapb.FieldData {
		return &schemapb.FieldData{FieldId: id, FieldName: name, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: v}}},
		}}
	}
	mockTest(t, (*requeryOperator).requery, &milvuspb.QueryResults{FieldsData: []*schemapb.FieldData{
		long(100, "pk", 1, 2, 3, 4),
		{FieldId: 101, FieldName: "vec", Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 0, 0, 1, -1, 0, 0, -1}}},
		}}},
		long(102, "tag", 10, 20, 30, 40),
	}}, segcore.StorageCost{}, nil)

	// Hits: (pk, element) = (1,0) (1,1) (2,0) (2,1) (3,0) (3,1) (4,0) (4,1).
	data := &schemapb.SearchResultData{
		NumQueries:     1,
		TopK:           8,
		Topks:          []int64{8},
		Scores:         []float32{0.95, 0.94, 0.93, 0.92, 0.91, 0.90, 0.89, 0.88},
		Ids:            &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 1, 2, 2, 3, 3, 4, 4}}}},
		ElementIndices: &schemapb.LongArray{Data: []int64{0, 1, 0, 1, 0, 1, 0, 1}},
	}
	blob, _ := proto.Marshal(data)
	qn := &internalpb.SearchResults{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, MetricType: metric.COSINE,
		NumQueries: 1, TopK: 8, SlicedBlob: blob, SlicedNumCount: 1,
	}

	task := &SearchTask{
		ctx: context.Background(),
		SearchRequest: &internalpb.SearchRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Search, Timestamp: uint64(time.Now().UnixNano())},
			Topk:         8,
			Nq:           1,
			FieldId:      101,
			DbID:         1,
			PartitionIDs: []int64{1},
		},
		schema:                 &schemaInfo{CollectionSchema: schema, PkField: schema.GetFields()[0]},
		queryInfos:             []*planpb.QueryInfo{{QueryFieldId: 101, Topk: 8, RoundDecimal: -1}},
		translatedOutputFields: []string{"tag"},
		rerankMeta:             mustNewRerankMeta(t, schema, funcScore),
		mmrPage:                &mmrPage{window: 8, limit: 8, offset: 0},
		needRequery:            true,
		request:                &milvuspb.SearchRequest{},
	}
	p, err := newPipeline(searchWithRequeryAndRerankByFieldDataPipe, task)
	require.NoError(t, err)
	require.NoError(t, p.AddNodes(task, endNode))
	_, sp := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	result, _, err := p.Run(context.Background(), sp, []*internalpb.SearchResults{qn}, segcore.StorageCost{})
	require.NoError(t, err)

	ids := result.GetResults().GetIds().GetIntId().GetData()
	require.Len(t, ids, 8)
	// Each row's best hit comes before any second hit of a row.
	assert.ElementsMatch(t, []int64{1, 2, 3, 4}, ids[:4])
	assert.Equal(t, int64(1), ids[0])
	assert.ElementsMatch(t, []int64{1, 2, 3, 4}, ids[4:])
	elems := result.GetResults().GetElementIndices().GetData()
	require.Len(t, elems, 8)
	assert.Equal(t, []int64{0, 0, 0, 0, 1, 1, 1, 1}, elems)
	tags := result.GetResults().GetFieldsData()[0].GetScalars().GetLongData().GetData()
	for i, id := range ids {
		assert.Equal(t, 10*id, tags[i])
	}
}

// The requery replaces the search output fields, so it must also fetch the
// fields that highlighting reads.
func TestRequeryOutputFieldsIncludeHighlightDependencies(t *testing.T) {
	task := &SearchTask{
		schema:                 mustNewSchemaInfo(mmrTestSchema()),
		translatedOutputFields: []string{"pk"},
		highlighter:            &mockHighlighter{requiredFieldIDs: []int64{102}, dynamicFieldNames: []string{"dyn"}},
	}
	assert.ElementsMatch(t, []string{"pk", "tag", "dyn"}, task.requeryOutputFields())
}
