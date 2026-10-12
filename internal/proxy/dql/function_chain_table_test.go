// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func postTableTestPlan(t *testing.T, ann bool, ops ...*schemapb.FunctionChainOp) *schemapb.FunctionChain {
	t.Helper()
	paramtable.Init()
	plan, err := transform.PostTable("table", ops...)
	require.NoError(t, err)
	if ann {
		plan.PostProcessOptions.Input = schemapb.PostProcessInput_PostProcessInputAnnInputs
	}
	return plan
}

func postTableCountOp(keys ...string) *schemapb.FunctionChainOp {
	keyValues := make([]*schemapb.FunctionParamValue, len(keys))
	for index, key := range keys {
		keyValues[index] = chainStringParam(key)
	}
	return &schemapb.FunctionChainOp{
		Op: "aggregate", Inputs: append([]string(nil), keys...), Outputs: append(append([]string(nil), keys...), "count"),
		Params: map[string]*schemapb.FunctionParamValue{
			"keys": chainArrayParam(keyValues...),
			"aggregates": chainArrayParam(&schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ObjectValue{
				ObjectValue: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{"op": chainStringParam("count")}},
			}}),
		},
	}
}

func decodePostTableTest(t *testing.T, result *schemapb.FunctionChainTableResult) arrow.Table {
	t.Helper()
	require.NotNil(t, result)
	require.NotEmpty(t, result.GetArrowIpc())
	reader, err := ipc.NewReader(bytes.NewReader(result.GetArrowIpc()))
	require.NoError(t, err)
	defer reader.Release()
	schema := reader.Schema()
	require.NotNil(t, schema)
	records := make([]arrow.Record, 0)
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()
	for reader.Next() {
		record := reader.Record()
		record.Retain()
		records = append(records, record)
	}
	require.NoError(t, reader.Err())
	table := array.NewTableFromRecords(schema, records)
	t.Cleanup(table.Release)
	require.Equal(t, result.GetRowCount(), table.NumRows())
	return table
}

func postTableIntColumn(t *testing.T, table arrow.Table, name string) []int64 {
	t.Helper()
	indexes := table.Schema().FieldIndices(name)
	require.Len(t, indexes, 1)
	values := make([]int64, 0, table.NumRows())
	for _, chunk := range table.Column(indexes[0]).Data().Chunks() {
		values = append(values, chunk.(*array.Int64).Int64Values()...)
	}
	return values
}

func postTableStringField(id int64, name string, values ...string) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: id, FieldName: name, Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}},
	}
}

func postTablePKField(values ...int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: 100, FieldName: "pk", Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}},
	}
}

func TestSearchPostTableCrossNQCountAndEmptyGroups(t *testing.T) {
	for _, test := range []struct {
		name        string
		topks, want []int64
		unionAll    bool
	}{
		{name: "per query", topks: []int64{2, 0, 1}, want: []int64{2, 0, 1}},
		{name: "cross query", topks: []int64{2, 0, 1}, unionAll: true, want: []int64{3}},
		{name: "all empty", topks: []int64{0, 0, 0}, unionAll: true, want: []int64{0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ops := []*schemapb.FunctionChainOp{postTableCountOp()}
			if test.unionAll {
				ops = append([]*schemapb.FunctionChainOp{{Op: "union_all"}}, ops...)
			}
			plan := postTableTestPlan(t, false, ops...)
			task := transformTestTask(&milvuspb.SearchRequest{Nq: int64(len(test.topks)), FunctionChains: []*schemapb.FunctionChain{plan}})
			require.NoError(t, task.prepareSearchTransforms())
			result := transformTestResults(test.topks)
			require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
			require.Nil(t, result.GetResults())
			table := decodePostTableTest(t, result.GetPostProcessResult())
			require.Equal(t, test.want, postTableIntColumn(t, table, "count"))
			require.Len(t, table.Schema().Fields(), 1)
		})
	}
}

func TestSearchPostTableProvenanceAndTypedZeroRows(t *testing.T) {
	plan := postTableTestPlan(t, false, transformTestSelect(t, "tag", types.IDFieldName, postQueryIndexColumn, postANNIndexColumn))
	task := transformTestTask(&milvuspb.SearchRequest{Nq: 3, FunctionChains: []*schemapb.FunctionChain{plan}})
	require.NoError(t, task.prepareSearchTransforms())
	result := transformTestResults([]int64{1, 0, 1})
	require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
	table := decodePostTableTest(t, result.GetPostProcessResult())
	require.Equal(t, []int64{0, 2}, postTableIntColumn(t, table, postQueryIndexColumn))
	require.Equal(t, []int64{-1, -1}, postTableIntColumn(t, table, postANNIndexColumn))
	result = transformTestResults([]int64{0, 0, 0})
	require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
	empty := decodePostTableTest(t, result.GetPostProcessResult())
	require.Zero(t, empty.NumRows())
	require.Equal(t, arrow.BinaryTypes.String, empty.Schema().Field(0).Type)
}

type postTableQueryRunner struct {
	*namespaceRequeryMockNode
	execute func(context.Context, taskmodel.Task, trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error)
}

func (n *postTableQueryRunner) ExecuteQuery(ctx context.Context, task taskmodel.Task, span trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
	return n.execute(ctx, task, span)
}

func postTableHybridTask(t *testing.T, plan *schemapb.FunctionChain, schemas ...*schemaInfo) *SearchTask {
	t.Helper()
	task := transformTestTask(&milvuspb.SearchRequest{
		Nq:             2,
		FunctionChains: []*schemapb.FunctionChain{plan}, SubReqs: []*milvuspb.SubSearchRequest{{}, {}},
	})
	if len(schemas) > 0 {
		task.schema = schemas[0]
	}
	task.ctx = context.Background()
	task.Base = &commonpb.MsgBase{}
	task.SearchRequest.SubReqs = []*internalpb.SubSearchRequest{{Nq: 2, Topk: 3}, {Nq: 2, Topk: 3}}
	task.queryInfos = []*planpb.QueryInfo{{Topk: 3, GroupByFieldId: -1}, {Topk: 3, GroupByFieldId: -1}}
	task.rankParams = &rankParams{limit: 1, offset: 1, roundDecimal: -1}
	require.NoError(t, task.prepareSearchTransforms())
	return task
}

func postTableHybridSource(t *testing.T) []*internalpb.SearchResults {
	t.Helper()
	first := transformTestResults([]int64{2, 0}).GetResults()
	first.TopK, first.AllSearchCount = 3, 7
	first.Ids.GetIntId().Data, first.Scores = []int64{10, 20}, []float32{0.9, 0.8}
	second := transformTestResults([]int64{1, 1}).GetResults()
	second.TopK, second.AllSearchCount = 3, 11
	second.Ids.GetIntId().Data, second.Scores = []int64{10, 30}, []float32{-0.4, -0.6}
	return []*internalpb.SearchResults{{IsAdvanced: true, SubResults: []*internalpb.SubSearchResults{
		{ReqIndex: 0, MetricType: "IP", NumQueries: 2, TopK: 3, ResultData: first},
		{ReqIndex: 1, MetricType: "L2", NumQueries: 2, TopK: 3, ResultData: second},
	}}}
}

func TestHybridANNPostTableFetchesEveryCandidateOnceAndKeepsScores(t *testing.T) {
	plan := postTableTestPlan(t, true, &schemapb.FunctionChainOp{Op: "union_all"},
		transformTestSelect(t, "tag", types.IDFieldName, types.ScoreFieldName, postQueryIndexColumn, postANNIndexColumn))
	task := postTableHybridTask(t, plan)
	task.needRequery = true
	task.queryChannelsTs = map[string]uint64{"channel": 123}
	task.queryChannelsNode = typeutil.NewConcurrentMap[string, int64]()
	task.queryChannelsNode.Insert("channel", 9)
	calls := 0
	task.node = &postTableQueryRunner{
		namespaceRequeryMockNode: &namespaceRequeryMockNode{},
		execute: func(ctx context.Context, task taskmodel.Task, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			calls++
			query := task.(*QueryTask)
			require.Equal(t, []string{"tag"}, query.Request().GetOutputFields())
			require.Equal(t, map[string]Timestamp{"channel": 123}, query.channelsMvcc)
			require.Equal(t, map[string]int64{"channel": 9}, query.preferredNodes)
			ids := query.plan.GetQuery().GetPredicates().GetTermExpr().GetValues()
			actual := make([]int64, len(ids))
			for index, id := range ids {
				actual[index] = id.GetInt64Val()
			}
			require.ElementsMatch(t, []int64{10, 20, 30}, actual)
			return &milvuspb.QueryResults{Status: merr.Success(), FieldsData: []*schemapb.FieldData{
				postTablePKField(30, 10, 20), postTableStringField(102, "tag", "C", "A", "B"),
			}}, segcore.StorageCost{ScannedTotalBytes: 50}, nil
		},
	}
	pipeline, err := newSearchPipeline(task)
	require.NoError(t, err)
	for _, node := range pipeline.nodes {
		require.NotEqual(t, rerankOp, node.opName)
		require.NotEqual(t, elementBestCollapseOp, node.opName)
	}
	result, cost, err := pipeline.Run(context.Background(), trace.SpanFromContext(context.Background()), postTableHybridSource(t), segcore.StorageCost{ScannedTotalBytes: 10})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, int64(60), cost.ScannedTotalBytes)
	require.Nil(t, result.GetResults())
	table := decodePostTableTest(t, result.GetPostProcessResult())
	require.Equal(t, []int64{10, 20, 10, 30}, postTableIntColumn(t, table, types.IDFieldName))
	require.Equal(t, []int64{0, 0, 1, 1}, postTableIntColumn(t, table, postANNIndexColumn))
	require.Equal(t, []int64{0, 0, 0, 1}, postTableIntColumn(t, table, postQueryIndexColumn))
	scores := table.Column(2).Data().Chunk(0).(*array.Float32).Float32Values()
	require.Equal(t, []float32{0.9, 0.8, 0.4, 0.6}, scores)
	tags := table.Column(0).Data().Chunk(0).(*array.String)
	require.Equal(t, []string{"A", "B", "A", "C"}, []string{tags.Value(0), tags.Value(1), tags.Value(2), tags.Value(3)})
	require.Equal(t, int64(18), task.AllSearchCount())
	require.Equal(t, int64(2), task.ResultQueryCount())
}

func TestPostTableModeRejectsIncompatibleRequestsBeforeExecution(t *testing.T) {
	plan := postTableTestPlan(t, true, transformTestSelect(t, types.IDFieldName))
	for _, test := range []struct {
		name    string
		request *milvuspb.SearchRequest
	}{
		{"ordinary ANN_INPUTS", &milvuspb.SearchRequest{FunctionChains: []*schemapb.FunctionChain{plan}}},
		{"function score", &milvuspb.SearchRequest{SubReqs: []*milvuspb.SubSearchRequest{{}}, FunctionChains: []*schemapb.FunctionChain{plan}, FunctionScore: &schemapb.FunctionScore{}}},
		{"L2", &milvuspb.SearchRequest{SubReqs: []*milvuspb.SubSearchRequest{{}}, FunctionChains: []*schemapb.FunctionChain{plan, l2LimitFunctionChain(1)}}},
		{"legacy ranker", &milvuspb.SearchRequest{SubReqs: []*milvuspb.SubSearchRequest{{}}, FunctionChains: []*schemapb.FunctionChain{plan}, SearchParams: []*commonpb.KeyValuePair{{Key: RankTypeKey, Value: "rrf"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := transformTestTask(test.request)
			err := task.prepareSearchTransforms()
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Equal(t, merr.InputError, merr.GetErrorType(err))
			require.Nil(t, task.postProcess)
		})
	}
}

func TestHybridFusedPostTableKeepsL2LimitBeforeAggregation(t *testing.T) {
	plan := postTableTestPlan(t, false, &schemapb.FunctionChainOp{Op: "union_all"}, postTableCountOp())
	task := postTableHybridTask(t, plan)
	task.request.FunctionChains = append(task.request.FunctionChains, l2FunctionChain(
		&schemapb.FunctionChainOp{Op: types.OpTypeMerge, Params: map[string]*schemapb.FunctionParamValue{"strategy": chainStringParam("rrf")}},
		l2LimitFunctionChain(1).GetOps()[0],
	))
	var err error
	task.rerankMeta, err = selectHybridRerankMeta(task.request, task.schema)
	require.NoError(t, err)
	pipeline, err := newSearchPipeline(task)
	require.NoError(t, err)
	result, _, err := pipeline.Run(context.Background(), trace.SpanFromContext(context.Background()), postTableHybridSource(t), segcore.StorageCost{})
	require.NoError(t, err)
	require.Nil(t, result.GetResults())
	table := decodePostTableTest(t, result.GetPostProcessResult())
	require.Equal(t, []int64{2}, postTableIntColumn(t, table, "count"), "one L2-limited candidate for each of the two queries")
	require.Equal(t, int64(18), task.AllSearchCount())
}

func TestPostTableConsumesStoredStructVectorArray(t *testing.T) {
	schema := proto.Clone(newFunctionChainTestSchema().CollectionSchema).(*schemapb.CollectionSchema)
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Name: "documents", Fields: []*schemapb.FieldSchema{
		{FieldID: 201, Name: "documents[embedding]", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
	}}}
	for _, rows := range []int64{0, 3} {
		plan := postTableTestPlan(t, false,
			&schemapb.FunctionChainOp{Op: "explode", Inputs: []string{"documents"}, Outputs: []string{"document"}},
			&schemapb.FunctionChainOp{Op: "select", Inputs: []string{"document.embedding"}, Outputs: []string{"embedding"}},
		)
		task := transformTestTask(&milvuspb.SearchRequest{Nq: 1, FunctionChains: []*schemapb.FunctionChain{plan}})
		task.schema = mustNewSchemaInfo(schema)
		require.NoError(t, task.prepareSearchTransforms())
		require.Contains(t, task.postProcess.inputIDs, int64(201))
		result := transformTestResults([]int64{rows})
		vectors := make([]*schemapb.VectorField, rows)
		for index := range vectors {
			vectors[index] = &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}
		}
		result.Results.FieldsData = append(result.Results.FieldsData, &schemapb.FieldData{
			FieldId: 201, Type: schemapb.DataType_ArrayOfVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 2, ElementType: schemapb.DataType_FloatVector, Data: vectors}}}},
		})
		batch, err := task.buildPostTableInput(context.Background(), result, -1, false)
		require.NoError(t, err)
		processed, err := executePostTable(context.Background(), task.postProcess, batch)
		chain.ReleaseArrowBatch(batch)
		require.NoError(t, err)
		table := decodePostTableTest(t, processed)
		require.Equal(t, rows*2, table.NumRows())
		require.Equal(t, "embedding", table.Schema().Field(0).Name)
		table.Release()
	}
}

func TestHybridANNPostTablePreservesFetchFailureCodes(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		fields []*schemapb.FieldData
		want   error
	}{
		{name: "inconsistent requery", err: merr.WrapErrInconsistentRequery("candidate disappeared"), want: merr.ErrInconsistentRequery},
		{name: "service unavailable", err: merr.Wrapf(merr.ErrServiceUnavailable, "fetch service is not ready"), want: merr.ErrServiceUnavailable},
		{name: "missing fetched primary key", fields: []*schemapb.FieldData{postTableStringField(102, "tag", "A")}, want: merr.ErrServiceInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := postTableTestPlan(t, true, transformTestSelect(t, "tag"))
			task := postTableHybridTask(t, plan)
			task.needRequery = true
			task.node = &postTableQueryRunner{
				namespaceRequeryMockNode: &namespaceRequeryMockNode{},
				execute: func(context.Context, taskmodel.Task, trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
					return &milvuspb.QueryResults{Status: merr.Success(), FieldsData: test.fields}, segcore.StorageCost{}, test.err
				},
			}
			pipeline, err := newSearchPipeline(task)
			require.NoError(t, err)
			_, _, err = pipeline.Run(context.Background(), trace.SpanFromContext(context.Background()), postTableHybridSource(t), segcore.StorageCost{})
			require.ErrorIs(t, err, test.want)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			require.Equal(t, merr.Code(test.want), merr.Status(err).GetCode())
			require.Nil(t, task.Result())
		})
	}
}

func TestQueryPostTableUsesReducedPaginatedRowsAndHiddenFields(t *testing.T) {
	plan := postTableTestPlan(t, false, transformTestSelect(t, "tag", types.IDFieldName, postQueryIndexColumn, postANNIndexColumn))
	task := &QueryTask{
		ctx: context.Background(), schema: newFunctionChainTestSchema(),
		request:         &milvuspb.QueryRequest{Expr: "pk > 0", FunctionChains: []*schemapb.FunctionChain{plan}, OutputFields: []string{"vec"}},
		queryParams:     &queryParams{limit: 1, offset: 1, reduceType: reduce.IReduceNoOrder},
		RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}, OutputFieldsId: []int64{100}},
	}
	require.NoError(t, task.prepareQueryPostProcess())
	require.NoError(t, task.createPlan(context.Background()))
	require.ElementsMatch(t, []int64{100, 102, common.TimeStampField}, task.GetOutputFieldsId())
	require.NotContains(t, task.GetOutputFieldsId(), int64(103))
	task.resultBuf = typeutil.NewConcurrentSet[*internalpb.RetrieveResults]()
	task.resultBuf.Insert(&internalpb.RetrieveResults{Ids: testSearchResultIDs(10, 20, 30), FieldsData: []*schemapb.FieldData{
		postTablePKField(10, 20, 30), postTableStringField(102, "tag", "one", "two", "three"),
	}})
	require.NoError(t, task.PostExecute(context.Background()))
	require.Empty(t, task.Result().GetFieldsData())
	require.Empty(t, task.Result().GetPrimaryFieldName())
	table := decodePostTableTest(t, task.Result().GetPostProcessResult())
	require.Equal(t, []int64{20}, postTableIntColumn(t, table, types.IDFieldName))
	require.Equal(t, []int64{-1}, postTableIntColumn(t, table, postQueryIndexColumn))
	require.Equal(t, []int64{-1}, postTableIntColumn(t, table, postANNIndexColumn))
	require.Equal(t, "two", table.Column(0).Data().Chunk(0).(*array.String).Value(0))
}

func TestQueryPostTableEmptyResultStillExecutesAggregate(t *testing.T) {
	plan := postTableTestPlan(t, false, postTableCountOp())
	task := &QueryTask{
		ctx: context.Background(), schema: newFunctionChainTestSchema(),
		request:         &milvuspb.QueryRequest{FunctionChains: []*schemapb.FunctionChain{plan}},
		queryParams:     &queryParams{limit: 10, reduceType: reduce.IReduceNoOrder},
		RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}, OutputFieldsId: []int64{100}},
		resultBuf:       typeutil.NewConcurrentSet[*internalpb.RetrieveResults](),
	}
	require.NoError(t, task.prepareQueryPostProcess())
	require.NoError(t, task.PostExecute(context.Background()))
	table := decodePostTableTest(t, task.Result().GetPostProcessResult())
	require.Equal(t, []int64{0}, postTableIntColumn(t, table, "count"))
}

func TestQueryPostTableRejectsUnavailableSourcesAndStages(t *testing.T) {
	for _, input := range []string{types.ScoreFieldName, types.ElementIndicesFieldName} {
		t.Run(input, func(t *testing.T) {
			plan := postTableTestPlan(t, false, transformTestSelect(t, input))
			task := &QueryTask{
				schema: newFunctionChainTestSchema(), queryParams: &queryParams{},
				request: &milvuspb.QueryRequest{FunctionChains: []*schemapb.FunctionChain{plan}},
			}
			require.ErrorIs(t, task.prepareQueryPostProcess(), merr.ErrParameterInvalid)
			require.Nil(t, task.postProcess)
		})
	}
	plan := postTableTestPlan(t, false, postTableCountOp())
	for _, test := range []struct {
		name     string
		iterator bool
		fields   []string
		ann      bool
	}{
		{name: "iterator", iterator: true},
		{name: "native aggregation", fields: []string{"count(*)"}},
		{name: "ANN_INPUTS", ann: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			requestPlan := proto.Clone(plan).(*schemapb.FunctionChain)
			if test.ann {
				requestPlan.PostProcessOptions.Input = schemapb.PostProcessInput_PostProcessInputAnnInputs
			}
			task := &QueryTask{
				schema: newFunctionChainTestSchema(), queryParams: &queryParams{isIterator: test.iterator},
				request: &milvuspb.QueryRequest{FunctionChains: []*schemapb.FunctionChain{requestPlan}, OutputFields: test.fields},
			}
			require.ErrorIs(t, task.prepareQueryPostProcess(), merr.ErrParameterInvalid)
			require.Nil(t, task.postProcess)
		})
	}
}

func TestPostTableCancellationMalformedSourceAndOutputBudget(t *testing.T) {
	plan := postTableTestPlan(t, false, transformTestSelect(t, "tag"))
	task := transformTestTask(&milvuspb.SearchRequest{Nq: 1, FunctionChains: []*schemapb.FunctionChain{plan}})
	require.NoError(t, task.prepareSearchTransforms())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, task.executeSearchPostProcess(ctx, transformTestResults([]int64{1})), context.Canceled)
	malformed := transformTestResults([]int64{1})
	malformed.Results.Scores = nil
	err := task.executeSearchPostProcess(context.Background(), malformed)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	key := paramtable.Get().QuotaConfig.MaxOutputSize.Key
	original := paramtable.Get().QuotaConfig.MaxOutputSize.GetValue()
	require.NoError(t, paramtable.Get().Save(key, "1"))
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(key, original)) })
	err = task.executeSearchPostProcess(context.Background(), transformTestResults([]int64{1}))
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

func TestPostTableArrowIPCPreservesIndependentNestedSchema(t *testing.T) {
	paramtable.Init()
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { pool.AssertSize(t, 0) })
	dataType := arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true}, arrow.Field{Name: "start", Type: arrow.PrimitiveTypes.Int64})
	builder := array.NewStructBuilder(pool, dataType)
	builder.Append(true)
	builder.FieldBuilder(0).(*array.StringBuilder).Append("highlight")
	builder.FieldBuilder(1).(*array.Int64Builder).Append(3)
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	record := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "summary", Type: dataType}}, nil), []arrow.Array{values}, 1)
	defer record.Release()
	table := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
	defer table.Release()
	encoded, err := encodePostTable(context.Background(), table)
	require.NoError(t, err)
	decoded := decodePostTableTest(t, encoded)
	require.True(t, arrow.TypeEqual(dataType, decoded.Schema().Field(0).Type))
	nested := decoded.Column(0).Data().Chunk(0).(*array.Struct)
	require.Equal(t, "highlight", nested.Field(0).(*array.String).Value(0))
	require.Equal(t, int64(3), nested.Field(1).(*array.Int64).Value(0))
	clone := proto.Clone(encoded).(*schemapb.FunctionChainTableResult)
	require.Equal(t, encoded.GetRowCount(), clone.GetRowCount())
}

func TestPostTableRejectsMalformedGeneratedArrowBeforeSuccessfulResponse(t *testing.T) {
	registerSearchTransformTest(t)
	for _, invalid := range []string{"utf8", "struct_duplicate"} {
		t.Run(invalid, func(t *testing.T) {
			op := transformTestMap("summary", "tag", "")
			op.Expr.Params["invalid"] = chainStringParam(invalid)
			plan := postTableTestPlan(t, false, op, transformTestSelect(t, "summary"))
			task := transformTestTask(&milvuspb.SearchRequest{Nq: 1, FunctionChains: []*schemapb.FunctionChain{plan}})
			require.NoError(t, task.prepareSearchTransforms())
			result := transformTestResults([]int64{1})
			err := task.executeSearchPostProcess(context.Background(), result)
			require.ErrorIs(t, err, merr.ErrFunctionFailed)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			require.Nil(t, result.GetPostProcessResult())
			require.NotNil(t, result.GetResults(), "a failed codec cannot publish an independent table")
		})
	}
}
