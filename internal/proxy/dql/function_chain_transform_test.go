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
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

const searchTransformTestFunction = "dql_transform_test_function"

var registerSearchTransformTestOnce sync.Once

type searchTransformTestExpr struct {
	prefix  string
	fail    bool
	vector  bool
	summary bool
	invalid string
}

func (expr *searchTransformTestExpr) Name() string { return searchTransformTestFunction }
func (expr *searchTransformTestExpr) OutputDataTypes() []arrow.DataType {
	if expr.invalid == "json" {
		return []arrow.DataType{arrow.BinaryTypes.Binary}
	}
	if expr.vector {
		return []arrow.DataType{arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)}
	}
	return []arrow.DataType{arrow.BinaryTypes.String}
}
func (expr *searchTransformTestExpr) IsRunnable(stage string) bool {
	return stage == types.StagePreProcess || stage == types.StagePostProcess
}
func (expr *searchTransformTestExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if expr.fail {
		return nil, merr.Wrapf(merr.ErrServiceUnavailable, "injected model service unavailable")
	}
	if expr.vector && ctx.Stage() == types.StagePreProcess && len(inputs[0].Chunks()) != 1 {
		return nil, merr.WrapErrFunctionFailedMsg("PRE_PROCESS must batch query rows in one request group")
	}
	chunks := make([]arrow.Array, len(inputs[0].Chunks()))
	for index, input := range inputs[0].Chunks() {
		if expr.invalid == "json" {
			builder := array.NewBinaryBuilder(ctx.Pool(), arrow.BinaryTypes.Binary)
			for row := 0; row < input.Len(); row++ {
				builder.Append([]byte(`{"broken":`))
			}
			chunks[index] = builder.NewArray()
			builder.Release()
			continue
		}
		if expr.vector {
			builder := array.NewFixedSizeListBuilder(ctx.Pool(), 4, arrow.PrimitiveTypes.Float32)
			for row := 0; row < input.Len(); row++ {
				builder.Append(true)
				builder.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{float32(index + row + 1), 2, 3, 4}, nil)
			}
			chunks[index] = builder.NewArray()
			builder.Release()
			continue
		}
		values, ok := input.(*array.String)
		if !ok && !expr.summary {
			return nil, merr.WrapErrFunctionFailedMsg("test function requires string input")
		}
		builder := array.NewStringBuilder(ctx.Pool())
		for row := 0; row < input.Len(); row++ {
			if expr.invalid == "utf8" {
				builder.Append(string([]byte{0xff}))
			} else if expr.summary {
				builder.Append("SUMMARY")
			} else if values.IsNull(row) {
				builder.AppendNull()
			} else {
				builder.Append(expr.prefix + strings.ToUpper(values.Value(row)))
			}
		}
		chunks[index] = builder.NewArray()
		builder.Release()
	}
	result := arrow.NewChunked(expr.OutputDataTypes()[0], chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	return []*arrow.Chunked{result}, nil
}

func registerSearchTransformTest(t *testing.T) {
	t.Helper()
	paramtable.Init()
	registerSearchTransformTestOnce.Do(func() {
		err := types.RegisterFunction(searchTransformTestFunction, func(_ types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
			return &searchTransformTestExpr{
				prefix: cfg.Params["prefix"].GetStringValue(), fail: cfg.Params["fail"].GetBoolValue(), vector: cfg.Params["vector"].GetBoolValue(),
				summary: cfg.Params["summary"].GetBoolValue(),
				invalid: cfg.Params["invalid"].GetStringValue(),
			}, nil
		})
		require.NoError(t, err)
	})
}

func transformTestMap(output, input, prefix string) *schemapb.FunctionChainOp {
	op := mapOp(output, searchTransformTestFunction, columnArg(input))
	op.Expr.Params = map[string]*schemapb.FunctionParamValue{"prefix": chainStringParam(prefix)}
	return op
}

func transformTestSelect(t *testing.T, names ...string) *schemapb.FunctionChainOp {
	t.Helper()
	op, err := transform.Select(names, nil)
	require.NoError(t, err)
	return op
}

func transformTestPlaceholder(t *testing.T, values ...string) []byte {
	t.Helper()
	queries := make([][]byte, len(values))
	for index, value := range values {
		queries[index] = []byte(value)
	}
	encoded, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$custom", Type: commonpb.PlaceholderType_VarChar, Values: queries,
	}}})
	require.NoError(t, err)
	return encoded
}

func transformTestTask(request *milvuspb.SearchRequest) *SearchTask {
	return &SearchTask{
		request: request, schema: newFunctionChainTestSchema(),
		SearchRequest: &internalpb.SearchRequest{Nq: request.GetNq(), IsAdvanced: len(request.GetSubReqs()) > 0},
	}
}

func TestSearchTransformStageClassification(t *testing.T) {
	selectQuery := transformTestSelect(t, searchQueryColumn)
	pre, err := transform.PreProcess("pre", selectQuery)
	require.NoError(t, err)
	post, err := transform.PostProcess("post", transformTestSelect(t, types.IDFieldName))
	require.NoError(t, err)
	request := &milvuspb.SearchRequest{FunctionChains: []*schemapb.FunctionChain{pre, post}}
	require.False(t, hasFunctionRerank(request))
	request.FunctionScore = &schemapb.FunctionScore{}
	require.NoError(t, validateFunctionChainSearchRequest(request, false))
	request.FunctionScore = nil
	request.FunctionChains = append(request.FunctionChains, l2LimitFunctionChain(3))
	require.True(t, hasFunctionRerank(request))
	l2, querynode, err := splitFunctionChainsByStage(request.GetFunctionChains())
	require.NoError(t, err)
	require.Len(t, l2, 1)
	require.Empty(t, querynode)
	request.FunctionScore = &schemapb.FunctionScore{}
	require.ErrorIs(t, validateFunctionChainSearchRequest(request, false), merr.ErrParameterInvalid)
}

func TestSearchTransformOrdinaryFunctionScoreCoexistence(t *testing.T) {
	registerSearchTransformTest(t)
	preOp := transformTestMap(searchQueryColumn, searchQueryColumn, "")
	preOp.Expr.Params["vector"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}}
	pre, err := transform.PreProcess("query embedding", preOp)
	require.NoError(t, err)
	post, err := transform.PostProcess("payload projection", transformTestSelect(t, types.ScoreFieldName))
	require.NoError(t, err)
	request := &milvuspb.SearchRequest{
		Nq: 1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one")},
		SearchParams: []*commonpb.KeyValuePair{
			{Key: AnnsFieldKey, Value: "vec"}, {Key: TopKKey, Value: "10"},
			{Key: common.MetricTypeKey, Value: metric.IP}, {Key: ParamsKey, Value: `{}`},
		},
		FunctionChains: []*schemapb.FunctionChain{pre, post},
		FunctionScore: &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{
			Name: "decay", Type: schemapb.FunctionType_Rerank, InputFieldNames: []string{"ts"},
			Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "decay"}, {Key: "origin", Value: "100"}, {Key: "scale", Value: "10"}},
		}}},
	}
	task := transformTestTask(request)
	task.ctx, task.tr = context.Background(), timerecord.NewTimeRecorder("search transform coexistence")
	task.queryInfos = []*planpb.QueryInfo{{}}
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.NoError(t, task.initSearchRequest(context.Background()))
	require.IsType(t, &funcScoreRerankMeta{}, task.rerankMeta)
	require.Equal(t, []int64{101}, task.rerankMeta.GetInputFieldIDs())
	serialized := &planpb.PlanNode{}
	require.NoError(t, proto.Unmarshal(task.GetSerializedExprPlan(), serialized))
	require.Empty(t, serialized.GetQuerynodeFunctionChains(), "PRE/POST must not be routed as QueryNode rerank chains")
}

func TestSearchPreProcessPreservesQueriesAndOriginalRequest(t *testing.T) {
	registerSearchTransformTest(t)
	plan, err := transform.PreProcess("translate", transformTestMap(searchQueryColumn, searchQueryColumn, "CN:"))
	require.NoError(t, err)
	request := &milvuspb.SearchRequest{
		Nq: 2, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one", "two")},
		FunctionChains: []*schemapb.FunctionChain{plan},
	}
	original := proto.Clone(request)
	task := transformTestTask(request)
	require.NoError(t, task.prepareSearchTransforms())
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	processed := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(task.request.GetPlaceholderGroup(), processed))
	require.Equal(t, "$custom", processed.Placeholders[0].GetTag())
	require.Equal(t, [][]byte{[]byte("CN:ONE"), []byte("CN:TWO")}, processed.Placeholders[0].GetValues())
	require.True(t, proto.Equal(original, request), "retry input must remain unchanged")
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	require.NoError(t, proto.Unmarshal(task.request.GetPlaceholderGroup(), processed))
	require.Equal(t, []byte("CN:ONE"), processed.Placeholders[0].GetValues()[0], "one task does not preprocess twice")
}

func TestHybridPreProcessPerSubRequestAndLegacyRanker(t *testing.T) {
	registerSearchTransformTest(t)
	request := &milvuspb.SearchRequest{
		Nq: 1, SearchParams: []*commonpb.KeyValuePair{{Key: RankTypeKey, Value: "rrf"}},
		SubReqs: []*milvuspb.SubSearchRequest{{Nq: 1}, {Nq: 1}},
	}
	for index, prefix := range []string{"DENSE:", "BM25:"} {
		plan, err := transform.PreProcess("prepare", transformTestMap(searchQueryColumn, searchQueryColumn, prefix))
		require.NoError(t, err)
		request.SubReqs[index].PlaceholderGroup = transformTestPlaceholder(t, "query")
		request.SubReqs[index].FunctionChains = []*schemapb.FunctionChain{plan}
	}
	task := transformTestTask(request)
	require.NoError(t, task.prepareSearchTransforms())
	meta, err := selectHybridRerankMeta(task.request, task.schema)
	require.NoError(t, err)
	require.IsType(t, &legacyRerankMeta{}, meta)
	require.NoError(t, task.executeSearchPreProcess(context.Background()))
	for index, expected := range []string{"DENSE:QUERY", "BM25:QUERY"} {
		processed := &commonpb.PlaceholderGroup{}
		require.NoError(t, proto.Unmarshal(task.request.SubReqs[index].GetPlaceholderGroup(), processed))
		require.Equal(t, []byte(expected), processed.Placeholders[0].GetValues()[0])
	}
}

func TestSearchPreProcessTextToVectorPlaceholder(t *testing.T) {
	registerSearchTransformTest(t)
	op := transformTestMap(searchQueryColumn, searchQueryColumn, "")
	op.Expr.Params["vector"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}}
	plan, err := transform.PreProcess("embedding", op)
	require.NoError(t, err)
	meta, err := newSearchTransformMeta(plan, newFunctionChainTestSchema(), "")
	require.NoError(t, err)
	encoded, err := meta.processQueryInput(context.Background(), transformTestPlaceholder(t, "one", "two"), 2)
	require.NoError(t, err)
	phg := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(encoded, phg))
	require.Equal(t, commonpb.PlaceholderType_FloatVector, phg.Placeholders[0].GetType())
	require.Equal(t, "$custom", phg.Placeholders[0].GetTag())
	require.Len(t, phg.Placeholders[0].GetValues(), 2)
	for index, value := range phg.Placeholders[0].GetValues() {
		require.Len(t, value, 16)
		require.Equal(t, float32(index+1), math.Float32frombits(binary.LittleEndian.Uint32(value[:4])))
	}
}

func TestSearchPreProcessVectorBindingBeforeNativeSearch(t *testing.T) {
	registerSearchTransformTest(t)
	for _, dimension := range []string{"4", "3"} {
		t.Run(dimension, func(t *testing.T) {
			op := transformTestMap(searchQueryColumn, searchQueryColumn, "")
			op.Expr.Params["vector"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}}
			plan, err := transform.PreProcess("embedding", op)
			require.NoError(t, err)
			task := transformTestTask(&milvuspb.SearchRequest{
				Nq: 2, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: transformTestPlaceholder(t, "one", "two")},
				SearchParams:   []*commonpb.KeyValuePair{{Key: AnnsFieldKey, Value: "vec"}, {Key: TopKKey, Value: "10"}, {Key: common.MetricTypeKey, Value: metric.IP}, {Key: ParamsKey, Value: `{}`}},
				FunctionChains: []*schemapb.FunctionChain{plan},
			})
			schema := proto.Clone(task.schema.CollectionSchema).(*schemapb.CollectionSchema)
			schema.Fields[3].DataType = schemapb.DataType_Float16Vector
			schema.Fields[3].TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: dimension}}
			task.schema = mustNewSchemaInfo(schema)
			task.ctx, task.tr = context.Background(), timerecord.NewTimeRecorder("search transform vector binding")
			task.queryInfos = []*planpb.QueryInfo{{}}
			require.NoError(t, task.prepareSearchTransforms())
			require.NoError(t, task.executeSearchPreProcess(context.Background()))
			err = task.initSearchRequest(context.Background())
			if dimension == "3" {
				require.ErrorIs(t, err, merr.ErrFunctionFailed)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
				return
			}
			require.NoError(t, err)
			phg := &commonpb.PlaceholderGroup{}
			require.NoError(t, proto.Unmarshal(task.GetPlaceholderGroup(), phg))
			require.Equal(t, commonpb.PlaceholderType_Float16Vector, phg.GetPlaceholders()[0].GetType())
			require.Len(t, phg.GetPlaceholders()[0].GetValues(), 2)
			require.Len(t, phg.GetPlaceholders()[0].GetValues()[0], 8)
		})
	}
}

func TestSearchTransformUnsupportedScopesBeforeFunctionConstruction(t *testing.T) {
	registerSearchTransformTest(t)
	pre, err := transform.PreProcess("pre", mapOp(searchQueryColumn, "unknown_model", columnArg(searchQueryColumn)))
	require.NoError(t, err)
	post, err := transform.PostProcess("post", transformTestSelect(t, types.IDFieldName))
	require.NoError(t, err)
	for _, test := range []struct {
		name    string
		request *milvuspb.SearchRequest
		message string
	}{
		{"iterator", &milvuspb.SearchRequest{FunctionChains: []*schemapb.FunctionChain{pre}, SearchParams: []*commonpb.KeyValuePair{{Key: IteratorField, Value: "true"}}}, "iterator"},
		{"hybrid top pre", &milvuspb.SearchRequest{FunctionChains: []*schemapb.FunctionChain{pre}, SubReqs: []*milvuspb.SubSearchRequest{{}}}, "top-level hybrid"},
		{"hybrid nested post", &milvuspb.SearchRequest{SubReqs: []*milvuspb.SubSearchRequest{{FunctionChains: []*schemapb.FunctionChain{post}}}}, "POST_PROCESS belongs"},
		{"aggregation post", &milvuspb.SearchRequest{FunctionChains: []*schemapb.FunctionChain{post}, SearchAggregation: &commonpb.SearchAggregationSpec{}}, "search_aggregation"},
		{"IDs post", &milvuspb.SearchRequest{FunctionChains: []*schemapb.FunctionChain{post}, SearchInput: &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{}}}, "search by IDs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := transformTestTask(test.request).prepareSearchTransforms()
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Contains(t, err.Error(), test.message)
			require.NotContains(t, err.Error(), "unknown_model")
		})
	}
}

func transformTestResults(topks []int64) *milvuspb.SearchResults {
	rows := 0
	for _, count := range topks {
		rows += int(count)
	}
	ids := make([]int64, rows)
	scores := make([]float32, rows)
	tags := make([]string, rows)
	for index := range ids {
		ids[index], scores[index], tags[index] = int64(index+1), float32(index)/10, "document"
	}
	fields := []*schemapb.FieldData{}
	if rows > 0 {
		fields = append(fields,
			&schemapb.FieldData{FieldId: 102, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: tags}}}}},
			&schemapb.FieldData{FieldId: 101, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}}}}},
		)
	}
	return &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{
		NumQueries: int64(len(topks)), Topks: topks, Scores: scores,
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}, FieldsData: fields,
	}}
}

func TestSearchPostProcessGeneratedProjectionAndQueryBoundaries(t *testing.T) {
	registerSearchTransformTest(t)
	for _, topks := range [][]int64{{2, 1}, {0, 3}, {0, 0}} {
		for _, selectFinal := range []bool{false, true} {
			ops := []*schemapb.FunctionChainOp{transformTestMap("snippet", "tag", "HL:")}
			if selectFinal {
				ops = append(ops, transformTestSelect(t, "snippet"))
			}
			plan, err := transform.PostProcess("highlight", ops...)
			require.NoError(t, err)
			task := transformTestTask(&milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{plan}})
			task.translatedOutputFields, task.userOutputFields = []string{"tag"}, []string{"tag"}
			require.NoError(t, task.prepareSearchTransforms())
			result := transformTestResults(topks)
			identity := proto.Clone(result.GetResults()).(*schemapb.SearchResultData)
			require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
			require.True(t, proto.Equal(identity.GetIds(), result.GetResults().GetIds()))
			require.Equal(t, append([]float32{}, identity.GetScores()...), result.GetResults().GetScores())
			require.Equal(t, topks, result.GetResults().GetTopks())
			end := &endOperator{postProcess: true, roundDecimal: -1}
			_, err = end.run(context.Background(), trace.SpanFromContext(context.Background()), result, []*milvuspb.SearchResults{result})
			require.NoError(t, err)
			payload := result.GetResults().GetFieldsData()
			if selectFinal {
				require.Len(t, payload, 1)
				require.Equal(t, []string{"snippet"}, result.GetResults().GetOutputFields())
			} else {
				require.Equal(t, []string{"tag", "snippet"}, result.GetResults().GetOutputFields())
				if topks[0]+topks[1] > 0 {
					require.Len(t, payload, 2)
				}
			}
			for _, field := range payload {
				require.NotEqual(t, "ts", field.GetFieldName(), "hidden fetched fields stay hidden")
				if field.GetFieldName() == "snippet" {
					require.Zero(t, field.GetFieldId())
					require.Equal(t, schemapb.DataType_VarChar, field.GetType())
					require.Len(t, field.GetScalars().GetStringData().GetData(), int(topks[0]+topks[1]))
					for _, value := range field.GetScalars().GetStringData().GetData() {
						require.Equal(t, "HL:DOCUMENT", value)
					}
				}
			}
		}
	}
}

func TestSearchTransformFailureClassificationAndCancellation(t *testing.T) {
	registerSearchTransformTest(t)
	op := transformTestMap(searchQueryColumn, searchQueryColumn, "")
	op.Expr.Params["fail"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}}
	plan, err := transform.PreProcess("failing model", op)
	require.NoError(t, err)
	meta, err := newSearchTransformMeta(plan, newFunctionChainTestSchema(), "")
	require.NoError(t, err)
	_, err = meta.processQueryInput(context.Background(), transformTestPlaceholder(t, "query"), 1)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = meta.processQueryInput(cancelled, transformTestPlaceholder(t, "query"), 1)
	require.ErrorIs(t, err, context.Canceled)
	_, err = meta.processQueryInput(context.Background(), []byte{0xff}, 1)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Equal(t, merr.InputError, merr.GetErrorType(err))
	post, err := transform.PostProcess("projection", transformTestSelect(t, "tag"))
	require.NoError(t, err)
	task := transformTestTask(&milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{post}})
	require.NoError(t, task.prepareSearchTransforms())
	result := transformTestResults([]int64{2, 1})
	result.Results.FieldsData[0].GetScalars().GetStringData().Data = []string{"too few"}
	err = task.executeSearchPostProcess(context.Background(), result)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err), "querynode output is not user input")
}

func TestSearchTransformRejectsMalformedGeneratedPayload(t *testing.T) {
	registerSearchTransformTest(t)
	for _, invalid := range []string{"utf8", "json"} {
		t.Run(invalid, func(t *testing.T) {
			op := transformTestMap("generated", "tag", "")
			op.Expr.Params["invalid"] = chainStringParam(invalid)
			post, err := transform.PostProcess("invalid payload", op, transformTestSelect(t, "generated"))
			require.NoError(t, err)
			task := transformTestTask(&milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{post}})
			require.NoError(t, task.prepareSearchTransforms())
			err = task.executeSearchPostProcess(context.Background(), transformTestResults([]int64{1, 2}))
			require.ErrorIs(t, err, merr.ErrFunctionFailed)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			if invalid == "utf8" {
				op = transformTestMap(searchQueryColumn, searchQueryColumn, "")
				op.Expr.Params["invalid"] = chainStringParam(invalid)
				pre, err := transform.PreProcess("invalid query", op)
				require.NoError(t, err)
				meta, err := newSearchTransformMeta(pre, task.schema, "")
				require.NoError(t, err)
				_, err = meta.processQueryInput(context.Background(), transformTestPlaceholder(t, "one", "two"), 2)
				require.ErrorIs(t, err, merr.ErrFunctionFailed)
			}
		})
	}
}

func TestSearchPostProcessNullablePayload(t *testing.T) {
	registerSearchTransformTest(t)
	plan, err := transform.PostProcess("nullable highlight",
		transformTestMap("snippet", "tag", "HL:"), transformTestSelect(t, "snippet"))
	require.NoError(t, err)
	task := transformTestTask(&milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{plan}})
	require.NoError(t, task.prepareSearchTransforms())
	result := transformTestResults([]int64{1, 2})
	input := result.Results.FieldsData[0]
	input.GetScalars().ValidData = []bool{false, true, true}
	input.GetScalars().GetStringData().Data = []string{"b", "c"}
	require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
	payload := result.GetResults().GetFieldsData()[0]
	require.Equal(t, []bool{false, true, true}, payload.GetScalars().GetValidData())
	require.Equal(t, []string{"", "HL:B", "HL:C"}, payload.GetScalars().GetStringData().GetData())
	require.Equal(t, []int64{1, 2}, result.GetResults().GetTopks())
}

func TestSearchPostProcessIsExecutedBySearchPipeline(t *testing.T) {
	registerSearchTransformTest(t)
	plan, err := transform.PostProcess("highlight",
		transformTestMap("snippet", "tag", "HL:"), transformTestSelect(t, "snippet"))
	require.NoError(t, err)
	task := transformTestTask(&milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{plan}})
	task.translatedOutputFields, task.userOutputFields = []string{"tag"}, []string{"tag"}
	require.NoError(t, task.prepareSearchTransforms())
	result := transformTestResults([]int64{2, 1})
	originalFactory := opFactory[searchReduceOp]
	defer func() { opFactory[searchReduceOp] = originalFactory }()
	opFactory[searchReduceOp] = func(_ *SearchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(context.Context, trace.Span, ...any) ([]any, error) {
			return []any{[]*milvuspb.SearchResults{result}, []string{"IP"}}, nil
		}), nil
	}
	pipeline, err := newSearchPipeline(task)
	require.NoError(t, err)
	processed, _, err := pipeline.Run(context.Background(), trace.SpanFromContext(context.Background()), nil, segcore.StorageCost{})
	require.NoError(t, err)
	require.Equal(t, []string{"snippet"}, processed.GetResults().GetOutputFields())
	require.Len(t, processed.GetResults().GetFieldsData(), 1)
	require.Equal(t, []string{"HL:DOCUMENT", "HL:DOCUMENT", "HL:DOCUMENT"}, processed.GetResults().GetFieldsData()[0].GetScalars().GetStringData().GetData())
	require.Equal(t, []int64{1, 2, 3}, processed.GetResults().GetIds().GetIntId().GetData())
}

type searchTransformTestExecutor func(context.Context, *schemapb.FunctionChain, transform.Batch) (transform.Result, error)

func (executor searchTransformTestExecutor) Execute(ctx context.Context, plan *schemapb.FunctionChain, input transform.Batch) (transform.Result, error) {
	return executor(ctx, plan, input)
}

func TestSearchPostProcessRejectsProtectedValueTampering(t *testing.T) {
	registerSearchTransformTest(t)
	plan, err := transform.PostProcess("identity", transformTestSelect(t, types.ScoreFieldName))
	require.NoError(t, err)
	task := transformTestTask(&milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{plan}})
	require.NoError(t, task.prepareSearchTransforms())
	result := transformTestResults([]int64{2, 1})
	result.Results.Scores[0] = math.Float32frombits(0x7fc00001)
	result.Results.Scores[1] = math.Float32frombits(0x80000000)
	// NaN and signed-zero bit patterns are legal opaque candidate scores;
	// unchanged values must pass the identity guard.
	require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
	actual := task.postProcess.executor
	task.postProcess.executor = searchTransformTestExecutor(func(ctx context.Context, plan *schemapb.FunctionChain, input transform.Batch) (transform.Result, error) {
		output, err := actual.Execute(ctx, plan, input)
		if err != nil {
			return output, err
		}
		group := output.Data.Groups[0]
		index := group.Schema().FieldIndices(types.ScoreFieldName)[0]
		builder := array.NewFloat32Builder(memory.DefaultAllocator)
		builder.AppendValues([]float32{1, 2}, nil)
		values := builder.NewArray()
		builder.Release()
		data := arrow.NewChunked(values.DataType(), []arrow.Array{values})
		values.Release()
		column := arrow.NewColumn(group.Schema().Field(index), data)
		data.Release()
		columns := make([]arrow.Column, group.NumCols())
		for position := range columns {
			columns[position] = *group.Column(position)
		}
		columns[index] = *column
		output.Data.Groups[0] = array.NewTable(group.Schema(), columns, group.NumRows())
		column.Release()
		group.Release()
		return output, nil
	})
	identity := proto.Clone(result.Results)
	err = task.executeSearchPostProcess(context.Background(), result)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	require.True(t, proto.Equal(identity, result.Results), "candidate identity must remain unchanged on failed POST")
}

func TestSearchPostProcessStructArraySourcesAndPayloadPrivacy(t *testing.T) {
	registerSearchTransformTest(t)
	schema := proto.Clone(newFunctionChainTestSchema().CollectionSchema).(*schemapb.CollectionSchema)
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Name: "sections", Fields: []*schemapb.FieldSchema{
		{FieldID: 201, Name: "sections[text]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		{FieldID: 202, Name: "sections[label]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
	}}}
	for _, rows := range []int64{0, 2} {
		for _, selectRoot := range []bool{false, true} {
			var op *schemapb.FunctionChainOp
			if selectRoot {
				op = transformTestSelect(t, "sections")
			} else {
				op = transformTestMap("summary", "sections[label]", "")
				op.Expr.Params["summary"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}}
			}
			plan, err := transform.PostProcess("struct payload", op)
			require.NoError(t, err)
			task := transformTestTask(&milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{plan}})
			task.schema = mustNewSchemaInfo(schema)
			task.translatedOutputFields, task.userOutputFields = []string{"sections[text]"}, []string{"sections[text]"}
			require.NoError(t, task.prepareSearchTransforms())
			if selectRoot {
				require.ElementsMatch(t, []int64{201, 202}, task.postProcess.inputIDs)
			} else {
				require.Equal(t, []int64{202}, task.postProcess.inputIDs)
			}
			result := transformTestResults([]int64{0, rows})
			if rows > 0 {
				for _, child := range schema.StructArrayFields[0].GetFields() {
					values := make([]*schemapb.ScalarField, rows)
					for index := range values {
						values[index] = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"first", "second"}}}}
					}
					result.Results.FieldsData = append(result.Results.FieldsData, &schemapb.FieldData{
						FieldId: child.GetFieldID(), Type: child.GetDataType(), Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_VarChar, Data: values}}}},
					})
				}
			}
			require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
			payload := result.Results.GetFieldsData()
			if selectRoot {
				require.Len(t, payload, 1)
				require.Len(t, payload[0].GetStructArrays().GetFields(), 2)
				require.Equal(t, []string{"sections"}, result.Results.GetOutputFields())
			} else {
				require.Len(t, payload, 2)
				require.Equal(t, "sections", payload[0].GetFieldName())
				require.Len(t, payload[0].GetStructArrays().GetFields(), 1, "hidden label inputs must not leak into original nested payload")
				require.Equal(t, "text", payload[0].GetStructArrays().GetFields()[0].GetFieldName())
				require.Equal(t, []string{"sections", "summary"}, result.Results.GetOutputFields())
			}
		}
	}
}

func TestSearchPostProcessDynamicInputsAndPayloadPrivacy(t *testing.T) {
	registerSearchTransformTest(t)
	schema := proto.Clone(newFunctionChainTestSchema().CollectionSchema).(*schemapb.CollectionSchema)
	schema.EnableDynamicField = true
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 110, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true})
	for _, selected := range []string{"", "snippet", common.MetaFieldName} {
		for _, rows := range []int64{0, 3} {
			op := transformTestMap("snippet", `$meta["secret"]`, "HL:")
			op.Params = map[string]*schemapb.FunctionParamValue{types.InputDataTypesParam: chainDataTypesParam(schemapb.DataType_VarChar)}
			ops := []*schemapb.FunctionChainOp{op}
			if selected != "" {
				ops = append(ops, transformTestSelect(t, selected))
			}
			plan, err := transform.PostProcess("dynamic highlight", ops...)
			require.NoError(t, err)
			task := transformTestTask(&milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{plan}})
			task.schema = mustNewSchemaInfo(schema)
			task.translatedOutputFields, task.userOutputFields, task.userDynamicFields = []string{common.MetaFieldName}, []string{"color"}, []string{"color"}
			require.NoError(t, task.prepareSearchTransforms())
			fetchPlan := &planpb.PlanNode{DynamicFields: []string{"color"}}
			task.retainRerankDynamicFields(fetchPlan)
			if selected == common.MetaFieldName {
				require.Empty(t, fetchPlan.GetDynamicFields(), "selecting the whole root must fetch the whole root")
			} else {
				require.ElementsMatch(t, []string{"color", "secret"}, fetchPlan.GetDynamicFields())
			}
			result := transformTestResults([]int64{0, rows})
			if rows > 0 {
				values := make([][]byte, rows)
				for row := range values {
					values[row] = []byte(`{"color":"red","secret":"needle","unrequested":"private"}`)
				}
				result.Results.FieldsData = append(result.Results.FieldsData, &schemapb.FieldData{FieldId: 110, Type: schemapb.DataType_JSON,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: values}}}}})
			}
			require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
			for _, field := range result.Results.GetFieldsData() {
				switch field.GetFieldName() {
				case common.MetaFieldName:
					require.True(t, field.GetIsDynamic())
					for _, value := range field.GetScalars().GetJsonData().GetData() {
						var object map[string]interface{}
						require.NoError(t, json.Unmarshal(value, &object))
						if selected == "" {
							require.Equal(t, map[string]interface{}{"color": "red"}, object, "hidden model inputs must not leak into dynamic payload")
						} else {
							require.Equal(t, "needle", object["secret"])
						}
					}
				case "snippet":
					for _, value := range field.GetScalars().GetStringData().GetData() {
						require.Equal(t, "HL:NEEDLE", value)
					}
				default:
					t.Fatalf("unexpected payload field %q", field.GetFieldName())
				}
			}
			if selected == "" {
				require.Equal(t, []string{"color", "snippet"}, result.Results.GetOutputFields())
			} else {
				require.Equal(t, []string{selected}, result.Results.GetOutputFields())
			}
		}
	}
}

func TestSearchPostProcessUntouchedVectorArrayPayload(t *testing.T) {
	registerSearchTransformTest(t)
	schema := proto.Clone(newFunctionChainTestSchema().CollectionSchema).(*schemapb.CollectionSchema)
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Name: "documents", Fields: []*schemapb.FieldSchema{
		{FieldID: 201, Name: "documents[embedding]", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
	}}}
	for _, rows := range []int64{0, 3} {
		plan, err := transform.PostProcess("highlight", transformTestMap("snippet", "tag", "HL:"))
		require.NoError(t, err)
		task := transformTestTask(&milvuspb.SearchRequest{Nq: 2, FunctionChains: []*schemapb.FunctionChain{plan}})
		task.schema = mustNewSchemaInfo(schema)
		task.translatedOutputFields, task.userOutputFields = []string{"documents[embedding]"}, []string{"documents[embedding]"}
		require.NoError(t, task.prepareSearchTransforms())
		result := transformTestResults([]int64{0, rows})
		if rows > 0 {
			vectors := make([]*schemapb.VectorField, rows)
			for index := range vectors {
				vectors[index] = &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}
			}
			result.Results.FieldsData = append(result.Results.FieldsData, &schemapb.FieldData{FieldId: 201, Type: schemapb.DataType_ArrayOfVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 2, ElementType: schemapb.DataType_FloatVector, Data: vectors}}}}})
		}
		require.NoError(t, task.executeSearchPostProcess(context.Background(), result))
		require.Len(t, result.Results.GetFieldsData(), 2)
		root := result.Results.GetFieldsData()[0]
		require.Equal(t, "documents", root.GetFieldName())
		require.Len(t, root.GetStructArrays().GetFields(), 1)
		payload := root.GetStructArrays().GetFields()[0]
		require.Equal(t, "embedding", payload.GetFieldName())
		require.Equal(t, schemapb.DataType_ArrayOfVector, payload.GetType())
		require.Len(t, payload.GetVectors().GetVectorArray().GetData(), int(rows))
		for _, row := range payload.GetVectors().GetVectorArray().GetData() {
			require.Equal(t, []float32{1, 2, 3, 4}, row.GetFloatVector().GetData())
		}
	}
	for _, source := range []string{"documents", "documents[embedding]"} {
		plan, err := transform.PostProcess("unsupported vector array input", transformTestSelect(t, source))
		require.NoError(t, err)
		task := transformTestTask(&milvuspb.SearchRequest{Nq: 1, FunctionChains: []*schemapb.FunctionChain{plan}})
		task.schema = mustNewSchemaInfo(schema)
		err = task.prepareSearchTransforms()
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.Contains(t, err.Error(), "ArrayOfVector source")
	}
}
