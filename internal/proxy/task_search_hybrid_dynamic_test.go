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

package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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

func newHybridDynamicSearchTask(t *testing.T, partitionMode bool, outputs []string, topChain *schemapb.FunctionChain) *searchTask {
	t.Helper()
	schema := newFunctionChainJSONTestSchema().CollectionSchema
	schema.Name = "hybrid_dynamic"
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID: 104, Name: "vec", DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
	})
	if partitionMode {
		schema.EnableNamespace = true
		schema.Properties = []*commonpb.KeyValuePair{{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition}}
	}
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 16), make([]byte, 16)},
	}}})
	require.NoError(t, err)
	request := &milvuspb.HybridSearchRequest{
		CollectionName: schema.Name, OutputFields: outputs,
		RankParams: []*commonpb.KeyValuePair{{Key: LimitKey, Value: "3"}},
	}
	if partitionMode {
		namespace := "_default"
		request.Namespace = &namespace
	}
	if topChain != nil {
		request.FunctionChains = []*schemapb.FunctionChain{topChain}
	}
	for range 2 {
		request.Requests = append(request.Requests, &milvuspb.SearchRequest{
			Nq:          2,
			SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder},
			SearchParams: []*commonpb.KeyValuePair{
				{Key: AnnsFieldKey, Value: "vec"},
				{Key: TopKKey, Value: "3"},
				{Key: common.MetricTypeKey, Value: metric.IP},
				{Key: ParamsKey, Value: `{}`},
			},
		})
	}
	// Same ANN field, different stages and physical roots. Nested dependencies
	// belong only to their own QueryNode plan, even with top-level L2 inputs.
	l0 := mapOp(types.ScoreFieldName, "num_combine", columnArg(`metadata["nested_only"]`))
	l0.Params = map[string]*schemapb.FunctionParamValue{types.InputDataTypesParam: chainDataTypesParam(schemapb.DataType_Int64)}
	l1 := mapOp(types.ScoreFieldName, "num_combine", columnArg(`$meta["nested_only"]`))
	l1.Params = map[string]*schemapb.FunctionParamValue{types.InputDataTypesParam: chainDataTypesParam(schemapb.DataType_Int64)}
	request.Requests[0].FunctionChains = []*schemapb.FunctionChain{l0FunctionChain(l0)}
	request.Requests[1].FunctionChains = []*schemapb.FunctionChain{l1FunctionChain(l1)}
	task := &searchTask{
		ctx: context.Background(), collectionName: schema.Name,
		SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{Timestamp: 1}, CollectionID: 1, PartitionIDs: []int64{1}, IsAdvanced: true, Nq: 2},
		request:       convertHybridSearchToSearch(request), schema: mustNewSchemaInfo(schema),
		tr: timerecord.NewTimeRecorder("hybrid-dynamic-test"),
	}
	task.translatedOutputFields, task.userOutputFields, task.userDynamicFields, _, task.userRequestedPkFieldExplicitly, err = translateOutputFields(outputs, task.schema, true)
	require.NoError(t, err)
	task.OutputFieldsId, err = translateToOutputFieldIDs(task.translatedOutputFields, schema)
	require.NoError(t, err)
	require.NoError(t, task.initAdvancedSearchRequest(task.ctx))
	return task
}

func hybridDynamicFields(ids []int64) []*schemapb.FieldData {
	regular, dynamic := make([][]byte, len(ids)), make([][]byte, len(ids))
	for i, id := range ids {
		rank := map[int64]int{1: 10, 2: 30, 3: 20}[id]
		regular[i] = []byte(fmt.Sprintf(`{"rank":%d,"payload":"row-%d"}`, rank, id))
		dynamic[i] = []byte(fmt.Sprintf(`{"profile":{"rank":%d},"title":"title-%d"}`, rank, id))
	}
	return []*schemapb.FieldData{
		{FieldId: 100, FieldName: "pk", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}}},
		}},
		{FieldId: 102, FieldName: "metadata", Type: schemapb.DataType_JSON, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: regular}}},
		}},
		{FieldId: 103, FieldName: common.MetaFieldName, Type: schemapb.DataType_JSON, IsDynamic: true, Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: dynamic}}},
		}},
	}
}

func TestHybridDynamicPipeline(t *testing.T) {
	paramtable.Init()
	for _, direct := range []bool{false, true} {
		for _, returnRoots := range []bool{false, true} {
			for _, inputKind := range []string{"json", "dynamic", "both"} {
				t.Run(fmt.Sprintf("direct=%v/roots=%v/%s", direct, returnRoots, inputKind), func(t *testing.T) {
					path := `metadata["rank"]`
					if inputKind == "dynamic" {
						path = `$meta["profile"]["rank"]`
					}
					topChain := hybridDynamicSortChain(path)
					if inputKind == "both" {
						op := mapOp(types.ScoreFieldName, "num_combine", columnArg(`metadata["rank"]`), columnArg(`$meta["profile"]["rank"]`))
						op.Expr.Params["mode"] = chainStringParam("sum")
						op.Params = map[string]*schemapb.FunctionParamValue{types.InputDataTypesParam: chainDataTypesParam(schemapb.DataType_Int64, schemapb.DataType_Int64)}
						topChain.Ops = append(topChain.Ops[:1], op, &schemapb.FunctionChainOp{
							Op: types.OpTypeSort, Inputs: []string{types.ScoreFieldName, types.IDFieldName},
							Params: map[string]*schemapb.FunctionParamValue{"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}}},
						})
					}
					outputs := []string{"pk"}
					if returnRoots {
						outputs = []string{"metadata", "title"}
					}
					task := newHybridDynamicSearchTask(t, direct, outputs, topChain)
					// Duplicate PK 2 occurs in both sub-searches; the first sub-search
					// has no hits for query 2. Fields must survive merge in both cases.
					makeResult := func(ids []int64, topks []int64) *milvuspb.SearchResults {
						return &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{
							NumQueries: 2, TopK: 3, Topks: topks, Ids: testSearchResultIDs(ids...),
							Scores: make([]float32, len(ids)), FieldsData: hybridDynamicFields(ids),
						}}
					}
					original := opFactory[hybridSearchReduceOp]
					t.Cleanup(func() { opFactory[hybridSearchReduceOp] = original })
					opFactory[hybridSearchReduceOp] = func(*searchTask, map[string]any) (operator, error) {
						return searchPipelineTestOperator(func(context.Context, trace.Span, ...any) ([]any, error) {
							return []any{[]*milvuspb.SearchResults{
								makeResult([]int64{1, 2}, []int64{2, 0}),
								makeResult([]int64{2, 3, 1, 3}, []int64{2, 2}),
							}, []string{metric.IP, metric.IP}}, nil
						}), nil
					}
					queryMock := mockey.Mock((*requeryOperator).requery).To(func(_ *requeryOperator, _ context.Context, _ trace.Span, ids *schemapb.IDs, fields []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
						assert.ElementsMatch(t, []int64{1, 2, 3}, ids.GetIntId().GetData())
						for _, name := range task.rerankMeta.GetInputFieldNames() {
							assert.Contains(t, fields, name)
						}
						assert.NotContains(t, fields, path)
						// Requery order differs from every sub-result's order.
						return &milvuspb.QueryResults{FieldsData: hybridDynamicFields([]int64{3, 1, 2})}, segcore.StorageCost{}, nil
					}).Build()
					t.Cleanup(func() { queryMock.UnPatch() })
					pipeline, err := newSearchPipeline(task)
					require.NoError(t, err)
					result, _, err := pipeline.Run(task.ctx, trace.SpanFromContext(task.ctx), nil, segcore.StorageCost{})
					require.NoError(t, err)
					assert.Equal(t, []int64{3, 2}, result.Results.Topks)
					assert.Equal(t, []int64{2, 3, 1, 3, 1}, result.Results.Ids.GetIntId().GetData())
					if inputKind == "both" {
						assert.Equal(t, []float32{60, 40, 20, 40, 20}, result.Results.Scores)
					}
					if returnRoots {
						require.Len(t, result.Results.FieldsData, 2)
						want := hybridDynamicFields(result.Results.Ids.GetIntId().GetData())
						for _, field := range result.Results.FieldsData {
							assert.True(t, proto.Equal(want[field.FieldId-101], field))
						}
					} else {
						assert.Empty(t, result.Results.FieldsData)
					}
					if direct {
						assert.Zero(t, queryMock.Times())
					} else {
						assert.Equal(t, 1, queryMock.Times())
					}
				})
			}
		}
	}
}

func hybridDynamicSortChain(path string) *schemapb.FunctionChain {
	return l2FunctionChain(
		&schemapb.FunctionChainOp{Op: types.OpTypeMerge, Params: map[string]*schemapb.FunctionParamValue{"strategy": chainStringParam("rrf")}},
		&schemapb.FunctionChainOp{
			Op: types.OpTypeSort, Inputs: []string{path, types.IDFieldName},
			Params: map[string]*schemapb.FunctionParamValue{
				"desc":                    {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}},
				types.InputDataTypesParam: chainDataTypesParam(schemapb.DataType_Int64, schemapb.DataType_None),
			},
		},
	)
}

func TestHybridDynamicInputPlan(t *testing.T) {
	paramtable.Init()
	for _, partitionMode := range []bool{false, true} {
		for _, output := range []string{"pk", "title", common.MetaFieldName} {
			for _, path := range []string{`$meta["rank"]`, `$meta["profile"]["rank"]`, `metadata["rank"]`} {
				t.Run(output+"/"+path+"/"+map[bool]string{true: "direct", false: "requery"}[partitionMode], func(t *testing.T) {
					task := newHybridDynamicSearchTask(t, partitionMode, []string{output}, hybridDynamicSortChain(path))
					assert.Equal(t, !partitionMode, task.needRequery)
					input := task.rerankMeta.GetInputPlan().Inputs[0]
					assert.Equal(t, []int64{input.SourceFieldID}, task.rerankMeta.GetInputFieldIDs())
					for i, sub := range task.GetSubReqs() {
						plan := &planpb.PlanNode{}
						require.NoError(t, proto.Unmarshal(sub.SerializedExprPlan, plan))
						require.Len(t, plan.QuerynodeFunctionChains, 1)
						assert.True(t, proto.Equal(task.request.SubReqs[i].FunctionChains[0], plan.QuerynodeFunctionChains[0]))
						assert.NotContains(t, plan.DynamicFields, "nested_only")
						if !partitionMode {
							assert.Equal(t, []int64{input.SourceFieldID}, plan.OutputFieldIds)
							assert.Empty(t, plan.DynamicFields)
						} else if output == "title" {
							wantKeys := []string{"title"}
							if input.FieldName == common.MetaFieldName {
								wantKeys = append(wantKeys, input.NestedPath[0])
							}
							assert.ElementsMatch(t, wantKeys, plan.DynamicFields)
						} else {
							assert.Empty(t, plan.DynamicFields, "full dynamic roots must remain unpruned")
						}
					}
					assert.NotContains(t, task.userOutputFields, path)
					assert.NotContains(t, task.userDynamicFields, "rank")
					assert.NotContains(t, task.userDynamicFields, "profile")
				})
			}
		}
	}
	t.Run("nested only does not add proxy inputs", func(t *testing.T) {
		task := newHybridDynamicSearchTask(t, true, []string{"title"}, nil)
		assert.Empty(t, task.rerankMeta.GetInputFieldIDs())
		for _, sub := range task.GetSubReqs() {
			plan := &planpb.PlanNode{}
			require.NoError(t, proto.Unmarshal(sub.SerializedExprPlan, plan))
			assert.ElementsMatch(t, []int64{100, 103}, plan.OutputFieldIds)
			assert.Equal(t, []string{"title"}, plan.DynamicFields)
		}
	})
}
