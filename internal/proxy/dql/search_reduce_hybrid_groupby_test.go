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

package dql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestHybridGroupByWithoutRequeryAcrossShards(t *testing.T) {
	paramtable.Init()
	policyKey := paramtable.Get().CommonCfg.HybridSearchRequeryPolicy.Key
	originalPolicy := paramtable.Get().CommonCfg.HybridSearchRequeryPolicy.GetValue()
	t.Cleanup(func() { paramtable.Get().Save(policyKey, originalPolicy) })

	for _, tc := range []struct {
		name      string
		policy    string
		namespace bool
	}{
		{name: "no output fields", policy: "OutputFields"},
		{name: "namespace output vector", policy: "OutputVector", namespace: true},
		{name: "namespace always", policy: "Always", namespace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paramtable.Get().Save(policyKey, tc.policy)
			ctx := context.Background()
			span := trace.SpanFromContext(ctx)
			schema := &schemapb.CollectionSchema{
				Name: "hybrid_group_by",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "group", DataType: schemapb.DataType_VarChar, Nullable: true},
					{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, Nullable: true,
						TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
					{FieldID: 103, Name: "value", DataType: schemapb.DataType_Int64, Nullable: true},
				},
			}
			placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{
				Placeholders: []*commonpb.PlaceholderValue{{
					Tag: "$0", Type: commonpb.PlaceholderType_FloatVector,
					Values: [][]byte{make([]byte, 8), make([]byte, 8)},
				}},
			})
			require.NoError(t, err)
			task := &SearchTask{
				ctx: ctx, collectionName: schema.Name,
				SearchRequest: &internalpb.SearchRequest{CollectionID: 1, IsAdvanced: true, Nq: 2},
				request: &milvuspb.SearchRequest{
					CollectionName: schema.Name,
					SearchParams: []*commonpb.KeyValuePair{
						{Key: LimitKey, Value: "3"},
						{Key: GroupByFieldKey, Value: "group"},
						{Key: RankTypeKey, Value: "rrf"},
						{Key: ParamsKey, Value: `{"k": 60}`},
					},
				},
				tr: timerecord.NewTimeRecorder("test"),
			}
			if tc.namespace {
				schema.EnableNamespace = true
				schema.Properties = []*commonpb.KeyValuePair{{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition}}
				task.request.Namespace = proto.String("tenant")
				task.PartitionIDs = []int64{1}
				task.request.OutputFields = []string{"vec", "value"}
				task.translatedOutputFields = task.request.OutputFields
				task.OutputFieldsId = []int64{102, 103}
			}
			task.schema = mustNewSchemaInfo(schema)
			for range 2 {
				task.request.SubReqs = append(task.request.SubReqs, &milvuspb.SubSearchRequest{
					Nq: 2, PlaceholderGroup: placeholder,
					SearchParams: []*commonpb.KeyValuePair{
						{Key: AnnsFieldKey, Value: "vec"},
						{Key: TopKKey, Value: "3"},
						{Key: common.MetricTypeKey, Value: metric.IP},
						{Key: ParamsKey, Value: `{}`},
					},
				})
			}
			require.NoError(t, task.initAdvancedSearchRequest(ctx))
			require.False(t, task.needRequery)

			// QueryNode returns requested output columns, including the PK even
			// when the client did not request any output fields. Put an empty
			// shard first and vary per-query hit counts to exercise row offsets.
			shards := []*schemapb.SearchResultData{
				{NumQueries: 2, TopK: 3, Topks: []int64{0, 0}, Ids: testSearchResultIDs()},
				{NumQueries: 2, TopK: 3, Topks: []int64{2, 1}, Ids: testSearchResultIDs(10, 20, 30), Scores: []float32{.9, .8, .7}},
				{NumQueries: 2, TopK: 3, Topks: []int64{1, 2}, Ids: testSearchResultIDs(40, 50, 60), Scores: []float32{.85, .95, .75}},
			}
			groupA := multiGroupByTestStringField(101, []string{"A", "B"})
			typeutil.SetFieldDataValidData(groupA, []bool{true, false, true})
			groupB := multiGroupByTestStringField(101, []string{"C", "D"})
			typeutil.SetFieldDataValidData(groupB, []bool{true, true, false})
			shards[1].GroupByFieldValues = []*schemapb.FieldData{groupA}
			shards[2].GroupByFieldValues = []*schemapb.FieldData{groupB}

			wireResults := make([]*internalpb.SearchResults, len(shards))
			for i := range wireResults {
				wireResults[i] = &internalpb.SearchResults{IsAdvanced: true}
			}
			for reqIdx, subReq := range task.SubReqs {
				plan := &planpb.PlanNode{}
				require.NoError(t, proto.Unmarshal(subReq.GetSerializedExprPlan(), plan))
				expectedFields := append([]int64{100}, task.OutputFieldsId...)
				require.ElementsMatch(t, expectedFields, plan.GetOutputFieldIds())
				require.Equal(t, int64(101), task.queryInfos[reqIdx].GetGroupByFieldId())
				for shardIdx, shard := range shards {
					data := proto.Clone(shard).(*schemapb.SearchResultData)
					if shardIdx > 0 {
						ids := data.GetIds().GetIntId().GetData()
						value := multiGroupByTestLongField(103, append([]int64(nil), ids...))
						valid := []bool{true, false, true}
						vectors := []float32{20, 20, 30, 30}
						vectorValid := []bool{false, true, true}
						if shardIdx == 2 {
							valid = []bool{true, true, false}
							vectors = []float32{40, 40, 60, 60}
							vectorValid = []bool{true, false, true}
						}
						typeutil.SetFieldDataValidData(value, valid)
						fields := map[int64]*schemapb.FieldData{
							100: multiGroupByTestLongField(100, ids),
							103: value,
							102: {FieldId: 102, Type: schemapb.DataType_FloatVector,
								Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
									Dim: 2, ValidData: vectorValid,
									Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectors}},
								}}},
						}
						for _, fieldID := range plan.GetOutputFieldIds() {
							data.FieldsData = append(data.FieldsData, fields[fieldID])
						}
					}
					blob, err := proto.Marshal(data)
					require.NoError(t, err)
					wireResults[shardIdx].SubResults = append(wireResults[shardIdx].SubResults, &internalpb.SubSearchResults{
						ReqIndex: int64(reqIdx), NumQueries: 2, TopK: 3, MetricType: metric.IP, SlicedBlob: blob,
					})
				}
			}

			reducer, err := newHybridSearchReduceOperator(task, nil)
			require.NoError(t, err)
			reduced, err := reducer.run(ctx, span, wireResults)
			require.NoError(t, err)
			results := reduced[0].([]*milvuspb.SearchResults)
			for _, result := range results {
				require.Equal(t, []int64{3, 3}, result.Results.Topks)
				require.Equal(t, []int64{10, 20, 40, 30, 50, 60}, result.Results.Ids.GetIntId().GetData())
				group := result.Results.GetGroupByFieldValues()[0]
				require.Equal(t, []bool{true, false, true, true, true, false}, typeutil.GetFieldDataValidData(group))
				require.Equal(t, []string{"A", "", "C", "B", "D", ""}, group.GetScalars().GetStringData().GetData())
			}
			ranker, err := newRerankOperator(task, nil)
			require.NoError(t, err)
			ranked, err := ranker.run(ctx, span, reduced...)
			require.NoError(t, err)
			assembler, err := newHybridAssembleOperator(task, nil)
			require.NoError(t, err)
			var assembled []any
			require.NotPanics(t, func() {
				assembled, err = assembler.run(ctx, span, results, ranked[0])
			})
			require.NoError(t, err)
			result := assembled[0].(*milvuspb.SearchResults).GetResults()
			require.Equal(t, []int64{3, 3}, result.GetTopks())
			ids := result.GetIds().GetIntId().GetData()
			require.Equal(t, ids, reduce.FindFieldDataByID(result.FieldsData, 100).GetScalars().GetLongData().GetData())
			if tc.namespace {
				value := reduce.FindFieldDataByID(result.FieldsData, 103)
				require.Equal(t, ids, value.GetScalars().GetLongData().GetData())
				vector := reduce.FindFieldDataByID(result.FieldsData, 102)
				var expectedValueValid, expectedVectorValid []bool
				var expectedVectors []float32
				for _, id := range ids {
					expectedValueValid = append(expectedValueValid, id != 20 && id != 60)
					valid := id != 10 && id != 50
					expectedVectorValid = append(expectedVectorValid, valid)
					if valid {
						expectedVectors = append(expectedVectors, float32(id), float32(id))
					}
				}
				require.Equal(t, expectedValueValid, typeutil.GetFieldDataValidData(value))
				require.Equal(t, expectedVectorValid, typeutil.GetFieldDataValidData(vector))
				require.Equal(t, expectedVectors, vector.GetVectors().GetFloatVector().GetData())
			}
		})
	}
}
