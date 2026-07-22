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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/sbbf"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func bloomPlanSizeTestLeaf(blob []byte) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_BloomFilterExpr{
		BloomFilterExpr: &planpb.BloomFilterExpr{FilterBlob: blob},
	}}
}

func bloomPlanSizeTestPlan(blob []byte, copies int) *planpb.PlanNode {
	if copies < 1 {
		copies = 1
	}
	predicate := bloomPlanSizeTestLeaf(blob)
	for i := 1; i < copies; i++ {
		predicate = &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op:    planpb.BinaryExpr_LogicalAnd,
				Left:  predicate,
				Right: bloomPlanSizeTestLeaf(blob),
			},
		}}
	}
	return &planpb.PlanNode{Node: &planpb.PlanNode_Query{
		Query: &planpb.QueryPlanNode{Predicates: predicate},
	}}
}

func TestMarshalPlanWithBloomFilterSizeLimit(t *testing.T) {
	params := paramtable.Get()
	key := params.ProxyCfg.MaxBloomFilterPlanSize.Key
	t.Cleanup(func() { params.Reset(key) })

	blob := make([]byte, 4*1024)
	repeatedPlan := bloomPlanSizeTestPlan(blob, 3)
	repeatedPlanSize := proto.Size(repeatedPlan)

	t.Run("exact limit succeeds", func(t *testing.T) {
		params.Save(key, strconv.Itoa(repeatedPlanSize))
		serialized, accumulated, err := marshalPlanWithBloomFilterSizeLimit(repeatedPlan, 0)
		require.NoError(t, err)
		assert.Len(t, serialized, repeatedPlanSize)
		assert.Equal(t, int64(repeatedPlanSize), accumulated)
	})

	t.Run("repeated reference over limit is input error 1102", func(t *testing.T) {
		params.Save(key, strconv.Itoa(repeatedPlanSize-1))
		serialized, accumulated, err := marshalPlanWithBloomFilterSizeLimit(repeatedPlan, 0)
		require.Error(t, err)
		assert.Nil(t, serialized, "the amplified plan must be rejected before proto.Marshal allocates its output")
		assert.Zero(t, accumulated)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Equal(t, int32(1102), merr.Code(err))
		assert.Equal(t, merr.InputError, merr.GetErrorType(err))
		assert.False(t, merr.IsRetryableErr(err))
		assert.Contains(t, err.Error(), "proxy.maxBloomFilterPlanSize")

		status := merr.Status(err)
		assert.Equal(t, int32(1102), status.GetCode())
		assert.False(t, status.GetRetriable())
		_, markedInput := status.GetExtraInfo()[merr.InputErrorFlagKey]
		assert.True(t, markedInput)
	})

	t.Run("hybrid sub-plans share one aggregate budget", func(t *testing.T) {
		singlePlan := bloomPlanSizeTestPlan(blob, 1)
		singlePlanSize := proto.Size(singlePlan)
		params.Save(key, strconv.Itoa(singlePlanSize*2-1))

		first, accumulated, err := marshalPlanWithBloomFilterSizeLimit(singlePlan, 0)
		require.NoError(t, err)
		assert.Len(t, first, singlePlanSize)
		assert.Equal(t, int64(singlePlanSize), accumulated)

		second, unchanged, err := marshalPlanWithBloomFilterSizeLimit(singlePlan, accumulated)
		require.Error(t, err)
		assert.Nil(t, second)
		assert.Equal(t, accumulated, unchanged)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("scorer filter is included", func(t *testing.T) {
		scorerPlan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
			Scorers: []*planpb.ScoreFunction{{
				Filter: bloomPlanSizeTestLeaf(blob),
			}},
		}
		params.Save(key, strconv.Itoa(proto.Size(scorerPlan)-1))
		serialized, _, err := marshalPlanWithBloomFilterSizeLimit(scorerPlan, 0)
		require.Error(t, err)
		assert.Nil(t, serialized)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("large non-bloom plan is unaffected", func(t *testing.T) {
		nonBloomPlan := &planpb.PlanNode{
			Node:           &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
			OutputFieldIds: make([]int64, 4096),
		}
		params.Save(key, "1")
		serialized, accumulated, err := marshalPlanWithBloomFilterSizeLimit(nonBloomPlan, 0)
		require.NoError(t, err)
		assert.NotEmpty(t, serialized)
		assert.Zero(t, accumulated)
	})
}

func bloomPlanSizeIntegrationFixture(t *testing.T) (*schemaInfo, map[string]*schemapb.TemplateValue) {
	builder, err := sbbf.NewBuilder(3, 0.001)
	require.NoError(t, err)
	for _, value := range []int64{1, 2, 3} {
		builder.AddInt64(value)
	}
	blob := builder.Marshal()

	schema := &schemapb.CollectionSchema{
		Name: "bloom_plan_size_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
			},
		},
	}
	schemaInfo, err := newSchemaInfo(schema)
	require.NoError(t, err)
	return schemaInfo, map[string]*schemapb.TemplateValue{
		"bf": {Val: &schemapb.TemplateValue_BytesVal{BytesVal: blob}},
	}
}

func bloomPlanSizeSearchParams() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{Key: AnnsFieldKey, Value: "vec"},
		{Key: TopKKey, Value: "10"},
		{Key: common.MetricTypeKey, Value: metric.L2},
		{Key: ParamsKey, Value: `{"nprobe": 10}`},
	}
}

func TestSearchTaskBloomFilterPlanSizeLimit(t *testing.T) {
	params := paramtable.Get()
	key := params.ProxyCfg.MaxBloomFilterPlanSize.Key
	params.Save(key, "1")
	t.Cleanup(func() { params.Reset(key) })

	ctx := context.Background()
	schema, templateValues := bloomPlanSizeIntegrationFixture(t)

	t.Run("normal search", func(t *testing.T) {
		task := &searchTask{
			ctx:            ctx,
			collectionName: schema.GetName(),
			SearchRequest: &internalpb.SearchRequest{
				CollectionID: 1,
				DslType:      commonpb.DslType_BoolExprV1,
			},
			request: &milvuspb.SearchRequest{
				CollectionName:     schema.GetName(),
				Dsl:                "bloom_match(pk, {bf})",
				ExprTemplateValues: templateValues,
				SearchParams:       bloomPlanSizeSearchParams(),
			},
			schema: schema,
			tr:     timerecord.NewTimeRecorder("bloom-plan-size-normal-search"),
		}

		err := task.initSearchRequest(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Equal(t, int32(1102), merr.Code(err))
	})

	t.Run("hybrid search", func(t *testing.T) {
		task := &searchTask{
			ctx:            ctx,
			collectionName: schema.GetName(),
			SearchRequest: &internalpb.SearchRequest{
				CollectionID: 1,
			},
			request: &milvuspb.SearchRequest{
				CollectionName: schema.GetName(),
				SearchParams: []*commonpb.KeyValuePair{
					{Key: LimitKey, Value: "10"},
				},
				SubReqs: []*milvuspb.SubSearchRequest{{
					Nq:                 1,
					Dsl:                "bloom_match(pk, {bf})",
					ExprTemplateValues: templateValues,
					SearchParams:       bloomPlanSizeSearchParams(),
				}},
			},
			schema: schema,
			tr:     timerecord.NewTimeRecorder("bloom-plan-size-hybrid-search"),
		}

		err := task.initAdvancedSearchRequest(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assert.Equal(t, int32(1102), merr.Code(err))
	})
}

func TestQueryTaskBloomFilterPlanSizeLimit(t *testing.T) {
	params := paramtable.Get()
	key := params.ProxyCfg.MaxBloomFilterPlanSize.Key
	params.Save(key, "1")
	t.Cleanup(func() { params.Reset(key) })

	ctx := context.Background()
	schema, templateValues := bloomPlanSizeIntegrationFixture(t)
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
	cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&collectionInfo{}, nil).Maybe()
	cache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).
		Return(schema, nil).Maybe()
	cache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{}, nil)

	oldCache := globalMetaCache
	globalMetaCache = cache
	t.Cleanup(func() { globalMetaCache = oldCache })

	task := &queryTask{
		ctx: ctx,
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base:       &commonpb.MsgBase{},
			QueryLabel: "query",
		},
		request: &milvuspb.QueryRequest{
			CollectionName:     schema.GetName(),
			Expr:               "bloom_match(pk, {bf})",
			ExprTemplateValues: templateValues,
		},
	}

	err := task.PreExecute(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	assert.Equal(t, int32(1102), merr.Code(err))
}
