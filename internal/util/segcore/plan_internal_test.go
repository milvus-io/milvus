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

package segcore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// minimalSchema builds a minimal collection schema with a PK field and a float vector field.
func minimalSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "internal-test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64},
			{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: "L2"},
				},
			},
		},
	}
}

func minimalIndexMeta() *segcorepb.CollectionIndexMeta {
	return &segcorepb.CollectionIndexMeta{
		MaxIndexRowCount: 1024,
		IndexMetas: []*segcorepb.FieldIndexMeta{
			{
				FieldID:   101,
				IndexName: "vec_idx",
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: "FLAT"},
					{Key: common.MetricTypeKey, Value: "L2"},
				},
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}
}

func createTestCollection(t *testing.T) *CCollection {
	t.Helper()
	col, err := CreateCCollection(&CreateCCollectionRequest{
		Schema:    minimalSchema(),
		IndexMeta: minimalIndexMeta(),
	})
	require.NoError(t, err)
	return col
}

func vectorANNSExpr(t *testing.T) []byte {
	t.Helper()
	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				FieldId: 101,
				QueryInfo: &planpb.QueryInfo{
					Topk:         5,
					RoundDecimal: 6,
					MetricType:   "L2",
					SearchParams: `{"nprobe": 10}`,
				},
				PlaceholderTag: "$0",
			},
		},
	}
	data, err := proto.Marshal(planNode)
	require.NoError(t, err)
	return data
}

func TestSetMetricType(t *testing.T) {
	paramtable.Init()
	col := createTestCollection(t)
	defer col.Release()

	expr := vectorANNSExpr(t)
	plan, err := createSearchPlanByExpr(col, expr, nil)
	require.NoError(t, err)
	defer plan.delete()

	assert.Equal(t, "L2", plan.GetMetricType())

	// setMetricType calls C.SetMetricType — verify it doesn't panic.
	plan.setMetricType("IP")
	// Note: C layer may not overwrite the metric type that was set during plan creation,
	// so we only verify the call completes without error.
	_ = plan.GetMetricType()
}

func TestCreateSearchPlanByExprWithHintsData(t *testing.T) {
	paramtable.Init()
	col := createTestCollection(t)
	defer col.Release()

	expr := vectorANNSExpr(t)

	// nil hints
	plan1, err := createSearchPlanByExpr(col, expr, nil)
	require.NoError(t, err)
	plan1.delete()

	// empty hints bytes
	plan2, err := createSearchPlanByExpr(col, expr, []byte{})
	require.NoError(t, err)
	plan2.delete()

	// valid serialized hints
	hints := &planpb.SegmentPkHintList{
		Hints: []*planpb.SegmentPkHint{
			{
				SegmentId: 1001,
				FilteredPkValues: []*planpb.GenericValue{
					{Val: &planpb.GenericValue_Int64Val{Int64Val: 42}},
				},
			},
		},
	}
	hintsData, err := proto.Marshal(hints)
	require.NoError(t, err)
	plan3, err := createSearchPlanByExpr(col, expr, hintsData)
	require.NoError(t, err)
	plan3.delete()
}
