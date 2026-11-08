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

package querynodev2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// legacyCountPlan is the plan a 2.6 Proxy builds for count(*): is_count set,
// no aggregates, no output fields.
func legacyCountPlan(t *testing.T) []byte {
	t.Helper()
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{IsCount: true},
		},
	}
	serialized, err := proto.Marshal(plan)
	require.NoError(t, err)
	return serialized
}

func unmarshalPlan(t *testing.T, serialized []byte) *planpb.PlanNode {
	t.Helper()
	plan := &planpb.PlanNode{}
	require.NoError(t, proto.Unmarshal(serialized, plan))
	return plan
}

func TestUpgradeLegacyCountRequest(t *testing.T) {
	countAll := []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 0}}

	t.Run("legacy count request gets one count aggregate and keeps is_count", func(t *testing.T) {
		req := &internalpb.RetrieveRequest{
			IsCount:            true,
			SerializedExprPlan: legacyCountPlan(t),
		}

		require.NoError(t, upgradeLegacyCountRequest(context.Background(), req))

		require.True(t, req.GetIsCount())
		require.True(t, proto.Equal(&planpb.Aggregate{Op: planpb.AggregateOp_count}, req.GetAggregates()[0]))
		require.Len(t, req.GetAggregates(), 1)
		query := unmarshalPlan(t, req.GetSerializedExprPlan()).GetQuery()
		require.True(t, query.GetIsCount())
		require.Len(t, query.GetAggregates(), 1)
		require.True(t, proto.Equal(countAll[0], query.GetAggregates()[0]))
	})

	t.Run("aggregate request from a current proxy is unchanged", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{Aggregates: countAll},
			},
		}
		serialized, err := proto.Marshal(plan)
		require.NoError(t, err)
		req := &internalpb.RetrieveRequest{
			Aggregates:         countAll,
			SerializedExprPlan: serialized,
		}
		before := proto.Clone(req).(*internalpb.RetrieveRequest)

		require.NoError(t, upgradeLegacyCountRequest(context.Background(), req))

		require.True(t, proto.Equal(before, req))
	})

	t.Run("plain retrieve request is unchanged", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node:           &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
			OutputFieldIds: []int64{100, 1},
		}
		serialized, err := proto.Marshal(plan)
		require.NoError(t, err)
		req := &internalpb.RetrieveRequest{
			OutputFieldsId:     []int64{100, 1},
			SerializedExprPlan: serialized,
		}
		before := proto.Clone(req).(*internalpb.RetrieveRequest)

		require.NoError(t, upgradeLegacyCountRequest(context.Background(), req))

		require.True(t, proto.Equal(before, req))
	})

	t.Run("legacy request with a plan that does not unmarshal returns an error", func(t *testing.T) {
		req := &internalpb.RetrieveRequest{
			IsCount:            true,
			SerializedExprPlan: []byte{0xff, 0xff, 0xff},
		}

		require.Error(t, upgradeLegacyCountRequest(context.Background(), req))
	})

	t.Run("legacy request whose plan is not a query plan returns an error", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{}},
		}
		serialized, err := proto.Marshal(plan)
		require.NoError(t, err)
		req := &internalpb.RetrieveRequest{
			IsCount:            true,
			SerializedExprPlan: serialized,
		}
		before := proto.Clone(req).(*internalpb.RetrieveRequest)

		require.Error(t, upgradeLegacyCountRequest(context.Background(), req))

		require.True(t, proto.Equal(before, req))
	})
}
