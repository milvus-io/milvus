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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSearchTask_SearchShardRecordsNodeHint(t *testing.T) {
	ctx := context.Background()
	const channel = "by-dev-rootcoord-dml_0_100v0"
	const nodeID int64 = 101

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Search(mock.Anything, mock.MatchedBy(func(req *querypb.SearchRequest) bool {
		return len(req.GetDmlChannels()) == 1 && req.GetDmlChannels()[0] == channel
	})).Return(&internalpb.SearchResults{
		Status:          merr.Success(),
		CostAggregation: &internalpb.CostAggregation{},
	}, nil)

	lb := shardclient.NewMockLBPolicy(t)
	lb.EXPECT().UpdateCostMetrics(nodeID, mock.Anything).Return()

	task := &SearchTask{
		ctx: ctx,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: paramtable.GetNodeID(),
			},
		},
		request: &milvuspb.SearchRequest{
			DbName:         "default",
			CollectionName: "test_search_shard_records_node_hint",
		},
		Condition:         NewTaskCondition(ctx),
		lb:                lb,
		resultBuf:         typeutil.NewConcurrentSet[*internalpb.SearchResults](),
		queryChannelsNode: typeutil.NewConcurrentMap[string, int64](),
	}

	err := task.searchShard(ctx, nodeID, qn, channel)
	require.NoError(t, err)
	recordedNodeID, ok := task.queryChannelsNode.Get(channel)
	require.True(t, ok)
	assert.Equal(t, nodeID, recordedNodeID)
}

func TestSearchTask_SearchShardInvalidatesCacheOnCollectionNotLoaded(t *testing.T) {
	ctx := context.Background()
	const channel = "by-dev-rootcoord-dml_0_100v0"
	const nodeID int64 = 101
	const collectionID int64 = 100

	newTask := func(manager shardclient.ShardClientMgr) *SearchTask {
		return &SearchTask{
			ctx: ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Search,
					SourceID: paramtable.GetNodeID(),
				},
				CollectionID: collectionID,
			},
			request: &milvuspb.SearchRequest{
				DbName:         "default",
				CollectionName: "test_search_shard_invalidate_on_not_loaded",
			},
			Condition:         NewTaskCondition(ctx),
			shardClientMgr:    manager,
			resultBuf:         typeutil.NewConcurrentSet[*internalpb.SearchResults](),
			queryChannelsNode: typeutil.NewConcurrentMap[string, int64](),
		}
	}

	t.Run("collection_not_loaded_invalidates_cache", func(t *testing.T) {
		qn := mocks.NewMockQueryNodeClient(t)
		qn.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
			Status: merr.Status(merr.WrapErrCollectionNotLoaded(collectionID)),
		}, nil)
		manager := shardclient.NewMockShardClientManager(t)
		manager.EXPECT().InvalidateShardLeaderCache([]int64{collectionID}).Return()

		err := newTask(manager).searchShard(ctx, nodeID, qn, channel)
		require.Error(t, err)
	})

	t.Run("not_shard_leader_invalidates_cache", func(t *testing.T) {
		qn := mocks.NewMockQueryNodeClient(t)
		qn.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_NotShardLeader},
		}, nil)
		manager := shardclient.NewMockShardClientManager(t)
		manager.EXPECT().InvalidateShardLeaderCache([]int64{collectionID}).Return()

		err := newTask(manager).searchShard(ctx, nodeID, qn, channel)
		require.Error(t, err)
	})

	t.Run("other_system_error_keeps_cache", func(t *testing.T) {
		qn := mocks.NewMockQueryNodeClient(t)
		qn.EXPECT().Search(mock.Anything, mock.Anything).Return(&internalpb.SearchResults{
			Status: merr.Status(merr.WrapErrServiceInternal("boom")),
		}, nil)
		manager := shardclient.NewMockShardClientManager(t)

		err := newTask(manager).searchShard(ctx, nodeID, qn, channel)
		require.Error(t, err)
	})
}
