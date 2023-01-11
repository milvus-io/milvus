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

package querynode

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

type ImplUtilsSuite struct {
	suite.Suite

	querynode *QueryNode
	client    *clientv3.Client
}

func (s *ImplUtilsSuite) SetupSuite() {
	s.querynode = newQueryNodeMock()
	client := v3client.New(embedetcdServer.Server)
	s.querynode.SetSession(sessionutil.NewSession(context.Background(), "milvus_ut/sessions", client))
	s.querynode.UpdateStateCode(commonpb.StateCode_Healthy)

	s.querynode.ShardClusterService = newShardClusterService(client, s.querynode.session, s.querynode)
}

func (s *ImplUtilsSuite) TearDownSuite() {
	s.querynode.Stop()
}

func (s *ImplUtilsSuite) SetupTest() {

	nodeEvent := []nodeEvent{
		{
			nodeID:   s.querynode.GetSession().ServerID,
			nodeAddr: s.querynode.GetSession().ServerName,
			isLeader: true,
		},
	}
	cs := NewShardCluster(defaultCollectionID, defaultReplicaID, defaultChannelName, defaultVersion, &mockNodeDetector{
		initNodes: nodeEvent,
	}, &mockSegmentDetector{}, buildMockQueryNode)

	s.querynode.ShardClusterService.clusters.Store(defaultChannelName, cs)
	cs.SetupFirstVersion()

}

func (s *ImplUtilsSuite) TearDownTest() {
	s.querynode.ShardClusterService.releaseCollection(defaultCollectionID)
}

func (s *ImplUtilsSuite) TestTransferLoad() {
	ctx := context.Background()
	s.Run("normal transfer load", func() {
		status, err := s.querynode.TransferLoad(ctx, &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				TargetID: s.querynode.GetSession().ServerID,
			},
			DstNodeID: s.querynode.GetSession().ServerID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     defaultSegmentID,
					InsertChannel: defaultChannelName,
					CollectionID:  defaultCollectionID,
					PartitionID:   defaultPartitionID,
				},
			},
		})

		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	s.Run("transfer non-exist channel load", func() {
		status, err := s.querynode.TransferLoad(ctx, &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				TargetID: s.querynode.GetSession().ServerID,
			},
			DstNodeID: s.querynode.GetSession().ServerID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     defaultSegmentID,
					InsertChannel: "invalid_channel",
					CollectionID:  defaultCollectionID,
					PartitionID:   defaultPartitionID,
				},
			},
		})

		s.NoError(err)
		s.Equal(commonpb.ErrorCode_NotShardLeader, status.GetErrorCode())
	})

	s.Run("transfer empty load segments", func() {
		status, err := s.querynode.TransferLoad(ctx, &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				TargetID: s.querynode.GetSession().ServerID,
			},
			DstNodeID: s.querynode.GetSession().ServerID,
			Infos:     []*querypb.SegmentLoadInfo{},
		})

		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	s.Run("transfer load fail", func() {
		cs, ok := s.querynode.ShardClusterService.getShardCluster(defaultChannelName)
		s.Require().True(ok)
		cs.nodes[100] = &shardNode{
			nodeID:   100,
			nodeAddr: "test",
			client: &mockShardQueryNode{
				loadSegmentsResults: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "error",
				},
			},
		}

		status, err := s.querynode.TransferLoad(ctx, &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				TargetID: s.querynode.GetSession().ServerID,
			},
			DstNodeID: 100,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     defaultSegmentID,
					InsertChannel: defaultChannelName,
					CollectionID:  defaultCollectionID,
					PartitionID:   defaultPartitionID,
				},
			},
		})

		s.NoError(err)
		s.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})

	s.Run("insufficient memory", func() {
		cs, ok := s.querynode.ShardClusterService.getShardCluster(defaultChannelName)
		s.Require().True(ok)
		cs.nodes[100] = &shardNode{
			nodeID:   100,
			nodeAddr: "test",
			client: &mockShardQueryNode{
				loadSegmentsResults: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_InsufficientMemoryToLoad,
					Reason:    "mock InsufficientMemoryToLoad",
				},
			},
		}

		status, err := s.querynode.TransferLoad(ctx, &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				TargetID: s.querynode.session.ServerID,
			},
			DstNodeID: 100,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     defaultSegmentID,
					InsertChannel: defaultChannelName,
					CollectionID:  defaultCollectionID,
					PartitionID:   defaultPartitionID,
				},
			},
		})

		s.NoError(err)
		s.Equal(commonpb.ErrorCode_InsufficientMemoryToLoad, status.GetErrorCode())
	})
}

func (s *ImplUtilsSuite) TestTransferRelease() {
	ctx := context.Background()
	s.Run("normal transfer release", func() {
		status, err := s.querynode.TransferRelease(ctx, &querypb.ReleaseSegmentsRequest{
			Base: &commonpb.MsgBase{
				TargetID: s.querynode.GetSession().ServerID,
			},
			SegmentIDs: []int64{},
			Scope:      querypb.DataScope_All,
			Shard:      defaultChannelName,
			NodeID:     s.querynode.GetSession().ServerID,
		})

		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	s.Run("transfer non-exist channel release", func() {
		status, err := s.querynode.TransferRelease(ctx, &querypb.ReleaseSegmentsRequest{
			Base: &commonpb.MsgBase{
				TargetID: s.querynode.GetSession().ServerID,
			},
			SegmentIDs: []int64{},
			Scope:      querypb.DataScope_All,
			Shard:      "invalid_channel",
			NodeID:     s.querynode.GetSession().ServerID,
		})

		s.NoError(err)
		s.Equal(commonpb.ErrorCode_NotShardLeader, status.GetErrorCode())
	})

	s.Run("transfer release fail", func() {
		cs, ok := s.querynode.ShardClusterService.getShardCluster(defaultChannelName)
		s.Require().True(ok)
		cs.nodes[100] = &shardNode{
			nodeID:   100,
			nodeAddr: "test",
			client: &mockShardQueryNode{
				releaseSegmentsResult: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
				},
			},
		}

		status, err := s.querynode.TransferRelease(ctx, &querypb.ReleaseSegmentsRequest{
			Base: &commonpb.MsgBase{
				TargetID: s.querynode.GetSession().ServerID,
			},
			SegmentIDs: []int64{},
			Scope:      querypb.DataScope_All,
			Shard:      defaultChannelName,
			NodeID:     100,
		})

		s.NoError(err)
		s.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})
}

func TestImplUtils(t *testing.T) {
	suite.Run(t, new(ImplUtilsSuite))
}
