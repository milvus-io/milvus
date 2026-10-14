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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSubscribeChannelSelectsRecoveryTargetConsistently(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name          string
		next, removed bool
		scope         int32
	}{
		{name: "current_only", scope: meta.CurrentTarget},
		{name: "next_preferred", next: true, scope: meta.NextTarget},
		{name: "removed_from_next", next: true, removed: true, scope: meta.NextTarget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			metadata := &meta.Meta{CollectionManager: meta.NewCollectionManager(nil)}
			require.NoError(t, metadata.PutCollectionWithoutSave(ctx, &meta.Collection{
				CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 1, LoadType: querypb.LoadType_LoadCollection},
			}))
			broker := meta.NewMockBroker(t)
			broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(&milvuspb.DescribeCollectionResponse{
				Schema: &schemapb.CollectionSchema{Name: "recovery"},
			}, nil)
			broker.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{}, nil)
			broker.EXPECT().ListIndexes(mock.Anything, int64(1)).Return([]*indexpb.IndexInfo{}, nil)
			target := meta.NewMockTargetManager(t)
			target.EXPECT().GetPartitions(mock.Anything, int64(1), meta.NextTargetFirst).Return([]int64{10}, nil)
			target.EXPECT().IsNextTargetExist(mock.Anything, int64(1)).Return(tc.next)
			var channel *meta.DmChannel
			if !tc.removed {
				channel = &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{
					CollectionID: 1, ChannelName: "channel", SeekPosition: &msgpb.MsgPosition{Timestamp: 12345},
				}}
				target.EXPECT().GetCollectionTargetVersion(mock.Anything, int64(1), tc.scope).Return(int64(99))
				target.EXPECT().GetSealedSegmentsByChannel(mock.Anything, int64(1), "channel", tc.scope).Return(map[int64]*datapb.SegmentInfo{
					17: {ID: 17, NumOfRows: 321},
				})
			}
			target.EXPECT().GetDmChannel(mock.Anything, int64(1), "channel", tc.scope).Return(channel)
			cluster := session.NewMockCluster(t)
			if !tc.removed {
				cluster.EXPECT().WatchDmChannels(mock.Anything, int64(2), mock.Anything).Run(func(_ context.Context, _ int64, req *querypb.WatchDmChannelsRequest) {
					require.EqualValues(t, 99, req.GetTargetVersion())
					require.Len(t, req.GetInfos(), 1)
					require.EqualValues(t, 12345, req.GetInfos()[0].GetSeekPosition().GetTimestamp())
					require.Equal(t, map[int64]int64{17: 321}, req.GetSealedSegmentRowCount())
				}).Return(merr.Success(), nil).Once()
			}
			nodes := session.NewNodeManager()
			nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2, Version: common.Version}))
			ex := NewExecutor(2, metadata, nil, broker, target, cluster, nodes)
			replica := meta.NewReplica(&querypb.Replica{
				ID: 10, CollectionID: 1, ResourceGroup: "recovery-rg", Nodes: []int64{2},
			})
			work, err := NewChannelTask(ctx, time.Minute, WrapIDSource(0), 1, replica, NewChannelAction(2, ActionTypeGrow, "channel"))
			require.NoError(t, err)
			defer work.Cancel(nil)
			ex.channelTaskNum.Inc()
			err = ex.subscribeChannel(work, 0)
			if tc.removed {
				require.Error(t, err, "an existing Next target must not fall back to a retired Current channel")
				cluster.AssertNotCalled(t, "WatchDmChannels", mock.Anything, mock.Anything, mock.Anything)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
