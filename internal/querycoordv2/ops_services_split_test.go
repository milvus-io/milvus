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

package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitMoveCase is the shard split view one manual move request meets: the
// shard states the coordinator answers with (or describeErr), the window
// targets the next target marks, and the channels the current target lists.
type splitMoveCase struct {
	states      *milvuspb.DescribeCollectionResponse
	describeErr error
	window      []string
	current     []string
}

// newSplitMoveServer builds the smallest healthy server a manual move reaches
// the split gate on: collection 1 fully loaded, one replica over nodes 1 and 2,
// nothing in dist.
func newSplitMoveServer(t *testing.T, c splitMoveCase) *Server {
	ctx := context.Background()
	nodeMgr := session.NewNodeManager()
	for _, node := range []int64{1, 2} {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: node, Address: "localhost", Hostname: "localhost"}))
	}
	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
	m := meta.NewMeta(params.RandomIncrementIDAllocator(), catalog, nodeMgr)
	partition := utils.CreateTestPartition(1, 10)
	partition.LoadPercentage = 100
	require.NoError(t, m.PutCollection(ctx, utils.CreateTestCollection(1, 1), partition))
	require.NoError(t, m.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2})))
	dist := meta.NewDistributionManager(nodeMgr)

	targetMgr := meta.NewMockTargetManager(t)
	var window typeutil.Set[string]
	if len(c.window) > 0 {
		window = typeutil.NewSet(c.window...)
	}
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, int64(1), meta.NextTarget).Return(window).Maybe()
	current := make(map[string]*meta.DmChannel, len(c.current))
	for _, name := range c.current {
		current[name] = &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: name}}
	}
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.CurrentTarget).Return(current).Maybe()
	// the freeze reads the next target for the adopted targets awaiting the flip.
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.NextTarget).Return(nil).Maybe()

	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(1)).Return(c.states, c.describeErr).Maybe()

	server := &Server{
		meta:       m,
		dist:       dist,
		nodeMgr:    nodeMgr,
		targetMgr:  targetMgr,
		splitState: meta.NewShardSplitStateCache(broker, time.Minute),
	}
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	return server
}

// Manual moves -- LoadBalance, TransferSegment and TransferChannel -- rebuild a
// split source on another node without the in-process children it fronts,
// exactly as a balance move would. They are refused under the same rule as the
// balance freeze (meta.CheckShardSplitMovable), with a retriable System error.
func TestManualMovesRespectTheShardSplitFreeze(t *testing.T) {
	ctx := context.Background()
	normal := &milvuspb.DescribeCollectionResponse{VirtualChannelNames: []string{"v0", "v9"}}
	splitting := &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1", "v2", "v9"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting},
			{State: schemapb.ShardState_ShardCreating},
			{State: schemapb.ShardState_ShardCreating},
			{State: schemapb.ShardState_ShardNormal},
		},
	}
	adopted := &milvuspb.DescribeCollectionResponse{VirtualChannelNames: []string{"v1", "v2", "v9"}}

	moves := map[string]func(*Server) *commonpb.Status{
		"LoadBalance": func(s *Server) *commonpb.Status {
			resp, err := s.LoadBalance(ctx, &querypb.LoadBalanceRequest{CollectionID: 1, SourceNodeIDs: []int64{1}, DstNodeIDs: []int64{2}})
			require.NoError(t, err)
			return resp
		},
		"TransferSegment": func(s *Server) *commonpb.Status {
			resp, err := s.TransferSegment(ctx, &querypb.TransferSegmentRequest{SourceNodeID: 1, ToAllNodes: true, TransferAll: true})
			require.NoError(t, err)
			return resp
		},
		"TransferChannel": func(s *Server) *commonpb.Status {
			resp, err := s.TransferChannel(ctx, &querypb.TransferChannelRequest{SourceNodeID: 1, ToAllNodes: true, TransferAll: true})
			require.NoError(t, err)
			return resp
		},
	}
	refused := map[string]splitMoveCase{
		"split window open":                {states: splitting, current: []string{"v0", "v9"}},
		"retired source still current":     {states: adopted, current: []string{"v0", "v9"}},
		"next target marks window targets": {states: normal, window: []string{"v1", "v2"}, current: []string{"v0", "v9"}},
		"shard states unknown":             {describeErr: merr.WrapErrServiceUnavailable("rootcoord down"), current: []string{"v0", "v9"}},
	}

	// A move the gate lets through reaches the balancing step, which is stubbed
	// here: it counts the move instead of planning tasks.
	balanced := 0
	mockSegments := mockey.Mock((*Server).balanceSegments).To(func(*Server, context.Context, int64, *meta.Replica,
		int64, []int64, []*meta.Segment, bool, bool,
	) error {
		balanced++
		return nil
	}).Build()
	defer mockSegments.UnPatch()
	mockChannels := mockey.Mock((*Server).balanceChannels).To(func(*Server, context.Context, int64, *meta.Replica,
		int64, []int64, []*meta.DmChannel, bool, bool,
	) error {
		balanced++
		return nil
	}).Build()
	defer mockChannels.UnPatch()

	for name, move := range moves {
		for why, c := range refused {
			t.Run(name+"/refused: "+why, func(t *testing.T) {
				balanced = 0
				status := move(newSplitMoveServer(t, c))
				assert.Zero(t, balanced, "a refused move must move nothing")
				err := merr.Error(status)
				assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
				assert.True(t, merr.IsRetryableErr(err))
			})
		}
		t.Run(name+"/allowed: not splitting", func(t *testing.T) {
			balanced = 0
			status := move(newSplitMoveServer(t, splitMoveCase{states: normal, current: []string{"v0", "v9"}}))
			assert.True(t, merr.Ok(status), merr.Error(status))
			assert.Equal(t, 1, balanced)
		})
	}
}
