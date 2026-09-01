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

package checkers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// A collection whose split has been adopted: v0 is the retired source, v1 and v2
// are the targets that replaced it.
func adoptedSplitResp() *milvuspb.DescribeCollectionResponse {
	return &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1", "v2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardDropped},
			{State: schemapb.ShardState_ShardNormal},
			{State: schemapb.ShardState_ShardNormal},
		},
	}
}

func servingChannel(name string, node int64) *meta.DmChannel {
	return &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: name},
		Node:         node,
		Version:      1,
		View: &meta.LeaderView{
			ID: node, Channel: name, Version: 1,
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	}
}

// releasedSources runs one round of the diff and reports which channels it chose
// to release.
func releasedSources(t *testing.T, currentTarget map[string]*meta.DmChannel) []string {
	nodeMgr := session.NewNodeManager()
	for _, id := range []int64{1} {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID: id, Address: "localhost", Hostname: "localhost",
		}))
	}
	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
	m := meta.NewMeta(RandomIncrementIDAllocator(), catalog, nodeMgr)
	ctx := context.Background()
	require.NoError(t, m.PutCollection(ctx, utils.CreateTestCollection(1, 1)))
	require.NoError(t, m.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1})))

	dist := meta.NewDistributionManager(nodeMgr)
	// the retired source and both of its targets are loaded and serving
	dist.ChannelDistManager.Update(1, servingChannel("v0", 1), servingChannel("v1", 1), servingChannel("v2", 1))

	targetMgr := meta.NewMockTargetManager(t)
	next := map[string]*meta.DmChannel{"v1": servingChannel("v1", 1), "v2": servingChannel("v2", 1)}
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.NextTarget).Return(next).Maybe()
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.CurrentTarget).Return(currentTarget).Maybe()

	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(adoptedSplitResp(), nil).Maybe()

	// The checker's constructor reaches for the global assign policy factory.
	scheduler := task.NewMockScheduler(t)
	assign.InitGlobalAssignPolicyFactory(scheduler, nodeMgr, dist, m, targetMgr)
	t.Cleanup(assign.ResetGlobalAssignPolicyFactoryForTest)

	checker := NewChannelChecker(m, dist, targetMgr, nodeMgr, scheduler,
		meta.NewShardSplitStateCache(broker, time.Minute))

	_, toRelease := checker.getDmChannelDiff(ctx, 1, 1)
	names := make([]string, 0, len(toRelease))
	for _, ch := range toRelease {
		names = append(names, ch.GetChannelName())
	}
	return names
}

func TestARetiredSourceIsHeldWhileReadsStillRouteToIt(t *testing.T) {
	// GetShardLeaders enumerates the CURRENT target, and one channel with no
	// leader fails the whole call rather than just that shard. So releasing the
	// source while it is still listed there takes every read of the collection
	// down until the current target catches up -- 44s in an E2E run.
	current := map[string]*meta.DmChannel{
		"v0": servingChannel("v0", 1), // still routed
		"v1": servingChannel("v1", 1),
		"v2": servingChannel("v2", 1),
	}
	assert.NotContains(t, releasedSources(t, current), "v0",
		"the source must stay loaded while the current target still lists it")
}

func TestARetiredSourceIsReleasedOnceTheCurrentTargetDropsIt(t *testing.T) {
	// The other half: waiting must not mean waiting forever. Once the current
	// target advances past the source, reads no longer fan out to it and it is
	// safe to let go.
	current := map[string]*meta.DmChannel{
		"v1": servingChannel("v1", 1),
		"v2": servingChannel("v2", 1),
	}
	assert.Contains(t, releasedSources(t, current), "v0")
}
