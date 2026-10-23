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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type versionProbeCatalog struct{ metastore.QueryCoordCatalog }

func (versionProbeCatalog) SaveReplica(context.Context, ...*querypb.Replica) error { return nil }

func TestReusedChannelVersionControlsBalanceReduce(t *testing.T) {
	paramtable.Init()
	for _, serviceable := range []bool{true, false} {
		name := "both_serviceable"
		if !serviceable {
			name = "both_unserviceable"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			nodes := session.NewNodeManager()
			for _, id := range []int64{5, 13} {
				nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: id}))
			}
			m := meta.NewMeta(func() (int64, error) { return 1, nil }, versionProbeCatalog{}, nodes)
			replica := meta.NewReplica(&querypb.Replica{ID: 871, CollectionID: 1000, ResourceGroup: meta.DefaultResourceGroupName, RwSqNodes: []int64{5}, RoSqNodes: []int64{13}})
			require.NoError(t, m.Put(ctx, replica))
			dist := meta.NewDistributionManager(nodes)
			makeChannel := func(node, version int64, ready bool) *meta.DmChannel {
				return &meta.DmChannel{
					VchannelInfo: &datapb.VchannelInfo{CollectionID: 1000, ChannelName: "reused-channel"}, Node: node, Version: version,
					View: &meta.LeaderView{ID: node, CollectionID: 1000, Channel: "reused-channel", Status: &querypb.LeaderViewStatus{Serviceable: ready}},
				}
			}
			const old5 int64 = 1
			const old13 int64 = 2
			const new5 int64 = 3
			dist.ChannelDistManager.Update(5, makeChannel(5, old5, serviceable))
			dist.ChannelDistManager.Update(13, makeChannel(13, old13, serviceable))
			scheduler := &taskScheduler{ctx: ctx, meta: m, distMgr: dist, nodeMgr: nodes}
			task, err := NewChannelTask(ctx, time.Minute, WrapIDSource(1), 1000, replica,
				NewChannelAction(5, ActionTypeGrow, "reused-channel"), NewChannelAction(13, ActionTypeReduce, "reused-channel"))
			require.NoError(t, err)
			require.True(t, task.Actions()[0].IsFinished(dist), "target channel already present: Grow has finished")
			require.Equal(t, int64(13), dist.ChannelDistManager.GetShardLeader("reused-channel", replica).Node)
			scheduler.preProcess(task)
			require.Equal(t, 0, task.Step(), "old RO leader blocks advancement to Reduce")
			require.Equal(t, TaskStatusStarted, task.Status())
			t.Logf("old versions: leader=13, Grow present, step=%d (blocked)", task.Step())
			dist.ChannelDistManager.Update(5, makeChannel(5, new5, serviceable))
			require.Equal(t, int64(5), dist.ChannelDistManager.GetShardLeader("reused-channel", replica).Node)
			scheduler.preProcess(task)
			require.Equal(t, 1, task.Step(), "updating target generation allows Reduce")
			t.Logf("only target version updated: leader=5, step=%d (Reduce allowed)", task.Step())
			dist.ChannelDistManager.Update(13)
			scheduler.preProcess(task)
			require.Equal(t, TaskStatusSucceeded, task.Status())
			// Counterexample: a serviceable destination wins even at the older version.
			dist.ChannelDistManager.Update(5, makeChannel(5, old5, true))
			dist.ChannelDistManager.Update(13, makeChannel(13, old13, false))
			require.Equal(t, int64(5), dist.ChannelDistManager.GetShardLeader("reused-channel", replica).Node)
			t.Log("serviceability counterexample: older but serviceable node5 wins")
		})
	}
}
