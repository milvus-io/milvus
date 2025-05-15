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

package balance

import (
	"context"
	"fmt"
	"sort"

	"github.com/blang/semver/v4"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type SegmentAssignPlan struct {
	Segment      *meta.Segment
	Replica      *meta.Replica
	From         int64 // -1 if empty
	To           int64
	FromScore    int64
	ToScore      int64
	SegmentScore int64
}

func (segPlan *SegmentAssignPlan) String() string {
	return fmt.Sprintf("SegmentPlan:[collectionID: %d, replicaID: %d, segmentID: %d, from: %d, to: %d, fromScore: %d, toScore: %d, segmentScore: %d]\n",
		segPlan.Segment.CollectionID, segPlan.Replica.GetID(), segPlan.Segment.ID, segPlan.From, segPlan.To, segPlan.FromScore, segPlan.ToScore, segPlan.SegmentScore)
}

type ChannelAssignPlan struct {
	Channel      *meta.DmChannel
	Replica      *meta.Replica
	From         int64
	To           int64
	FromScore    int64
	ToScore      int64
	ChannelScore int64
}

func (chanPlan *ChannelAssignPlan) String() string {
	return fmt.Sprintf("ChannelPlan:[collectionID: %d, channel: %s, replicaID: %d, from: %d, to: %d]\n",
		chanPlan.Channel.CollectionID, chanPlan.Channel.ChannelName, chanPlan.Replica.GetID(), chanPlan.From, chanPlan.To)
}

type Balance interface {
	AssignSegment(ctx context.Context, collectionID int64, segments []*meta.Segment, nodes []int64, forceAssign bool) []SegmentAssignPlan
	AssignChannel(ctx context.Context, collectionID int64, channels []*meta.DmChannel, nodes []int64, forceAssign bool) []ChannelAssignPlan
	BalanceReplica(ctx context.Context, replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan)
}

type RoundRobinBalancer struct {
	scheduler   task.Scheduler
	nodeManager *session.NodeManager
}

func (b *RoundRobinBalancer) AssignSegment(ctx context.Context, collectionID int64, segments []*meta.Segment, nodes []int64, forceAssign bool) []SegmentAssignPlan {
	// skip out suspend node and stopping node during assignment, but skip this check for manual balance
	if !forceAssign {
		nodes = lo.Filter(nodes, func(node int64, _ int) bool {
			info := b.nodeManager.Get(node)
			return info != nil && info.GetState() == session.NodeStateNormal
		})
	}

	nodesInfo := b.getNodes(nodes)
	if len(nodesInfo) == 0 {
		return nil
	}
	sort.Slice(nodesInfo, func(i, j int) bool {
		cnt1, cnt2 := nodesInfo[i].SegmentCnt(), nodesInfo[j].SegmentCnt()
		id1, id2 := nodesInfo[i].ID(), nodesInfo[j].ID()
		delta1, delta2 := b.scheduler.GetSegmentTaskDelta(id1, -1), b.scheduler.GetSegmentTaskDelta(id2, -1)
		return cnt1+delta1 < cnt2+delta2
	})

	balanceBatchSize := paramtable.Get().QueryCoordCfg.BalanceSegmentBatchSize.GetAsInt()
	ret := make([]SegmentAssignPlan, 0, len(segments))
	for i, s := range segments {
		plan := SegmentAssignPlan{
			Segment: s,
			From:    -1,
			To:      nodesInfo[i%len(nodesInfo)].ID(),
		}
		ret = append(ret, plan)
		if len(ret) > balanceBatchSize {
			break
		}
	}
	return ret
}

func (b *RoundRobinBalancer) AssignChannel(ctx context.Context, collectionID int64, channels []*meta.DmChannel, nodes []int64, forceAssign bool) []ChannelAssignPlan {
	nodes = filterSQNIfStreamingServiceEnabled(nodes)

	// skip out suspend node and stopping node during assignment, but skip this check for manual balance
	if !forceAssign {
		versionRangeFilter := semver.MustParseRange(">2.3.x")
		nodes = lo.Filter(nodes, func(node int64, _ int) bool {
			info := b.nodeManager.Get(node)
			// balance channel to qn with version < 2.4 is not allowed since l0 segment supported
			// if watch channel on qn with version < 2.4, it may cause delete data loss
			return info != nil && info.GetState() == session.NodeStateNormal && versionRangeFilter(info.Version())
		})
	}
	nodesInfo := b.getNodes(nodes)
	if len(nodesInfo) == 0 {
		return nil
	}

	plans := make([]ChannelAssignPlan, 0)
	scoreDelta := make(map[int64]int)
	if streamingutil.IsStreamingServiceEnabled() {
		channels, plans, scoreDelta = assignChannelToWALLocatedFirstForNodeInfo(channels, nodesInfo)
	}

	sort.Slice(nodesInfo, func(i, j int) bool {
		cnt1, cnt2 := nodesInfo[i].ChannelCnt(), nodesInfo[j].ChannelCnt()
		id1, id2 := nodesInfo[i].ID(), nodesInfo[j].ID()
		delta1, delta2 := b.scheduler.GetChannelTaskDelta(id1, -1)+scoreDelta[id1], b.scheduler.GetChannelTaskDelta(id2, -1)+scoreDelta[id2]
		return cnt1+delta1 < cnt2+delta2
	})

	for i, c := range channels {
		plan := ChannelAssignPlan{
			Channel: c,
			From:    -1,
			To:      nodesInfo[i%len(nodesInfo)].ID(),
		}
		plans = append(plans, plan)
	}
	return plans
}

func (b *RoundRobinBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	// TODO by chun.han
	return nil, nil
}

func (b *RoundRobinBalancer) permitBalanceChannel(collectionID int64) bool {
	return b.scheduler.GetSegmentTaskNum(task.WithCollectionID2TaskFilter(collectionID), task.WithTaskTypeFilter(task.TaskTypeMove)) == 0
}

func (b *RoundRobinBalancer) permitBalanceSegment(collectionID int64) bool {
	return b.scheduler.GetChannelTaskNum(task.WithCollectionID2TaskFilter(collectionID), task.WithTaskTypeFilter(task.TaskTypeMove)) == 0
}

func (b *RoundRobinBalancer) getNodes(nodes []int64) []*session.NodeInfo {
	ret := make([]*session.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		node := b.nodeManager.Get(n)
		if node != nil && !node.IsStoppingState() {
			ret = append(ret, node)
		}
	}
	return ret
}

func NewRoundRobinBalancer(scheduler task.Scheduler, nodeManager *session.NodeManager) *RoundRobinBalancer {
	return &RoundRobinBalancer{
		scheduler:   scheduler,
		nodeManager: nodeManager,
	}
}
