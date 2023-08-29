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
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type SegmentAssignPlan struct {
	Segment   *meta.Segment
	ReplicaID int64
	From      int64 // -1 if empty
	To        int64
}

func (segPlan *SegmentAssignPlan) ToString() string {
	return fmt.Sprintf("SegmentPlan:[collectionID: %d, replicaID: %d, segmentID: %d, from: %d, to: %d]\n",
		segPlan.Segment.CollectionID, segPlan.ReplicaID, segPlan.Segment.ID, segPlan.From, segPlan.To)
}

type ChannelAssignPlan struct {
	Channel   *meta.DmChannel
	ReplicaID int64
	From      int64
	To        int64
}

func (chanPlan *ChannelAssignPlan) ToString() string {
	return fmt.Sprintf("ChannelPlan:[collectionID: %d, channel: %s, replicaID: %d, from: %d, to: %d]\n",
		chanPlan.Channel.CollectionID, chanPlan.Channel.ChannelName, chanPlan.ReplicaID, chanPlan.From, chanPlan.To)
}

var (
	RoundRobinBalancerName    = "RoundRobinBalancer"
	RowCountBasedBalancerName = "RowCountBasedBalancer"
	ScoreBasedBalancerName    = "ScoreBasedBalancer"
)

type Balance interface {
	AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64) []SegmentAssignPlan
	AssignChannel(channels []*meta.DmChannel, nodes []int64) []ChannelAssignPlan
	BalanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan)
}

type RoundRobinBalancer struct {
	scheduler   task.Scheduler
	nodeManager *session.NodeManager
}

func (b *RoundRobinBalancer) AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
	nodesInfo := b.getNodes(nodes)
	if len(nodesInfo) == 0 {
		return nil
	}
	sort.Slice(nodesInfo, func(i, j int) bool {
		cnt1, cnt2 := nodesInfo[i].SegmentCnt(), nodesInfo[j].SegmentCnt()
		id1, id2 := nodesInfo[i].ID(), nodesInfo[j].ID()
		delta1, delta2 := b.scheduler.GetNodeSegmentDelta(id1), b.scheduler.GetNodeSegmentDelta(id2)
		return cnt1+delta1 < cnt2+delta2
	})
	ret := make([]SegmentAssignPlan, 0, len(segments))
	for i, s := range segments {
		plan := SegmentAssignPlan{
			Segment: s,
			From:    -1,
			To:      nodesInfo[i%len(nodesInfo)].ID(),
		}
		ret = append(ret, plan)
	}
	return ret
}

func (b *RoundRobinBalancer) AssignChannel(channels []*meta.DmChannel, nodes []int64) []ChannelAssignPlan {
	nodesInfo := b.getNodes(nodes)
	if len(nodesInfo) == 0 {
		return nil
	}
	sort.Slice(nodesInfo, func(i, j int) bool {
		cnt1, cnt2 := nodesInfo[i].ChannelCnt(), nodesInfo[j].ChannelCnt()
		id1, id2 := nodesInfo[i].ID(), nodesInfo[j].ID()
		delta1, delta2 := b.scheduler.GetNodeChannelDelta(id1), b.scheduler.GetNodeChannelDelta(id2)
		return cnt1+delta1 < cnt2+delta2
	})
	ret := make([]ChannelAssignPlan, 0, len(channels))
	for i, c := range channels {
		plan := ChannelAssignPlan{
			Channel: c,
			From:    -1,
			To:      nodesInfo[i%len(nodesInfo)].ID(),
		}
		ret = append(ret, plan)
	}
	return ret
}

func (b *RoundRobinBalancer) BalanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	//TODO by chun.han
	return nil, nil
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
