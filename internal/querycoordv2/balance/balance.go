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
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"sort"
)

type Weight = int

const (
	weightLow int = iota - 1
	weightNormal
	weightHigh
)

func GetWeight(w int) Weight {
	if w > 0 {
		return weightHigh
	} else if w < 0 {
		return weightLow
	}
	return weightNormal
}

func GetTaskPriorityFromWeight(w Weight) task.Priority {
	switch w {
	case weightHigh:
		return task.TaskPriorityHigh
	case weightLow:
		return task.TaskPriorityLow
	default:
		return task.TaskPriorityNormal
	}
}

type SegmentAssignPlan struct {
	Segment   *meta.Segment
	ReplicaID int64
	From      int64 // -1 if empty
	To        int64
	Weight    Weight
}

func (segPlan SegmentAssignPlan) ToString() string {
	str := "SegmentPlan:["
	str += fmt.Sprintf("collectionID: %d, ", segPlan.Segment.CollectionID)
	str += fmt.Sprintf("segmentID: %d, ", segPlan.Segment.ID)
	str += fmt.Sprintf("replicaID: %d, ", segPlan.ReplicaID)
	str += fmt.Sprintf("from: %d, ", segPlan.From)
	str += fmt.Sprintf("to: %d, ", segPlan.To)
	str += fmt.Sprintf("weight: %d, ", segPlan.Weight)
	str += "]\n"
	return str
}

type ChannelAssignPlan struct {
	Channel   *meta.DmChannel
	ReplicaID int64
	From      int64
	To        int64
	Weight    Weight
}

func (chanPlan ChannelAssignPlan) ToString() string {
	str := "ChannelPlan:["
	str += fmt.Sprintf("collectionID: %d, ", chanPlan.Channel.CollectionID)
	str += fmt.Sprintf("channel: %s, ", chanPlan.Channel.ChannelName)
	str += fmt.Sprintf("replicaID: %d, ", chanPlan.ReplicaID)
	str += fmt.Sprintf("from: %d, ", chanPlan.From)
	str += fmt.Sprintf("to: %d, ", chanPlan.To)
	str += fmt.Sprintf("weight: %d, ", chanPlan.Weight)
	str += "]\n"
	return str
}

const (
	BalanceInfoPrefix = "Balance-Info:"
)

type Balance interface {
	AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64) []SegmentAssignPlan
	AssignChannel(channels []*meta.DmChannel, nodes []int64) []ChannelAssignPlan
	Balance() ([]SegmentAssignPlan, []ChannelAssignPlan)
	PrintNewBalancePlans(collectionID int64, replicaID int64, segmentPlans []SegmentAssignPlan, channelPlans []ChannelAssignPlan)
	PrintCurrentReplicaDist(replica *meta.Replica,
		stoppingNodesSegments map[int64][]*meta.Segment, nodeSegments map[int64][]*meta.Segment,
		channelManager *meta.ChannelDistManager)
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

func (b *RoundRobinBalancer) Balance() ([]SegmentAssignPlan, []ChannelAssignPlan) {
	// TODO(sunby)
	return nil, nil
}

func NewRoundRobinBalancer(scheduler task.Scheduler, nodeManager *session.NodeManager) *RoundRobinBalancer {
	return &RoundRobinBalancer{
		scheduler:   scheduler,
		nodeManager: nodeManager,
	}
}

func (b *RoundRobinBalancer) PrintNewBalancePlans(collectionID int64, replicaID int64, segmentPlans []SegmentAssignPlan,
	channelPlans []ChannelAssignPlan) {
	balanceInfo := BalanceInfoPrefix + "{"
	balanceInfo += fmt.Sprintf("collectionID:%d, replicaID:%d, ", collectionID, replicaID)
	for _, segmentPlan := range segmentPlans {
		balanceInfo += segmentPlan.ToString()
	}
	for _, channelPlan := range channelPlans {
		balanceInfo += channelPlan.ToString()
	}
	balanceInfo += "}"
	log.Debug(balanceInfo)
}

func (b *RoundRobinBalancer) PrintCurrentReplicaDist(replica *meta.Replica,
	stoppingNodesSegments map[int64][]*meta.Segment, nodeSegments map[int64][]*meta.Segment,
	channelManager *meta.ChannelDistManager) {
	distInfo := BalanceInfoPrefix + "{"
	distInfo += fmt.Sprintf("collectionID:%d, replicaID:%d, ", replica.CollectionID, replica.GetID())
	//1. print stopping nodes segment distribution
	distInfo += fmt.Sprintf("[stoppingNodesSegmentDist:")
	for stoppingNodeID, stoppedSegments := range stoppingNodesSegments {
		distInfo += fmt.Sprintf("[nodeID:%d, ", stoppingNodeID)
		distInfo += "stopped-segments:["
		for _, stoppedSegment := range stoppedSegments {
			distInfo += fmt.Sprintf("%d,", stoppedSegment.GetID())
		}
		distInfo += "]]"
	}
	distInfo += "]\n"
	//2. print normal nodes segment distribution
	distInfo += "[normalNodesSegmentDist:"
	for normalNodeID, normalNodeSegments := range nodeSegments {
		distInfo += fmt.Sprintf("[nodeID:%d, ", normalNodeID)
		distInfo += "loaded-segments:["
		nodeRowSum := int64(0)
		for _, normalSegment := range normalNodeSegments {
			distInfo += fmt.Sprintf("[segmentID: %d,", normalSegment.GetID())
			distInfo += fmt.Sprintf("rowCount: %d]", normalSegment.GetNumOfRows())
			nodeRowSum += normalSegment.GetNumOfRows()
		}
		distInfo += fmt.Sprintf("] nodeRowSum:%d]", nodeRowSum)
	}
	distInfo += "]\n"

	//3. print stopping nodes channel distribution
	distInfo += "[stoppingNodesChannelDist:"
	for stoppingNodeID, _ := range stoppingNodesSegments {
		distInfo += fmt.Sprintf("[nodeID:%d, ", stoppingNodeID)
		stoppingNodeChannels := channelManager.GetByNode(stoppingNodeID)
		distInfo += fmt.Sprintf("count:%d, ", len(stoppingNodeChannels))
		distInfo += "channels:["
		for _, stoppingChan := range stoppingNodeChannels {
			distInfo += fmt.Sprintf("%s,", stoppingChan.GetChannelName())
		}
		distInfo += "]]"
	}
	distInfo += "]\n"

	//4. print normal nodes channel distribution
	distInfo += "[normalNodesChannelDist:"
	for normalNodeID, _ := range nodeSegments {
		distInfo += fmt.Sprintf("[nodeID:%d, ", normalNodeID)
		normalNodeChannels := channelManager.GetByNode(normalNodeID)
		distInfo += fmt.Sprintf("count:%d, ", len(normalNodeChannels))
		distInfo += "channels:["
		for _, normalNodeChan := range normalNodeChannels {
			distInfo += fmt.Sprintf("%s,", normalNodeChan.GetChannelName())
		}
		distInfo += "]]"
	}
	distInfo += "]\n"

	log.Debug(distInfo)
}
