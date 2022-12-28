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
	"sort"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/samber/lo"
)

type ShardRowCountBasedBalancer struct {
	*RoundRobinBalancer
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	meta        *meta.Meta
	targetMgr   *meta.TargetManager
}

func (b *ShardRowCountBasedBalancer) assignSegment(nInfos *nodeInfos, segments []*meta.Segment, nodes []int64, replica *meta.Replica) []SegmentAssignPlan {
	if len(nodes) == 0 {
		return nil
	}
	var replicaID int64
	if replica == nil {
		replicaID = 0
	} else {
		replicaID = replica.GetID()
	}
	dstNodeMap := make(map[int64]struct{})
	for _, nodeID := range nodes {
		dstNodeMap[nodeID] = struct{}{}
	}
	var incTotal int64
	for _, segment := range segments {
		incTotal += segment.GetNumOfRows()
	}
	nInfos.updateTargetAvg(incTotal)
	//fmt.Println("TargetAvg:", nInfos.TargetAvg)
	if replica != nil {
		shardLeaders := b.dist.ChannelDistManager.GetShardLeadersByReplica(replica)
		for shard, leaderID := range shardLeaders {
			_, exist := dstNodeMap[leaderID]
			if exist {
				nInfos.addNode(leaderID, 0, shard, false)
			}
		}
	}
	nInfos.sortNodes()

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
	})
	var plans []SegmentAssignPlan
	//plans := make([]SegmentAssignPlan, 0, len(segments))
	lastNodeID := int64(-1)
	for len(segments) > 0 {
		segment := segments[0]
		if lastNodeID == -1 {
			lastNodeID = nInfos.AssignNode(segment.GetNumOfRows(), segment.GetInsertChannel())
		} else {
			ok := nInfos.AssignNodeByID(lastNodeID, segment.GetNumOfRows(), segment.GetInsertChannel())
			if !ok {
				lastNodeID = -1
			}
		}
		if lastNodeID != -1 {
			segments = segments[1:]
			fromID := segment.Node
			if fromID == 0 {
				fromID = -1
			}
			if fromID != lastNodeID {
				plan := SegmentAssignPlan{
					ReplicaID: replicaID,
					From:      fromID,
					To:        lastNodeID,
					Segment:   segment,
				}
				plans = append(plans, plan)
				//nodeInfo := nInfos.getNode(lastNodeID)
				//fmtStr := fmt.Sprintf("segment%d (%d rows, channel:%s) --> node%d(%d)", segment.GetID(), segment.GetNumOfRows(), segment.GetInsertChannel(), lastNodeID, nodeInfo.NumRows)
				//fmt.Println(fmtStr)
			}
		}
	}
	return plans
}

func (b *ShardRowCountBasedBalancer) AssignSegment(segments []*meta.Segment, nodes []int64, replica *meta.Replica) []SegmentAssignPlan {
	if len(nodes) == 0 {
		return nil
	}
	nInfos := b.convertToNodeInfos(nodes)
	return b.assignSegment(nInfos, segments, nodes, replica)
}

func (b *ShardRowCountBasedBalancer) convertToNodeInfos(nodeIDs []int64) *nodeInfos {
	ret := &nodeInfos{}
	for _, node := range nodeIDs {
		segments := b.dist.SegmentDistManager.GetByNode(node)
		for _, s := range segments {
			ret.addNode(node, s.GetNumOfRows(), s.GetInsertChannel(), false)
		}
		ret.addNode(node, 0, "", false)
	}
	return ret
}

func (b *ShardRowCountBasedBalancer) Balance() ([]SegmentAssignPlan, []ChannelAssignPlan) {
	ids := b.meta.CollectionManager.GetAll()

	// loading collection should skip balance
	loadedCollections := lo.Filter(ids, func(cid int64, _ int) bool {
		return b.meta.GetStatus(cid) == querypb.LoadStatus_Loaded
	})

	segmentPlans, channelPlans := make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	for _, cid := range loadedCollections {
		replicas := b.meta.ReplicaManager.GetByCollection(cid)
		for _, replica := range replicas {
			splans, cplans := b.balanceReplica(replica)
			segmentPlans = append(segmentPlans, splans...)
			channelPlans = append(channelPlans, cplans...)
		}
	}
	return segmentPlans, channelPlans
}

func (b *ShardRowCountBasedBalancer) balanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	nodes := replica.Nodes.Collect()
	if len(nodes) == 0 {
		return nil, nil
	}
	nInfos := b.convertToNodeInfos(nodes)
	curSD := nInfos.calculateSD()
	if curSD == 0 {
		return nil, nil
	}
	nodesRowCnt := make(map[int64]int64)
	nodesSegments := make(map[int64][]*meta.Segment)
	for _, nid := range nodes {
		segments := b.dist.SegmentDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nid)
		// Only balance segments in targets
		segments = lo.Filter(segments, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.GetHistoricalSegment(segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil
		})
		cnt := int64(0)
		for _, s := range segments {
			cnt += s.GetNumOfRows()
		}
		nodesRowCnt[nid] = cnt
		nodesSegments[nid] = segments
	}
	average := int64(nInfos.calculateAvg())
	neededRowCnt := int64(0)
	for _, rowCnt := range nodesRowCnt {
		if rowCnt < average {
			neededRowCnt += average - rowCnt
		}
	}

	if neededRowCnt == 0 {
		return nil, nil
	}
	segmentsToMove := make([]*meta.Segment, 0)
	// select segments to be moved
outer:
	for nodeID, rowCnt := range nodesRowCnt {
		if rowCnt <= average {
			continue
		}
		segments := nodesSegments[nodeID]
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
		})
		for _, s := range segments {
			if rowCnt-s.GetNumOfRows() < average {
				continue
			}
			rowCnt -= s.GetNumOfRows()
			segmentsToMove = append(segmentsToMove, s)
			neededRowCnt -= s.GetNumOfRows()
			nInfos.removeNode(nodeID, s.GetNumOfRows(), s.GetInsertChannel())
			if neededRowCnt <= 0 {
				break outer
			}
		}
	}
	plans := b.assignSegment(nInfos, segmentsToMove, nodes, replica)
	newSD := nInfos.calculateSD()
	if newSD > curSD {
		return nil, nil
	}
	return plans, nil
}

func NewShardRowCountBasedBalancer(
	scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
) *ShardRowCountBasedBalancer {
	return &ShardRowCountBasedBalancer{
		RoundRobinBalancer: NewRoundRobinBalancer(scheduler, nodeManager),
		nodeManager:        nodeManager,
		dist:               dist,
		meta:               meta,
		targetMgr:          targetMgr,
	}
}
