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

type shardAllocInfo struct {
	segments []*meta.Segment    // sort segment in descending order (segment num of rows)
	idx      int                // index indicates to next unassigned segment
	nodes    map[int64]struct{} // all nodes which hold the segment from this shard
}

func (shard *shardAllocInfo) IsFull() bool {
	return shard.idx >= len(shard.segments)
}

func (shard *shardAllocInfo) GetNextRowCount() int64 {
	return shard.segments[shard.idx].GetNumOfRows()
}

func (shard *shardAllocInfo) GetDistinctNodeNum() int {
	return len(shard.nodes)
}

func (shard *shardAllocInfo) Assign(nodeID int64) *meta.Segment {
	shard.nodes[nodeID] = struct{}{}
	curSegment := shard.segments[shard.idx]
	shard.idx += 1
	return curSegment
}

func (shard *shardAllocInfo) IsOnNode(nodeID int64) bool {
	_, ok := shard.nodes[nodeID]
	return ok
}

func (b *ShardRowCountBasedBalancer) AssignSegment(segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
	if len(nodes) == 0 {
		return nil
	}
	dstNodeMap := make(map[int64]struct{})

	for _, nodeID := range nodes {
		dstNodeMap[nodeID] = struct{}{}
	}

	var shardInfos []*shardAllocInfo
	{
		shardSegments := make(map[string][]*meta.Segment)
		for _, seg := range segments {
			channel := seg.GetInsertChannel()
			_, exist := shardSegments[channel]
			if exist {
				shardSegments[channel] = append(shardSegments[channel], seg)
			} else {
				segList := make([]*meta.Segment, 0)
				segList = append(segList, seg)
				shardSegments[channel] = segList
			}
		}

		for shard, segList := range shardSegments {
			sort.Slice(segList, func(i, j int) bool {
				return segList[i].GetNumOfRows() > segList[j].GetNumOfRows()
			})

			shardInfo := &shardAllocInfo{
				segments: segList,
				idx:      0,
				nodes:    make(map[int64]struct{}),
			}
			distSegments := b.dist.GetByShard(shard)
			for _, seg := range distSegments {
				_, exist := dstNodeMap[seg.Node]
				if exist {
					shardInfo.nodes[seg.Node] = struct{}{}
				}
			}
			shardInfos = append(shardInfos, shardInfo)
		}
	}

	nodeItems := b.convertToNodeItems(nodes)
	queue := newPriorityQueue()
	for _, item := range nodeItems {
		queue.push(item)
	}
	plans := make([]SegmentAssignPlan, 0, len(segments))
	for len(shardInfos) != 0 {
		// pick the node with the least row count and allocate to it.
		ni := queue.pop().(*nodeItem)
		nodeID := ni.nodeID
		sort.Slice(shardInfos, func(i, j int) bool {
			onLeft := shardInfos[i].IsOnNode(nodeID)
			onRight := shardInfos[j].IsOnNode(nodeID)
			if onLeft != onRight {
				return (i < j) != onLeft //XOR
			}
			distinctLeft := shardInfos[i].GetDistinctNodeNum()
			distinctRight := shardInfos[j].GetDistinctNodeNum()
			if distinctLeft != distinctRight {
				return distinctLeft < distinctRight
			}
			return shardInfos[i].GetNextRowCount() > shardInfos[j].GetNextRowCount()
		})
		shard := shardInfos[0]
		s := shard.Assign(nodeID)
		plan := SegmentAssignPlan{
			From:    -1,
			To:      nodeID,
			Segment: s,
		}
		plans = append(plans, plan)
		// change node's priority and push back
		p := ni.getPriority()
		ni.setPriority(p + int(s.GetNumOfRows()))
		queue.push(ni)

		var nonFullShards []*shardAllocInfo
		for _, shardInfo := range shardInfos {
			if shardInfo.IsFull() {
				continue
			}
			nonFullShards = append(nonFullShards, shardInfo)
		}
		shardInfos = nonFullShards
	}

	return plans
}

func (b *ShardRowCountBasedBalancer) convertToNodeItems(nodeIDs []int64) []*nodeItem {
	ret := make([]*nodeItem, 0, len(nodeIDs))
	for _, node := range nodeIDs {
		segments := b.dist.SegmentDistManager.GetByNode(node)
		rowcnt := 0
		for _, s := range segments {
			rowcnt += int(s.GetNumOfRows())
		}
		// more row count, less priority
		nodeItem := newNodeItem(rowcnt, node)
		ret = append(ret, &nodeItem)
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
	nodesRowCnt := make(map[int64]int)
	nodesSegments := make(map[int64][]*meta.Segment)
	totalCnt := 0
	for _, nid := range nodes {
		segments := b.dist.SegmentDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nid)
		// Only balance segments in targets
		segments = lo.Filter(segments, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.GetHistoricalSegment(segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil
		})
		cnt := 0
		for _, s := range segments {
			cnt += int(s.GetNumOfRows())
		}
		nodesRowCnt[nid] = cnt
		nodesSegments[nid] = segments
		totalCnt += cnt
	}

	average := totalCnt / len(nodes)
	neededRowCnt := 0
	for _, rowcnt := range nodesRowCnt {
		if rowcnt < average {
			neededRowCnt += average - rowcnt
		}
	}

	if neededRowCnt == 0 {
		return nil, nil
	}

	segmentsToMove := make([]*meta.Segment, 0)

	// select segments to be moved
outer:
	for nodeID, rowcnt := range nodesRowCnt {
		if rowcnt <= average {
			continue
		}
		segments := nodesSegments[nodeID]
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
		})

		for _, s := range segments {
			if rowcnt-int(s.GetNumOfRows()) < average {
				continue
			}
			rowcnt -= int(s.GetNumOfRows())
			segmentsToMove = append(segmentsToMove, s)
			neededRowCnt -= int(s.GetNumOfRows())
			if neededRowCnt <= 0 {
				break outer
			}
		}
	}

	sort.Slice(segmentsToMove, func(i, j int) bool {
		return segmentsToMove[i].GetNumOfRows() < segmentsToMove[j].GetNumOfRows()
	})

	// allocate segments to those nodes with row cnt less than average
	queue := newPriorityQueue()
	for nodeID, rowcnt := range nodesRowCnt {
		if rowcnt >= average {
			continue
		}
		item := newNodeItem(rowcnt, nodeID)
		queue.push(&item)
	}

	plans := make([]SegmentAssignPlan, 0)
	for _, s := range segmentsToMove {
		node := queue.pop().(*nodeItem)
		plan := SegmentAssignPlan{
			ReplicaID: replica.GetID(),
			From:      s.Node,
			To:        node.nodeID,
			Segment:   s,
		}
		plans = append(plans, plan)
		node.setPriority(node.getPriority() + int(s.GetNumOfRows()))
		queue.push(node)
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
