package balance

import (
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type RowCountBasedBalancer struct {
	RoundRobinBalancer
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	meta        *meta.Meta
}

func (b *RowCountBasedBalancer) AssignSegment(segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
	if len(nodes) == 0 {
		return nil
	}
	nodeItems := b.convertToNodeItems(nodes)
	queue := newPriorityQueue()
	for _, item := range nodeItems {
		queue.push(item)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
	})

	plans := make([]SegmentAssignPlan, 0, len(segments))
	for _, s := range segments {
		// pick the node with the least row count and allocate to it.
		ni := queue.pop().(*nodeItem)
		plan := SegmentAssignPlan{
			From:    -1,
			To:      ni.nodeID,
			Segment: s,
		}
		plans = append(plans, plan)
		// change node's priority and push back
		p := ni.getPriority()
		ni.setPriority(p + int(s.GetNumOfRows()))
		queue.push(ni)
	}
	return plans
}

func (b *RowCountBasedBalancer) convertToNodeItems(nodeIDs []int64) []*nodeItem {
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

func (b *RowCountBasedBalancer) Balance() ([]SegmentAssignPlan, []ChannelAssignPlan) {
	ids := b.meta.CollectionManager.GetAll()

	segmentPlans, channelPlans := make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	for _, cid := range ids {
		replicas := b.meta.ReplicaManager.GetByCollection(cid)
		for _, replica := range replicas {
			splans, cplans := b.balanceReplica(replica)
			segmentPlans = append(segmentPlans, splans...)
			channelPlans = append(channelPlans, cplans...)
		}
	}
	return segmentPlans, channelPlans
}

func (b *RowCountBasedBalancer) balanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	nodes := replica.Nodes.Collect()
	if len(nodes) == 0 {
		return nil, nil
	}
	nodesRowCnt := make(map[int64]int)
	nodesSegments := make(map[int64][]*meta.Segment)
	totalCnt := 0
	for _, nid := range nodes {
		segments := b.dist.SegmentDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nid)
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

func NewRowCountBasedBalancer(
	scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
) *RowCountBasedBalancer {
	return &RowCountBasedBalancer{
		RoundRobinBalancer: *NewRoundRobinBalancer(scheduler, nodeManager),
		nodeManager:        nodeManager,
		dist:               dist,
		meta:               meta,
	}
}

type nodeItem struct {
	baseItem
	nodeID int64
}

func newNodeItem(priority int, nodeID int64) nodeItem {
	return nodeItem{
		baseItem: baseItem{
			priority: priority,
		},
		nodeID: nodeID,
	}
}
