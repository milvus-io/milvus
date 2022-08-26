package balance

import (
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

type ChannelAssignPlan struct {
	Channel   *meta.DmChannel
	ReplicaID int64
	From      int64
	To        int64
}

type Balance interface {
	AssignSegment(segments []*meta.Segment, nodes []int64) []SegmentAssignPlan
	AssignChannel(channels []*meta.DmChannel, nodes []int64) []ChannelAssignPlan
	Balance() ([]SegmentAssignPlan, []ChannelAssignPlan)
}

type RoundRobinBalancer struct {
	scheduler   task.Scheduler
	nodeManager *session.NodeManager
}

func (b *RoundRobinBalancer) AssignSegment(segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
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
		if node != nil {
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
