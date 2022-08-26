package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type SegmentChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	balancer  balance.Balance
}

func NewSegmentChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	balancer balance.Balance,
) *SegmentChecker {
	return &SegmentChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		balancer:  balancer,
	}
}

func (c *SegmentChecker) Description() string {
	return "SegmentChecker checks the lack of segments, or some segments are redundant"
}

func (c *SegmentChecker) Check(ctx context.Context) []task.Task {
	collectionIDs := c.meta.CollectionManager.GetAll()
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		replicas := c.meta.ReplicaManager.GetByCollection(cid)
		for _, r := range replicas {
			tasks = append(tasks, c.checkReplica(ctx, r)...)
		}
	}

	// find already released segments which are not contained in target
	segments := c.dist.SegmentDistManager.GetAll()
	released := utils.FilterReleased(segments, collectionIDs)
	tasks = append(tasks, c.createSegmentReduceTasks(ctx, released, -1, querypb.DataScope_All)...)
	return tasks
}

func (c *SegmentChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)
	targets := c.targetMgr.GetSegmentsByCollection(replica.CollectionID)
	dists := c.getSegmentsDist(replica)

	// compare with targets to find the lack and redundancy of segments
	lacks, redundancies := diffSegments(targets, dists)
	tasks := c.createSegmentLoadTasks(ctx, lacks, replica)
	ret = append(ret, tasks...)

	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID(), querypb.DataScope_All)
	ret = append(ret, tasks...)

	// compare inner dists to find repeated loaded segments
	redundancies = findRepeatedSegments(dists)
	redundancies = c.filterExistedOnLeader(replica, redundancies)
	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID(), querypb.DataScope_All)
	ret = append(ret, tasks...)

	// release redundant growing segments
	leaderReduancies := c.findNeedReleasedGrowingSegments(replica)
	redundancies = make([]*meta.Segment, 0)
	for leaderID, segmentIDs := range leaderReduancies {
		segments := packSegments(segmentIDs, leaderID, replica.GetCollectionID())
		redundancies = append(redundancies, segments...)
	}
	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID(), querypb.DataScope_Streaming)
	ret = append(ret, tasks...)

	return ret
}

func (c *SegmentChecker) getSegmentsDist(replica *meta.Replica) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	for _, node := range replica.Nodes.Collect() {
		ret = append(ret, c.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, node)...)
	}
	return ret
}

func diffSegments(targets []*datapb.SegmentInfo, dists []*meta.Segment) (lacks []*datapb.SegmentInfo, redundancies []*meta.Segment) {
	distMap := typeutil.NewUniqueSet()
	targetMap := typeutil.NewUniqueSet()
	for _, s := range targets {
		targetMap.Insert(s.GetID())
	}
	for _, s := range dists {
		distMap.Insert(s.GetID())
		if !targetMap.Contain(s.GetID()) {
			redundancies = append(redundancies, s)
		}
	}
	for _, s := range targets {
		if !distMap.Contain(s.GetID()) {
			lacks = append(lacks, s)
		}
	}
	return
}

func findRepeatedSegments(dists []*meta.Segment) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	versions := make(map[int64]*meta.Segment)
	for _, s := range dists {
		maxVer, ok := versions[s.GetID()]
		if !ok {
			versions[s.GetID()] = s
			continue
		}
		if maxVer.Version <= s.Version {
			ret = append(ret, maxVer)
			versions[s.GetID()] = s
		} else {
			ret = append(ret, s)
		}
	}
	return ret
}

func (c *SegmentChecker) filterExistedOnLeader(replica *meta.Replica, segments []*meta.Segment) []*meta.Segment {
	filtered := make([]*meta.Segment, 0, len(segments))
	for _, s := range segments {
		leaderID, ok := c.dist.ChannelDistManager.GetShardLeader(replica, s.GetInsertChannel())
		if !ok {
			continue
		}
		onLeader := false
		leaderViews := c.dist.LeaderViewManager.GetLeaderView(leaderID)
		for _, view := range leaderViews {
			node, ok := view.Segments[s.GetID()]
			if ok && node == s.Node {
				onLeader = true
				break
			}
		}
		if onLeader {
			// if this segment is serving on leader, do not remove it for search available
			continue
		}
		filtered = append(filtered, s)
	}
	return filtered
}

func (c *SegmentChecker) findNeedReleasedGrowingSegments(replica *meta.Replica) map[int64][]int64 {
	ret := make(map[int64][]int64, 0) // leaderID -> segment ids
	leaders := c.dist.ChannelDistManager.GetShardLeadersByReplica(replica)
	for shard, leaderID := range leaders {
		lview := c.dist.LeaderViewManager.GetLeaderShardView(leaderID, shard)
		// find growing segments from leaderview's sealed segments
		// because growing segments should be released only after loading the compaction created segment successfully.
		for sid := range lview.Segments {
			segment := c.targetMgr.GetSegment(sid)
			if segment == nil {
				continue
			}
			sources := append(segment.GetCompactionFrom(), segment.GetID())
			for _, source := range sources {
				if lview.GrowingSegments.Contain(source) {
					ret[lview.ID] = append(ret[lview.ID], source)
				}
			}
		}
	}
	return ret
}

func packSegments(segmentIDs []int64, nodeID int64, collectionID int64) []*meta.Segment {
	ret := make([]*meta.Segment, 0, len(segmentIDs))
	for _, id := range segmentIDs {
		segment := &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           id,
				CollectionID: collectionID,
			},
			Node: nodeID,
		}
		ret = append(ret, segment)
	}
	return ret
}

func (c *SegmentChecker) createSegmentLoadTasks(ctx context.Context, segments []*datapb.SegmentInfo, replica *meta.Replica) []task.Task {
	if len(segments) == 0 {
		return nil
	}
	packedSegments := make([]*meta.Segment, 0, len(segments))
	for _, s := range segments {
		if len(c.dist.LeaderViewManager.GetLeadersByShard(s.GetInsertChannel())) == 0 {
			continue
		}
		packedSegments = append(packedSegments, &meta.Segment{SegmentInfo: s})
	}
	plans := c.balancer.AssignSegment(packedSegments, replica.Replica.GetNodes())
	for i := range plans {
		plans[i].ReplicaID = replica.GetID()
	}
	return balance.CreateSegmentTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.SegmentTaskTimeout, plans)
}

func (c *SegmentChecker) createSegmentReduceTasks(ctx context.Context, segments []*meta.Segment, replicaID int64, scope querypb.DataScope) []task.Task {
	ret := make([]task.Task, 0, len(segments))
	for _, s := range segments {
		action := task.NewSegmentActionWithScope(s.Node, task.ActionTypeReduce, s.GetID(), scope)
		ret = append(ret, task.NewSegmentTask(
			ctx,
			Params.QueryCoordCfg.SegmentTaskTimeout,
			c.ID(),
			s.GetCollectionID(),
			replicaID,
			action,
		))
	}
	return ret
}
