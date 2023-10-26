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
	"sort"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
)

type SegmentChecker struct {
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	balancer  balance.Balance
	nodeMgr   *session.NodeManager
}

func NewSegmentChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	balancer balance.Balance,
	nodeMgr *session.NodeManager,
) *SegmentChecker {
	return &SegmentChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		balancer:  balancer,
		nodeMgr:   nodeMgr,
	}
}

func (c *SegmentChecker) ID() task.Source {
	return segmentChecker
}

func (c *SegmentChecker) Description() string {
	return "SegmentChecker checks the lack of segments, or some segments are redundant"
}

func (c *SegmentChecker) readyToCheck(collectionID int64) bool {
	metaExist := (c.meta.GetCollection(collectionID) != nil)
	targetExist := c.targetMgr.IsNextTargetExist(collectionID) || c.targetMgr.IsCurrentTargetExist(collectionID)

	return metaExist && targetExist
}

func (c *SegmentChecker) Check(ctx context.Context) []task.Task {
	collectionIDs := c.meta.CollectionManager.GetAll()
	results := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		if c.readyToCheck(cid) {
			replicas := c.meta.ReplicaManager.GetByCollection(cid)
			for _, r := range replicas {
				results = append(results, c.checkReplica(ctx, r)...)
			}
		}
	}

	// find already released segments which are not contained in target
	segments := c.dist.SegmentDistManager.GetAll()
	released := utils.FilterReleased(segments, collectionIDs)
	reduceTasks := c.createSegmentReduceTasks(ctx, released, -1, querypb.DataScope_Historical)
	task.SetReason("collection released", reduceTasks...)
	results = append(results, reduceTasks...)
	task.SetPriority(task.TaskPriorityNormal, results...)
	return results
}

func (c *SegmentChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	log := log.Ctx(ctx).WithRateGroup("qcv2.SegmentChecker", 1, 60).With(
		zap.Int64("collectionID", replica.CollectionID),
		zap.Int64("replicaID", replica.ID))
	ret := make([]task.Task, 0)

	// get channel dist by replica (ch -> node list), cause more then one delegator may exists during channel balance.
	// if more than one delegator exist, load/release segment may causes chaos, so we can skip it until channel balance finished.
	dist := c.dist.ChannelDistManager.GetChannelDistByReplica(replica)
	for ch, nodes := range dist {
		if len(nodes) > 1 {
			log.Info("skip check segment due to two shard leader exists",
				zap.String("channelName", ch))
			return ret
		}
	}

	// compare with targets to find the lack and redundancy of segments
	lacks, redundancies := c.getSealedSegmentDiff(replica.GetCollectionID(), replica.GetID())
	tasks := c.createSegmentLoadTasks(ctx, lacks, replica)
	task.SetReason("lacks of segment", tasks...)
	ret = append(ret, tasks...)

	redundancies = c.filterSegmentInUse(replica, redundancies)
	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID(), querypb.DataScope_Historical)
	task.SetReason("segment not exists in target", tasks...)
	ret = append(ret, tasks...)

	// compare inner dists to find repeated loaded segments
	redundancies = c.findRepeatedSealedSegments(replica.GetID())
	redundancies = c.filterExistedOnLeader(replica, redundancies)
	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID(), querypb.DataScope_Historical)
	task.SetReason("redundancies of segment", tasks...)
	ret = append(ret, tasks...)

	// compare with target to find the lack and redundancy of segments
	_, redundancies = c.getGrowingSegmentDiff(replica.GetCollectionID(), replica.GetID())
	tasks = c.createSegmentReduceTasks(ctx, redundancies, replica.GetID(), querypb.DataScope_Streaming)
	task.SetReason("streaming segment not exists in target", tasks...)
	ret = append(ret, tasks...)

	return ret
}

// GetGrowingSegmentDiff get streaming segment diff between leader view and target
func (c *SegmentChecker) getGrowingSegmentDiff(collectionID int64,
	replicaID int64,
) (toLoad []*datapb.SegmentInfo, toRelease []*meta.Segment) {
	replica := c.meta.Get(replicaID)
	if replica == nil {
		log.Info("replica does not exist, skip it")
		return
	}

	log := log.Ctx(context.TODO()).WithRateGroup("qcv2.SegmentChecker", 1, 60).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64("replicaID", replica.ID))

	leaders := c.dist.ChannelDistManager.GetShardLeadersByReplica(replica)
	//	distMgr.LeaderViewManager.
	for channelName, node := range leaders {
		view := c.dist.LeaderViewManager.GetLeaderShardView(node, channelName)
		if view == nil {
			log.Info("leaderView is not ready, skip", zap.String("channelName", channelName), zap.Int64("node", node))
			continue
		}
		targetVersion := c.targetMgr.GetCollectionTargetVersion(collectionID, meta.CurrentTarget)
		if view.TargetVersion != targetVersion {
			// before shard delegator update it's readable version, skip release segment
			log.RatedInfo(20, "before shard delegator update it's readable version, skip release segment",
				zap.String("channelName", channelName),
				zap.Int64("nodeID", node),
				zap.Int64("leaderVersion", view.TargetVersion),
				zap.Int64("currentVersion", targetVersion),
			)
			continue
		}

		nextTargetSegmentIDs := c.targetMgr.GetGrowingSegmentsByCollection(collectionID, meta.NextTarget)
		currentTargetSegmentIDs := c.targetMgr.GetGrowingSegmentsByCollection(collectionID, meta.CurrentTarget)
		currentTargetChannelMap := c.targetMgr.GetDmChannelsByCollection(collectionID, meta.CurrentTarget)

		// get segment which exist on leader view, but not on current target and next target
		for _, segment := range view.GrowingSegments {
			if !currentTargetSegmentIDs.Contain(segment.GetID()) && !nextTargetSegmentIDs.Contain(segment.GetID()) {
				if channel, ok := currentTargetChannelMap[segment.InsertChannel]; ok {
					timestampInSegment := segment.GetStartPosition().GetTimestamp()
					timestampInTarget := channel.GetSeekPosition().GetTimestamp()
					// filter toRelease which seekPosition is newer than next target dmChannel
					if timestampInSegment < timestampInTarget {
						log.Info("growing segment not exist in target, so release it",
							zap.Int64("segmentID", segment.GetID()),
						)
						toRelease = append(toRelease, segment)
					}
				}
			}
		}
	}

	return
}

// GetSealedSegmentDiff get historical segment diff between target and dist
func (c *SegmentChecker) getSealedSegmentDiff(
	collectionID int64,
	replicaID int64,
) (toLoad []*datapb.SegmentInfo, toRelease []*meta.Segment) {
	replica := c.meta.Get(replicaID)
	if replica == nil {
		log.Info("replica does not exist, skip it")
		return
	}
	dist := c.getSealedSegmentsDist(replica)
	sort.Slice(dist, func(i, j int) bool {
		return dist[i].Version < dist[j].Version
	})
	distMap := make(map[int64]int64)
	for _, s := range dist {
		distMap[s.GetID()] = s.Node
	}

	nextTargetMap := c.targetMgr.GetSealedSegmentsByCollection(collectionID, meta.NextTarget)
	currentTargetMap := c.targetMgr.GetSealedSegmentsByCollection(collectionID, meta.CurrentTarget)

	// Segment which exist on next target, but not on dist
	for segmentID, segment := range nextTargetMap {
		leader := c.dist.LeaderViewManager.GetLatestLeadersByReplicaShard(replica,
			segment.GetInsertChannel(),
		)
		node, ok := distMap[segmentID]
		if !ok ||
			// the L0 segments have to been in the same node as the channel watched
			leader != nil &&
				segment.GetLevel() == datapb.SegmentLevel_L0 &&
				node != leader.ID {
			toLoad = append(toLoad, segment)
		}
	}

	// get segment which exist on dist, but not on current target and next target
	for _, segment := range dist {
		_, existOnCurrent := currentTargetMap[segment.GetID()]
		_, existOnNext := nextTargetMap[segment.GetID()]

		if !existOnNext && !existOnCurrent {
			toRelease = append(toRelease, segment)
		}
	}

	level0Segments := lo.Filter(toLoad, func(segment *datapb.SegmentInfo, _ int) bool {
		return segment.GetLevel() == datapb.SegmentLevel_L0
	})
	// L0 segment found,
	// QueryCoord loads the L0 segments first,
	// to make sure all L0 delta logs will be delivered to the other segments.
	if len(level0Segments) > 0 {
		toLoad = level0Segments
	}

	return
}

func (c *SegmentChecker) getSealedSegmentsDist(replica *meta.Replica) []*meta.Segment {
	ret := make([]*meta.Segment, 0)
	for _, node := range replica.GetNodes() {
		ret = append(ret, c.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, node)...)
	}
	return ret
}

func (c *SegmentChecker) findRepeatedSealedSegments(replicaID int64) []*meta.Segment {
	segments := make([]*meta.Segment, 0)
	replica := c.meta.Get(replicaID)
	if replica == nil {
		log.Info("replica does not exist, skip it")
		return segments
	}
	dist := c.getSealedSegmentsDist(replica)
	versions := make(map[int64]*meta.Segment)
	for _, s := range dist {
		maxVer, ok := versions[s.GetID()]
		if !ok {
			versions[s.GetID()] = s
			continue
		}
		if maxVer.Version <= s.Version {
			segments = append(segments, maxVer)
			versions[s.GetID()] = s
		} else {
			segments = append(segments, s)
		}
	}

	return segments
}

func (c *SegmentChecker) filterExistedOnLeader(replica *meta.Replica, segments []*meta.Segment) []*meta.Segment {
	filtered := make([]*meta.Segment, 0, len(segments))
	for _, s := range segments {
		leaderID, ok := c.dist.ChannelDistManager.GetShardLeader(replica, s.GetInsertChannel())
		if !ok {
			continue
		}

		view := c.dist.LeaderViewManager.GetLeaderShardView(leaderID, s.GetInsertChannel())
		seg, ok := view.Segments[s.GetID()]
		if ok && seg.NodeID == s.Node {
			// if this segment is serving on leader, do not remove it for search available
			continue
		}
		filtered = append(filtered, s)
	}
	return filtered
}

func (c *SegmentChecker) filterSegmentInUse(replica *meta.Replica, segments []*meta.Segment) []*meta.Segment {
	filtered := make([]*meta.Segment, 0, len(segments))
	for _, s := range segments {
		leaderID, ok := c.dist.ChannelDistManager.GetShardLeader(replica, s.GetInsertChannel())
		if !ok {
			continue
		}

		view := c.dist.LeaderViewManager.GetLeaderShardView(leaderID, s.GetInsertChannel())
		currentTargetVersion := c.targetMgr.GetCollectionTargetVersion(s.CollectionID, meta.CurrentTarget)
		partition := c.meta.CollectionManager.GetPartition(s.PartitionID)
		if partition != nil && view.TargetVersion != currentTargetVersion {
			// leader view version hasn't been updated, segment maybe still in use
			continue
		}
		filtered = append(filtered, s)
	}
	return filtered
}

func (c *SegmentChecker) createSegmentLoadTasks(ctx context.Context, segments []*datapb.SegmentInfo, replica *meta.Replica) []task.Task {
	if len(segments) == 0 {
		return nil
	}

	isLevel0 := segments[0].GetLevel() == datapb.SegmentLevel_L0

	shardSegments := make(map[string][]*meta.Segment)
	for _, s := range segments {
		if isLevel0 &&
			len(c.dist.LeaderViewManager.GetLeadersByShard(s.GetInsertChannel())) == 0 {
			continue
		}
		channel := s.GetInsertChannel()
		packedSegments := shardSegments[channel]
		packedSegments = append(packedSegments, &meta.Segment{
			SegmentInfo: s,
		})
		shardSegments[channel] = packedSegments
	}

	plans := make([]balance.SegmentAssignPlan, 0)
	for shard, segments := range shardSegments {
		outboundNodes := c.meta.ResourceManager.CheckOutboundNodes(replica)
		availableNodes := lo.Filter(replica.Replica.GetNodes(), func(node int64, _ int) bool {
			stop, err := c.nodeMgr.IsStoppingNode(node)
			if err != nil {
				return false
			}

			if isLevel0 {
				leader := c.dist.LeaderViewManager.GetLatestLeadersByReplicaShard(replica, shard)
				return !outboundNodes.Contain(node) && !stop && node == leader.ID
			}
			return !outboundNodes.Contain(node) && !stop
		})

		shardPlans := c.balancer.AssignSegment(replica.CollectionID, segments, availableNodes)
		for i := range shardPlans {
			shardPlans[i].ReplicaID = replica.GetID()
		}
		plans = append(plans, shardPlans...)
	}

	return balance.CreateSegmentTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond), plans)
}

func (c *SegmentChecker) createSegmentReduceTasks(ctx context.Context, segments []*meta.Segment, replicaID int64, scope querypb.DataScope) []task.Task {
	ret := make([]task.Task, 0, len(segments))
	for _, s := range segments {
		action := task.NewSegmentActionWithScope(s.Node, task.ActionTypeReduce, s.GetInsertChannel(), s.GetID(), scope)
		task, err := task.NewSegmentTask(
			ctx,
			Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
			c.ID(),
			s.GetCollectionID(),
			replicaID,
			action,
		)
		if err != nil {
			log.Warn("create segment reduce task failed",
				zap.Int64("collection", s.GetCollectionID()),
				zap.Int64("replica", replicaID),
				zap.String("channel", s.GetInsertChannel()),
				zap.Int64("from", s.Node),
				zap.Error(err),
			)
			continue
		}

		ret = append(ret, task)
	}
	return ret
}
