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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
)

var _ Checker = (*LeaderChecker)(nil)

// LeaderChecker perform segment index check.
type LeaderChecker struct {
	*checkerActivation
	meta    *meta.Meta
	dist    *meta.DistributionManager
	target  meta.TargetManagerInterface
	nodeMgr *session.NodeManager
}

func NewLeaderChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	target meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
) *LeaderChecker {
	return &LeaderChecker{
		checkerActivation: newCheckerActivation(),
		meta:              meta,
		dist:              dist,
		target:            target,
		nodeMgr:           nodeMgr,
	}
}

func (c *LeaderChecker) ID() utils.CheckerType {
	return utils.LeaderChecker
}

func (c *LeaderChecker) Description() string {
	return "LeaderChecker checks the difference of leader view between dist, and try to correct it"
}

func (c *LeaderChecker) readyToCheck(ctx context.Context, collectionID int64) bool {
	metaExist := (c.meta.GetCollection(ctx, collectionID) != nil)
	targetExist := c.target.IsNextTargetExist(ctx, collectionID) || c.target.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (c *LeaderChecker) Check(ctx context.Context) []task.Task {
	if !c.IsActive() {
		return nil
	}

	collectionIDs := c.meta.CollectionManager.GetAll(ctx)
	tasks := make([]task.Task, 0)

	for _, collectionID := range collectionIDs {
		if !c.readyToCheck(ctx, collectionID) {
			continue
		}
		collection := c.meta.CollectionManager.GetCollection(ctx, collectionID)
		if collection == nil {
			log.Warn("collection released during check leader", zap.Int64("collection", collectionID))
			continue
		}

		replicas := c.meta.ReplicaManager.GetByCollection(ctx, collectionID)
		for _, replica := range replicas {
			for _, node := range replica.GetRWNodes() {
				leaderViews := c.dist.LeaderViewManager.GetByFilter(meta.WithCollectionID2LeaderView(replica.GetCollectionID()), meta.WithNodeID2LeaderView(node))
				for _, leaderView := range leaderViews {
					dist := c.dist.SegmentDistManager.GetByFilter(meta.WithChannel(leaderView.Channel), meta.WithReplica(replica))
					tasks = append(tasks, c.findNeedLoadedSegments(ctx, replica, leaderView, dist)...)
					tasks = append(tasks, c.findNeedRemovedSegments(ctx, replica, leaderView, dist)...)
					tasks = append(tasks, c.findNeedSyncPartitionStats(ctx, replica, leaderView, node)...)
				}
			}
		}
	}

	return tasks
}

func (c *LeaderChecker) findNeedSyncPartitionStats(ctx context.Context, replica *meta.Replica, leaderView *meta.LeaderView, nodeID int64) []task.Task {
	ret := make([]task.Task, 0)
	curDmlChannel := c.target.GetDmChannel(ctx, leaderView.CollectionID, leaderView.Channel, meta.CurrentTarget)
	if curDmlChannel == nil {
		return ret
	}
	partStatsInTarget := curDmlChannel.GetPartitionStatsVersions()
	partStatsInLView := leaderView.PartitionStatsVersions
	partStatsToUpdate := make(map[int64]int64)

	for partID, psVersionInTarget := range partStatsInTarget {
		psVersionInLView := partStatsInLView[partID]
		if psVersionInLView < psVersionInTarget {
			partStatsToUpdate[partID] = psVersionInTarget
		} else {
			log.Ctx(ctx).RatedDebug(60, "no need to update part stats for partition",
				zap.Int64("partitionID", partID),
				zap.Int64("psVersionInLView", psVersionInLView),
				zap.Int64("psVersionInTarget", psVersionInTarget))
		}
	}
	if len(partStatsToUpdate) > 0 {
		action := task.NewLeaderUpdatePartStatsAction(leaderView.ID, nodeID, task.ActionTypeUpdate, leaderView.Channel, partStatsToUpdate)

		t := task.NewLeaderPartStatsTask(
			ctx,
			c.ID(),
			leaderView.CollectionID,
			replica,
			leaderView.ID,
			action,
		)

		// leader task shouldn't replace executing segment task
		t.SetPriority(task.TaskPriorityLow)
		t.SetReason("sync partition stats versions")
		ret = append(ret, t)
		log.Ctx(ctx).Debug("Created leader actions for partitionStats",
			zap.Int64("collectionID", leaderView.CollectionID),
			zap.Any("action", action.String()))
	}

	return ret
}

func (c *LeaderChecker) findNeedLoadedSegments(ctx context.Context, replica *meta.Replica, leaderView *meta.LeaderView, dist []*meta.Segment) []task.Task {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.Int64("replica", replica.GetID()),
		zap.String("channel", leaderView.Channel),
		zap.Int64("leaderViewID", leaderView.ID),
	)
	ret := make([]task.Task, 0)

	latestNodeDist := utils.FindMaxVersionSegments(dist)
	for _, s := range latestNodeDist {
		segment := c.target.GetSealedSegment(ctx, leaderView.CollectionID, s.GetID(), meta.CurrentTargetFirst)
		existInTarget := segment != nil
		isL0Segment := existInTarget && segment.GetLevel() == datapb.SegmentLevel_L0
		// shouldn't set l0 segment location to delegator. l0 segment should be reload in delegator
		if !existInTarget || isL0Segment {
			continue
		}

		// when segment's version in leader view doesn't match segment's version in dist
		// which means leader view store wrong segment location in leader view, then we should update segment location and segment's version
		version, ok := leaderView.Segments[s.GetID()]
		if !ok || version.GetVersion() != s.Version {
			log.RatedDebug(10, "leader checker append a segment to set",
				zap.Int64("segmentID", s.GetID()),
				zap.Int64("nodeID", s.Node))
			action := task.NewLeaderAction(leaderView.ID, s.Node, task.ActionTypeGrow, s.GetInsertChannel(), s.GetID(), time.Now().UnixNano())
			t := task.NewLeaderSegmentTask(
				ctx,
				c.ID(),
				s.GetCollectionID(),
				replica,
				leaderView.ID,
				action,
			)

			// leader task shouldn't replace executing segment task
			t.SetPriority(task.TaskPriorityLow)
			t.SetReason("add segment to leader view")
			ret = append(ret, t)
		}
	}
	return ret
}

func (c *LeaderChecker) findNeedRemovedSegments(ctx context.Context, replica *meta.Replica, leaderView *meta.LeaderView, dists []*meta.Segment) []task.Task {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", leaderView.CollectionID),
		zap.Int64("replica", replica.GetID()),
		zap.String("channel", leaderView.Channel),
		zap.Int64("leaderViewID", leaderView.ID),
	)

	ret := make([]task.Task, 0)
	distMap := make(map[int64]struct{})
	for _, s := range dists {
		distMap[s.GetID()] = struct{}{}
	}

	for sid, s := range leaderView.Segments {
		_, ok := distMap[sid]
		segment := c.target.GetSealedSegment(ctx, leaderView.CollectionID, sid, meta.CurrentTargetFirst)
		existInTarget := segment != nil
		isL0Segment := existInTarget && segment.GetLevel() == datapb.SegmentLevel_L0
		if ok || existInTarget || isL0Segment {
			continue
		}
		log.Debug("leader checker append a segment to remove",
			zap.Int64("segmentID", sid),
			zap.Int64("nodeID", s.NodeID))
		// reduce leader action won't be execute on worker, in  order to remove segment from delegator success even when worker done
		// set workerID to leader view's node
		action := task.NewLeaderAction(leaderView.ID, leaderView.ID, task.ActionTypeReduce, leaderView.Channel, sid, 0)
		t := task.NewLeaderSegmentTask(
			ctx,
			c.ID(),
			leaderView.CollectionID,
			replica,
			leaderView.ID,
			action,
		)

		// leader task shouldn't replace executing segment task
		t.SetPriority(task.TaskPriorityLow)
		t.SetReason("remove segment from leader view")
		ret = append(ret, t)
	}
	return ret
}
