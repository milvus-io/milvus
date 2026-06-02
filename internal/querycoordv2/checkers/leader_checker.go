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

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

	collectionIDs := c.meta.GetAll(ctx)
	tasks := make([]task.Task, 0)

	for _, collectionID := range collectionIDs {
		if !c.readyToCheck(ctx, collectionID) {
			continue
		}
		collection := c.meta.GetCollection(ctx, collectionID)
		if collection == nil {
			mlog.Warn(ctx, "collection released during check leader", mlog.Int64("collection", collectionID))
			continue
		}

		replicas := c.meta.GetByCollection(ctx, collectionID)
		for _, replica := range replicas {
			nodes := replica.GetRWNodes()
			if streamingutil.IsStreamingServiceEnabled() {
				nodes = replica.GetRWSQNodes()
			}
			for _, node := range nodes {
				delegatorList := c.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(replica.GetCollectionID()), meta.WithNodeID2Channel(node))
				for _, d := range delegatorList {
					dist := c.dist.SegmentDistManager.GetByFilter(meta.WithChannel(d.View.Channel), meta.WithReplica(replica))
					tasks = append(tasks, c.findNeedLoadedSegments(ctx, replica, d.View, dist)...)
					tasks = append(tasks, c.findNeedRemovedSegments(ctx, replica, d.View, dist)...)
					tasks = append(tasks, c.findNeedSyncPartitionStats(ctx, replica, d.View, node)...)
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
			mlog.RatedDebug(ctx, rate.Limit(60), "no need to update part stats for partition",
				mlog.FieldPartitionID(partID),
				mlog.Int64("psVersionInLView", psVersionInLView),
				mlog.Int64("psVersionInTarget", psVersionInTarget))
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
		mlog.Debug(ctx, "Created leader actions for partitionStats",
			mlog.FieldCollectionID(leaderView.CollectionID),
			mlog.Any("action", action.String()))
	}

	return ret
}

func (c *LeaderChecker) findNeedLoadedSegments(ctx context.Context, replica *meta.Replica, leaderView *meta.LeaderView, dist []*meta.Segment) []task.Task {
	log := mlog.With(
		mlog.FieldCollectionID(leaderView.CollectionID),
		mlog.Int64("replica", replica.GetID()),
		mlog.String("channel", leaderView.Channel),
		mlog.Int64("leaderViewID", leaderView.ID),
	)
	ret := make([]task.Task, 0)

	latestNodeDist := utils.FindMaxVersionSegments(dist)
	for _, s := range latestNodeDist {
		segment := c.target.GetSealedSegment(ctx, leaderView.CollectionID, s.GetID(), meta.CurrentTargetFirst)
		if segment == nil {
			continue
		}

		// The routing table on the delegator points to the nodes where segments are loaded. There are two scenarios that require updating the routing table on the delegator:
		// 1. Missing Segment Routing - The routing table lacks the route for a specific segment.
		// 2. Outdated Segment Routing - A segment has multiple copies loaded, but the routing table points to a node that does not host the most recently loaded copy.
		// This ensures the routing table remains accurate and up-to-date, reflecting the latest segment distribution.
		version, ok := leaderView.Segments[s.GetID()]
		if !ok || version.GetNodeID() != s.Node {
			log.RatedDebug(ctx, rate.Limit(10), "leader checker append a segment to set",
				mlog.FieldSegmentID(s.GetID()),
				mlog.FieldNodeID(s.Node))

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
	log := mlog.With(
		mlog.FieldCollectionID(leaderView.CollectionID),
		mlog.Int64("replica", replica.GetID()),
		mlog.String("channel", leaderView.Channel),
		mlog.Int64("leaderViewID", leaderView.ID),
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
		if ok || existInTarget {
			continue
		}
		log.Debug(ctx, "leader checker append a segment to remove",
			mlog.FieldSegmentID(sid),
			mlog.FieldNodeID(s.NodeID))
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
