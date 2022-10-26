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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
)

type RedundanciesChecker struct {
	baseChecker
	meta   *meta.Meta
	dist   *meta.DistributionManager
	broker meta.Broker
}

func NewRedundanciesChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	broker meta.Broker,
) *RedundanciesChecker {
	return &RedundanciesChecker{
		meta:   meta,
		dist:   dist,
		broker: broker,
	}
}

func (c *RedundanciesChecker) Description() string {
	return "RedundanciesChecker checks the redundant of segments"
}

func (c *RedundanciesChecker) Check(ctx context.Context) []task.Task {
	collectionIDs := c.meta.CollectionManager.GetAll()
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {

		partitions, err := utils.GetPartitions(c.meta.CollectionManager, c.broker, cid)
		if err != nil {
			log.Warn("RedundanciesChecker: get partition filed")
			return tasks
		}

		targetChannel, _, err := utils.PullTarget(ctx, c.broker, cid, partitions)
		if err != nil {
			log.Warn("RedundanciesChecker: pull target failed")
			return tasks
		}

		replicas := c.meta.ReplicaManager.GetByCollection(cid)
		for _, r := range replicas {
			tasks = append(tasks, c.checkReplica(ctx, r, targetChannel)...)
		}
	}

	return tasks
}

func (c *RedundanciesChecker) checkReplica(ctx context.Context,
	replica *meta.Replica,
	targetChannels map[string]*meta.DmChannel) []task.Task {
	ret := make([]task.Task, 0)

	// release redundant growing segments
	needReleaseGrowing := c.findNeedReleasedGrowingSegments(replica, targetChannels)
	tasks := c.createSegmentReduceTasks(ctx, needReleaseGrowing, replica.GetID(), querypb.DataScope_Streaming)
	ret = append(ret, tasks...)

	return ret
}

func (c *RedundanciesChecker) findNeedReleasedGrowingSegments(replica *meta.Replica, targetChannels map[string]*meta.DmChannel) []*meta.Segment {
	ret := make([]*meta.Segment, 0) // leaderID -> segment ids
	leaders := c.dist.ChannelDistManager.GetShardLeadersByReplica(replica)

	for shard, leaderID := range leaders {
		leaderView := c.dist.LeaderViewManager.GetLeaderShardView(leaderID, shard)
		if leaderView == nil {
			continue
		}

		// check growing which not exist in target, should be released.
		for ID, segment := range leaderView.GrowingSegments {
			existInTarget := false
			for _, targetGrowingID := range targetChannels[segment.InsertChannel].UnflushedSegmentIds {
				if ID == targetGrowingID {
					existInTarget = true
				}
			}

			if !existInTarget {
				shouldRelease := segment.GetStartPosition().GetTimestamp() < targetChannels[segment.InsertChannel].GetSeekPosition().GetTimestamp()
				if shouldRelease {
					ret = append(ret, &meta.Segment{
						SegmentInfo: &datapb.SegmentInfo{
							ID:            ID,
							CollectionID:  replica.GetCollectionID(),
							InsertChannel: leaderView.Channel,
						},
						Node: leaderID,
					})
				}
			}
		}
	}
	return ret
}

func (c *RedundanciesChecker) createSegmentReduceTasks(ctx context.Context, segments []*meta.Segment, replicaID int64, scope querypb.DataScope) []task.Task {
	ret := make([]task.Task, 0, len(segments))
	for _, s := range segments {
		action := task.NewSegmentActionWithScope(s.Node, task.ActionTypeReduce, s.GetInsertChannel(), s.GetID(), scope)
		task, err := task.NewSegmentTask(
			ctx,
			Params.QueryCoordCfg.SegmentTaskTimeout,
			c.ID(),
			s.GetCollectionID(),
			replicaID,
			action,
		)

		if err != nil {
			log.Warn("Create segment reduce task failed",
				zap.Int64("collectionID", s.GetCollectionID()),
				zap.Int64("replica", replicaID),
				zap.String("channel", s.GetInsertChannel()),
				zap.Int64("From", s.Node),
				zap.Error(err),
			)
			continue
		}

		ret = append(ret, task)
	}
	return ret
}
