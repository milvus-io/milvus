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

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// TODO(sunby): have too much similar codes with SegmentChecker
type ChannelChecker struct {
	*checkerActivation
	meta            *meta.Meta
	dist            *meta.DistributionManager
	targetMgr       meta.TargetManagerInterface
	nodeMgr         *session.NodeManager
	getBalancerFunc GetBalancerFunc
}

func NewChannelChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	getBalancerFunc GetBalancerFunc,
) *ChannelChecker {
	return &ChannelChecker{
		checkerActivation: newCheckerActivation(),
		meta:              meta,
		dist:              dist,
		targetMgr:         targetMgr,
		nodeMgr:           nodeMgr,
		getBalancerFunc:   getBalancerFunc,
	}
}

func (c *ChannelChecker) ID() utils.CheckerType {
	return utils.ChannelChecker
}

func (c *ChannelChecker) Description() string {
	return "DmChannelChecker checks the lack of DmChannels, or some DmChannels are redundant"
}

func (c *ChannelChecker) readyToCheck(ctx context.Context, collectionID int64) bool {
	metaExist := (c.meta.GetCollection(ctx, collectionID) != nil)
	targetExist := c.targetMgr.IsNextTargetExist(ctx, collectionID) || c.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (c *ChannelChecker) Check(ctx context.Context) []task.Task {
	if !c.IsActive() {
		return nil
	}
	collectionIDs := c.meta.CollectionManager.GetAll(ctx)
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		if c.readyToCheck(ctx, cid) {
			replicas := c.meta.ReplicaManager.GetByCollection(ctx, cid)
			for _, r := range replicas {
				tasks = append(tasks, c.checkReplica(ctx, r)...)
			}
		}
	}

	// clean channel which has been released
	channels := c.dist.ChannelDistManager.GetByFilter()
	released := utils.FilterReleased(channels, collectionIDs)
	releaseTasks := c.createChannelReduceTasks(ctx, released, meta.NilReplica)
	task.SetReason("collection released", releaseTasks...)
	tasks = append(tasks, releaseTasks...)

	// clean node which has been move out from replica
	for _, nodeInfo := range c.nodeMgr.GetAll() {
		nodeID := nodeInfo.ID()
		channelOnQN := c.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(nodeID))
		collectionChannels := lo.GroupBy(channelOnQN, func(ch *meta.DmChannel) int64 { return ch.CollectionID })
		for collectionID, channels := range collectionChannels {
			replica := c.meta.ReplicaManager.GetByCollectionAndNode(ctx, collectionID, nodeID)
			if replica == nil {
				reduceTasks := c.createChannelReduceTasks(ctx, channels, meta.NilReplica)
				task.SetReason("dirty channel exists", reduceTasks...)
				tasks = append(tasks, reduceTasks...)
			}
		}
	}
	return tasks
}

func (c *ChannelChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)

	lacks, redundancies := c.getDmChannelDiff(ctx, replica.GetCollectionID(), replica.GetID())
	tasks := c.createChannelLoadTask(c.getTraceCtx(ctx, replica.GetCollectionID()), lacks, replica)
	task.SetReason("lacks of channel", tasks...)
	ret = append(ret, tasks...)

	tasks = c.createChannelReduceTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), redundancies, replica)
	task.SetReason("collection released", tasks...)
	ret = append(ret, tasks...)

	repeated := c.findRepeatedChannels(ctx, replica.GetID())
	tasks = c.createChannelReduceTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), repeated, replica)
	task.SetReason("redundancies of channel", tasks...)
	ret = append(ret, tasks...)

	// All channel related tasks should be with high priority
	task.SetPriority(task.TaskPriorityHigh, tasks...)
	return ret
}

// GetDmChannelDiff get channel diff between target and dist
func (c *ChannelChecker) getDmChannelDiff(ctx context.Context, collectionID int64,
	replicaID int64,
) (toLoad, toRelease []*meta.DmChannel) {
	replica := c.meta.Get(ctx, replicaID)
	if replica == nil {
		log.Info("replica does not exist, skip it")
		return
	}

	dist := c.dist.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithReplica2Channel(replica))
	distMap := typeutil.NewSet[string]()
	for _, ch := range dist {
		distMap.Insert(ch.GetChannelName())
	}

	nextTargetMap := c.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)
	currentTargetMap := c.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)

	// get channels which exists on dist, but not exist on current and next
	for _, ch := range dist {
		_, existOnCurrent := currentTargetMap[ch.GetChannelName()]
		_, existOnNext := nextTargetMap[ch.GetChannelName()]
		if !existOnNext && !existOnCurrent {
			toRelease = append(toRelease, ch)
		}
	}

	// get channels which exists on next target, but not on dist
	for name, channel := range nextTargetMap {
		_, existOnDist := distMap[name]
		if !existOnDist {
			toLoad = append(toLoad, channel)
		}
	}

	return
}

func (c *ChannelChecker) findRepeatedChannels(ctx context.Context, replicaID int64) []*meta.DmChannel {
	log := log.Ctx(ctx).WithRateGroup("ChannelChecker.findRepeatedChannels", 1, 60)
	replica := c.meta.Get(ctx, replicaID)
	ret := make([]*meta.DmChannel, 0)

	if replica == nil {
		log.Info("replica does not exist, skip it")
		return ret
	}
	dist := c.dist.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithReplica2Channel(replica))

	versionsMap := make(map[string]*meta.DmChannel)
	for _, ch := range dist {
		leaderView := c.dist.LeaderViewManager.GetLeaderShardView(ch.Node, ch.GetChannelName())
		if leaderView == nil {
			log.Info("shard leader view is not ready, skip",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replicaID),
				zap.Int64("leaderID", ch.Node),
				zap.String("channel", ch.GetChannelName()))
			continue
		}

		if leaderView.UnServiceableError != nil {
			log.RatedInfo(10, "replica has unavailable shard leader",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replicaID),
				zap.Int64("leaderID", ch.Node),
				zap.String("channel", ch.GetChannelName()),
				zap.Error(leaderView.UnServiceableError))
			continue
		}

		maxVer, ok := versionsMap[ch.GetChannelName()]
		if !ok {
			versionsMap[ch.GetChannelName()] = ch
			continue
		}
		if maxVer.Version <= ch.Version {
			ret = append(ret, maxVer)
			versionsMap[ch.GetChannelName()] = ch
		} else {
			ret = append(ret, ch)
		}
	}
	return ret
}

func (c *ChannelChecker) createChannelLoadTask(ctx context.Context, channels []*meta.DmChannel, replica *meta.Replica) []task.Task {
	plans := make([]balance.ChannelAssignPlan, 0)
	for _, ch := range channels {
		rwNodes := replica.GetChannelRWNodes(ch.GetChannelName())
		if len(rwNodes) == 0 {
			rwNodes = replica.GetRWNodes()
		}
		plan := c.getBalancerFunc().AssignChannel(ctx, replica.GetCollectionID(), []*meta.DmChannel{ch}, rwNodes, true)
		plans = append(plans, plan...)
	}

	for i := range plans {
		plans[i].Replica = replica
	}

	return balance.CreateChannelTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), plans)
}

func (c *ChannelChecker) createChannelReduceTasks(ctx context.Context, channels []*meta.DmChannel, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0, len(channels))
	for _, ch := range channels {
		action := task.NewChannelAction(ch.Node, task.ActionTypeReduce, ch.GetChannelName())
		task, err := task.NewChannelTask(ctx, Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), c.ID(), ch.GetCollectionID(), replica, action)
		if err != nil {
			log.Warn("create channel reduce task failed",
				zap.Int64("collection", ch.GetCollectionID()),
				zap.Int64("replica", replica.GetID()),
				zap.String("channel", ch.GetChannelName()),
				zap.Int64("from", ch.Node),
				zap.Error(err),
			)
			continue
		}
		ret = append(ret, task)
	}
	return ret
}

func (c *ChannelChecker) getTraceCtx(ctx context.Context, collectionID int64) context.Context {
	coll := c.meta.GetCollection(ctx, collectionID)
	if coll == nil || coll.LoadSpan == nil {
		return ctx
	}

	return trace.ContextWithSpan(ctx, coll.LoadSpan)
}
