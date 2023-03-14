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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// TODO(sunby): have too much similar codes with SegmentChecker
type ChannelChecker struct {
	baseChecker
	meta      *meta.Meta
	dist      *meta.DistributionManager
	targetMgr *meta.TargetManager
	balancer  balance.Balance
}

func NewChannelChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	balancer balance.Balance,
) *ChannelChecker {
	return &ChannelChecker{
		meta:      meta,
		dist:      dist,
		targetMgr: targetMgr,
		balancer:  balancer,
	}
}

func (c *ChannelChecker) Description() string {
	return "DmChannelChecker checks the lack of DmChannels, or some DmChannels are redundant"
}

func (c *ChannelChecker) Check(ctx context.Context) []task.Task {
	collectionIDs := c.meta.CollectionManager.GetAll()
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		replicas := c.meta.ReplicaManager.GetByCollection(cid)
		for _, r := range replicas {
			tasks = append(tasks, c.checkReplica(ctx, r)...)
		}
	}

	channels := c.dist.ChannelDistManager.GetAll()
	released := utils.FilterReleased(channels, collectionIDs)
	tasks = append(tasks, c.createChannelReduceTasks(ctx, released, -1)...)
	return tasks
}

func (c *ChannelChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)

	lacks, redundancies := c.getDmChannelDiff(c.targetMgr, c.dist, c.meta, replica.GetCollectionID(), replica.GetID())
	tasks := c.createChannelLoadTask(ctx, lacks, replica)
	task.SetReason("lacks of channel", tasks...)
	ret = append(ret, tasks...)

	tasks = c.createChannelReduceTasks(ctx, redundancies, replica.GetID())
	task.SetReason("collection released", tasks...)
	ret = append(ret, tasks...)

	repeated := c.findRepeatedChannels(c.dist, c.meta, replica.GetID())
	tasks = c.createChannelReduceTasks(ctx, repeated, replica.GetID())
	task.SetReason("redundancies of channel")
	ret = append(ret, tasks...)

	// All channel related tasks should be with high priority
	task.SetPriority(task.TaskPriorityHigh, tasks...)
	return ret
}

// GetDmChannelDiff get channel diff between target and dist
func (c *ChannelChecker) getDmChannelDiff(targetMgr *meta.TargetManager,
	distMgr *meta.DistributionManager,
	metaInfo *meta.Meta,
	collectionID int64,
	replicaID int64) (toLoad, toRelease []*meta.DmChannel) {
	replica := metaInfo.Get(replicaID)
	if replica == nil {
		log.Info("replica does not exist, skip it")
		return
	}

	dist := c.getChannelDist(distMgr, replica)
	distMap := typeutil.NewSet[string]()
	for _, ch := range dist {
		distMap.Insert(ch.GetChannelName())
	}

	nextTargetMap := targetMgr.GetDmChannelsByCollection(collectionID, meta.NextTarget)
	currentTargetMap := targetMgr.GetDmChannelsByCollection(collectionID, meta.CurrentTarget)

	// get channels which exists on dist, but not exist on current and next
	for _, ch := range dist {
		_, existOnCurrent := currentTargetMap[ch.GetChannelName()]
		_, existOnNext := nextTargetMap[ch.GetChannelName()]
		if !existOnNext && !existOnCurrent {
			toRelease = append(toRelease, ch)
		}
	}

	//get channels which exists on next target, but not on dist
	for name, channel := range nextTargetMap {
		_, existOnDist := distMap[name]
		if !existOnDist {
			toLoad = append(toLoad, channel)
		}
	}

	return
}

func (c *ChannelChecker) getChannelDist(distMgr *meta.DistributionManager, replica *meta.Replica) []*meta.DmChannel {
	dist := make([]*meta.DmChannel, 0)
	for _, nodeID := range replica.GetNodes() {
		dist = append(dist, distMgr.ChannelDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nodeID)...)
	}
	return dist
}

func (c *ChannelChecker) findRepeatedChannels(distMgr *meta.DistributionManager,
	metaInfo *meta.Meta,
	replicaID int64) []*meta.DmChannel {
	replica := metaInfo.Get(replicaID)
	ret := make([]*meta.DmChannel, 0)

	if replica == nil {
		log.Info("replica does not exist, skip it")
		return ret
	}
	dist := c.getChannelDist(distMgr, replica)

	versionsMap := make(map[string]*meta.DmChannel)
	for _, ch := range dist {
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
	outboundNodes := c.meta.ResourceManager.CheckOutboundNodes(replica)
	availableNodes := lo.Filter(replica.Replica.GetNodes(), func(node int64, _ int) bool {
		return !outboundNodes.Contain(node)
	})
	plans := c.balancer.AssignChannel(channels, availableNodes)
	for i := range plans {
		plans[i].ReplicaID = replica.GetID()
	}

	return balance.CreateChannelTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), plans)
}

func (c *ChannelChecker) createChannelReduceTasks(ctx context.Context, channels []*meta.DmChannel, replicaID int64) []task.Task {
	ret := make([]task.Task, 0, len(channels))
	for _, ch := range channels {
		action := task.NewChannelAction(ch.Node, task.ActionTypeReduce, ch.GetChannelName())
		task, err := task.NewChannelTask(ctx, Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), c.ID(), ch.GetCollectionID(), replicaID, action)
		if err != nil {
			log.Warn("create channel reduce task failed",
				zap.Int64("collection", ch.GetCollectionID()),
				zap.Int64("replica", replicaID),
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
