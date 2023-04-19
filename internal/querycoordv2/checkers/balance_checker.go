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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/samber/lo"
	"go.uber.org/zap"
)

// BalanceChecker checks the cluster distribution and generates balance tasks.
type BalanceChecker struct {
	baseChecker
	balance.Balance
	meta                            *meta.Meta
	nodeManager                     *session.NodeManager
	balancedCollectionsCurrentRound typeutil.UniqueSet
	scheduler                       task.Scheduler
}

func NewBalanceChecker(meta *meta.Meta, balancer balance.Balance, nodeMgr *session.NodeManager, scheduler task.Scheduler) *BalanceChecker {
	return &BalanceChecker{
		Balance:                         balancer,
		meta:                            meta,
		nodeManager:                     nodeMgr,
		balancedCollectionsCurrentRound: typeutil.NewUniqueSet(),
		scheduler:                       scheduler,
	}
}

func (b *BalanceChecker) Description() string {
	return "BalanceChecker checks the cluster distribution and generates balance tasks"
}

func (b *BalanceChecker) collectionsToBalance() []int64 {
	ids := b.meta.GetAll()

	// loading collection should skip balance
	loadedCollections := lo.Filter(ids, func(cid int64, _ int) bool {
		collection := b.meta.GetCollection(cid)
		return collection != nil && b.meta.GetCollection(cid).Status == querypb.LoadStatus_Loaded
	})
	sort.Slice(loadedCollections, func(i, j int) bool {
		return loadedCollections[i] < loadedCollections[j]
	})

	// balance collections influenced by stopping nodes
	stoppingCollections := make([]int64, 0)
	for _, cid := range loadedCollections {
		replicas := b.meta.ReplicaManager.GetByCollection(cid)
	outer:
		for _, replica := range replicas {
			for _, nodeID := range replica.GetNodes() {
				isStopping, _ := b.nodeManager.IsStoppingNode(nodeID)
				if isStopping {
					stoppingCollections = append(stoppingCollections, cid)
					break outer
				}
			}
		}
	}
	//do stopping balance only in this round
	if len(stoppingCollections) > 0 {
		return stoppingCollections
	}
	//no stopping balance and auto balance is disabled, return empty collections for balance
	if !Params.QueryCoordCfg.AutoBalance {
		return nil
	}

	// scheduler is handling segment task, skip
	if b.scheduler.GetSegmentTaskNum() != 0 {
		return nil
	}

	//iterator one normal collection in one round
	normalCollectionsToBalance := make([]int64, 0)
	hasUnBalancedCollections := false
	for _, cid := range loadedCollections {
		if b.balancedCollectionsCurrentRound.Contain(cid) {
			log.Debug("ScoreBasedBalancer has balanced collection, skip balancing in this round",
				zap.Int64("collectionID", cid))
			continue
		}
		hasUnBalancedCollections = true
		normalCollectionsToBalance = append(normalCollectionsToBalance, cid)
		b.balancedCollectionsCurrentRound.Insert(cid)
		break
	}

	if !hasUnBalancedCollections {
		b.balancedCollectionsCurrentRound.Clear()
		log.Debug("ScoreBasedBalancer has balanced all " +
			"collections in one round, clear collectionIDs for this round")
	}
	return normalCollectionsToBalance
}

func (b *BalanceChecker) balanceCollections(collectionsToBalance []int64) ([]balance.SegmentAssignPlan, []balance.ChannelAssignPlan) {
	segmentPlans, channelPlans := make([]balance.SegmentAssignPlan, 0), make([]balance.ChannelAssignPlan, 0)
	for _, cid := range collectionsToBalance {
		replicas := b.meta.ReplicaManager.GetByCollection(cid)
		for _, replica := range replicas {
			sPlans, cPlans := b.Balance.BalanceReplica(replica)
			balance.PrintNewBalancePlans(cid, replica.GetID(), sPlans, cPlans)
			segmentPlans = append(segmentPlans, sPlans...)
			channelPlans = append(channelPlans, cPlans...)
		}
	}
	return segmentPlans, channelPlans
}

func (b *BalanceChecker) Check(ctx context.Context) []task.Task {
	ret := make([]task.Task, 0)

	collectionsToBalance := b.collectionsToBalance()
	segmentPlans, channelPlans := b.balanceCollections(collectionsToBalance)

	tasks := balance.CreateSegmentTasksFromPlans(ctx, b.ID(), Params.QueryCoordCfg.SegmentTaskTimeout, segmentPlans)
	task.SetPriority(task.TaskPriorityLow, tasks...)
	ret = append(ret, tasks...)

	tasks = balance.CreateChannelTasksFromPlans(ctx, b.ID(), Params.QueryCoordCfg.ChannelTaskTimeout, channelPlans)
	ret = append(ret, tasks...)
	return ret
}
