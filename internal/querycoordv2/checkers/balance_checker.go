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

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// BalanceChecker checks the cluster distribution and generates balance tasks.
type BalanceChecker struct {
	*checkerActivation
	meta            *meta.Meta
	nodeManager     *session.NodeManager
	scheduler       task.Scheduler
	targetMgr       meta.TargetManagerInterface
	dist            *meta.DistributionManager
	getBalancerFunc GetBalancerFunc

	// balancedCollectionInCurrentRound is used to record all balanced collections in current round
	balancedCollectionInCurrentRound typeutil.UniqueSet
}

func NewBalanceChecker(meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	getBalancerFunc GetBalancerFunc,
) *BalanceChecker {
	return &BalanceChecker{
		checkerActivation:                newCheckerActivation(),
		meta:                             meta,
		targetMgr:                        targetMgr,
		nodeManager:                      nodeMgr,
		scheduler:                        scheduler,
		getBalancerFunc:                  getBalancerFunc,
		dist:                             dist,
		balancedCollectionInCurrentRound: typeutil.NewUniqueSet(),
	}
}

func (b *BalanceChecker) ID() utils.CheckerType {
	return utils.BalanceChecker
}

func (b *BalanceChecker) Description() string {
	return "BalanceChecker checks the cluster distribution and generates balance tasks"
}

func (b *BalanceChecker) readyToCheck(collectionID int64) bool {
	metaExist := (b.meta.GetCollection(collectionID) != nil)
	targetExist := b.targetMgr.IsNextTargetExist(collectionID) || b.targetMgr.IsCurrentTargetExist(collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (b *BalanceChecker) replicasToBalance() []int64 {
	ids := b.meta.GetAll()

	// all replicas belonging to loading collection will be skipped
	loadedCollections := lo.Filter(ids, func(cid int64, _ int) bool {
		collection := b.meta.GetCollection(cid)
		return collection != nil && collection.GetStatus() == querypb.LoadStatus_Loaded && b.readyToCheck(cid)
	})

	// balance large collection first
	collectionRowCount := b.getCollectionRowCount(loadedCollections)
	sort.Slice(loadedCollections, func(i, j int) bool {
		if collectionRowCount[loadedCollections[i]] == collectionRowCount[loadedCollections[j]] {
			return loadedCollections[i] < loadedCollections[j]
		}
		return collectionRowCount[loadedCollections[i]] < collectionRowCount[loadedCollections[j]]
	})

	if paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
		// balance collections influenced by stopping nodes
		stoppingReplicas := make([]int64, 0)
		for _, cid := range loadedCollections {
			replicas := b.meta.ReplicaManager.GetByCollection(cid)
			for _, replica := range replicas {
				if replica.RONodesCount() > 0 {
					stoppingReplicas = append(stoppingReplicas, replica.GetID())
				}
			}
		}
		// do stopping balance only in this round
		if len(stoppingReplicas) > 0 {
			return stoppingReplicas
		}
	}

	// 1. no stopping balance and auto balance is disabled, return empty collections for balance
	// 2. when balancer isn't active, skip auto balance
	if !Params.QueryCoordCfg.AutoBalance.GetAsBool() || !b.IsActive() {
		return nil
	}

	collectionToBalance := int64(-1)
	// iterator one normal collection in one round
	for _, cid := range loadedCollections {
		if b.balancedCollectionInCurrentRound.Contain(cid) {
			log.RatedDebug(10, "ScoreBasedBalancer is balancing this collection, skip balancing in this round",
				zap.Int64("collectionID", cid))
			continue
		}
		b.balancedCollectionInCurrentRound.Insert(cid)
		collectionToBalance = cid
		break
	}
	// if all collection has been balanced in current round, clear and start next round
	if len(loadedCollections) == b.balancedCollectionInCurrentRound.Len() {
		b.balancedCollectionInCurrentRound.Clear()
	}

	return lo.Map(b.meta.ReplicaManager.GetByCollection(collectionToBalance), func(replica *meta.Replica, _ int) int64 {
		return replica.GetID()
	})
}

func (b *BalanceChecker) balanceReplicas(replicaIDs []int64) ([]balance.SegmentAssignPlan, []balance.ChannelAssignPlan) {
	segmentPlans, channelPlans := make([]balance.SegmentAssignPlan, 0), make([]balance.ChannelAssignPlan, 0)
	for _, rid := range replicaIDs {
		replica := b.meta.ReplicaManager.Get(rid)
		if replica == nil {
			continue
		}
		sPlans, cPlans := b.getBalancerFunc().BalanceReplica(replica)
		segmentPlans = append(segmentPlans, sPlans...)
		channelPlans = append(channelPlans, cPlans...)
		if len(segmentPlans) != 0 || len(channelPlans) != 0 {
			balance.PrintNewBalancePlans(replica.GetCollectionID(), replica.GetID(), sPlans, cPlans)
		}
	}
	return segmentPlans, channelPlans
}

func (b *BalanceChecker) Check(ctx context.Context) []task.Task {
	ret := make([]task.Task, 0)

	replicasToBalance := b.replicasToBalance()
	segmentPlans, channelPlans := b.balanceReplicas(replicasToBalance)
	// iterate all collection to find a collection to balance
	for len(segmentPlans) == 0 && len(channelPlans) == 0 && b.balancedCollectionInCurrentRound.Len() > 0 {
		// try to find next collection for balance
		replicasToBalance := b.replicasToBalance()
		segmentPlans, channelPlans = b.balanceReplicas(replicasToBalance)
	}

	tasks := balance.CreateSegmentTasksFromPlans(ctx, b.ID(), Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond), segmentPlans)
	task.SetPriority(task.TaskPriorityLow, tasks...)
	task.SetReason("segment unbalanced", tasks...)
	ret = append(ret, tasks...)

	tasks = balance.CreateChannelTasksFromPlans(ctx, b.ID(), Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), channelPlans)
	task.SetReason("channel unbalanced", tasks...)
	ret = append(ret, tasks...)
	return ret
}

func (b *BalanceChecker) getCollectionRowCount(collectionIDs []int64) map[int64]int64 {
	computeRowCount := func(collectionID int64) int64 {
		collectionRowCount := 0
		// calculate collection sealed segment row count
		collectionSegments := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(collectionID))
		for _, s := range collectionSegments {
			collectionRowCount += int(s.GetNumOfRows())
		}

		// calculate collection growing segment row count
		collectionViews := b.dist.LeaderViewManager.GetByFilter(meta.WithCollectionID2LeaderView(collectionID))
		for _, view := range collectionViews {
			collectionRowCount += int(float64(view.NumOfGrowingRows))
		}

		return int64(collectionRowCount)
	}
	collectionRowCount := make(map[int64]int64)
	for _, collectionID := range collectionIDs {
		collectionRowCount[collectionID] = computeRowCount(collectionID)
	}
	return collectionRowCount
}
