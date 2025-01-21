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

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// BalanceChecker checks the cluster distribution and generates balance tasks.
type BalanceChecker struct {
	*checkerActivation
	meta                                 *meta.Meta
	nodeManager                          *session.NodeManager
	normalBalanceCollectionsCurrentRound typeutil.UniqueSet
	scheduler                            task.Scheduler
	targetMgr                            meta.TargetManagerInterface
	getBalancerFunc                      GetBalancerFunc
}

func NewBalanceChecker(meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	getBalancerFunc GetBalancerFunc,
) *BalanceChecker {
	return &BalanceChecker{
		checkerActivation:                    newCheckerActivation(),
		meta:                                 meta,
		targetMgr:                            targetMgr,
		nodeManager:                          nodeMgr,
		normalBalanceCollectionsCurrentRound: typeutil.NewUniqueSet(),
		scheduler:                            scheduler,
		getBalancerFunc:                      getBalancerFunc,
	}
}

func (b *BalanceChecker) ID() utils.CheckerType {
	return utils.BalanceChecker
}

func (b *BalanceChecker) Description() string {
	return "BalanceChecker checks the cluster distribution and generates balance tasks"
}

func (b *BalanceChecker) readyToCheck(ctx context.Context, collectionID int64) bool {
	metaExist := (b.meta.GetCollection(ctx, collectionID) != nil)
	targetExist := b.targetMgr.IsNextTargetExist(ctx, collectionID) || b.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (b *BalanceChecker) replicasToBalance(ctx context.Context) []int64 {
	ids := b.meta.GetAll(ctx)

	// all replicas belonging to loading collection will be skipped
	loadedCollections := lo.Filter(ids, func(cid int64, _ int) bool {
		collection := b.meta.GetCollection(ctx, cid)
		return collection != nil && collection.GetStatus() == querypb.LoadStatus_Loaded
	})
	sort.Slice(loadedCollections, func(i, j int) bool {
		return loadedCollections[i] < loadedCollections[j]
	})

	if paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
		// balance collections influenced by stopping nodes
		stoppingReplicas := make([]int64, 0)
		for _, cid := range loadedCollections {
			// if target and meta isn't ready, skip balance this collection
			if !b.readyToCheck(ctx, cid) {
				continue
			}
			replicas := b.meta.ReplicaManager.GetByCollection(ctx, cid)
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

	// Before performing balancing, check the CurrentTarget/LeaderView/Distribution for all collections.
	// If any collection has unready info, skip the balance operation to avoid inconsistencies.
	notReadyCollections := lo.Filter(loadedCollections, func(cid int64, _ int) bool {
		// todo: should also check distribution and leader view in the future
		return !b.targetMgr.IsCurrentTargetReady(ctx, cid)
	})
	if len(notReadyCollections) > 0 {
		log.RatedInfo(10, "skip normal balance, cause collection not ready for balance", zap.Int64s("collectionIDs", notReadyCollections))
		return nil
	}

	// iterator one normal collection in one round
	normalReplicasToBalance := make([]int64, 0)
	hasUnbalancedCollection := false
	for _, cid := range loadedCollections {
		if b.normalBalanceCollectionsCurrentRound.Contain(cid) {
			log.RatedDebug(10, "ScoreBasedBalancer is balancing this collection, skip balancing in this round",
				zap.Int64("collectionID", cid))
			continue
		}
		hasUnbalancedCollection = true
		b.normalBalanceCollectionsCurrentRound.Insert(cid)
		for _, replica := range b.meta.ReplicaManager.GetByCollection(ctx, cid) {
			normalReplicasToBalance = append(normalReplicasToBalance, replica.GetID())
		}
		break
	}

	if !hasUnbalancedCollection {
		b.normalBalanceCollectionsCurrentRound.Clear()
		log.RatedDebug(10, "ScoreBasedBalancer has balanced all "+
			"collections in one round, clear collectionIDs for this round")
	}
	return normalReplicasToBalance
}

func (b *BalanceChecker) balanceReplicas(ctx context.Context, replicaIDs []int64) ([]balance.SegmentAssignPlan, []balance.ChannelAssignPlan) {
	segmentPlans, channelPlans := make([]balance.SegmentAssignPlan, 0), make([]balance.ChannelAssignPlan, 0)
	for _, rid := range replicaIDs {
		replica := b.meta.ReplicaManager.Get(ctx, rid)
		if replica == nil {
			continue
		}
		sPlans, cPlans := b.getBalancerFunc().BalanceReplica(ctx, replica)
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

	replicasToBalance := b.replicasToBalance(ctx)
	segmentPlans, channelPlans := b.balanceReplicas(ctx, replicasToBalance)
	// iterate all collection to find a collection to balance
	for len(segmentPlans) == 0 && len(channelPlans) == 0 && b.normalBalanceCollectionsCurrentRound.Len() > 0 {
		replicasToBalance := b.replicasToBalance(ctx)
		segmentPlans, channelPlans = b.balanceReplicas(ctx, replicasToBalance)
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
