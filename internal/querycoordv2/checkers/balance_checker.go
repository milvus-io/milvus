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
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// BalanceChecker checks the cluster distribution and generates balance tasks.
type BalanceChecker struct {
	*checkerActivation
	meta            *meta.Meta
	nodeManager     *session.NodeManager
	scheduler       task.Scheduler
	targetMgr       meta.TargetManagerInterface
	getBalancerFunc GetBalancerFunc

	normalBalanceCollectionsCurrentRound   typeutil.UniqueSet
	stoppingBalanceCollectionsCurrentRound typeutil.UniqueSet

	// record auto balance ts
	autoBalanceTs time.Time
}

func NewBalanceChecker(meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	getBalancerFunc GetBalancerFunc,
) *BalanceChecker {
	return &BalanceChecker{
		checkerActivation:                      newCheckerActivation(),
		meta:                                   meta,
		targetMgr:                              targetMgr,
		nodeManager:                            nodeMgr,
		normalBalanceCollectionsCurrentRound:   typeutil.NewUniqueSet(),
		stoppingBalanceCollectionsCurrentRound: typeutil.NewUniqueSet(),
		scheduler:                              scheduler,
		getBalancerFunc:                        getBalancerFunc,
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

func (b *BalanceChecker) getReplicaForStoppingBalance(ctx context.Context) []int64 {
	ids := b.meta.GetAll(ctx)

	// Sort collections using the configured sort order
	ids = b.sortCollections(ctx, ids)

	if paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
		hasUnbalancedCollection := false
		defer func() {
			if !hasUnbalancedCollection {
				b.stoppingBalanceCollectionsCurrentRound.Clear()
				log.RatedDebug(10, "BalanceChecker has triggered stopping balance for all "+
					"collections in one round, clear collectionIDs for this round")
			}
		}()
		for _, cid := range ids {
			// if target and meta isn't ready, skip balance this collection
			if !b.readyToCheck(ctx, cid) {
				continue
			}
			if b.stoppingBalanceCollectionsCurrentRound.Contain(cid) {
				log.RatedDebug(10, "BalanceChecker is balancing this collection, skip balancing in this round",
					zap.Int64("collectionID", cid))
				continue
			}
			replicas := b.meta.ReplicaManager.GetByCollection(ctx, cid)
			stoppingReplicas := make([]int64, 0)
			for _, replica := range replicas {
				if replica.RONodesCount() > 0 {
					stoppingReplicas = append(stoppingReplicas, replica.GetID())
				}
			}
			if len(stoppingReplicas) > 0 {
				hasUnbalancedCollection = true
				b.stoppingBalanceCollectionsCurrentRound.Insert(cid)
				return stoppingReplicas
			}
		}
	}

	return nil
}

func (b *BalanceChecker) getReplicaForNormalBalance(ctx context.Context) []int64 {
	// 1. no stopping balance and auto balance is disabled, return empty collections for balance
	// 2. when balancer isn't active, skip auto balance
	if !Params.QueryCoordCfg.AutoBalance.GetAsBool() || !b.IsActive() {
		return nil
	}

	ids := b.meta.GetAll(ctx)

	// all replicas belonging to loading collection will be skipped
	loadedCollections := lo.Filter(ids, func(cid int64, _ int) bool {
		collection := b.meta.GetCollection(ctx, cid)
		return collection != nil && collection.GetStatus() == querypb.LoadStatus_Loaded
	})

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

	// Sort collections using the configured sort order
	loadedCollections = b.sortCollections(ctx, loadedCollections)

	// iterator one normal collection in one round
	normalReplicasToBalance := make([]int64, 0)
	hasUnbalancedCollection := false
	for _, cid := range loadedCollections {
		if b.normalBalanceCollectionsCurrentRound.Contain(cid) {
			log.RatedDebug(10, "BalanceChecker is balancing this collection, skip balancing in this round",
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
		log.RatedDebug(10, "BalanceChecker has triggered normal balance for all "+
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
	var segmentPlans []balance.SegmentAssignPlan
	var channelPlans []balance.ChannelAssignPlan
	stoppingReplicas := b.getReplicaForStoppingBalance(ctx)
	if len(stoppingReplicas) > 0 {
		// check for stopping balance first
		segmentPlans, channelPlans = b.balanceReplicas(ctx, stoppingReplicas)
		// iterate all collection to find a collection to balance
		for len(segmentPlans) == 0 && len(channelPlans) == 0 && b.stoppingBalanceCollectionsCurrentRound.Len() > 0 {
			replicasToBalance := b.getReplicaForStoppingBalance(ctx)
			segmentPlans, channelPlans = b.balanceReplicas(ctx, replicasToBalance)
		}
	} else {
		// then check for auto balance
		if time.Since(b.autoBalanceTs) > paramtable.Get().QueryCoordCfg.AutoBalanceInterval.GetAsDuration(time.Millisecond) {
			b.autoBalanceTs = time.Now()
			replicasToBalance := b.getReplicaForNormalBalance(ctx)
			segmentPlans, channelPlans = b.balanceReplicas(ctx, replicasToBalance)
			// iterate all collection to find a collection to balance
			for len(segmentPlans) == 0 && len(channelPlans) == 0 && b.normalBalanceCollectionsCurrentRound.Len() > 0 {
				replicasToBalance := b.getReplicaForNormalBalance(ctx)
				segmentPlans, channelPlans = b.balanceReplicas(ctx, replicasToBalance)
			}
		}
	}

	ret := make([]task.Task, 0)
	tasks := balance.CreateSegmentTasksFromPlans(ctx, b.ID(), Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond), segmentPlans)
	task.SetPriority(task.TaskPriorityLow, tasks...)
	task.SetReason("segment unbalanced", tasks...)
	ret = append(ret, tasks...)

	tasks = balance.CreateChannelTasksFromPlans(ctx, b.ID(), Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), channelPlans)
	task.SetReason("channel unbalanced", tasks...)
	ret = append(ret, tasks...)
	return ret
}

func (b *BalanceChecker) sortCollections(ctx context.Context, collections []int64) []int64 {
	sortOrder := strings.ToLower(Params.QueryCoordCfg.BalanceTriggerOrder.GetValue())
	if sortOrder == "" {
		sortOrder = "byrowcount" // Default to ByRowCount
	}

	// Define sorting functions
	sortByRowCount := func(i, j int) bool {
		rowCount1 := b.targetMgr.GetCollectionRowCount(ctx, collections[i], meta.CurrentTargetFirst)
		rowCount2 := b.targetMgr.GetCollectionRowCount(ctx, collections[j], meta.CurrentTargetFirst)
		return rowCount1 > rowCount2 || (rowCount1 == rowCount2 && collections[i] < collections[j])
	}

	sortByCollectionID := func(i, j int) bool {
		return collections[i] < collections[j]
	}

	// Select the appropriate sorting function
	var sortFunc func(i, j int) bool
	switch sortOrder {
	case "byrowcount":
		sortFunc = sortByRowCount
	case "bycollectionid":
		sortFunc = sortByCollectionID
	default:
		log.Warn("Invalid balance sort order configuration, using default ByRowCount", zap.String("sortOrder", sortOrder))
		sortFunc = sortByRowCount
	}

	// Sort the collections
	sort.Slice(collections, sortFunc)
	return collections
}
