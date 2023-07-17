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

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

	"github.com/samber/lo"
	"go.uber.org/zap"
)

// BalanceChecker checks the cluster distribution and generates balance tasks.
type BalanceChecker struct {
	baseChecker
	balance.Balance
	meta                                 *meta.Meta
	nodeManager                          *session.NodeManager
	normalBalanceCollectionsCurrentRound typeutil.UniqueSet
	scheduler                            task.Scheduler
}

func NewBalanceChecker(meta *meta.Meta, balancer balance.Balance, nodeMgr *session.NodeManager, scheduler task.Scheduler) *BalanceChecker {
	return &BalanceChecker{
		Balance:                              balancer,
		meta:                                 meta,
		nodeManager:                          nodeMgr,
		normalBalanceCollectionsCurrentRound: typeutil.NewUniqueSet(),
		scheduler:                            scheduler,
	}
}

func (b *BalanceChecker) Description() string {
	return "BalanceChecker checks the cluster distribution and generates balance tasks"
}

func (b *BalanceChecker) replicasToBalance() []int64 {
	ids := b.meta.GetAll()

	// all replicas belonging to loading collection will be skipped
	loadedCollections := lo.Filter(ids, func(cid int64, _ int) bool {
		collection := b.meta.GetCollection(cid)
		return collection != nil && collection.GetStatus() == querypb.LoadStatus_Loaded
	})
	sort.Slice(loadedCollections, func(i, j int) bool {
		return loadedCollections[i] < loadedCollections[j]
	})

	// balance collections influenced by stopping nodes
	stoppingReplicas := make([]int64, 0)
	for _, cid := range loadedCollections {
		replicas := b.meta.ReplicaManager.GetByCollection(cid)
		for _, replica := range replicas {
			for _, nodeID := range replica.GetNodes() {
				isStopping, _ := b.nodeManager.IsStoppingNode(nodeID)
				if isStopping {
					stoppingReplicas = append(stoppingReplicas, replica.GetID())
					break
				}
			}
		}
	}
	//do stopping balance only in this round
	if len(stoppingReplicas) > 0 {
		return stoppingReplicas
	}

	//no stopping balance and auto balance is disabled, return empty collections for balance
	if !Params.QueryCoordCfg.AutoBalance.GetAsBool() {
		return nil
	}
	// scheduler is handling segment task, skip
	if b.scheduler.GetSegmentTaskNum() != 0 {
		return nil
	}

	//iterator one normal collection in one round
	normalReplicasToBalance := make([]int64, 0)
	hasUnbalancedCollection := false
	for _, cid := range loadedCollections {
		if b.normalBalanceCollectionsCurrentRound.Contain(cid) {
			log.Debug("ScoreBasedBalancer has balanced collection, skip balancing in this round",
				zap.Int64("collectionID", cid))
			continue
		}
		hasUnbalancedCollection = true
		b.normalBalanceCollectionsCurrentRound.Insert(cid)
		for _, replica := range b.meta.ReplicaManager.GetByCollection(cid) {
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

func (b *BalanceChecker) balanceReplicas(replicaIDs []int64) ([]balance.SegmentAssignPlan, []balance.ChannelAssignPlan) {
	segmentPlans, channelPlans := make([]balance.SegmentAssignPlan, 0), make([]balance.ChannelAssignPlan, 0)
	for _, rid := range replicaIDs {
		replica := b.meta.ReplicaManager.Get(rid)
		if replica == nil {
			continue
		}
		sPlans, cPlans := b.Balance.BalanceReplica(replica)
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

	tasks := balance.CreateSegmentTasksFromPlans(ctx, b.ID(), Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond), segmentPlans)
	task.SetPriority(task.TaskPriorityLow, tasks...)
	ret = append(ret, tasks...)

	tasks = balance.CreateChannelTasksFromPlans(ctx, b.ID(), Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), channelPlans)
	ret = append(ret, tasks...)
	return ret
}
