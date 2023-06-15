// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proxy

import (
	"context"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type executeFunc func(context.Context, UniqueID, types.QueryNode, ...string) error

type ChannelWorkload struct {
	db           string
	collection   string
	channel      string
	shardLeaders []int64
	nq           int64
	exec         executeFunc
	retryTimes   uint
}

type CollectionWorkLoad struct {
	db         string
	collection string
	nq         int64
	exec       executeFunc
}

type LBPolicy interface {
	Execute(ctx context.Context, workload CollectionWorkLoad) error
	ExecuteWithRetry(ctx context.Context, workload ChannelWorkload) error
	UpdateCostMetrics(node int64, cost *internalpb.CostAggregation)
	Start(ctx context.Context)
	Close()
}

type LBPolicyImpl struct {
	balancer  LBBalancer
	clientMgr shardClientMgr
}

func NewLBPolicyImpl(clientMgr shardClientMgr) *LBPolicyImpl {
	balancePolicy := params.Params.ProxyCfg.ReplicaSelectionPolicy.GetValue()

	var balancer LBBalancer
	switch balancePolicy {
	case "round_robin":
		log.Info("use round_robin policy on replica selection")
		balancer = NewRoundRobinBalancer()
	default:
		log.Info("use look_aside policy on replica selection")
		balancer = NewLookAsideBalancer(clientMgr)
	}

	return &LBPolicyImpl{
		balancer:  balancer,
		clientMgr: clientMgr,
	}
}

func (lb *LBPolicyImpl) Start(ctx context.Context) {
	lb.balancer.Start(ctx)
}

// try to select the best node from the available nodes
func (lb *LBPolicyImpl) selectNode(ctx context.Context, workload ChannelWorkload, excludeNodes typeutil.UniqueSet) (int64, error) {
	log := log.With(
		zap.String("collectionName", workload.collection),
		zap.String("channelName", workload.channel),
	)

	filterAvailableNodes := func(node int64, _ int) bool {
		return !excludeNodes.Contain(node)
	}

	getShardLeaders := func() ([]int64, error) {
		shardLeaders, err := globalMetaCache.GetShards(ctx, false, workload.db, workload.collection)
		if err != nil {
			return nil, err
		}

		return lo.Map(shardLeaders[workload.channel], func(node nodeInfo, _ int) int64 { return node.nodeID }), nil
	}

	availableNodes := lo.Filter(workload.shardLeaders, filterAvailableNodes)
	targetNode, err := lb.balancer.SelectNode(ctx, availableNodes, workload.nq)
	if err != nil {
		globalMetaCache.DeprecateShardCache(workload.db, workload.collection)
		nodes, err := getShardLeaders()
		if err != nil || len(nodes) == 0 {
			log.Warn("failed to get shard delegator",
				zap.Error(err))
			return -1, err
		}

		availableNodes := lo.Filter(nodes, filterAvailableNodes)
		if len(availableNodes) == 0 {
			log.Warn("no available shard delegator found",
				zap.Int64s("nodes", nodes),
				zap.Int64s("excluded", excludeNodes.Collect()))
			return -1, merr.WrapErrServiceUnavailable("no available shard delegator found")
		}

		targetNode, err = lb.balancer.SelectNode(ctx, availableNodes, workload.nq)
		if err != nil {
			log.Warn("failed to select shard",
				zap.Error(err))
			return -1, err
		}
	}

	return targetNode, nil
}

// ExecuteWithRetry will choose a qn to execute the workload, and retry if failed, until reach the max retryTimes.
func (lb *LBPolicyImpl) ExecuteWithRetry(ctx context.Context, workload ChannelWorkload) error {
	excludeNodes := typeutil.NewUniqueSet()
	log := log.Ctx(ctx).With(
		zap.String("collectionName", workload.collection),
		zap.String("channelName", workload.channel),
	)

	err := retry.Do(ctx, func() error {
		targetNode, err := lb.selectNode(ctx, workload, excludeNodes)
		if err != nil {
			log.Warn("failed to select node for shard",
				zap.Int64("nodeID", targetNode),
				zap.Error(err))
			return err
		}

		client, err := lb.clientMgr.GetClient(ctx, targetNode)
		if err != nil {
			log.Warn("query channel failed, node not available",
				zap.Int64("nodeID", targetNode),
				zap.Error(err))
			excludeNodes.Insert(targetNode)

			// cancel work load which assign to the target node
			lb.balancer.CancelWorkload(targetNode, workload.nq)
			return merr.WrapErrShardDelegatorAccessFailed(workload.channel, err.Error())
		}

		err = workload.exec(ctx, targetNode, client, workload.channel)
		if err != nil {
			log.Warn("query channel failed",
				zap.Int64("nodeID", targetNode),
				zap.Error(err))
			excludeNodes.Insert(targetNode)
			lb.balancer.CancelWorkload(targetNode, workload.nq)
			return merr.WrapErrShardDelegatorAccessFailed(workload.channel, err.Error())
		}

		lb.balancer.CancelWorkload(targetNode, workload.nq)
		return nil
	}, retry.Attempts(workload.retryTimes))

	return err
}

// Execute will execute collection workload in parallel
func (lb *LBPolicyImpl) Execute(ctx context.Context, workload CollectionWorkLoad) error {
	dml2leaders, err := globalMetaCache.GetShards(ctx, true, workload.db, workload.collection)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get shards", zap.Error(err))
		return err
	}

	wg, ctx := errgroup.WithContext(ctx)
	for channel, nodes := range dml2leaders {
		channel := channel
		nodes := lo.Map(nodes, func(node nodeInfo, _ int) int64 { return node.nodeID })
		wg.Go(func() error {
			err := lb.ExecuteWithRetry(ctx, ChannelWorkload{
				db:           workload.db,
				collection:   workload.collection,
				channel:      channel,
				shardLeaders: nodes,
				nq:           workload.nq,
				exec:         workload.exec,
				retryTimes:   uint(len(nodes)),
			})
			return err
		})
	}

	err = wg.Wait()
	return err
}

func (lb *LBPolicyImpl) UpdateCostMetrics(node int64, cost *internalpb.CostAggregation) {
	lb.balancer.UpdateCostMetrics(node, cost)
}

func (lb *LBPolicyImpl) Close() {
	lb.balancer.Close()
}
