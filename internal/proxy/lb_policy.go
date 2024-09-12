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

	"github.com/cockroachdb/errors"
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

type executeFunc func(context.Context, UniqueID, types.QueryNodeClient, string) error

type ChannelWorkload struct {
	db             string
	collectionName string
	collectionID   int64
	channel        string
	shardLeaders   []int64
	nq             int64
	exec           executeFunc
	retryTimes     uint
}

type CollectionWorkLoad struct {
	db             string
	collectionName string
	collectionID   int64
	nq             int64
	exec           executeFunc
}

type LBPolicy interface {
	Execute(ctx context.Context, workload CollectionWorkLoad) error
	ExecuteWithRetry(ctx context.Context, workload ChannelWorkload) error
	UpdateCostMetrics(node int64, cost *internalpb.CostAggregation)
	Start(ctx context.Context)
	Close()
}

const (
	RoundRobin = "round_robin"
	LookAside  = "look_aside"
)

type LBPolicyImpl struct {
	getBalancer func() LBBalancer
	clientMgr   shardClientMgr
	balancerMap map[string]LBBalancer
}

func NewLBPolicyImpl(clientMgr shardClientMgr) *LBPolicyImpl {
	balancerMap := make(map[string]LBBalancer)
	balancerMap[LookAside] = NewLookAsideBalancer(clientMgr)
	balancerMap[RoundRobin] = NewRoundRobinBalancer()

	getBalancer := func() LBBalancer {
		balancePolicy := params.Params.ProxyCfg.ReplicaSelectionPolicy.GetValue()
		if _, ok := balancerMap[balancePolicy]; !ok {
			return balancerMap[LookAside]
		}
		return balancerMap[balancePolicy]
	}

	return &LBPolicyImpl{
		getBalancer: getBalancer,
		clientMgr:   clientMgr,
		balancerMap: balancerMap,
	}
}

func (lb *LBPolicyImpl) Start(ctx context.Context) {
	for _, lb := range lb.balancerMap {
		lb.Start(ctx)
	}
}

// try to select the best node from the available nodes
func (lb *LBPolicyImpl) selectNode(ctx context.Context, workload ChannelWorkload, excludeNodes typeutil.UniqueSet) (int64, error) {
	availableNodes := lo.FilterMap(workload.shardLeaders, func(node int64, _ int) (int64, bool) { return node, !excludeNodes.Contain(node) })
	targetNode, err := lb.getBalancer().SelectNode(ctx, availableNodes, workload.nq)
	if err != nil {
		log := log.Ctx(ctx)
		globalMetaCache.DeprecateShardCache(workload.db, workload.collectionName)
		shardLeaders, err := globalMetaCache.GetShards(ctx, false, workload.db, workload.collectionName, workload.collectionID)
		if err != nil {
			log.Warn("failed to get shard delegator",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Error(err))
			return -1, err
		}

		availableNodes := lo.FilterMap(shardLeaders[workload.channel], func(node nodeInfo, _ int) (int64, bool) { return node.nodeID, !excludeNodes.Contain(node.nodeID) })
		if len(availableNodes) == 0 {
			nodes := lo.Map(shardLeaders[workload.channel], func(node nodeInfo, _ int) int64 { return node.nodeID })
			log.Warn("no available shard delegator found",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64s("nodes", nodes),
				zap.Int64s("excluded", excludeNodes.Collect()))
			return -1, merr.WrapErrChannelNotAvailable("no available shard delegator found")
		}

		targetNode, err = lb.getBalancer().SelectNode(ctx, availableNodes, workload.nq)
		if err != nil {
			log.Warn("failed to select shard",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64s("availableNodes", availableNodes),
				zap.Error(err))
			return -1, err
		}
	}

	return targetNode, nil
}

// ExecuteWithRetry will choose a qn to execute the workload, and retry if failed, until reach the max retryTimes.
func (lb *LBPolicyImpl) ExecuteWithRetry(ctx context.Context, workload ChannelWorkload) error {
	excludeNodes := typeutil.NewUniqueSet()

	var lastErr error
	err := retry.Do(ctx, func() error {
		targetNode, err := lb.selectNode(ctx, workload, excludeNodes)
		if err != nil {
			log.Warn("failed to select node for shard",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64("nodeID", targetNode),
				zap.Error(err),
			)
			if lastErr != nil {
				return lastErr
			}
			return err
		}

		client, err := lb.clientMgr.GetClient(ctx, targetNode)
		if err != nil {
			log.Warn("search/query channel failed, node not available",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64("nodeID", targetNode),
				zap.Error(err))
			excludeNodes.Insert(targetNode)

			// cancel work load which assign to the target node
			lb.getBalancer().CancelWorkload(targetNode, workload.nq)
			lastErr = errors.Wrapf(err, "failed to get delegator %d for channel %s", targetNode, workload.channel)
			return lastErr
		}

		err = workload.exec(ctx, targetNode, client, workload.channel)
		if err != nil {
			log.Warn("search/query channel failed",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64("nodeID", targetNode),
				zap.Error(err))
			excludeNodes.Insert(targetNode)
			lb.getBalancer().CancelWorkload(targetNode, workload.nq)
			lastErr = errors.Wrapf(err, "failed to search/query delegator %d for channel %s", targetNode, workload.channel)
			return lastErr
		}

		lb.getBalancer().CancelWorkload(targetNode, workload.nq)
		return nil
	}, retry.Attempts(workload.retryTimes))

	return err
}

// Execute will execute collection workload in parallel
func (lb *LBPolicyImpl) Execute(ctx context.Context, workload CollectionWorkLoad) error {
	dml2leaders, err := globalMetaCache.GetShards(ctx, true, workload.db, workload.collectionName, workload.collectionID)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get shards", zap.Error(err))
		return err
	}

	// let every request could retry at least twice, which could retry after update shard leader cache
	retryTimes := Params.ProxyCfg.RetryTimesOnReplica.GetAsInt()
	wg, ctx := errgroup.WithContext(ctx)
	for channel, nodes := range dml2leaders {
		channel := channel
		nodes := lo.Map(nodes, func(node nodeInfo, _ int) int64 { return node.nodeID })
		channelRetryTimes := retryTimes
		if len(nodes) > 0 {
			channelRetryTimes *= len(nodes)
		}
		wg.Go(func() error {
			return lb.ExecuteWithRetry(ctx, ChannelWorkload{
				db:             workload.db,
				collectionName: workload.collectionName,
				collectionID:   workload.collectionID,
				channel:        channel,
				shardLeaders:   nodes,
				nq:             workload.nq,
				exec:           workload.exec,
				retryTimes:     uint(channelRetryTimes),
			})
		})
	}

	return wg.Wait()
}

func (lb *LBPolicyImpl) UpdateCostMetrics(node int64, cost *internalpb.CostAggregation) {
	lb.getBalancer().UpdateCostMetrics(node, cost)
}

func (lb *LBPolicyImpl) Close() {
	for _, lb := range lb.balancerMap {
		lb.Close()
	}
}
