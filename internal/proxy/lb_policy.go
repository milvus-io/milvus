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

	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
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
	shardLeaders   []nodeInfo
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
	getBalancer    func() LBBalancer
	clientMgr      shardClientMgr
	balancerMap    map[string]LBBalancer
	retryOnReplica int
}

func NewLBPolicyImpl(clientMgr shardClientMgr) *LBPolicyImpl {
	balancerMap := make(map[string]LBBalancer)
	balancerMap[LookAside] = NewLookAsideBalancer(clientMgr)
	balancerMap[RoundRobin] = NewRoundRobinBalancer()

	balancePolicy := params.Params.ProxyCfg.ReplicaSelectionPolicy.GetValue()
	getBalancer := func() LBBalancer {
		if _, ok := balancerMap[balancePolicy]; !ok {
			return balancerMap[LookAside]
		}
		return balancerMap[balancePolicy]
	}

	retryOnReplica := Params.ProxyCfg.RetryTimesOnReplica.GetAsInt()

	return &LBPolicyImpl{
		getBalancer:    getBalancer,
		clientMgr:      clientMgr,
		balancerMap:    balancerMap,
		retryOnReplica: retryOnReplica,
	}
}

func (lb *LBPolicyImpl) Start(ctx context.Context) {
	for _, lb := range lb.balancerMap {
		lb.Start(ctx)
	}
}

// GetShardLeaders should always retry until ctx done, except the collection is not loaded.
func (lb *LBPolicyImpl) GetShardLeaders(ctx context.Context, dbName string, collName string, collectionID int64, withCache bool) (map[string][]nodeInfo, error) {
	var shardLeaders map[string][]nodeInfo
	// use retry to handle query coord service not ready
	err := retry.Handle(ctx, func() (bool, error) {
		var err error
		shardLeaders, err = globalMetaCache.GetShards(ctx, withCache, dbName, collName, collectionID)
		if err != nil {
			return !errors.Is(err, merr.ErrCollectionNotLoaded), err
		}
		return false, nil
	})

	return shardLeaders, err
}

// try to select the best node from the available nodes
func (lb *LBPolicyImpl) selectNode(ctx context.Context, balancer LBBalancer, workload ChannelWorkload, excludeNodes typeutil.UniqueSet) (nodeInfo, error) {
	filterDelegator := func(nodes []nodeInfo) map[int64]nodeInfo {
		ret := make(map[int64]nodeInfo)
		for _, node := range nodes {
			if !excludeNodes.Contain(node.nodeID) {
				ret[node.nodeID] = node
			}
		}
		return ret
	}

	availableNodes := filterDelegator(workload.shardLeaders)
	balancer.RegisterNodeInfo(lo.Values(availableNodes))
	targetNode, err := balancer.SelectNode(ctx, lo.Keys(availableNodes), workload.nq)
	if err != nil {
		log := log.Ctx(ctx)
		globalMetaCache.DeprecateShardCache(workload.db, workload.collectionName)
		shardLeaders, err := lb.GetShardLeaders(ctx, workload.db, workload.collectionName, workload.collectionID, false)
		if err != nil {
			log.Warn("failed to get shard delegator",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Error(err))
			return nodeInfo{}, err
		}

		availableNodes = filterDelegator(shardLeaders[workload.channel])
		if len(availableNodes) == 0 {
			log.Warn("no available shard delegator found",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64s("availableNodes", lo.Keys(availableNodes)),
				zap.Int64s("excluded", excludeNodes.Collect()))
			return nodeInfo{}, merr.WrapErrChannelNotAvailable("no available shard delegator found")
		}

		balancer.RegisterNodeInfo(lo.Values(availableNodes))
		targetNode, err = balancer.SelectNode(ctx, lo.Keys(availableNodes), workload.nq)
		if err != nil {
			log.Warn("failed to select shard",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64s("availableNodes", lo.Keys(availableNodes)),
				zap.Int64s("excluded", excludeNodes.Collect()),
				zap.Error(err))
			return nodeInfo{}, err
		}
	}

	return availableNodes[targetNode], nil
}

// ExecuteWithRetry will choose a qn to execute the workload, and retry if failed, until reach the max retryTimes.
func (lb *LBPolicyImpl) ExecuteWithRetry(ctx context.Context, workload ChannelWorkload) error {
	excludeNodes := typeutil.NewUniqueSet()

	var lastErr error
	err := retry.Do(ctx, func() error {
		balancer := lb.getBalancer()
		targetNode, err := lb.selectNode(ctx, balancer, workload, excludeNodes)
		if err != nil {
			log.Warn("failed to select node for shard",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64("nodeID", targetNode.nodeID),
				zap.Error(err),
			)
			if lastErr != nil {
				return lastErr
			}
			return err
		}
		// cancel work load which assign to the target node
		defer balancer.CancelWorkload(targetNode.nodeID, workload.nq)

		client, err := lb.clientMgr.GetClient(ctx, targetNode)
		if err != nil {
			log.Warn("search/query channel failed, node not available",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64("nodeID", targetNode.nodeID),
				zap.Error(err))
			excludeNodes.Insert(targetNode.nodeID)

			lastErr = errors.Wrapf(err, "failed to get delegator %d for channel %s", targetNode.nodeID, workload.channel)
			return lastErr
		}

		err = workload.exec(ctx, targetNode.nodeID, client, workload.channel)
		if err != nil {
			log.Warn("search/query channel failed",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64("nodeID", targetNode.nodeID),
				zap.Error(err))
			excludeNodes.Insert(targetNode.nodeID)
			lastErr = errors.Wrapf(err, "failed to search/query delegator %d for channel %s", targetNode.nodeID, workload.channel)
			return lastErr
		}

		return nil
	}, retry.Attempts(workload.retryTimes))

	return err
}

// Execute will execute collection workload in parallel
func (lb *LBPolicyImpl) Execute(ctx context.Context, workload CollectionWorkLoad) error {
	dml2leaders, err := lb.GetShardLeaders(ctx, workload.db, workload.collectionName, workload.collectionID, true)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get shards", zap.Error(err))
		return err
	}

	// let every request could retry at least twice, which could retry after update shard leader cache
	wg, ctx := errgroup.WithContext(ctx)
	for k, v := range dml2leaders {
		channel := k
		nodes := v
		channelRetryTimes := lb.retryOnReplica
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
