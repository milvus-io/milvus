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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
func (lb *LBPolicyImpl) selectNode(ctx context.Context, balancer LBBalancer, workload *ChannelWorkload, excludeNodes typeutil.UniqueSet) (nodeInfo, error) {
	log := log.Ctx(ctx)
	// Select node using specified nodes
	trySelectNode := func(nodes []nodeInfo) (nodeInfo, error) {
		candidateNodes := make(map[int64]nodeInfo)
		serviceableNodes := make(map[int64]nodeInfo)
		// Filter nodes based on excludeNodes
		for _, node := range nodes {
			if !excludeNodes.Contain(node.nodeID) {
				if node.serviceable {
					serviceableNodes[node.nodeID] = node
				}
				candidateNodes[node.nodeID] = node
			}
		}

		var err error
		defer func() {
			if err != nil {
				candidatesInStr := lo.Map(nodes, func(node nodeInfo, _ int) string {
					return node.String()
				})
				serviceableNodesInStr := lo.Map(lo.Values(serviceableNodes), func(node nodeInfo, _ int) string {
					return node.String()
				})
				log.Warn("failed to select shard",
					zap.Int64("collectionID", workload.collectionID),
					zap.String("channelName", workload.channel),
					zap.Int64s("excluded", excludeNodes.Collect()),
					zap.String("candidates", strings.Join(candidatesInStr, ", ")),
					zap.String("serviceableNodes", strings.Join(serviceableNodesInStr, ", ")),
					zap.Error(err))
			}
		}()

		if len(candidateNodes) == 0 {
			err = merr.WrapErrChannelNotAvailable(workload.channel)
			return nodeInfo{}, err
		}

		balancer.RegisterNodeInfo(lo.Values(candidateNodes))
		// prefer serviceable nodes
		var targetNodeID int64
		if len(serviceableNodes) > 0 {
			targetNodeID, err = balancer.SelectNode(ctx, lo.Keys(serviceableNodes), workload.nq)
		} else {
			targetNodeID, err = balancer.SelectNode(ctx, lo.Keys(candidateNodes), workload.nq)
		}
		if err != nil {
			return nodeInfo{}, err
		}

		if _, ok := candidateNodes[targetNodeID]; !ok {
			err = merr.WrapErrNodeNotAvailable(targetNodeID)
			return nodeInfo{}, err
		}

		return candidateNodes[targetNodeID], nil
	}

	// First attempt with current shard leaders
	targetNode, err := trySelectNode(workload.shardLeaders)
	// If failed, refresh cache and retry
	if err != nil {
		globalMetaCache.DeprecateShardCache(workload.db, workload.collectionName)
		shardLeaders, err := lb.GetShardLeaders(ctx, workload.db, workload.collectionName, workload.collectionID, false)
		if err != nil {
			log.Warn("failed to get shard delegator",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Error(err))
			return nodeInfo{}, err
		}

		workload.shardLeaders = shardLeaders[workload.channel]
		// Second attempt with fresh shard leaders
		targetNode, err = trySelectNode(workload.shardLeaders)
		if err != nil {
			return nodeInfo{}, err
		}
	}

	return targetNode, nil
}

// ExecuteWithRetry will choose a qn to execute the workload, and retry if failed, until reach the max retryTimes.
func (lb *LBPolicyImpl) ExecuteWithRetry(ctx context.Context, workload ChannelWorkload) error {
	var lastErr error
	excludeNodes := typeutil.NewUniqueSet()
	tryExecute := func() (bool, error) {
		// if keeping retry after all nodes are excluded, try to clean excludeNodes
		if excludeNodes.Len() == len(workload.shardLeaders) {
			excludeNodes.Clear()
		}

		balancer := lb.getBalancer()
		targetNode, err := lb.selectNode(ctx, balancer, &workload, excludeNodes)
		if err != nil {
			log.Warn("failed to select node for shard",
				zap.Int64("collectionID", workload.collectionID),
				zap.String("channelName", workload.channel),
				zap.Int64("nodeID", targetNode.nodeID),
				zap.Int64s("excluded", excludeNodes.Collect()),
				zap.Error(err),
			)
			if lastErr != nil {
				return true, lastErr
			}
			return true, err
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
			return true, lastErr
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
			return true, lastErr
		}

		return true, nil
	}

	// if failed, try to execute with partial result
	err := retry.Handle(ctx, tryExecute, retry.Attempts(workload.retryTimes))
	if err != nil {
		log.Ctx(ctx).Warn("failed to execute with partial result",
			zap.String("channel", workload.channel),
			zap.Error(err))
	}

	return err
}

// Execute will execute collection workload in parallel
func (lb *LBPolicyImpl) Execute(ctx context.Context, workload CollectionWorkLoad) error {
	dml2leaders, err := lb.GetShardLeaders(ctx, workload.db, workload.collectionName, workload.collectionID, true)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get shards", zap.Error(err))
		return err
	}

	totalChannels := len(dml2leaders)
	if totalChannels == 0 {
		log.Ctx(ctx).Info("no shard leaders found", zap.Int64("collectionID", workload.collectionID))
		return merr.WrapErrCollectionNotLoaded(workload.collectionID)
	}

	wg, _ := errgroup.WithContext(ctx)
	// Launch a goroutine for each channel
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
