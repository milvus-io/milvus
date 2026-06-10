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

package shardclient

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ExecuteFunc func(context.Context, UniqueID, types.QueryNodeClient, string) error

type ChannelWorkload struct {
	Db              string
	CollectionName  string
	CollectionID    int64
	Channel         string
	Nq              int64
	Exec            ExecuteFunc
	PreferredNodeID int64
}

type CollectionWorkLoad struct {
	Db             string
	CollectionName string
	CollectionID   int64
	Nq             int64
	Exec           ExecuteFunc
	PreferredNodes map[string]int64
}

type LBPolicy interface {
	Execute(ctx context.Context, workload CollectionWorkLoad) error
	ExecuteOneChannel(ctx context.Context, workload CollectionWorkLoad) error
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
	clientMgr      ShardClientMgr
	balancerMap    map[string]LBBalancer
	retryOnReplica int
}

func NewLBPolicyImpl(clientMgr ShardClientMgr) *LBPolicyImpl {
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

	retryOnReplica := paramtable.Get().ProxyCfg.RetryTimesOnReplica.GetAsInt()

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

// GetShard will retry until ctx done, except the collection is not loaded.
// return all replicas of shard from cache if withCache is true, otherwise return shard leaders from coord.
func (lb *LBPolicyImpl) GetShard(ctx context.Context, dbName string, collName string, collectionID int64, channel string, withCache bool) ([]NodeInfo, error) {
	var shardLeaders []NodeInfo
	err := retry.Handle(ctx, func() (bool, error) {
		var err error
		shardLeaders, err = lb.clientMgr.GetShard(ctx, withCache, dbName, collName, collectionID, channel)
		return !errors.Is(err, merr.ErrCollectionNotLoaded), err
	})
	return shardLeaders, err
}

// GetShardLeaderList will retry until ctx done, except the collection is not loaded.
// return all shard(channel) from cache if withCache is true, otherwise return shard leaders from coord.
func (lb *LBPolicyImpl) GetShardLeaderList(ctx context.Context, dbName string, collName string, collectionID int64, withCache bool) ([]string, error) {
	var ret []string
	err := retry.Handle(ctx, func() (bool, error) {
		var err error
		ret, err = lb.clientMgr.GetShardLeaderList(ctx, dbName, collName, collectionID, withCache)
		return !errors.Is(err, merr.ErrCollectionNotLoaded), err
	})
	return ret, err
}

func recordPreferredNodeSelection(status string) {
	metrics.ProxyShardLeaderPreferredNodeCount.WithLabelValues(
		status,
	).Inc()
}

func preferredNodeID(workload CollectionWorkLoad, channel string) int64 {
	if workload.PreferredNodes == nil {
		return 0
	}
	nodeID := workload.PreferredNodes[channel]
	if nodeID == 0 {
		recordPreferredNodeSelection(metrics.PreferredNodeMissLabel)
	}
	return nodeID
}

func isResourceInsufficientError(err error) bool {
	return errors.Is(err, merr.ErrServiceTooManyRequests) ||
		errors.Is(err, merr.ErrServiceResourceInsufficient)
}

// try to select the best node from the available nodes
func (lb *LBPolicyImpl) selectNode(ctx context.Context, balancer LBBalancer, workload ChannelWorkload, excludeNodes *typeutil.UniqueSet) (NodeInfo, bool, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", workload.CollectionID),
		zap.String("channelName", workload.Channel),
	)
	// Select node using specified nodes
	trySelectNode := func(withCache bool) (NodeInfo, bool, error) {
		shardLeaders, err := lb.GetShard(ctx, workload.Db, workload.CollectionName, workload.CollectionID, workload.Channel, withCache)
		if err != nil {
			log.Warn("failed to get shard delegator",
				zap.Error(err))
			return NodeInfo{}, false, err
		}

		// if all available delegator has been excluded even after refresh shard leader cache
		// we should clear excludeNodes and try to select node again instead of failing the request at selectNode
		if !withCache && len(shardLeaders) > 0 && len(shardLeaders) <= excludeNodes.Len() {
			allReplicaExcluded := true
			for _, node := range shardLeaders {
				if !excludeNodes.Contain(node.NodeID) {
					allReplicaExcluded = false
					break
				}
			}
			if allReplicaExcluded {
				log.Warn("all replicas are excluded after refresh shard leader cache, clear it and try to select node")
				excludeNodes.Clear()
			}
		}

		candidateNodes := make(map[int64]NodeInfo)
		serviceableNodes := make(map[int64]NodeInfo)
		defer func() {
			if err != nil {
				candidatesInStr := lo.Map(shardLeaders, func(node NodeInfo, _ int) string {
					return node.String()
				})
				serviceableNodesInStr := lo.Map(lo.Values(serviceableNodes), func(node NodeInfo, _ int) string {
					return node.String()
				})
				log.Warn("failed to select shard",
					zap.Int64s("excluded", excludeNodes.Collect()),
					zap.String("candidates", strings.Join(candidatesInStr, ", ")),
					zap.String("serviceableNodes", strings.Join(serviceableNodesInStr, ", ")),
					zap.Error(err))
			}
		}()

		// Filter nodes based on excludeNodes
		for _, node := range shardLeaders {
			if !excludeNodes.Contain(node.NodeID) {
				if node.Serviceable {
					serviceableNodes[node.NodeID] = node
				}
				candidateNodes[node.NodeID] = node
			}
		}
		if len(candidateNodes) == 0 {
			err = merr.WrapErrChannelNotAvailable(workload.Channel, "no available shard leaders")
			return NodeInfo{}, false, err
		}

		if preferredNode, ok := serviceableNodes[workload.PreferredNodeID]; ok {
			recordPreferredNodeSelection(metrics.PreferredNodeHitLabel)
			return preferredNode, false, nil
		} else if workload.PreferredNodeID != 0 {
			recordPreferredNodeSelection(metrics.PreferredNodeUnavailableLabel)
		}

		balancer.RegisterNodeInfo(lo.Values(candidateNodes))

		// prefer serviceable nodes
		var targetNodeID int64
		if len(serviceableNodes) > 0 {
			targetNodeID, err = balancer.SelectNode(ctx, lo.Keys(serviceableNodes), workload.Nq)
		} else {
			targetNodeID, err = balancer.SelectNode(ctx, lo.Keys(candidateNodes), workload.Nq)
		}
		if err != nil {
			return NodeInfo{}, false, err
		}

		if _, ok := candidateNodes[targetNodeID]; !ok {
			err = merr.WrapErrNodeNotAvailable(targetNodeID)
			return NodeInfo{}, false, err
		}

		return candidateNodes[targetNodeID], true, nil
	}

	// First attempt with current shard leaders cache
	withShardLeaderCache := true
	targetNode, selectedByBalancer, err := trySelectNode(withShardLeaderCache)
	if err != nil {
		// Second attempt with fresh shard leaders
		withShardLeaderCache = false
		targetNode, selectedByBalancer, err = trySelectNode(withShardLeaderCache)
		if err != nil {
			return NodeInfo{}, false, err
		}
	}

	return targetNode, selectedByBalancer, nil
}

// ExecuteWithRetry will choose a qn to execute the workload, and retry if failed, until reach the max retryTimes.
func (lb *LBPolicyImpl) ExecuteWithRetry(ctx context.Context, workload ChannelWorkload) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", workload.CollectionID),
		zap.String("channelName", workload.Channel),
	)
	var lastErr error
	excludeNodes := typeutil.NewUniqueSet()
	tryExecute := func() (bool, error) {
		balancer := lb.getBalancer()
		targetNode, selectedByBalancer, err := lb.selectNode(ctx, balancer, workload, &excludeNodes)
		if err != nil {
			log.Warn("failed to select node for shard",
				zap.Int64("nodeID", targetNode.NodeID),
				zap.Int64s("excluded", excludeNodes.Collect()),
				zap.Error(err),
			)
			if lastErr != nil {
				return true, lastErr
			}
			return true, err
		}
		// cancel work load which assign to the target node
		if selectedByBalancer {
			defer balancer.CancelWorkload(targetNode.NodeID, workload.Nq)
		}

		client, err := lb.clientMgr.GetClient(ctx, targetNode)
		if err != nil {
			log.Warn("search/query channel failed, node not available",
				zap.Int64("nodeID", targetNode.NodeID),
				zap.Error(err))
			excludeNodes.Insert(targetNode.NodeID)

			lastErr = errors.Wrapf(err, "failed to get delegator %d for channel %s", targetNode.NodeID, workload.Channel)
			return true, lastErr
		}

		err = workload.Exec(ctx, targetNode.NodeID, client, workload.Channel)
		if err != nil {
			log.Warn("search/query channel failed",
				zap.Int64("nodeID", targetNode.NodeID),
				zap.Error(err))
			// An input error is the request's own fault: re-dispatching it to
			// other replicas cannot make it succeed. Abort immediately without
			// retrying or excluding the (healthy) serving node.
			if merr.GetErrorType(err) == merr.InputError {
				return false, err
			}
			lastErr = errors.Wrapf(err, "failed to search/query delegator %d for channel %s", targetNode.NodeID, workload.Channel)
			if isResourceInsufficientError(err) {
				return false, lastErr
			}
			excludeNodes.Insert(targetNode.NodeID)
			return true, lastErr
		}

		return true, nil
	}

	shardLeaders, err := lb.GetShard(ctx, workload.Db, workload.CollectionName, workload.CollectionID, workload.Channel, true)
	if err != nil {
		log.Warn("failed to get shard leaders", zap.Error(err))
		return err
	}
	// Sweep all shard leaders once, then allow configured request-level retries after every leader has been tried.
	retryTimes := len(shardLeaders) + max(lb.retryOnReplica, 1)
	err = retry.Handle(ctx, tryExecute, retry.Attempts(uint(retryTimes)))
	if err != nil {
		log.Warn("failed to execute",
			zap.String("channel", workload.Channel),
			zap.Error(err))
	}

	return err
}

// Execute will execute collection workload in parallel
func (lb *LBPolicyImpl) Execute(ctx context.Context, workload CollectionWorkLoad) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", workload.CollectionID),
	)
	channelList, err := lb.GetShardLeaderList(ctx, workload.Db, workload.CollectionName, workload.CollectionID, true)
	if err != nil {
		log.Warn("failed to get shards", zap.Error(err))
		return err
	}

	if len(channelList) == 0 {
		log.Info("no shard leaders found", zap.Int64("collectionID", workload.CollectionID))
		return merr.WrapErrCollectionNotLoaded(workload.CollectionID)
	}

	// Single channel fast path: skip errgroup/goroutine overhead
	if len(channelList) == 1 {
		return lb.ExecuteWithRetry(ctx, ChannelWorkload{
			Db:              workload.Db,
			CollectionName:  workload.CollectionName,
			CollectionID:    workload.CollectionID,
			Channel:         channelList[0],
			Nq:              workload.Nq,
			Exec:            workload.Exec,
			PreferredNodeID: preferredNodeID(workload, channelList[0]),
		})
	}

	wg, groupCtx := errgroup.WithContext(ctx)
	for _, channel := range channelList {
		channel := channel
		wg.Go(func() error {
			return lb.ExecuteWithRetry(groupCtx, ChannelWorkload{
				Db:              workload.Db,
				CollectionName:  workload.CollectionName,
				CollectionID:    workload.CollectionID,
				Channel:         channel,
				Nq:              workload.Nq,
				Exec:            workload.Exec,
				PreferredNodeID: preferredNodeID(workload, channel),
			})
		})
	}
	return wg.Wait()
}

// ExecuteOneChannel will execute at any one channel in collection
func (lb *LBPolicyImpl) ExecuteOneChannel(ctx context.Context, workload CollectionWorkLoad) error {
	channelList, err := lb.GetShardLeaderList(ctx, workload.Db, workload.CollectionName, workload.CollectionID, true)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get shards", zap.Error(err))
		return err
	}

	// let every request could retry at least twice, which could retry after update shard leader cache
	for _, channel := range channelList {
		return lb.ExecuteWithRetry(ctx, ChannelWorkload{
			Db:              workload.Db,
			CollectionName:  workload.CollectionName,
			CollectionID:    workload.CollectionID,
			Channel:         channel,
			Nq:              workload.Nq,
			Exec:            workload.Exec,
			PreferredNodeID: preferredNodeID(workload, channel),
		})
	}
	// An empty leader list here is a transient routing-cache state (leaders are
	// re-discovered on retry); reporting "collection not loaded" would tell the
	// user to re-load a collection that is loaded.
	return merr.WrapErrServiceUnavailable(fmt.Sprintf("no available shard leader for collection %d", workload.CollectionID))
}

func (lb *LBPolicyImpl) UpdateCostMetrics(node int64, cost *internalpb.CostAggregation) {
	lb.getBalancer().UpdateCostMetrics(node, cost)
}

func (lb *LBPolicyImpl) Close() {
	for _, lb := range lb.balancerMap {
		lb.Close()
	}
}
