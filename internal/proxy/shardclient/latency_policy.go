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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// statsKey is the key outcome-learning balancers keep statistics under: the
// channel, and the request class when set.
func statsKey(workload ChannelWorkload) string {
	if workload.StatsClass == "" {
		return workload.Channel
	}
	return workload.Channel + "|" + workload.StatsClass
}

// latencyWeighted returns the balancer as a LatencyWeightedBalancer when it
// is one and its switch is on.
func latencyWeighted(balancer LBBalancer) *LatencyWeightedBalancer {
	if lw, ok := balancer.(*LatencyWeightedBalancer); ok && lw.Enabled() {
		return lw
	}
	return nil
}

// hedgeStatsKey separates pooled timing and hedge outcomes by request routing scope.
// Replica latency and in-flight observations remain shared for the same node.
func hedgeStatsKey(workload ChannelWorkload) string {
	if workload.ResourceGroup == "" {
		return statsKey(workload)
	}
	return fmt.Sprintf("%s|resource_group=%q", statsKey(workload), workload.ResourceGroup)
}

// snapshotBalancer captures the accounting owner for this request. In particular,
// disabled latency weighting uses the underlying look-aside balancer directly,
// so a later configuration change cannot change how its workload is released.
func snapshotBalancer(balancer LBBalancer) (LBBalancer, *LatencyWeightedBalancer) {
	if lw, ok := balancer.(*LatencyWeightedBalancer); ok {
		if lw.Enabled() {
			return lw, lw
		}
		return lw.LookAsideBalancer, nil
	}
	return balancer, nil
}

// selectNodeWithPolicy may refresh shard locations. Hedge execution performs
// that lookup asynchronously and calls selectCandidate only after it completes.
func (lb *LBPolicyImpl) selectNodeWithPolicy(ctx context.Context, balancer LBBalancer, lw *LatencyWeightedBalancer, workload ChannelWorkload, excludeNodes *typeutil.UniqueSet) (NodeInfo, bool, error) {
	trySelectNode := func(withCache bool) (NodeInfo, bool, error) {
		leaders, err := lb.GetShard(ctx, workload.Db, workload.CollectionName, workload.CollectionID, workload.Channel, withCache)
		if err != nil {
			return NodeInfo{}, false, err
		}
		leaders = FilterByResourceGroup(leaders, workload.ResourceGroup)
		if !withCache && len(leaders) > 0 {
			allExcluded := true
			for _, node := range leaders {
				if !excludeNodes.Contain(node.NodeID) {
					allExcluded = false
					break
				}
			}
			if allExcluded {
				excludeNodes.Clear()
			}
		}
		return lb.selectCandidate(ctx, balancer, lw, workload, leaders, excludeNodes)
	}
	node, selected, err := trySelectNode(true)
	if err != nil {
		return trySelectNode(false)
	}
	return node, selected, nil
}

// selectCandidate never performs a coordinator RPC or clears exclusions. Its
// selection and workload reservation both belong to the calling goroutine.
func (lb *LBPolicyImpl) selectCandidate(ctx context.Context, balancer LBBalancer, lw *LatencyWeightedBalancer, workload ChannelWorkload, shardLeaders []NodeInfo, excludeNodes *typeutil.UniqueSet) (_ NodeInfo, _ bool, err error) {
	log := mlog.With(mlog.FieldCollectionID(workload.CollectionID), mlog.String("channelName", workload.Channel))
	shardLeaders = FilterByResourceGroup(shardLeaders, workload.ResourceGroup)
	if len(shardLeaders) == 0 && workload.ResourceGroup != "" {
		return NodeInfo{}, false, merr.WrapErrCollectionNotFullyLoaded(workload.CollectionID,
			fmt.Sprintf("no shard leader for channel %s in resource group %s", workload.Channel, workload.ResourceGroup))
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
			log.Warn(ctx, "failed to select shard",
				mlog.Int64s("excluded", excludeNodes.Collect()),
				mlog.String("candidates", strings.Join(candidatesInStr, ", ")),
				mlog.String("serviceableNodes", strings.Join(serviceableNodesInStr, ", ")),
				mlog.Err(err))
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
	nodes := lo.Keys(candidateNodes)
	if len(serviceableNodes) > 0 {
		nodes = lo.Keys(serviceableNodes)
	}
	var targetNodeID int64
	if lw != nil {
		targetNodeID, err = lw.SelectNodeForKey(ctx, statsKey(workload), nodes)
	} else {
		targetNodeID, err = balancer.SelectNode(ctx, nodes, workload.Nq)
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

// executeWeightedWithRetry uses request exclusions and decaying penalties rather
// than the legacy channel blacklist. The policy is captured at request entry.
func (lb *LBPolicyImpl) executeWeightedWithRetry(ctx context.Context, workload ChannelWorkload, balancer LBBalancer, lw *LatencyWeightedBalancer) error {
	excludeNodes := typeutil.NewUniqueSet()
	var lastErr error
	tryExecute := func() (bool, error) {
		node, selected, err := lb.selectNodeWithPolicy(ctx, balancer, lw, workload, &excludeNodes)
		if err != nil {
			// Preserve the existing scoped retry contract when a group's last
			// leader disappears after an execution error.
			scopedFreshRefusal := workload.ResourceGroup != "" && errors.Is(err, merr.ErrCollectionNotFullyLoaded) && !merr.IsRetryableErr(lastErr)
			if lastErr != nil && !scopedFreshRefusal {
				return true, lastErr
			}
			return true, err
		}
		start := time.Now()
		if workload.Hedgeable {
			if delay, enabled := lw.HedgeDelay(hedgeStatsKey(workload)); enabled {
				again, err := lb.executeHedged(ctx, workload, lw, node, selected, start, delay, &excludeNodes)
				if err != nil {
					lastErr = err
				}
				return again, err
			}
		}
		client, err := lb.clientMgr.GetClient(ctx, node)
		if err == nil {
			err = workload.Exec(ctx, node.NodeID, client, workload.Channel)
		}
		lw.ObserveAttempt(ctx, statsKey(workload), node.NodeID, selected, start, err)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) || merr.GetErrorType(err) == merr.InputError {
				return false, err
			}
			excludeNodes.Insert(node.NodeID)
			lastErr = merr.Wrapf(err, "failed to search/query delegator %d for channel %s", node.NodeID, workload.Channel)
			return true, lastErr
		}
		lw.ObserveRequest(hedgeStatsKey(workload), time.Since(start))
		return true, nil
	}
	leaders, err := lb.GetShard(ctx, workload.Db, workload.CollectionName, workload.CollectionID, workload.Channel, true)
	if err != nil {
		return err
	}
	// Retain the unscoped retry budget used by the existing resource-group
	// path. Sleep starts at 10ms and is capped at 20ms by retry options.
	attempts := len(leaders) + max(lb.retryOnReplica, 1)
	return retry.Handle(ctx, tryExecute, retry.Attempts(uint(attempts)), retry.Sleep(latencyWeightedRetryInterval), retry.MaxSleepTime(latencyWeightedRetryInterval))
}
