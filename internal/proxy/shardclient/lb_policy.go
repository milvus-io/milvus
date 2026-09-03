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
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	// ResourceGroup, when non-empty, restricts selectNode to the leaders whose
	// replica lives in that resource group (NodeInfo.ResourceGroup). It scopes
	// the candidates of this one channel only -- the channel itself is still
	// executed -- and an empty scoped candidate set is reported as
	// ErrCollectionNotFullyLoaded (retriable), see selectNode. Empty is the
	// absence of a scope and leaves routing exactly as before.
	//
	// A construction site does NOT have to set it. Every entry point of this
	// package stamps the request's scope on from the context (see
	// extension_seam.go), because a site that forgot would not fail - it would
	// route that subset of requests to another group's leader, silently, the
	// same wrong-routing-with-no-signal failure as filtering the channel map.
	// Three paths build a ChannelWorkload directly rather than going through
	// Execute - the namespace single-shard fast paths in task_search.go,
	// task_query.go and task_delete.go - and they are covered by the stamp on
	// ExecuteWithRetry. Setting it explicitly still wins, for a caller that
	// names a scope the context does not carry.
	ResourceGroup string
}

type CollectionWorkLoad struct {
	Db             string
	CollectionName string
	CollectionID   int64
	Nq             int64
	Exec           ExecuteFunc
	// ResourceGroup is copied onto every ChannelWorkload the fan-out creates,
	// through ForChannel. The fan-out itself stays unscoped: every shard is
	// visited, and a shard the group cannot serve fails its channel rather
	// than vanishing from the answer. See ChannelWorkload.ResourceGroup.
	ResourceGroup  string
	PreferredNodes map[string]int64
}

// ForChannel derives the ChannelWorkload for one shard of w, carrying every
// collection-level field -- including ResourceGroup -- so that a caller which
// dispatches a single channel itself (the namespace fast paths) cannot build a
// workload that silently drops the scope. preferredNodeID is passed in rather
// than derived because the fast paths and Execute resolve it from different
// sources; the caller keeps whatever it did before.
func (w CollectionWorkLoad) ForChannel(channel string, preferredNodeID int64) ChannelWorkload {
	return ChannelWorkload{
		Db:              w.Db,
		CollectionName:  w.CollectionName,
		CollectionID:    w.CollectionID,
		Channel:         channel,
		Nq:              w.Nq,
		Exec:            w.Exec,
		PreferredNodeID: preferredNodeID,
		ResourceGroup:   w.ResourceGroup,
	}
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
	blacklist      *ChannelBlacklist
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
		blacklist:      NewChannelBlacklist(),
	}
}

func (lb *LBPolicyImpl) Start(ctx context.Context) {
	for _, lb := range lb.balancerMap {
		lb.Start(ctx)
	}
	lb.blacklist.Start()
}

// GetShard retries a bounded number of times (retry.Handle's default: 10
// attempts, backing off from 200ms) or until ctx is done, whichever comes first, except
// when the collection is not loaded.
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

// GetShardLeaderList retries a bounded number of times (retry.Handle's
// default: 10 attempts, backing off from 200ms) or until ctx is done, whichever comes
// first, except when the collection is not loaded.
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

// GetShardLeaders retries a bounded number of times (retry.Handle's default:
// 10 attempts, backing off from 200ms) or until ctx is done, whichever comes first,
// except when the collection is not loaded -- the same policy as its two
// siblings above, so a transient coordinator error does not fail a request
// the other two reads would have retried through. Returns every channel of
// the collection with its leaders in one read; with withCache=false that is
// one coordinator call refreshing all of them.
func (lb *LBPolicyImpl) GetShardLeaders(ctx context.Context, dbName string, collName string, collectionID int64, withCache bool) (map[string][]NodeInfo, error) {
	var ret map[string][]NodeInfo
	err := retry.Handle(ctx, func() (bool, error) {
		var err error
		ret, err = lb.clientMgr.GetShardLeaders(ctx, withCache, dbName, collName, collectionID)
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

// try to select the best node from the available nodes
func (lb *LBPolicyImpl) selectNode(ctx context.Context, balancer LBBalancer, workload ChannelWorkload, excludeNodes *typeutil.UniqueSet) (NodeInfo, bool, error) {
	log := mlog.With(
		mlog.Int64("collectionID", workload.CollectionID),
		mlog.String("channelName", workload.Channel),
	)
	// Select node using specified nodes
	trySelectNode := func(withCache bool) (NodeInfo, bool, error) {
		shardLeaders, err := lb.GetShard(ctx, workload.Db, workload.CollectionName, workload.CollectionID, workload.Channel, withCache)
		if err != nil {
			log.Warn(ctx, "failed to get shard delegator",
				mlog.Err(err))
			return NodeInfo{}, false, err
		}

		// The resource-group scope is applied to THIS channel's candidates,
		// before the exclusion logic below so that "every candidate excluded"
		// is judged on the scoped set. The channel itself is never dropped:
		// Execute fans out over the unscoped GetShardLeaderList, so a shard the
		// group cannot serve fails here, visibly, instead of being silently
		// left out of the answer.
		shardLeaders = FilterByResourceGroup(shardLeaders, workload.ResourceGroup)
		if len(shardLeaders) == 0 && workload.ResourceGroup != "" {
			// A scoped request finding no candidate is a resource group that
			// is still coming up: its replica exists, but its delegator for
			// this channel is not serviceable yet (or the cache predates it;
			// the second, uncached attempt covers that). That is a
			// seconds-to-minutes transient the caller polls through, so it is
			// reported with ErrCollectionNotFullyLoaded (103, retriable) --
			// the same code the strict GetShardLeaders gate uses for a
			// collection still coming up. ErrChannelNotAvailable is (503,
			// non-retriable) and would tell the SDK and every upper layer to
			// stop on a state that heals itself; the unscoped answer below
			// keeps the code it has always used.
			err = merr.WrapErrCollectionNotFullyLoaded(workload.CollectionID,
				fmt.Sprintf("no shard leader for channel %s in resource group %s", workload.Channel, workload.ResourceGroup))
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
				log.Warn(ctx, "all replicas are excluded after refresh shard leader cache, clear it and try to select node")
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
	// Extension seam, see extension_seam.go: the request's routing scope is
	// stamped on here, at the load balancer's own entry, so that no call site
	// can silently omit it. Empty with no provider installed.
	workload = scopedChannelWorkload(ctx, workload)
	log := mlog.With(
		mlog.Int64("collectionID", workload.CollectionID),
		mlog.String("channelName", workload.Channel),
	)
	var lastErr error
	var err error
	var shardLeaders []NodeInfo
	requestExcludedNodes := typeutil.NewUniqueSet()
	tryExecute := func() (bool, error) {
		// Get fresh blacklist on each retry to include newly blacklisted nodes
		blacklist := lb.blacklist.GetBlacklistedNodes(workload.Channel)
		// The "every leader excluded" recovery is judged on the SCOPED leader
		// set: under a scope the request can only ever exclude the group's
		// own leaders, so comparing against the unscoped count would never
		// fire for a group holding a subset of the channel's leaders, and the
		// refresh-and-clear would be dead code for exactly the requests that
		// poll through a group coming up. shardLeaders itself stays unscoped
		// (it is also the retry budget, see below).
		scopedLeaders := FilterByResourceGroup(shardLeaders, workload.ResourceGroup)
		if len(scopedLeaders) > 0 && requestExcludedNodes.Len() >= len(scopedLeaders) {
			shardLeaders, err = lb.GetShard(ctx, workload.Db, workload.CollectionName, workload.CollectionID, workload.Channel, false)
			if err != nil {
				log.Warn(ctx, "failed to refresh shard leaders", mlog.Err(err))
				if lastErr != nil {
					return true, lastErr
				}
				return true, err
			}

			scopedLeaders = FilterByResourceGroup(shardLeaders, workload.ResourceGroup)
			allReplicaExcluded := len(scopedLeaders) > 0
			for _, node := range scopedLeaders {
				if !requestExcludedNodes.Contain(node.NodeID) {
					allReplicaExcluded = false
					break
				}
			}
			if allReplicaExcluded {
				log.Warn(ctx, "all replicas are request-level excluded after refresh, clear it and retry")
				requestExcludedNodes.Clear()
			}
		}
		excludeNodes := typeutil.NewUniqueSet(blacklist...)
		excludeNodes.Insert(requestExcludedNodes.Collect()...)
		balancer := lb.getBalancer()
		targetNode, selectedByBalancer, err := lb.selectNode(ctx, balancer, workload, &excludeNodes)
		if err != nil {
			log.Warn(ctx, "failed to select node for shard",
				mlog.Int64("nodeID", targetNode.NodeID),
				mlog.Int64s("excluded", excludeNodes.Collect()),
				mlog.Err(err),
			)
			// The exec error from an earlier attempt is normally the more
			// informative one to end on, and for an unscoped request it stays
			// that way -- unchanged from before the scope existed.
			//
			// Under a scope there is one ordering where that is wrong: the
			// group's leader failed with a non-retriable error and then
			// disappeared from the channel, so the next selection is the
			// retriable ErrCollectionNotFullyLoaded. Ending on the earlier
			// error would tell the layer waiting for the group to stop,
			// undoing the code in exactly the case it exists for.
			//
			// Scoped ONLY, and ONLY for that one refusal -- the
			// ErrCollectionNotFullyLoaded selectNode raises when the scoped
			// candidate set is empty. Both halves of the gate are load-bearing:
			// selectNode also propagates the balancer's error, and the balancer
			// answers a RETRIABLE ErrServiceUnavailable whenever every
			// candidate is unreachable (look_aside_balancer.go), so a rule
			// keyed on "any retriable error" would silently reclassify the
			// ordinary "all nodes down after a terminal exec error" from
			// terminal to retriable and drop the cause the caller could act on
			// -- on the scoped path just as much as the unscoped one.
			// TestExecuteWithRetryUnscopedKeepsTheExecError and
			// TestExecuteWithRetryScopedKeepsTheExecErrorOnBalancerFailure pin
			// the two halves.
			scopedFreshRefusal := workload.ResourceGroup != "" &&
				errors.Is(err, merr.ErrCollectionNotFullyLoaded) && !merr.IsRetryableErr(lastErr)
			if lastErr != nil && !scopedFreshRefusal {
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
			log.Warn(ctx, "search/query channel failed, node not available",
				mlog.Int64("nodeID", targetNode.NodeID),
				mlog.Err(err))
			lb.blacklist.Add(workload.Channel, targetNode.NodeID)

			lastErr = errors.Wrapf(err, "failed to get delegator %d for channel %s", targetNode.NodeID, workload.Channel)
			return true, lastErr
		}

		err = workload.Exec(ctx, targetNode.NodeID, client, workload.Channel)
		if err != nil {
			log.Warn(ctx, "search/query channel failed",
				mlog.Int64("nodeID", targetNode.NodeID),
				mlog.Err(err))
			// An input error is the request's own fault: re-dispatching it to
			// other replicas cannot make it succeed, and blacklisting the
			// (healthy) serving node would penalize it for a bad request. Abort
			// immediately without retrying or touching the blacklist.
			if merr.GetErrorType(err) == merr.InputError {
				return false, err
			}
			if merr.IsRetryableErr(err) {
				requestExcludedNodes.Insert(targetNode.NodeID)
			} else {
				lb.blacklist.Add(workload.Channel, targetNode.NodeID)
			}
			lastErr = errors.Wrapf(err, "failed to search/query delegator %d for channel %s", targetNode.NodeID, workload.Channel)
			return true, lastErr
		}

		return true, nil
	}

	shardLeaders, err = lb.GetShard(ctx, workload.Db, workload.CollectionName, workload.CollectionID, workload.Channel, true)
	if err != nil {
		log.Warn(ctx, "failed to get shard leaders", mlog.Err(err))
		return err
	}
	// Sweep all shard leaders once, then allow configured request-level retries after every leader returns a retriable error.
	//
	// Deliberately the UNSCOPED leader count. Under a scope the request may
	// have no leader to switch to, so each round is one forced cache refresh
	// plus one more try at the group's own leader -- a poll, not a sweep --
	// and the budget is what bounds how long that poll runs before the
	// retriable ErrCollectionNotFullyLoaded reaches the caller. Computing it
	// from the filtered list would shorten the poll for precisely the group
	// that needs it most.
	retryTimes := len(shardLeaders) + max(lb.retryOnReplica, 1)
	err = retry.Handle(ctx, tryExecute, retry.Attempts(uint(retryTimes)))
	if err != nil {
		log.Warn(ctx, "failed to execute",
			mlog.String("channel", workload.Channel),
			mlog.Err(err))
	}

	return err
}

// Execute will execute collection workload in parallel
func (lb *LBPolicyImpl) Execute(ctx context.Context, workload CollectionWorkLoad) error {
	// Extension seam, see extension_seam.go.
	workload = scopedCollectionWorkload(ctx, workload)
	log := mlog.With(
		mlog.Int64("collectionID", workload.CollectionID),
	)
	channelList, err := lb.GetShardLeaderList(ctx, workload.Db, workload.CollectionName, workload.CollectionID, true)
	if err != nil {
		log.Warn(ctx, "failed to get shards", mlog.Err(err))
		return err
	}

	if len(channelList) == 0 {
		log.Info(ctx, "no shard leaders found", mlog.Int64("collectionID", workload.CollectionID))
		return merr.WrapErrCollectionNotLoaded(workload.CollectionID)
	}

	// Single channel fast path: skip errgroup/goroutine overhead
	if len(channelList) == 1 {
		return lb.ExecuteWithRetry(ctx, workload.ForChannel(channelList[0], preferredNodeID(workload, channelList[0])))
	}

	wg, _ := errgroup.WithContext(ctx)
	for _, channel := range channelList {
		wg.Go(func() error {
			return lb.ExecuteWithRetry(ctx, workload.ForChannel(channel, preferredNodeID(workload, channel)))
		})
	}
	return wg.Wait()
}

// ExecuteOneChannel will execute at any one channel in collection
func (lb *LBPolicyImpl) ExecuteOneChannel(ctx context.Context, workload CollectionWorkLoad) error {
	// Extension seam, see extension_seam.go.
	workload = scopedCollectionWorkload(ctx, workload)
	// Unscoped: any one channel will do, so the first is taken. Scoped: the
	// channel list is a map's key order, so the first channel may be one the
	// group has no leader on; that refusal is the retriable
	// ErrCollectionNotFullyLoaded, and rather than hand it back when a
	// sibling channel could have served, move on to the next channel and end
	// on the refusal only if none can.
	//
	// Each channel that is tried and refused burns a full retry budget first
	// (~1.6s), so a group that can serve no shard of a wide collection would
	// take shards x budget to fail. A pre-pass keeps that bounded, and it is
	// made on FRESH data so that it is allowed to refuse: one uncached
	// GetShardLeaders is one coordinator call that refreshes every channel of
	// the collection at once (updateShardLocationCache replaces the whole
	// entry), so what it returns is authoritative rather than stale. The
	// scoped channel list is derived from that table alone -- the cached
	// channel list is read only on the unscoped branch, so the scoped path
	// pays for exactly one read and never mixes a stale key set with a fresh
	// leader table; sorted, so one state always tries one order. None
	// servable at all is refused right here, with the same retriable code
	// selectNode would reach after a full sweep -- in zero budgets instead of
	// shards x budget. The cost is one RPC on the scoped path (through the
	// same retry wrapper the other reads use), and one cache-metric hit
	// (caller="GetShardLeaders") rather than one per channel.
	var channelList []string
	if workload.ResourceGroup != "" {
		fresh, err := lb.GetShardLeaders(ctx, workload.Db, workload.CollectionName, workload.CollectionID, false)
		if err != nil {
			mlog.Warn(ctx, "failed to refresh shard leaders for the resource group pre-pass", mlog.Err(err))
			return err
		}
		servable := make([]string, 0, len(fresh))
		for channel, leaders := range fresh {
			if len(FilterByResourceGroup(leaders, workload.ResourceGroup)) > 0 {
				servable = append(servable, channel)
			}
		}
		sort.Strings(servable)
		if len(servable) == 0 {
			return merr.WrapErrCollectionNotFullyLoaded(workload.CollectionID,
				fmt.Sprintf("no shard leader in resource group %s", workload.ResourceGroup))
		}
		channelList = servable
	} else {
		var err error
		channelList, err = lb.GetShardLeaderList(ctx, workload.Db, workload.CollectionName, workload.CollectionID, true)
		if err != nil {
			mlog.Warn(ctx, "failed to get shards", mlog.Err(err))
			return err
		}
	}
	var lastErr error
	for _, channel := range channelList {
		err := lb.ExecuteWithRetry(ctx, workload.ForChannel(channel, preferredNodeID(workload, channel)))
		if workload.ResourceGroup == "" || !errors.Is(err, merr.ErrCollectionNotFullyLoaded) {
			return err
		}
		lastErr = err
	}
	if lastErr != nil {
		return lastErr
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
	lb.blacklist.Close()
}
