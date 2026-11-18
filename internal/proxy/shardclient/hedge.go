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
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ErrAttemptSuperseded is returned by an ExecuteFunc whose attempt succeeded
// after another attempt of the same channel had already published its result.
// Its result has been discarded.
var ErrAttemptSuperseded = errors.New("attempt superseded by another attempt of the same channel")

// decisions of a hedge, as metric labels
const (
	hedgeSent              = "sent"
	hedgeWon               = "won"
	hedgeSkippedLowWinRate = "skipped_low_win_rate"
	hedgeSkippedNoReplica  = "skipped_no_replica"
)

func recordHedge(decision string) {
	metrics.ProxyReplicaHedgeCount.WithLabelValues(paramtable.GetStringNodeID(), decision).Inc()
}

type attemptGateKey struct{}

// attemptGate lets exactly one of the concurrent attempts of a channel publish
// its result.
type attemptGate struct {
	committed atomic.Bool
}

// CommitAttempt must be called by a hedgeable ExecuteFunc right before it
// publishes the result of an attempt. It returns false when another attempt
// of the same channel has already published; the ExecuteFunc must then drop
// its result and return ErrAttemptSuperseded. Outside of hedged execution it
// always returns true.
func CommitAttempt(ctx context.Context) bool {
	gate, ok := ctx.Value(attemptGateKey{}).(*attemptGate)
	if !ok {
		return true
	}
	return gate.committed.CompareAndSwap(false, true)
}

type hedgeResult struct {
	node int64
	err  error
}

// hasOtherReplica tells, from the cached shard leaders, whether a replica
// other than primary and the excluded ones serves the channel.
func (lb *LBPolicyImpl) hasOtherReplica(ctx context.Context, workload ChannelWorkload, primary int64, excludeNodes *typeutil.UniqueSet) bool {
	leaders, err := lb.clientMgr.GetShard(ctx, true, workload.Db, workload.CollectionName, workload.CollectionID, workload.Channel)
	if err != nil {
		return false
	}
	for _, l := range leaders {
		if l.NodeID != primary && !excludeNodes.Contain(l.NodeID) {
			return true
		}
	}
	return false
}

// executeHedged runs an attempt of the channel on primary, selected at
// primaryStart, and, if it has not returned within delay, a second one on
// another replica, unless the latest hedges of the key mostly lost (see
// LatencyWeightedBalancer.AllowHedge). The first attempt to succeed publishes
// its result (see CommitAttempt). Both attempts run on the request's context:
// they keep its deadline, and the losing one is canceled when the request
// returns. There is at most one hedge per call. It returns like a
// retry.Handle function, whether to keep retrying and the error, and whether
// a hedge was sent: the request gets one extra attempt at most, see
// ChannelWorkload.Hedgeable.
func (lb *LBPolicyImpl) executeHedged(ctx context.Context, workload ChannelWorkload, balancer *LatencyWeightedBalancer,
	primary NodeInfo, primarySelected bool, primaryStart time.Time, delay time.Duration, excludeNodes *typeutil.UniqueSet,
) (retry bool, hedged bool, err error) {
	// With drops the rate group, so it goes last.
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", workload.CollectionID),
		zap.String("channelName", workload.Channel),
	).WithRateGroup("proxy.hedge", 1, 60)
	key := statsKey(workload)
	gate := &attemptGate{}
	results := make(chan hedgeResult, 2)
	launch := func(node NodeInfo, selected bool, start time.Time) {
		go func() {
			attemptCtx := context.WithValue(ctx, attemptGateKey{}, gate)
			client, err := lb.clientMgr.GetClient(attemptCtx, node)
			if err == nil {
				err = workload.Exec(attemptCtx, node.NodeID, client, workload.Channel)
			}
			balancer.ObserveAttempt(attemptCtx, key, node.NodeID, selected, start, err)
			results <- hedgeResult{node: node.NodeID, err: err}
		}()
	}

	launch(primary, primarySelected, primaryStart)
	pending := 1
	timer := time.NewTimer(max(delay-time.Since(primaryStart), 0))
	defer timer.Stop()
	hedgeTimer := timer.C
	hedgeNode := int64(-1) // the node of the hedge, once one was sent
	// settle reports the outcome of the hedge, if one was sent.
	settle := func(won bool) {
		if hedgeNode >= 0 {
			balancer.ObserveHedge(key, won)
		}
	}

	var lastErr error
	// overloadErr is the refusal of an overloaded replica, when one of the
	// attempts met one. The round reports it rather than what another attempt
	// failed with: the retry loop ends the request on it once no replica is
	// left.
	var overloadErr error
	for {
		select {
		case <-ctx.Done():
			settle(false)
			if lastErr != nil {
				return false, hedgeNode >= 0, lastErr
			}
			return false, hedgeNode >= 0, ctx.Err()

		case <-hedgeTimer:
			hedgeTimer = nil
			node, selected, start, decision := lb.selectHedgeNode(ctx, workload, balancer, primary.NodeID, excludeNodes)
			recordHedge(decision)
			if decision != hedgeSent {
				continue
			}
			log.RatedInfo(5, "attempt is slow, sending a hedged attempt to another replica",
				zap.Int64("primary", primary.NodeID),
				zap.Int64("hedge", node.NodeID),
				zap.Duration("delay", delay))
			hedgeNode = node.NodeID
			launch(node, selected, start)
			pending++

		case r := <-results:
			pending--
			if r.err == nil {
				won := r.node == hedgeNode
				if won {
					recordHedge(hedgeWon)
					if pending > 0 {
						// the first attempt has not returned: its replica is slower
						// than this one for the same request
						balancer.ObserveOvertaken(ctx, key, primary.NodeID, primaryStart)
					}
				}
				settle(won)
				balancer.ObserveRequest(key, time.Since(primaryStart))
				return true, hedgeNode >= 0, nil
			}
			if errors.Is(r.err, ErrAttemptSuperseded) {
				// the other attempt committed and is about to report success
				continue
			}
			log.Warn("search/query channel failed",
				zap.Int64("nodeID", r.node),
				zap.Bool("hedge", r.node == hedgeNode),
				zap.Error(r.err))
			if merr.GetErrorType(r.err) == merr.InputError {
				return false, hedgeNode >= 0, r.err
			}
			excludeNodes.Insert(r.node)
			lastErr = errors.Wrapf(r.err, "failed to search/query delegator %d for channel %s", r.node, workload.Channel)
			if isOverloadErr(r.err) {
				overloadErr = lastErr
			}
			if pending == 0 {
				settle(false)
				if overloadErr != nil {
					return true, hedgeNode >= 0, overloadErr
				}
				return true, hedgeNode >= 0, lastErr
			}
		}
	}
}

// selectHedgeNode picks the replica for a hedge of an attempt running on
// primary. The decision is hedgeSent when a hedge is to be sent to the
// returned node, selected at the returned time; otherwise it tells why not.
func (lb *LBPolicyImpl) selectHedgeNode(ctx context.Context, workload ChannelWorkload, balancer *LatencyWeightedBalancer,
	primary int64, excludeNodes *typeutil.UniqueSet,
) (NodeInfo, bool, time.Time, string) {
	// Checked on the cached shard leaders, so that a request of a single
	// replica collection does not refresh them each time it could hedge.
	if !lb.hasOtherReplica(ctx, workload, primary, excludeNodes) {
		return NodeInfo{}, false, time.Time{}, hedgeSkippedNoReplica
	}
	key := statsKey(workload)
	if !balancer.AllowHedge(key) {
		return NodeInfo{}, false, time.Time{}, hedgeSkippedLowWinRate
	}
	candidates := typeutil.NewUniqueSet(excludeNodes.Collect()...)
	candidates.Insert(primary)
	start := time.Now()
	node, selected, err := lb.selectNode(ctx, balancer, workload, &candidates)
	if err != nil {
		return NodeInfo{}, false, time.Time{}, hedgeSkippedNoReplica
	}
	if node.NodeID == primary {
		if selected {
			balancer.ReleaseSelection(key, node.NodeID)
		}
		return NodeInfo{}, false, time.Time{}, hedgeSkippedNoReplica
	}
	return node, selected, start, hedgeSent
}
