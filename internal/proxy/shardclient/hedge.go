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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ErrAttemptSuperseded is returned by an ExecuteFunc whose attempt succeeded
// after another attempt of the same channel had already published its result.
// Its result has been discarded.
var ErrAttemptSuperseded = errors.New("attempt superseded by another attempt of the same channel")

// decisions of a hedge, as metric labels
const (
	hedgeSent                 = "sent"
	hedgeWon                  = "won"
	hedgeSkippedLowWinRate    = "skipped_low_win_rate"
	hedgeSkippedNoReplica     = "skipped_no_replica"
	hedgeSkippedInflightLimit = "skipped_inflight_limit"
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
	node         int64
	err          error
	getClientErr bool
}

// attemptFeedback serializes the two observations of a primary: its actual
// completion and an earlier hedge win. A single attempt can count as failed
// only once; a late success still contributes latency and resets failures.
type attemptFeedback struct {
	mu              sync.Mutex
	completed       bool
	failureRecorded bool
	balancer        *LatencyWeightedBalancer
	ctx             context.Context
	key             string
	node            int64
	selected        bool
	start           time.Time
}

func (a *attemptFeedback) complete(err error) {
	if a.balancer == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.completed = true
	now := a.balancer.now()
	outcome, latency := classifyAttempt(a.ctx, a.start, now, err)
	if outcome == attemptFailed && a.failureRecorded {
		outcome = attemptIgnored
	}
	a.balancer.observeAttempt(a.ctx, a.key, a.node, a.selected, now, outcome, latency, err)
}

func (a *attemptFeedback) overtaken() {
	if a.balancer == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.completed || a.failureRecorded || errors.Is(a.ctx.Err(), context.Canceled) {
		return
	}
	a.failureRecorded = true
	a.balancer.ObserveOvertaken(a.ctx, a.key, a.node, a.start)
}

type hedgeLookupResult struct {
	nodes     []NodeInfo
	err       error
	refreshed bool
}

// executeHedged runs at most one hedge beside the primary. Dispatched attempts
// keep the outer request context: canceling one earlier can affect other reads
// merged into its QueryNode task. Only the candidate lookup has a round-local
// context, so it cannot delay a primary result or outlive a completed round.
func (lb *LBPolicyImpl) executeHedged(ctx context.Context, workload ChannelWorkload, balancer LBBalancer, lw *LatencyWeightedBalancer,
	primary NodeInfo, primarySelected bool, primaryStart time.Time, delay time.Duration, requestExcludedNodes, excludeNodes *typeutil.UniqueSet,
) (bool, bool, error) {
	log := mlog.With(mlog.FieldCollectionID(workload.CollectionID), mlog.String("channelName", workload.Channel))
	key := statsKey(workload)
	hedgeKey := hedgeStatsKey(workload)
	gate := &attemptGate{}
	results := make(chan hedgeResult, 2)
	launch := func(node NodeInfo, selected bool, start time.Time, release func()) *attemptFeedback {
		attemptCtx := context.WithValue(ctx, attemptGateKey{}, gate)
		feedback := &attemptFeedback{balancer: lw, ctx: attemptCtx, key: key, node: node.NodeID, selected: selected, start: start}
		go func() {
			defer release()
			if lw == nil && selected {
				defer balancer.CancelWorkload(node.NodeID, workload.Nq)
			}
			client, err := lb.clientMgr.GetClient(attemptCtx, node)
			getClientErr := err != nil
			if err == nil {
				err = workload.Exec(attemptCtx, node.NodeID, client, workload.Channel)
			}
			feedback.complete(err)
			results <- hedgeResult{node: node.NodeID, err: err, getClientErr: getClientErr}
		}()
		return feedback
	}
	primaryFeedback := launch(primary, primarySelected, primaryStart, lb.hedging.EnterPrimary())
	pending := 1
	timer := time.NewTimer(max(delay-time.Since(primaryStart), 0))
	defer timer.Stop()
	hedgeTimer := timer.C
	hedgeNode := int64(-1)
	settle := func(won bool) {
		if hedgeNode >= 0 {
			lb.hedging.ObserveHedge(hedgeKey, won)
		}
	}

	lookupCtx, cancelLookup := context.WithCancel(ctx)
	defer cancelLookup()
	lookupResults := make(chan hedgeLookupResult, 1)
	lookup := func(refresh bool) {
		go func() {
			nodes, err := lb.GetShard(lookupCtx, workload.Db, workload.CollectionName, workload.CollectionID, workload.Channel, !refresh)
			select {
			case lookupResults <- hedgeLookupResult{nodes: nodes, err: err, refreshed: refresh}:
			case <-lookupCtx.Done():
			}
		}()
	}
	var lastErr error
	var inputErr error
	for {
		select {
		case <-ctx.Done():
			settle(false)
			if inputErr != nil {
				return false, hedgeNode >= 0, inputErr
			}
			if lastErr != nil {
				return false, hedgeNode >= 0, lastErr
			}
			return false, hedgeNode >= 0, ctx.Err()
		case <-hedgeTimer:
			hedgeTimer = nil
			if gate.committed.Load() {
				continue
			}
			if !lb.hedging.CanAcquireHedge() {
				recordHedge(hedgeSkippedInflightLimit)
				continue
			}
			if !lb.hedging.AllowHedge(hedgeKey) {
				recordHedge(hedgeSkippedLowWinRate)
				continue
			}
			lookup(false)
		case candidates := <-lookupResults:
			if gate.committed.Load() || ctx.Err() != nil {
				continue
			}
			if candidates.err != nil {
				recordHedge(hedgeSkippedNoReplica)
				continue
			}
			// The lookup goroutine never reads this mutable exclusion set and never
			// reserves workload. Exclusions are checked against the latest round state.
			excluded := typeutil.NewUniqueSet(excludeNodes.Collect()...)
			excluded.Insert(primary.NodeID)
			hasAlternative := false
			candidates.nodes = FilterByResourceGroup(candidates.nodes, workload.ResourceGroup)
			for _, node := range candidates.nodes {
				if !excluded.Contain(node.NodeID) {
					hasAlternative = true
					break
				}
			}
			if !hasAlternative {
				recordHedge(hedgeSkippedNoReplica)
				continue
			}
			start := time.Now()
			node, selected, err := lb.selectCandidate(ctx, balancer, lw, workload, candidates.nodes, &excluded)
			if err != nil {
				if !candidates.refreshed {
					lookup(true)
				} else {
					recordHedge(hedgeSkippedNoReplica)
				}
				continue
			}
			releaseHedge, ok := lb.hedging.TryAcquireHedge()
			if !ok {
				if lw != nil && selected {
					lw.ReleaseSelection(key, node.NodeID)
				} else if lw == nil && selected {
					balancer.CancelWorkload(node.NodeID, workload.Nq)
				}
				recordHedge(hedgeSkippedInflightLimit)
				continue
			}
			recordHedge(hedgeSent)
			log.RatedInfo(ctx, 5, "attempt is slow, sending a hedged attempt to another replica",
				mlog.Int64("primary", primary.NodeID), mlog.Int64("hedge", node.NodeID), mlog.Duration("delay", delay))
			hedgeNode = node.NodeID
			launch(node, selected, start, releaseHedge)
			pending++
		case r := <-results:
			pending--
			if r.err == nil {
				won := r.node == hedgeNode
				if won {
					recordHedge(hedgeWon)
					primaryFeedback.overtaken()
				}
				settle(won)
				lb.hedging.ObserveRequest(hedgeKey, time.Since(primaryStart))
				return true, hedgeNode >= 0, nil
			}
			if errors.Is(r.err, ErrAttemptSuperseded) {
				continue
			}
			log.Warn(ctx, "search/query channel failed", mlog.FieldNodeID(r.node), mlog.Bool("hedge", r.node == hedgeNode), mlog.Err(r.err))
			if merr.GetErrorType(r.err) == merr.InputError {
				// Stop retrying, but allow an already dispatched sibling to succeed.
				if inputErr == nil {
					inputErr = r.err
				}
			} else {
				excludeNodes.Insert(r.node)
				if lw == nil && !errors.Is(ctx.Err(), context.Canceled) {
					if r.getClientErr || !merr.IsRetryableErr(r.err) {
						lb.blacklist.Add(workload.Channel, r.node)
					} else if requestExcludedNodes != nil {
						requestExcludedNodes.Insert(r.node)
					}
				}
				lastErr = merr.Wrapf(r.err, "failed to search/query delegator %d for channel %s", r.node, workload.Channel)
			}
			if pending == 0 {
				settle(false)
				if inputErr != nil {
					return false, hedgeNode >= 0, inputErr
				}
				return true, hedgeNode >= 0, lastErr
			}
		}
	}
}
