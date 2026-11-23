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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	hedgeTestChannel = "hedge-ch"
	hedgeTestClass   = "search/Bounded"
)

var hedgeTestKey = hedgeTestChannel + "|" + hedgeTestClass

type hedgeHarness struct {
	t        *testing.T
	policy   *LBPolicyImpl
	balancer *LatencyWeightedBalancer

	mu         sync.Mutex
	published  []int64
	callNodes  []int64
	superseded atomic.Int32
	canceled   atomic.Int32
	calls      atomic.Int32
	uncached   atomic.Int32
}

func newHedgeHarness(t *testing.T, nodes ...int64) *hedgeHarness {
	paramtable.Init()
	// time the tests against a short hedge delay
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.ReplicaSelectionPolicy.Key, LatencyWeighted)
	pt.Save(pt.ProxyCfg.HedgingEnabled.Key, "true")
	pt.Save(pt.ProxyCfg.HedgingMinDelay.Key, "20ms")
	t.Cleanup(func() {
		pt.Reset(pt.ProxyCfg.ReplicaSelectionPolicy.Key)
		pt.Reset(pt.ProxyCfg.HedgingEnabled.Key)
		pt.Reset(pt.ProxyCfg.HedgingMinDelay.Key)
	})

	h := &hedgeHarness{t: t}
	mgr := NewMockShardClientManager(t)
	infos := make([]NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		infos = append(infos, NodeInfo{NodeID: n, Address: "localhost", Serviceable: true})
	}
	mgr.EXPECT().GetShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, withCache bool, _ string, _ string, _ int64, _ string) {
			if !withCache {
				h.uncached.Inc()
			}
		}).Return(infos, nil).Maybe()
	mgr.EXPECT().GetShardLeaderList(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]string{hedgeTestChannel}, nil).Maybe()
	qn := mocks.NewMockQueryNodeClient(t)
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Maybe()

	h.policy = NewLBPolicyImpl(mgr)
	h.balancer = NewLatencyWeightedBalancer(mgr)
	h.policy.getBalancer = func() LBBalancer { return h.balancer }
	t.Cleanup(func() {
		assert.Eventually(t, func() bool { return h.inflight() == 0 }, 3*time.Second, time.Millisecond)
	})
	return h
}

// warm gives every node a 5ms history and the key 200 requests of 5ms, so the
// hedge delay is the minimum delay.
func (h *hedgeHarness) warm(nodes ...int64) {
	for i := 0; i < 200; i++ {
		for _, n := range nodes {
			h.balancer.ObserveAttempt(context.Background(), hedgeTestKey, n, false, time.Now().Add(-5*time.Millisecond), nil)
		}
		h.policy.hedging.ObserveRequest(hedgeTestKey, 5*time.Millisecond)
	}
}

func (h *hedgeHarness) exec(delay func(call int32, node int64) (time.Duration, error)) ExecuteFunc {
	return func(ctx context.Context, node UniqueID, qn types.QueryNodeClient, channel string) error {
		call := h.calls.Inc()
		h.mu.Lock()
		h.callNodes = append(h.callNodes, node)
		h.mu.Unlock()
		d, err := delay(call, node)
		select {
		case <-time.After(d):
		case <-ctx.Done():
			h.canceled.Inc()
			return ctx.Err()
		}
		if err != nil {
			return err
		}
		if !CommitAttempt(ctx) {
			h.superseded.Inc()
			return ErrAttemptSuperseded
		}
		h.mu.Lock()
		h.published = append(h.published, node)
		h.mu.Unlock()
		return nil
	}
}

// run executes one channel request; delay gives how long the n-th attempt
// (1-based) takes and what it returns.
func (h *hedgeHarness) run(delay func(call int32, node int64) (time.Duration, error)) (time.Duration, error) {
	return h.runCtx(context.Background(), delay)
}

func (h *hedgeHarness) runCtx(ctx context.Context, delay func(call int32, node int64) (time.Duration, error)) (time.Duration, error) {
	start := time.Now()
	err := h.policy.ExecuteWithRetry(ctx, ChannelWorkload{
		Channel:    hedgeTestChannel,
		Nq:         1,
		StatsClass: hedgeTestClass,
		Hedgeable:  true,
		Exec:       h.exec(delay),
	})
	return time.Since(start), err
}

func (h *hedgeHarness) publishedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.published)
}

func (h *hedgeHarness) firstNode() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.callNodes[0]
}

func (h *hedgeHarness) inflight() int {
	h.balancer.mu.Lock()
	defer h.balancer.mu.Unlock()
	return h.inflightLocked()
}

func (h *hedgeHarness) inflightLocked() int {
	n := 0
	for _, s := range h.balancer.replicas {
		n += s.inflight
	}
	return n
}

func TestCommitAttempt(t *testing.T) {
	assert.True(t, CommitAttempt(context.Background()))
	assert.True(t, CommitAttempt(context.Background()))
	ctx := context.WithValue(context.Background(), attemptGateKey{}, &attemptGate{})
	assert.True(t, CommitAttempt(ctx))
	assert.False(t, CommitAttempt(ctx))
}

// A slow attempt is overtaken by a hedge on the other replica: one result is
// published, the slow attempt runs to its end and is measured, and the
// request latency, not the slow attempt's, feeds the hedge delay.
func TestHedgeSlowPrimary(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	elapsed, err := h.run(func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 300 * time.Millisecond, nil
		}
		return 0, nil
	})
	require.NoError(t, err)
	assert.Less(t, elapsed, 200*time.Millisecond)
	assert.Equal(t, int32(2), h.calls.Load())
	assert.Eventually(t, func() bool { return h.superseded.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, 1, h.publishedCount())
	assert.Eventually(t, func() bool { return h.inflight() == 0 }, time.Second, 10*time.Millisecond)

	h.policy.hedging.mu.Lock()
	assert.Less(t, h.policy.hedging.keys[hedgeTestKey].requests.newest(0).latency, 200*time.Millisecond, "request latency")
	h.policy.hedging.mu.Unlock()
	h.balancer.mu.Lock()
	defer h.balancer.mu.Unlock()
	slow, _ := h.balancer.replicas[replicaKey{hedgeTestKey, h.callNodes[0]}].mean(time.Now(), testDecay)
	assert.Greater(t, slow, 5*time.Millisecond, "the loser's latency reached its replica")
}

// A hedge that wins while the first attempt still runs counts as a failure of
// the first attempt, at once: that attempt will not be heard of again.
func TestHedgeWinCountsOvertakenAttempt(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	_, err := h.run(func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return time.Second, nil
		}
		return 0, nil
	})
	require.NoError(t, err)
	h.balancer.mu.Lock()
	s := h.balancer.replicas[replicaKey{hedgeTestKey, h.callNodes[0]}]
	failures, penalty := s.failures, s.currentPenalty(time.Now(), testDecay)
	h.balancer.mu.Unlock()
	assert.Equal(t, 1, failures)
	assert.Equal(t, time.Duration(0), penalty, "one failure is not penalized")
	assert.Equal(t, int32(0), h.superseded.Load(), "the first attempt is still running")
}

// When the first attempt wins, the slower hedge does not count as failed.
func TestHedgeLossIsNotAFailure(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	_, err := h.run(func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 40 * time.Millisecond, nil
		}
		return time.Second, nil
	})
	require.NoError(t, err)
	h.balancer.mu.Lock()
	defer h.balancer.mu.Unlock()
	for _, n := range []int64{1, 2} {
		assert.Equal(t, 0, h.balancer.replicas[replicaKey{hedgeTestKey, n}].failures)
	}
}

// The losing attempt runs on the request's context: it is canceled when the
// request returns. Being overtaken counted as its failure; the cancellation
// is not counted again.
func TestHedgeLoserCanceledWithRequest(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	ctx, cancel := context.WithCancel(context.Background())
	_, err := h.runCtx(ctx, func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return time.Minute, nil
		}
		return 0, nil
	})
	require.NoError(t, err)
	cancel() // the request returned
	assert.Eventually(t, func() bool { return h.canceled.Load() == 1 && h.inflight() == 0 }, 2*time.Second, 10*time.Millisecond)
	h.balancer.mu.Lock()
	defer h.balancer.mu.Unlock()
	assert.Equal(t, 1, h.balancer.replicas[replicaKey{hedgeTestKey, h.callNodes[0]}].failures)
}

// A failure after being overtaken is the same attempt, not a second failure.
func TestHedgeLoserFailureCountedOnce(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	_, err := h.run(func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 100 * time.Millisecond, merr.ErrServiceUnavailable
		}
		return 0, nil
	})
	require.NoError(t, err)
	assert.Eventually(t, func() bool {
		h.balancer.mu.Lock()
		defer h.balancer.mu.Unlock()
		return h.balancer.replicas[replicaKey{hedgeTestKey, h.callNodes[0]}].failures == 1 && h.inflightLocked() == 0
	}, 2*time.Second, 10*time.Millisecond)
}

// Without request history the hedge delay is the minimum delay, so a rare
// request class is covered too.
func TestHedgeWithoutHistory(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	elapsed, err := h.run(func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 300 * time.Millisecond, nil
		}
		return 0, nil
	})
	require.NoError(t, err)
	assert.Less(t, elapsed, 200*time.Millisecond)
	assert.Equal(t, int32(2), h.calls.Load())
	assert.Eventually(t, func() bool { return h.superseded.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
}

// A single replica has nothing to hedge to, and finding out does not refresh
// the shard leaders.
func TestHedgeSingleReplica(t *testing.T) {
	h := newHedgeHarness(t, 1)
	h.warm(1)
	_, err := h.run(func(int32, int64) (time.Duration, error) { return 60 * time.Millisecond, nil })
	require.NoError(t, err)
	assert.Equal(t, int32(1), h.calls.Load())
	assert.Equal(t, int32(0), h.uncached.Load())
	assert.Equal(t, 0, h.inflight())
}

// When the latest hedges lost, as when every replica is slow under overall
// overload, hedges are cut to the probe rate.
func TestHedgeSkippedWhenHedgesLose(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	for i := 0; i < 20; i++ {
		h.policy.hedging.ObserveHedge(hedgeTestKey, false)
	}
	// the draws of this seed are above the probe rate
	h.policy.hedging.rng = rand.New(rand.NewSource(1))
	_, err := h.run(func(int32, int64) (time.Duration, error) { return 60 * time.Millisecond, nil })
	require.NoError(t, err)
	assert.Equal(t, int32(1), h.calls.Load())
	assert.Equal(t, 0, h.inflight())
}

// Every sent hedge reports whether it won.
func TestHedgeOutcomeReported(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	// the hedge wins
	_, err := h.run(func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 300 * time.Millisecond, nil
		}
		return 0, nil
	})
	require.NoError(t, err)
	// the first attempt wins after the hedge was sent
	h.calls.Store(0)
	_, err = h.run(func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 40 * time.Millisecond, nil
		}
		return 200 * time.Millisecond, nil
	})
	require.NoError(t, err)
	h.policy.hedging.mu.Lock()
	defer h.policy.hedging.mu.Unlock()
	hedges := h.policy.hedging.keys[hedgeTestKey].hedges
	require.Equal(t, 2, hedges.count)
	assert.True(t, hedges.ring[0].won)
	assert.False(t, hedges.ring[1].won)
}

// A fast failure goes back to the retry loop, on the other replica.
func TestHedgePrimaryFailsFast(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	_, err := h.run(func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 0, merr.ErrServiceUnavailable
		}
		return 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), h.calls.Load())
	assert.Equal(t, 1, h.publishedCount())
	h.mu.Lock()
	assert.NotEqual(t, h.callNodes[0], h.callNodes[1])
	h.mu.Unlock()
}

// When every attempt fails, the request fails.
func TestHedgeEveryAttemptFails(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	_, err := h.run(func(int32, int64) (time.Duration, error) {
		return 30 * time.Millisecond, merr.ErrServiceUnavailable
	})
	assert.Error(t, err)
	assert.Equal(t, 0, h.publishedCount())
}

// An input error aborts the request.
func TestHedgeInputError(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	_, err := h.run(func(int32, int64) (time.Duration, error) {
		return 0, merr.WrapErrParameterInvalidMsg("bad")
	})
	assert.Error(t, err)
	assert.Equal(t, int32(1), h.calls.Load())
}

// The switch turns hedging off.
func TestHedgeDisabled(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.HedgingEnabled.Key, "false")
	defer pt.Reset(pt.ProxyCfg.HedgingEnabled.Key)
	h.warm(1, 2)
	_, err := h.run(func(int32, int64) (time.Duration, error) { return 60 * time.Millisecond, nil })
	require.NoError(t, err)
	assert.Equal(t, int32(1), h.calls.Load())
	// plain attempts still feed the statistics
	h.policy.hedging.mu.Lock()
	defer h.policy.hedging.mu.Unlock()
	assert.Len(t, h.policy.hedging.keys[hedgeTestKey].requests.ring, 201)
}

// The request class reaches the statistics through Execute.
func TestExecutePassesStatsClass(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	err := h.policy.Execute(context.Background(), CollectionWorkLoad{
		Nq:         1,
		StatsClass: hedgeTestClass,
		Hedgeable:  true,
		Exec:       h.exec(func(int32, int64) (time.Duration, error) { return 0, nil }),
	})
	require.NoError(t, err)
	h.policy.hedging.mu.Lock()
	defer h.policy.hedging.mu.Unlock()
	_, ok := h.policy.hedging.keys[hedgeTestKey]
	assert.True(t, ok)
}

// overloadErr is the refusal of an overloaded replica as the Proxy gets it:
// through a response status.
func overloadErr() error {
	return merr.Error(merr.Status(merr.WrapErrTooManyRequests(200, "read task queue")))
}

func (h *hedgeHarness) failures(node int64) int {
	h.balancer.mu.Lock()
	defer h.balancer.mu.Unlock()
	return h.balancer.replicas[replicaKey{hedgeTestKey, node}].failures
}

// request runs one channel request that may get an extra attempt, or not.
func (h *hedgeHarness) request(extra bool, delay func(call int32, node int64) (time.Duration, error)) error {
	return h.policy.ExecuteWithRetry(context.Background(), ChannelWorkload{
		Channel: hedgeTestChannel, Nq: 1, StatsClass: hedgeTestClass, Hedgeable: extra, Exec: h.exec(delay),
	})
}

func (h *hedgeHarness) nodes() []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int64(nil), h.callNodes...)
}

// A request that may get an extra attempt and is refused by its first replica
// as overloaded goes to another replica at once. The refusal does not count
// against the replica.
func TestOverloadGetsOneExtraAttempt(t *testing.T) {
	h := newHedgeHarness(t, 1, 2, 3)
	h.warm(1, 2, 3)
	start := time.Now()
	err := h.request(true, func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 0, overloadErr()
		}
		return 0, nil
	})
	require.NoError(t, err)
	assert.Less(t, time.Since(start), time.Second)
	nodes := h.nodes()
	require.Len(t, nodes, 2)
	assert.NotEqual(t, nodes[0], nodes[1])
	assert.Equal(t, 0, h.failures(nodes[0]))
}

// When the extra attempt is refused too, the request ends with the refusal:
// it is not tried on a third replica, and the shard leaders are not refreshed
// to go around again.
func TestOverloadTwiceEndsRequest(t *testing.T) {
	h := newHedgeHarness(t, 1, 2, 3)
	h.warm(1, 2, 3)
	err := h.request(true, func(int32, int64) (time.Duration, error) { return 0, overloadErr() })
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	nodes := h.nodes()
	require.Len(t, nodes, 2)
	assert.NotEqual(t, nodes[0], nodes[1])
	assert.Zero(t, h.uncached.Load())
	assert.Equal(t, 0, h.failures(nodes[0]))
	assert.Equal(t, 0, h.failures(nodes[1]))
}

// A request that may not get an extra attempt ends with the first refusal.
func TestOverloadWithoutExtraAttemptEndsRequest(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	err := h.request(false, func(int32, int64) (time.Duration, error) { return 0, overloadErr() })
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.Len(t, h.nodes(), 1)
	assert.Zero(t, h.uncached.Load())
}

// With a single replica there is no other replica for the extra attempt.
func TestOverloadOnSingleReplicaEndsRequest(t *testing.T) {
	h := newHedgeHarness(t, 1)
	h.warm(1)
	err := h.request(true, func(int32, int64) (time.Duration, error) { return 0, overloadErr() })
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.Len(t, h.nodes(), 1)
	assert.Zero(t, h.uncached.Load())
}

// A hedge is the extra attempt: when the first replica is slow and both
// attempts are refused in the end, the request ends, with a third replica
// left untried.
func TestOverloadAfterHedgeEndsRequest(t *testing.T) {
	h := newHedgeHarness(t, 1, 2, 3)
	h.warm(1, 2, 3)
	err := h.request(true, func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 80 * time.Millisecond, overloadErr()
		}
		return 100 * time.Millisecond, overloadErr()
	})
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.Len(t, h.nodes(), 2)
}

// A refused hedge leaves the first attempt to answer.
func TestOverloadedHedgeLeavesPrimary(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.warm(1, 2)
	_, err := h.run(func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 80 * time.Millisecond, nil
		}
		return 0, overloadErr()
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), h.calls.Load())
	assert.Equal(t, 1, h.publishedCount())
}

// A replica that is down does not use up the extra attempt: the request moves
// on as it did before, and a refusal after that still gets its extra attempt.
func TestFailedReplicaDoesNotUseTheExtraAttempt(t *testing.T) {
	h := newHedgeHarness(t, 1, 2, 3)
	h.warm(1, 2, 3)
	err := h.request(true, func(call int32, _ int64) (time.Duration, error) {
		switch call {
		case 1:
			return 0, merr.ErrServiceUnavailable
		case 2:
			return 0, overloadErr()
		}
		return 0, nil
	})
	require.NoError(t, err)
	assert.Len(t, h.nodes(), 3)
}

// Without an extra attempt, a replica that is down and one that is overloaded
// end the request as overloaded.
func TestOverloadAfterFailedReplicaEndsRequest(t *testing.T) {
	h := newHedgeHarness(t, 1, 2, 3)
	h.warm(1, 2, 3)
	err := h.request(false, func(call int32, _ int64) (time.Duration, error) {
		if call == 1 {
			return 0, merr.ErrServiceUnavailable
		}
		return 0, overloadErr()
	})
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.Len(t, h.nodes(), 2)
}

// A request fails with its first failed channel, and stops the work for its
// other channels then.
func TestExecuteCancelsOtherChannelsOnFailure(t *testing.T) {
	paramtable.Init()
	mgr := NewMockShardClientManager(t)
	mgr.EXPECT().GetShardLeaderList(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"ch-a", "ch-b"}, nil)
	mgr.EXPECT().GetShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]NodeInfo{{NodeID: 1, Address: "localhost", Serviceable: true}}, nil).Maybe()
	mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(mocks.NewMockQueryNodeClient(t), nil).Maybe()
	policy := NewLBPolicyImpl(mgr)
	balancer := NewLatencyWeightedBalancer(mgr)
	policy.getBalancer = func() LBBalancer { return balancer }

	canceled := atomic.NewBool(false)
	start := time.Now()
	err := policy.Execute(context.Background(), CollectionWorkLoad{
		Nq: 1,
		Exec: func(ctx context.Context, _ UniqueID, _ types.QueryNodeClient, channel string) error {
			if channel == "ch-a" {
				return overloadErr()
			}
			select {
			case <-ctx.Done():
				canceled.Store(true)
				return ctx.Err()
			case <-time.After(10 * time.Second):
				return nil
			}
		},
	})
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.True(t, canceled.Load())
	assert.Less(t, time.Since(start), 5*time.Second)
}

// The handling of an overloaded replica does not depend on the replica
// selection policy: the extra attempt goes to the next replica at once, also
// when less than the retry interval of the policy is left of the request,
// and the request ends when that attempt is refused too.
func TestOverloadUnderEveryPolicy(t *testing.T) {
	paramtable.Init()
	for name, newBalancer := range map[string]func(ShardClientMgr) LBBalancer{
		"round_robin": func(ShardClientMgr) LBBalancer { return NewRoundRobinBalancer() },
		"look_aside":  func(mgr ShardClientMgr) LBBalancer { return NewLookAsideBalancer(mgr) },
	} {
		t.Run(name, func(t *testing.T) {
			var uncached atomic.Int32
			mgr := NewMockShardClientManager(t)
			mgr.EXPECT().GetShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Run(func(_ context.Context, withCache bool, _ string, _ string, _ int64, _ string) {
					if !withCache {
						uncached.Inc()
					}
				}).Return([]NodeInfo{
				{NodeID: 1, Address: "localhost", Serviceable: true},
				{NodeID: 2, Address: "localhost", Serviceable: true},
				{NodeID: 3, Address: "localhost", Serviceable: true},
			}, nil).Maybe()
			mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(mocks.NewMockQueryNodeClient(t), nil).Maybe()
			policy := NewLBPolicyImpl(mgr)
			balancer := newBalancer(mgr)
			policy.getBalancer = func() LBBalancer { return balancer }

			run := func(extra bool, refusals int) ([]int64, error) {
				var nodes []int64
				// less than the 200ms the policy waits before a retry
				ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
				defer cancel()
				err := policy.ExecuteWithRetry(ctx, ChannelWorkload{
					Channel: hedgeTestChannel, Nq: 1, Hedgeable: extra,
					Exec: func(_ context.Context, node UniqueID, _ types.QueryNodeClient, _ string) error {
						nodes = append(nodes, node)
						if len(nodes) <= refusals {
							return overloadErr()
						}
						return nil
					},
				})
				return nodes, err
			}

			nodes, err := run(true, 1)
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			assert.NotEqual(t, nodes[0], nodes[1])

			nodes, err = run(true, 3)
			assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
			assert.Len(t, nodes, 2)

			nodes, err = run(false, 3)
			assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
			assert.Len(t, nodes, 1)
			assert.Zero(t, uncached.Load())
		})
	}
}
