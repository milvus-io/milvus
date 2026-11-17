// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"context"
	"strconv"
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
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestAttemptFeedbackCompletesBeforeOvertake(t *testing.T) {
	for _, result := range []error{nil, ErrAttemptSuperseded, merr.ErrServiceUnavailable} {
		b, clock := newTestBalancer(t)
		node, err := b.SelectNodeForKey(context.Background(), testKey, []int64{1})
		require.NoError(t, err)
		a := &attemptFeedback{balancer: b, ctx: context.Background(), key: testKey, node: node, selected: true, start: clock.ago(time.Millisecond)}
		a.complete(result)
		// The result may still be queued when the hedge success is consumed.
		a.overtaken()
		stats := b.replicas[replicaKey{testKey, node}]
		expected := 0
		if result == merr.ErrServiceUnavailable {
			expected = 1
		}
		assert.Equal(t, expected, stats.failures)
		assert.Zero(t, stats.inflight)
	}
}

func TestAttemptFeedbackOvertakeThenComplete(t *testing.T) {
	for _, result := range []error{nil, ErrAttemptSuperseded, merr.ErrServiceUnavailable, context.Canceled} {
		b, clock := newTestBalancer(t)
		ctx, cancel := context.WithCancel(context.Background())
		node, err := b.SelectNodeForKey(ctx, testKey, []int64{1})
		require.NoError(t, err)
		a := &attemptFeedback{balancer: b, ctx: ctx, key: testKey, node: node, selected: true, start: clock.ago(time.Millisecond)}
		a.overtaken()
		require.Equal(t, 1, b.replicas[replicaKey{testKey, node}].failures)
		clock.advance(time.Millisecond)
		if result == context.Canceled {
			cancel()
		}
		a.complete(result)
		cancel()
		stats := b.replicas[replicaKey{testKey, node}]
		expected := 1
		if result == nil || result == ErrAttemptSuperseded {
			expected = 0
			assert.Positive(t, stats.count)
		}
		assert.Equal(t, expected, stats.failures)
		assert.Zero(t, stats.inflight)
	}
}

func TestAttemptFeedbackContextDoneBeforeOvertake(t *testing.T) {
	for _, canceled := range []bool{true, false} {
		t.Run(strconv.FormatBool(canceled), func(t *testing.T) {
			b, clock := newTestBalancer(t)
			var ctx context.Context
			var cancel context.CancelFunc
			if canceled {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			} else {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			}
			defer cancel()
			node, err := b.SelectNodeForKey(ctx, testKey, []int64{1})
			require.NoError(t, err)
			a := &attemptFeedback{balancer: b, ctx: ctx, key: testKey, node: node, selected: true, start: clock.ago(time.Millisecond)}
			// The result loop can consume a queued hedge success while ctx.Done
			// is also ready. Cancellation is not replica failure evidence;
			// deadline expiry retains the existing failure classification.
			a.overtaken()
			stats := b.replicas[replicaKey{testKey, node}]
			expected := 1
			if canceled {
				expected = 0
			}
			require.Equal(t, expected, stats.failures)
			require.Equal(t, !canceled, a.failureRecorded)
			require.Equal(t, 1, stats.inflight)
			a.complete(ctx.Err())
			require.Equal(t, expected, stats.failures)
			require.Zero(t, stats.inflight)
		})
	}
}

func TestAttemptFeedbackConcurrentCompletionAndOvertake(t *testing.T) {
	b, clock := newTestBalancer(t)
	for i := 0; i < 100; i++ {
		node, err := b.SelectNodeForKey(context.Background(), testKey, []int64{1})
		require.NoError(t, err)
		feedback := &attemptFeedback{balancer: b, ctx: context.Background(), key: testKey, node: node, selected: true, start: clock.ago(time.Millisecond)}
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() { defer workers.Done(); <-start; feedback.complete(merr.ErrServiceUnavailable) }()
		go func() { defer workers.Done(); <-start; feedback.overtaken() }()
		close(start)
		workers.Wait()
		stats := b.replicas[replicaKey{testKey, node}]
		require.Equal(t, i+1, stats.failures)
		require.Zero(t, stats.inflight)
	}
}

// A coordinator lookup must neither delay completion of the primary nor leave
// a later hedge/workload reservation behind when the round has already ended.
func TestHedgeLookupDoesNotBlockPrimary(t *testing.T) {
	for _, primaryErr := range []error{nil, merr.ErrServiceUnavailable} {
		t.Run(strconv.FormatBool(primaryErr == nil), func(t *testing.T) {
			paramtable.Init()
			pt := paramtable.Get()
			prevEnabled := pt.ProxyCfg.HedgingEnabled.GetValue()
			_ = pt.Save(pt.ProxyCfg.HedgingEnabled.Key, "true")
			t.Cleanup(func() { _ = pt.Save(pt.ProxyCfg.HedgingEnabled.Key, prevEnabled) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lookupStarted := make(chan struct{})
			lookupStopped := make(chan struct{})
			releasePrimary := make(chan struct{})
			mgr := NewMockShardClientManager(t)
			qn := mocks.NewMockQueryNodeClient(t)
			mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Once()
			mgr.EXPECT().GetShard(mock.Anything, true, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, _ bool, _, _ string, _ int64, _ string) ([]NodeInfo, error) {
					close(lookupStarted)
					<-ctx.Done()
					close(lookupStopped)
					// Even a client that returns a late snapshot after cancellation cannot
					// cause the ended round to reserve workload or dispatch a hedge.
					return []NodeInfo{{NodeID: 1, Serviceable: true}, {NodeID: 2, Serviceable: true}}, nil
				}).Once()
			lb := NewLBPolicyImpl(mgr)
			b := NewLatencyWeightedBalancer(mgr)
			calls := atomic.NewInt32(0)
			workload := ChannelWorkload{Channel: hedgeTestChannel, StatsClass: hedgeTestClass, Hedgeable: true, Exec: func(ctx context.Context, _ int64, _ types.QueryNodeClient, _ string) error {
				calls.Inc()
				select {
				case <-releasePrimary:
				case <-ctx.Done():
					return ctx.Err()
				}
				if primaryErr != nil {
					return primaryErr
				}
				if !CommitAttempt(ctx) {
					return ErrAttemptSuperseded
				}
				return nil
			}}
			done := make(chan error, 1)
			go func() {
				excluded := typeutil.NewUniqueSet()
				_, _, err := lb.executeHedged(ctx, workload, b, b, NodeInfo{NodeID: 1, Serviceable: true}, false, time.Now(), 0, &excluded)
				done <- err
			}()
			select {
			case <-lookupStarted:
			case <-time.After(time.Second):
				t.Fatal("lookup did not start")
			}
			close(releasePrimary)
			select {
			case err := <-done:
				if primaryErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, primaryErr)
				}
			case <-time.After(time.Second):
				t.Fatal("primary result blocked by lookup")
			}
			select {
			case <-lookupStopped:
			case <-time.After(time.Second):
				t.Fatal("lookup not canceled at round end")
			}
			assert.Equal(t, int32(1), calls.Load())
			b.mu.Lock()
			defer b.mu.Unlock()
			for _, s := range b.replicas {
				assert.Zero(t, s.inflight)
			}
		})
	}
}

func TestLatencyWeightedAccountingAcrossSwitch(t *testing.T) {
	for _, initiallyEnabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(initiallyEnabled), func(t *testing.T) {
			paramtable.Init()
			pt := paramtable.Get()
			initialPolicy, nextPolicy := LookAside, LatencyWeighted
			if initiallyEnabled {
				initialPolicy, nextPolicy = nextPolicy, initialPolicy
			}
			pt.Save(pt.ProxyCfg.ReplicaSelectionPolicy.Key, initialPolicy)
			pt.Save(pt.ProxyCfg.HedgingEnabled.Key, "false")
			t.Cleanup(func() {
				pt.Reset(pt.ProxyCfg.ReplicaSelectionPolicy.Key)
				pt.Reset(pt.ProxyCfg.HedgingEnabled.Key)
			})
			mgr := NewMockShardClientManager(t)
			qn := mocks.NewMockQueryNodeClient(t)
			mgr.EXPECT().GetShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]NodeInfo{{NodeID: 1, Serviceable: true}}, nil)
			mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Run(func(context.Context, NodeInfo) {
				pt.Save(pt.ProxyCfg.ReplicaSelectionPolicy.Key, nextPolicy)
			}).Return(qn, nil)
			lb := NewLBPolicyImpl(mgr)
			b := lb.balancerMap[LatencyWeighted].(*LatencyWeightedBalancer)
			b.metricsMap.Insert(1, &CostMetrics{})
			require.NoError(t, lb.ExecuteWithRetry(context.Background(), ChannelWorkload{Channel: hedgeTestChannel, StatsClass: hedgeTestClass, Nq: 7, Exec: func(context.Context, int64, types.QueryNodeClient, string) error { return nil }}))
			if m, ok := b.metricsMap.Get(1); ok {
				assert.Zero(t, m.executingNQ.Load())
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			for _, s := range b.replicas {
				assert.Zero(t, s.inflight)
			}
			if initiallyEnabled {
				require.Contains(t, b.replicas, replicaKey{hedgeTestKey, 1})
			} else {
				assert.Empty(t, b.replicas)
			}
		})
	}
}

func TestHedgeTrackerScheduledCleanup(t *testing.T) {
	tracker := newHedgeTracker()
	now := time.Now()
	tracker.now = func() time.Time { return now }
	tracker.cleanupInterval = 5 * time.Millisecond

	tracker.ObserveRequest(hedgeTestKey, 10*time.Millisecond)
	tracker.mu.Lock()
	require.Len(t, tracker.keys, 1)
	tracker.mu.Unlock()

	now = now.Add(statsIdleTimeout + time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracker.Start(ctx)
	defer tracker.Close()

	require.Eventually(t, func() bool {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		return len(tracker.keys) == 0
	}, time.Second, 5*time.Millisecond)
}
