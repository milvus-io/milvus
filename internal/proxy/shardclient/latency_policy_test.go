// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestLatencyPolicyBlacklistAndSwitch(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		name := "weighted"
		if !enabled {
			name = "disabled_legacy"
		}
		t.Run(name, func(t *testing.T) {
			h := newHedgeHarness(t, 1, 2)
			pt := paramtable.Get()
			value, nextValue := LatencyWeighted, LookAside
			if !enabled {
				value, nextValue = nextValue, value
			}
			pt.Save(pt.ProxyCfg.ReplicaSelectionPolicy.Key, value)
			t.Cleanup(func() { pt.Reset(pt.ProxyCfg.ReplicaSelectionPolicy.Key) })
			h.policy.getBalancer = NewLBPolicyImpl(h.policy.clientMgr).getBalancer
			h.balancer = h.policy.balancerMap[LatencyWeighted].(*LatencyWeightedBalancer)
			var visited []int64
			err := h.policy.ExecuteWithRetry(context.Background(), ChannelWorkload{
				Channel: hedgeTestChannel, Nq: 1, StatsClass: hedgeTestClass,
				Exec: func(_ context.Context, node int64, _ types.QueryNodeClient, _ string) error {
					visited = append(visited, node)
					if len(visited) == 1 {
						// Releasing this attempt and dispatching its retry must retain
						// the accounting owner and blacklist policy captured at entry.
						pt.Save(pt.ProxyCfg.ReplicaSelectionPolicy.Key, nextValue)
						return merr.ErrChannelNotAvailable
					}
					return nil
				},
			})
			require.NoError(t, err)
			require.Len(t, visited, 2)
			require.NotEqual(t, visited[0], visited[1])
			blocked := h.policy.blacklist.GetBlacklistedNodes(hedgeTestChannel)
			if enabled {
				require.Empty(t, blocked)
			} else {
				require.Equal(t, []int64{visited[0]}, blocked)
			}
			require.Zero(t, h.inflight())
			h.balancer.metricsMap.Range(func(_ int64, cost *CostMetrics) bool {
				require.Zero(t, cost.executingNQ.Load())
				return true
			})
		})
	}
}

func TestLatencyPolicyDoesNotInheritLegacyBlacklist(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	h.policy.blacklist.Add(hedgeTestChannel, 1)
	var selected int64
	err := h.policy.ExecuteWithRetry(context.Background(), ChannelWorkload{
		Channel: hedgeTestChannel, Nq: 1, PreferredNodeID: 1,
		Exec: func(_ context.Context, node int64, _ types.QueryNodeClient, _ string) error {
			selected = node
			return nil
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, selected)
}

func TestLatencyCandidateRetainsResourceGroupScope(t *testing.T) {
	h := newHedgeHarness(t, 1, 2)
	leaders := []NodeInfo{
		{NodeID: 1, ResourceGroup: "group-a", Serviceable: true},
		{NodeID: 2, ResourceGroup: "group-b", Serviceable: true},
		{NodeID: 3, ResourceGroup: "group-b", Serviceable: true},
	}
	work := ChannelWorkload{Channel: hedgeTestChannel, ResourceGroup: "group-b", PreferredNodeID: 1}
	excluded := typeutil.NewUniqueSet(int64(2))
	node, selected, err := h.policy.selectCandidate(context.Background(), h.balancer, h.balancer, work, leaders, &excluded, false)
	require.NoError(t, err)
	require.True(t, selected)
	require.EqualValues(t, 3, node.NodeID)
	h.balancer.ReleaseSelection(statsKey(work), node.NodeID)
	excluded.Insert(3)
	_, _, err = h.policy.selectCandidate(context.Background(), h.balancer, h.balancer, work, leaders, &excluded, false)
	require.ErrorIs(t, err, merr.ErrChannelNotAvailable)
	require.True(t, excluded.Contain(2))
	require.True(t, excluded.Contain(3), "hedge candidate selection must not clear exclusions")
	work.ResourceGroup = "missing"
	_, _, err = h.policy.selectCandidate(context.Background(), h.balancer, h.balancer, work, leaders, &excluded, false)
	require.ErrorIs(t, err, merr.ErrCollectionNotFullyLoaded)
}

func TestLatencyHedgeHistoryIsScoped(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.HedgingEnabled.Key, "true")
	defer pt.Reset(pt.ProxyCfg.HedgingEnabled.Key)
	h := newHedgeTracker()
	unscoped := ChannelWorkload{Channel: hedgeTestChannel, StatsClass: hedgeTestClass}
	slow, fast := unscoped, unscoped
	slow.ResourceGroup, fast.ResourceGroup = "slow", "fast"
	for i := 0; i < 20; i++ {
		h.ObserveRequest(hedgeStatsKey(slow), 5*time.Second)
		h.ObserveHedge(hedgeStatsKey(slow), false)
		h.ObserveRequest(hedgeStatsKey(fast), time.Millisecond)
	}
	delay, ok := h.HedgeDelay(hedgeStatsKey(slow))
	require.True(t, ok)
	require.Equal(t, 5*time.Second, delay)
	for _, work := range []ChannelWorkload{fast, unscoped} {
		delay, ok = h.HedgeDelay(hedgeStatsKey(work))
		require.True(t, ok)
		require.Equal(t, 200*time.Millisecond, delay)
		for i := 0; i < 20; i++ {
			require.True(t, h.AllowHedge(hedgeStatsKey(work)))
		}
	}
	require.Equal(t, statsKey(slow), statsKey(fast), "the same serving node retains shared latency observations")
}

func TestForChannelCarriesHedgeClassAndScope(t *testing.T) {
	work := CollectionWorkLoad{StatsClass: "query/Bounded", Hedgeable: true, ResourceGroup: "group"}
	channel := work.ForChannel("ch", 42)
	require.True(t, channel.Hedgeable)
	require.Equal(t, work.StatsClass, channel.StatsClass)
	require.Equal(t, work.ResourceGroup, channel.ResourceGroup)
	require.EqualValues(t, 42, channel.PreferredNodeID)
}

type scopedHedgeManager struct {
	ShardClientMgr
	leaders []NodeInfo
}

func (m *scopedHedgeManager) GetShard(context.Context, bool, string, string, int64, string) ([]NodeInfo, error) {
	return m.leaders, nil
}

func TestHedgeTargetStaysInRequestScope(t *testing.T) {
	for _, hasAlternative := range []bool{false, true} {
		name := "one_replica_in_group"
		if hasAlternative {
			name = "two_replicas_in_group"
		}
		t.Run(name, func(t *testing.T) {
			h := newHedgeHarness(t, 1, 2, 3)
			leaders := []NodeInfo{{NodeID: 1, ResourceGroup: "group-a", Serviceable: true}, {NodeID: 2, ResourceGroup: "group-b", Serviceable: true}}
			if hasAlternative {
				leaders = append(leaders, NodeInfo{NodeID: 3, ResourceGroup: "group-b", Serviceable: true})
			}
			h.policy.clientMgr = &scopedHedgeManager{ShardClientMgr: h.policy.clientMgr, leaders: leaders}
			ctx, cancel := context.WithTimeout(extension.WithQueryResourceGroup(context.Background(), "group-b"), 5*time.Second)
			defer cancel()
			releasePrimary := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releasePrimary) }) }
			defer release()
			skipped := metrics.ProxyReplicaHedgeCount.WithLabelValues(paramtable.GetStringNodeID(), hedgeSkippedNoReplica)
			before := testutil.ToFloat64(skipped)
			done := make(chan error, 1)
			go func() {
				done <- h.policy.ExecuteWithRetry(ctx, ChannelWorkload{
					Channel: hedgeTestChannel, Nq: 1, PreferredNodeID: 2, Hedgeable: true, StatsClass: hedgeTestClass,
					Exec: h.exec(func(_ int32, node int64) (time.Duration, error) {
						if node == 2 {
							select {
							case <-releasePrimary:
							case <-ctx.Done():
								return 0, ctx.Err()
							}
						}
						return 0, nil
					}),
				})
			}()
			if !hasAlternative {
				// Wait until the lookup has been consumed and rejected before
				// allowing the primary to finish. A replica in another group
				// must not be dispatched while the primary is still pending.
				require.Eventually(t, func() bool { return testutil.ToFloat64(skipped) > before }, 3*time.Second, time.Millisecond)
				release()
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("scoped channel request did not finish")
			}
			release()
			h.mu.Lock()
			visited := append([]int64(nil), h.callNodes...)
			h.mu.Unlock()
			if hasAlternative {
				require.Equal(t, []int64{2, 3}, visited)
			} else {
				require.Equal(t, []int64{2}, visited)
			}
		})
	}
}

// Exercise the existing scoped retry contract through the weighted policy,
// including topology changes after an execution failure.
func TestLatencyPolicyScopedRetry(t *testing.T) {
	for _, scenario := range []string{"exhausted_group", "leader_disappears", "remaining_unreachable"} {
		t.Run(scenario, func(t *testing.T) {
			h := newHedgeHarness(t, 1, 2, 3)
			manager := &scopedHedgeManager{ShardClientMgr: h.policy.clientMgr, leaders: []NodeInfo{
				{NodeID: 1, ResourceGroup: "other", Serviceable: true},
				{NodeID: 2, ResourceGroup: "requested", Serviceable: true},
				{NodeID: 3, ResourceGroup: "requested", Serviceable: true},
			}}
			h.policy.clientMgr = manager
			h.policy.retryOnReplica = 1
			execErr := merr.WrapErrSegmentNotLoaded(42)
			var visited []int64
			err := h.policy.ExecuteWithRetry(context.Background(), ChannelWorkload{
				Channel: hedgeTestChannel, ResourceGroup: "requested", Nq: 1,
				Exec: func(_ context.Context, node int64, _ types.QueryNodeClient, _ string) error {
					visited = append(visited, node)
					require.Contains(t, []int64{2, 3}, node)
					switch scenario {
					case "exhausted_group":
						if len(visited) == 3 {
							return nil
						}
					case "leader_disappears":
						manager.leaders = manager.leaders[:1]
					case "remaining_unreachable":
						for _, candidate := range []int64{2, 3} {
							cost := &CostMetrics{}
							cost.unavailable.Store(true)
							h.balancer.metricsMap.Insert(candidate, cost)
						}
					}
					return execErr
				},
			})
			switch scenario {
			case "exhausted_group":
				require.NoError(t, err)
				require.Len(t, visited, 3)
				require.NotEqual(t, visited[0], visited[1])
			case "leader_disappears":
				require.ErrorIs(t, err, merr.ErrCollectionNotFullyLoaded)
				require.True(t, merr.IsRetryableErr(err))
				require.Len(t, visited, 1)
			case "remaining_unreachable":
				require.ErrorIs(t, err, merr.ErrSegmentNotLoaded)
				require.False(t, merr.IsRetryableErr(err))
				require.Len(t, visited, 1)
			}
			require.Zero(t, h.inflight())
		})
	}
}
