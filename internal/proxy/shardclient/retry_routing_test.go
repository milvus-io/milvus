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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestHedgeIgnoresPreferredNodeHint(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	require.NoError(t, pt.Save(pt.ProxyCfg.HedgingEnabled.Key, "true"))
	require.NoError(t, pt.Save(pt.ProxyCfg.HedgingMinDelay.Key, "1ms"))
	t.Cleanup(func() {
		pt.Reset(pt.ProxyCfg.HedgingEnabled.Key)
		pt.Reset(pt.ProxyCfg.HedgingMinDelay.Key)
	})
	for _, weighted := range []bool{false, true} {
		for _, preferred := range []int64{1, 2} {
			t.Run(fmt.Sprintf("weighted=%v/preferred=%d", weighted, preferred), func(t *testing.T) {
				initial := []NodeInfo{
					{NodeID: 1, Address: "one", Serviceable: true},
					{NodeID: 2, Address: "two", Serviceable: false},
					{NodeID: 3, Address: "three", Serviceable: false},
				}
				refreshed := append([]NodeInfo(nil), initial...)
				refreshed[1].Serviceable, refreshed[2].Serviceable = true, true
				mgr := NewMockShardClientManager(t)
				// Entry and primary selection see the old serviceability flags;
				// the hedge sees node 2 as serviceable but heartbeat-unavailable.
				mgr.EXPECT().GetShard(mock.Anything, true, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(initial, nil).Twice()
				mgr.EXPECT().GetShard(mock.Anything, true, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(refreshed, nil).Once()
				mgr.EXPECT().GetClient(mock.Anything, mock.Anything).
					Return(mocks.NewMockQueryNodeClient(t), nil).Twice()
				policy := NewLBPolicyImpl(mgr)
				look := NewLookAsideBalancer(mgr)
				var balancer LBBalancer = look
				if weighted {
					lw := NewLatencyWeightedBalancer(mgr)
					look, balancer = lw.LookAsideBalancer, lw
				}
				nodeMetrics, _ := look.metricsMap.GetOrInsert(2, &CostMetrics{})
				nodeMetrics.unavailable.Store(true)
				policy.getBalancer = func() LBBalancer { return balancer }
				hit := metrics.ProxyShardLeaderPreferredNodeCount.WithLabelValues(metrics.PreferredNodeHitLabel)
				unavailable := metrics.ProxyShardLeaderPreferredNodeCount.WithLabelValues(metrics.PreferredNodeUnavailableLabel)
				hitBefore, unavailableBefore := testutil.ToFloat64(hit), testutil.ToFloat64(unavailable)
				primaryDone := make(chan struct{})
				release := make(chan struct{})
				hedgeNode := make(chan int64, 1)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := policy.ExecuteWithRetry(ctx, ChannelWorkload{
					Channel: "ch", Nq: 1, Hedgeable: true, PreferredNodeID: preferred,
					Exec: func(ctx context.Context, node int64, _ types.QueryNodeClient, _ string) error {
						if node == 1 {
							defer close(primaryDone)
							select {
							case <-release:
							case <-ctx.Done():
								return ctx.Err()
							}
						} else {
							hedgeNode <- node
						}
						if !CommitAttempt(ctx) {
							return ErrAttemptSuperseded
						}
						return nil
					},
				})
				close(release)
				select {
				case <-primaryDone:
				case <-ctx.Done():
					t.Fatal("primary did not finish")
				}
				require.NoError(t, err)
				require.Equal(t, int64(3), <-hedgeNode, "hedge must honor heartbeat health even for a preferred node")
				if preferred == 1 {
					require.Equal(t, hitBefore+1, testutil.ToFloat64(hit))
					require.Equal(t, unavailableBefore, testutil.ToFloat64(unavailable))
				} else {
					require.Equal(t, hitBefore, testutil.ToFloat64(hit))
					require.Equal(t, unavailableBefore+1, testutil.ToFloat64(unavailable))
				}
			})
		}
	}
}

func TestOverloadUsesRefreshedSelectionSnapshot(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	require.NoError(t, pt.Save(pt.ProxyCfg.HedgingEnabled.Key, "false"))
	t.Cleanup(func() { pt.Reset(pt.ProxyCfg.HedgingEnabled.Key) })
	for _, hedgeable := range []bool{false, true} {
		for _, twice := range []bool{false, true} {
			t.Run(fmt.Sprintf("hedgeable=%v/twice=%v", hedgeable, twice), func(t *testing.T) {
				coord := mocks.NewMockMixCoordClient(t)
				coord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
					Status: merr.Success(),
					Shards: []*querypb.ShardLeadersList{{
						ChannelName: "ch", NodeIds: []int64{2, 3},
						NodeAddrs: []string{"two", "three"}, Serviceable: []bool{true, true},
					}},
				}, nil).Once()
				mgr := NewShardClientMgr(coord)
				mgr.collLeader[100] = &shardLeaders{shardLeaders: map[string][]NodeInfo{
					"ch": {{NodeID: 1, Address: "one", Serviceable: true}, {NodeID: 2, Address: "two", Serviceable: true}},
				}}
				qn := mocks.NewMockQueryNodeClient(t)
				mgr.SetClientCreatorFunc(func(context.Context, string, int64) (types.QueryNodeClient, error) { return qn, nil })
				policy := NewLBPolicyImpl(mgr)
				balancer := NewMockLBBalancer(t)
				policy.getBalancer = func() LBBalancer { return balancer }
				balancer.EXPECT().RegisterNodeInfo(mock.Anything)
				// Fix selection order while keeping the real manager, cache refresh,
				// exclusion set, and overload retry budget in the exercised path.
				selections := 0
				balancer.EXPECT().SelectNode(mock.Anything, mock.Anything, int64(1)).RunAndReturn(
					func(_ context.Context, nodes []int64, _ int64) (int64, error) {
						candidates := [][]int64{{1, 2}, {2, 3}, {3}}
						require.Less(t, selections, len(candidates))
						require.ElementsMatch(t, candidates[selections], nodes)
						selections++
						return int64(selections), nil
					}).Times(3)
				balancer.EXPECT().CancelWorkload(mock.Anything, int64(1)).Times(3)
				var calls []int64
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := policy.ExecuteWithRetry(ctx, ChannelWorkload{
					CollectionID: 100, Channel: "ch", Nq: 1,
					Hedgeable: hedgeable, OverloadRetryable: !hedgeable,
					Exec: func(_ context.Context, node int64, _ types.QueryNodeClient, _ string) error {
						calls = append(calls, node)
						switch node {
						case 1:
							// Same invalidation and status decoding as Search/Query
							// callbacks when their old delegator is no longer leader.
							mgr.InvalidateShardLeaderCache([]int64{100})
							return merr.Error(&commonpb.Status{ErrorCode: commonpb.ErrorCode_NotShardLeader, Reason: "replica migrated"})
						case 2:
							// Failover must reuse [2,3], even if another request
							// invalidates the cache again before the refusal.
							mgr.InvalidateShardLeaderCache([]int64{100})
							return overloadErr()
						default:
							if twice {
								return overloadErr()
							}
							return nil
						}
					},
				})
				require.Equal(t, []int64{1, 2, 3}, calls)
				require.Nil(t, mgr.collLeader[100], "overload failover must not refresh the invalidated cache")
				require.NoError(t, ctx.Err())
				if twice {
					require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}
