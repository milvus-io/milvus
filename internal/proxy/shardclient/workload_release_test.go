// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type workloadReleaseFixture struct {
	balancer LBBalancer
	base     *LookAsideBalancer
	counter  func(int64) *atomic.Int64
	evict    func(int64)
}

func newWorkloadReleaseFixture(refined bool) workloadReleaseFixture {
	base := NewLookAsideBalancer(nil)
	f := workloadReleaseFixture{balancer: base, base: base}
	f.counter = func(node int64) *atomic.Int64 {
		metrics, ok := base.metricsMap.Get(node)
		if !ok {
			return nil
		}
		return &metrics.executingNQ
	}
	var cleanup func()
	if refined {
		b := NewRefinedLookAsideBalancer(nil)
		base = b.LookAsideBalancer
		f.balancer, f.base = b, base
		f.counter = func(node int64) *atomic.Int64 {
			metrics, ok := b.refinedMetricsMap.Get(node)
			if !ok {
				return nil
			}
			return &metrics.executingNQ
		}
		cleanup = b.cleanupEvictedMetrics
	}
	f.evict = func(node int64) {
		// Exercise the real health-check eviction path without waiting for RPCs.
		base.failedHeartBeatCounter.Insert(node, atomic.NewInt64(1_000_000))
		base.trySetQueryNodeUnReachable(node, context.DeadlineExceeded)
		if cleanup != nil {
			cleanup()
		}
	}
	return f
}

func TestWorkloadReleaseAfterHealthEviction(t *testing.T) {
	paramtable.Init()
	for _, refined := range []bool{false, true} {
		for _, idx := range []int64{0, 1} {
			t.Run(map[bool]string{false: LookAside, true: RefinedLookAside}[refined]+map[int64]string{0: "/score", 1: "/fast"}[idx], func(t *testing.T) {
				f := newWorkloadReleaseFixture(refined)
				f.base.idx.Store(idx)
				f.balancer.RegisterNodeInfo([]NodeInfo{{NodeID: 1}})
				node, releaseOld, err := f.balancer.SelectNode(context.Background(), []int64{1}, 20)
				require.NoError(t, err)
				oldCounter := f.counter(node)
				require.EqualValues(t, 20, oldCounter.Load())

				f.evict(node)
				require.False(t, f.base.metricsMap.Contain(node))
				require.Nil(t, f.counter(node))

				// A successful old RPC can recreate metrics before its release runs.
				f.balancer.UpdateCostMetrics(node, &internalpb.CostAggregation{ResponseTime: 2, ServiceTime: 1})
				newCounter := f.counter(node)
				require.NotSame(t, oldCounter, newCounter)
				f.balancer.RegisterNodeInfo([]NodeInfo{{NodeID: node}})
				_, releaseNew, err := f.balancer.SelectNode(context.Background(), []int64{node}, 7)
				require.NoError(t, err)
				require.EqualValues(t, 7, newCounter.Load())

				releaseOld()
				require.Zero(t, oldCounter.Load())
				require.EqualValues(t, 7, newCounter.Load(), "old completion must not debit the new generation")
				releaseNew()
				require.Zero(t, newCounter.Load())
			})
		}
	}
}

func TestWorkloadReleaseConcurrentMetricsCreation(t *testing.T) {
	paramtable.Init()
	for _, refined := range []bool{false, true} {
		t.Run(map[bool]string{false: LookAside, true: RefinedLookAside}[refined], func(t *testing.T) {
			f := newWorkloadReleaseFixture(refined)
			const requests = 128
			releases := make([]func(), requests)
			errs := make([]error, requests)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range requests {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					f.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 2, ServiceTime: 1})
					_, releases[i], errs[i] = f.balancer.SelectNode(context.Background(), []int64{1}, 3)
					f.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 3, ServiceTime: 1})
				}()
			}
			close(start)
			wg.Wait()
			for _, err := range errs {
				require.NoError(t, err)
			}
			require.EqualValues(t, requests*3, f.counter(1).Load(), "metrics updates must not overwrite reservations")
			for _, release := range releases {
				release()
			}
			require.Zero(t, f.counter(1).Load())
		})
	}
}

func TestWorkloadReleaseFailedSelection(t *testing.T) {
	paramtable.Init()
	for _, refined := range []bool{false, true} {
		for _, idx := range []int64{0, 1} {
			t.Run(map[bool]string{false: LookAside, true: RefinedLookAside}[refined]+map[int64]string{0: "/score", 1: "/fast"}[idx], func(t *testing.T) {
				f := newWorkloadReleaseFixture(refined)
				f.base.idx.Store(idx)
				_, release, err := f.balancer.SelectNode(context.Background(), nil, 3)
				require.Error(t, err)
				require.Nil(t, release)
				require.Nil(t, f.counter(-1), "failed selection must not create a reservation")

				f.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 2})
				metrics, _ := f.base.metricsMap.Get(int64(1))
				metrics.unavailable.Store(true)
				f.base.idx.Store(idx)
				_, release, err = f.balancer.SelectNode(context.Background(), []int64{1}, 3)
				require.Error(t, err)
				require.Nil(t, release)
				require.Zero(t, f.counter(1).Load())
			})
		}
	}
}

func TestWorkloadReleaseOnRequestExit(t *testing.T) {
	paramtable.Init()
	for _, refined := range []bool{false, true} {
		for _, outcome := range []string{"success", "input_error", "cancel", "deadline", "retry", "client_error", "eviction"} {
			t.Run(map[bool]string{false: LookAside, true: RefinedLookAside}[refined]+"/"+outcome, func(t *testing.T) {
				f := newWorkloadReleaseFixture(refined)
				mgr := NewMockShardClientManager(t)
				qn := mocks.NewMockQueryNodeClient(t)
				nodes := []NodeInfo{{NodeID: 1, Serviceable: true}}
				mgr.EXPECT().GetShard(mock.Anything, mock.Anything, "db", "coll", int64(1), "channel").Return(nodes, nil)
				if outcome == "client_error" {
					mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(nil, merr.ErrServiceUnavailable).Once()
				}
				mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil)
				lb := NewLBPolicyImpl(mgr)
				lb.getBalancer = func() LBBalancer { return f.balancer }
				lb.retryOnReplica = 1
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var attempts int
				var oldCounter *atomic.Int64
				err := lb.ExecuteWithRetry(ctx, ChannelWorkload{
					Db: "db", CollectionName: "coll", CollectionID: 1, Channel: "channel", Nq: 3,
					Exec: func(ctx context.Context, node UniqueID, _ types.QueryNodeClient, _ string) error {
						attempts++
						oldCounter = f.counter(node)
						require.EqualValues(t, 3, oldCounter.Load(), "previous attempt must release before retry")
						switch outcome {
						case "input_error":
							return merr.WrapErrParameterInvalidMsg("invalid request")
						case "cancel":
							cancel()
							return ctx.Err()
						case "deadline":
							return context.DeadlineExceeded
						case "retry":
							if attempts == 1 {
								return merr.ErrServiceUnavailable
							}
						case "eviction":
							f.evict(node)
							f.balancer.UpdateCostMetrics(node, &internalpb.CostAggregation{ResponseTime: 2})
						}
						return nil
					},
				})
				if outcome == "input_error" || outcome == "cancel" || outcome == "deadline" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Zero(t, oldCounter.Load())
				require.Zero(t, f.counter(1).Load())
				if outcome == "retry" || outcome == "deadline" {
					require.Equal(t, 2, attempts)
				} else {
					require.Equal(t, 1, attempts)
				}
			})
		}
	}
}

func TestSelectNodeReleasesRejectedReservation(t *testing.T) {
	paramtable.Init()
	mgr := NewMockShardClientManager(t)
	balancer := NewMockLBBalancer(t)
	nodes := []NodeInfo{{NodeID: 1, Serviceable: true}}
	mgr.EXPECT().GetShard(mock.Anything, mock.Anything, "db", "coll", int64(1), "channel").Return(nodes, nil)
	balancer.EXPECT().RegisterNodeInfo(mock.Anything)
	var rejectedReleases, acceptedReleases int
	balancer.EXPECT().SelectNode(mock.Anything, mock.Anything, int64(3)).
		Return(int64(99), func() { rejectedReleases++ }, nil).Once()
	balancer.EXPECT().SelectNode(mock.Anything, mock.Anything, int64(3)).
		Return(int64(1), func() { acceptedReleases++ }, nil).Once()
	lb := NewLBPolicyImpl(mgr)
	excluded := typeutil.NewUniqueSet()
	node, release, err := lb.selectNode(context.Background(), balancer, ChannelWorkload{
		Db: "db", CollectionName: "coll", CollectionID: 1, Channel: "channel", Nq: 3,
	}, &excluded)
	require.NoError(t, err)
	require.EqualValues(t, 1, node.NodeID)
	require.Equal(t, 1, rejectedReleases)
	require.Zero(t, acceptedReleases)
	release()
	require.Equal(t, 1, acceptedReleases)
}
