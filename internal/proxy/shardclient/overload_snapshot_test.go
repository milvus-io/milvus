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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestOverloadCacheMissUsesRequestSnapshot(t *testing.T) {
	paramtable.Init()
	for _, weighted := range []bool{false, true} {
		for _, capability := range []string{"hedgeable", "serial", "neither"} {
			for _, twice := range []bool{false, true} {
				t.Run(fmt.Sprintf("weighted=%v/%s/twice=%v", weighted, capability, twice), func(t *testing.T) {
					// No GetShardLeaders expectation: any cache-miss refresh is a test failure.
					coord := mocks.NewMockMixCoordClient(t)
					mgr := NewShardClientMgr(coord)
					mgr.collLeader[100] = &shardLeaders{shardLeaders: map[string][]NodeInfo{
						"ch": {{NodeID: 1, Address: "one", Serviceable: true}, {NodeID: 2, Address: "two", Serviceable: true}, {NodeID: 3, Address: "three", Serviceable: true}},
					}}
					mgr.SetClientCreatorFunc(func(context.Context, string, int64) (types.QueryNodeClient, error) {
						return mocks.NewMockQueryNodeClient(t), nil
					})
					policy := NewLBPolicyImpl(mgr)
					var balancer LBBalancer = NewLookAsideBalancer(mgr)
					if weighted {
						balancer = NewLatencyWeightedBalancer(mgr)
					}
					policy.getBalancer = func() LBBalancer { return balancer }
					var calls []int64
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					err := policy.Execute(ctx, CollectionWorkLoad{
						CollectionID: 100, Nq: 1,
						Hedgeable: capability == "hedgeable", OverloadRetryable: capability == "serial",
						Exec: func(_ context.Context, node int64, _ types.QueryNodeClient, _ string) error {
							calls = append(calls, node)
							if len(calls) == 1 {
								// Reproduce another request invalidating the collection between the
								// successful first selection and overload failover.
								mgr.InvalidateShardLeaderCache([]int64{100})
								return merr.WrapErrTooManyRequests(10)
							}
							if twice {
								return merr.WrapErrTooManyRequests(10)
							}
							return nil
						},
					})
					require.NoError(t, ctx.Err())
					if capability == "neither" {
						require.Len(t, calls, 1)
					} else {
						require.Len(t, calls, 2)
						require.NotEqual(t, calls[0], calls[1])
					}
					if twice || capability == "neither" {
						require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
					} else {
						require.NoError(t, err)
					}
					require.Nil(t, mgr.collLeader[100], "failover must not repopulate the cache")
				})
			}
		}
	}
}

func TestLocalDeadlineAfterClientSelectionDoesNotPenalizeReplica(t *testing.T) {
	paramtable.Init()
	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.collLeader[100] = &shardLeaders{shardLeaders: map[string][]NodeInfo{"ch": {{NodeID: 1, Address: "one", Serviceable: true}}}}
	mgr.SetClientCreatorFunc(func(context.Context, string, int64) (types.QueryNodeClient, error) {
		return mocks.NewMockQueryNodeClient(t), nil
	})
	policy := NewLBPolicyImpl(mgr)
	balancer := NewLatencyWeightedBalancer(mgr)
	policy.getBalancer = func() LBBalancer { return balancer }
	workload := ChannelWorkload{CollectionID: 100, Channel: "ch", Nq: 1}
	key := statsKey(workload)
	stats := balancer.replicaLocked(key, 1)
	stats.failures = 2
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ran := false
	workload.Exec = func(ctx context.Context, _ int64, _ types.QueryNodeClient, _ string) error {
		ran = true
		// GetClient succeeded; the deadline expires before ClientBase enters the
		// RPC callback. This is the previously unclassified window.
		<-ctx.Done()
		base := &grpcclient.ClientBase[types.QueryNodeClient]{}
		_, err := base.Call(ctx, func(types.QueryNodeClient) (any, error) {
			t.Fatal("RPC dispatched after local expiry")
			return nil, nil
		})
		return err
	}
	err := policy.ExecuteWithRetry(ctx, workload)
	require.True(t, ran)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, grpcclient.IsRequestNotDispatched(err))
	require.Equal(t, 2, stats.failures)
	require.Zero(t, stats.inflight)
	require.Zero(t, stats.count, "local expiry must not become a success sample")
	// A deadline error without the local marker still counts as a replica failure.
	outcome, _ := classifyAttempt(context.Background(), time.Now().Add(-time.Second), time.Now(), context.DeadlineExceeded)
	require.Equal(t, attemptFailed, outcome)
}
