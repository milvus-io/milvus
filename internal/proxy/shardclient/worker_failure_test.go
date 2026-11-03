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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newWorkerFailurePolicy(t *testing.T) *LBPolicyImpl {
	paramtable.Init()
	mgr := NewShardClientMgr(nil)
	nodes := []NodeInfo{
		{NodeID: 1, Address: "worker-path-1", Serviceable: true},
		{NodeID: 2, Address: "worker-path-2", Serviceable: true},
	}
	shard := mockey.Mock((*shardClientMgrImpl).GetShard).Return(nodes, nil).Build()
	t.Cleanup(func() { shard.UnPatch() })
	client := mockey.Mock((*shardClientMgrImpl).GetClient).Return(nil, nil).Build()
	t.Cleanup(func() { client.UnPatch() })
	lb := NewLBPolicyImpl(mgr)
	balancer := NewRoundRobinBalancer()
	lb.getBalancer = func() LBBalancer { return balancer }
	t.Cleanup(lb.blacklist.Close)
	return lb
}

func TestWorkerConnectionFailureBlacklist(t *testing.T) {
	lb := newWorkerFailurePolicy(t)
	var visited []int64
	broken := true
	workload := ChannelWorkload{
		CollectionID: 1, Channel: "shard-a", PreferredNodeID: 1,
		Exec: func(_ context.Context, nodeID int64, _ types.QueryNodeClient, _ string) error {
			visited = append(visited, nodeID)
			if nodeID == 1 && broken {
				// Exercise the actual wire reconstruction, including Retriable=true.
				return merr.Error(merr.Status(merr.WrapErrChannelWorkerUnavailable(status.Error(codes.Unavailable, "connection lost"))))
			}
			return nil
		},
	}
	require.NoError(t, lb.ExecuteWithRetry(context.Background(), workload))
	require.Equal(t, []int64{1, 2}, visited)
	require.Equal(t, []int64{1}, lb.blacklist.GetBlacklistedNodes("shard-a"))

	visited = nil
	require.NoError(t, lb.ExecuteWithRetry(context.Background(), workload))
	require.Equal(t, []int64{2}, visited, "new requests must also avoid the failed preferred leader")

	broken = false
	visited = nil
	workload.Channel = "shard-b"
	require.NoError(t, lb.ExecuteWithRetry(context.Background(), workload))
	require.Equal(t, []int64{1}, visited, "do not blacklist the entire SN")

	lb.blacklist.mu.Lock()
	lb.blacklist.blacklist["shard-a"][1] = time.Now().Add(-time.Second)
	lb.blacklist.mu.Unlock()
	workload.Channel = "shard-a"
	visited = nil
	require.NoError(t, lb.ExecuteWithRetry(context.Background(), workload))
	require.Equal(t, []int64{1}, visited, "allow recovery after the existing blacklist TTL")
}

func TestWorkerConnectionFailureDoesNotBlacklistCancellation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		cancel bool
	}{
		{"parent canceled", status.Error(codes.Canceled, "caller canceled"), true},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"grpc canceled", status.Error(codes.Canceled, "canceled"), false},
		{"grpc deadline", status.Error(codes.DeadlineExceeded, "slow query"), false},
		{"wire canceled", merr.Error(merr.Status(context.Canceled)), false},
		{"wire deadline", merr.Error(merr.Status(context.DeadlineExceeded)), false},
		{"application unavailable", merr.ErrServiceUnavailable, false},
		{"input", merr.ErrParameterInvalid, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lb := newWorkerFailurePolicy(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			err := lb.ExecuteWithRetry(ctx, ChannelWorkload{
				CollectionID: 1, Channel: "shard", PreferredNodeID: 1,
				Exec: func(_ context.Context, id int64, _ types.QueryNodeClient, _ string) error {
					calls++
					if tc.cancel {
						cancel()
					}
					if id == 1 {
						return tc.err
					}
					return nil
				},
			})
			if tc.cancel {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 1, calls)
			}
			if tc.name == "input" {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Equal(t, 1, calls)
			}
			require.Empty(t, lb.blacklist.GetBlacklistedNodes("shard"))
		})
	}
}

func TestWorkerConnectionFailureClientAcquisitionCanceled(t *testing.T) {
	paramtable.Init()
	mgr := NewShardClientMgr(nil)
	shard := mockey.Mock((*shardClientMgrImpl).GetShard).Return([]NodeInfo{{NodeID: 1, Serviceable: true}}, nil).Build()
	defer shard.UnPatch()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := mockey.Mock((*shardClientMgrImpl).GetClient).To(func(*shardClientMgrImpl, context.Context, NodeInfo) (types.QueryNodeClient, error) {
		cancel()
		return nil, context.Canceled
	}).Build()
	defer client.UnPatch()
	lb := NewLBPolicyImpl(mgr)
	lb.getBalancer = func() LBBalancer { return NewRoundRobinBalancer() }
	err := lb.ExecuteWithRetry(ctx, ChannelWorkload{Channel: "shard", PreferredNodeID: 1})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, lb.blacklist.GetBlacklistedNodes("shard"))
}
