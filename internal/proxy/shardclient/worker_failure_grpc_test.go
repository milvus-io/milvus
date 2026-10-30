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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	qnclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// queryPathServer is a real gRPC fault-injection endpoint. The forwarding
// endpoint uses the production remoteWorker and the service Status boundary.
// This is a component test, not a replacement for a loaded-cluster experiment.
type queryPathServer struct {
	querypb.UnimplementedQueryNodeServer
	worker      cluster.Worker
	unavailable atomic.Bool
	legacy      atomic.Bool
	searches    atomic.Int64
	queries     atomic.Int64
}

func (s *queryPathServer) SearchSegments(context.Context, *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	if s.legacy.Load() {
		return nil, status.Error(codes.Unimplemented, "legacy worker")
	}
	s.searches.Add(1)
	if s.unavailable.Load() {
		return nil, status.Error(codes.Unavailable, "error reading from server: connection timed out")
	}
	return &internalpb.SearchResults{Status: merr.Success()}, nil
}

func (s *queryPathServer) QuerySegments(context.Context, *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	if s.legacy.Load() {
		return nil, status.Error(codes.Unimplemented, "legacy worker")
	}
	s.queries.Add(1)
	if s.unavailable.Load() {
		return nil, status.Error(codes.Unavailable, "error reading from server: connection timed out")
	}
	return &internalpb.RetrieveResults{Status: merr.Success()}, nil
}

func (s *queryPathServer) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	s.searches.Add(1)
	if s.worker == nil {
		if s.unavailable.Load() {
			return nil, status.Error(codes.Unavailable, "connection lost")
		}
		return &internalpb.SearchResults{Status: merr.Success()}, nil
	}
	ret, err := s.worker.SearchSegments(retry.WithMaxAttemptsContext(ctx, 3), req)
	if err != nil {
		return &internalpb.SearchResults{Status: merr.Status(err)}, nil
	}
	return ret, nil
}

func (s *queryPathServer) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	s.queries.Add(1)
	if s.worker == nil {
		if s.unavailable.Load() {
			return nil, status.Error(codes.Unavailable, "connection lost")
		}
		return &internalpb.RetrieveResults{Status: merr.Success()}, nil
	}
	ret, err := s.worker.QuerySegments(retry.WithMaxAttemptsContext(ctx, 3), req)
	if err != nil {
		return &internalpb.RetrieveResults{Status: merr.Status(err)}, nil
	}
	return ret, nil
}

func startQueryPathServer(t *testing.T, server *queryPathServer, id int64) types.QueryNodeClient {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	querypb.RegisterQueryNodeServer(srv, server)
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(listener) }()
	t.Cleanup(func() { srv.Stop(); <-done })
	client, err := qnclient.NewClient(context.Background(), listener.Addr().String(), id)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func TestWorkerConnectionFailureGRPCFailover(t *testing.T) {
	paramtable.Init()
	workerServer := &queryPathServer{}
	workerClient := startQueryPathServer(t, workerServer, 101)
	worker := cluster.NewRemoteWorker(workerClient)
	sn := &queryPathServer{worker: worker}
	badPath := startQueryPathServer(t, sn, 1)
	alternative := &queryPathServer{}
	goodPath := startQueryPathServer(t, alternative, 2)

	// Establish the SN -> QN transport before injecting the failure.
	_, err := worker.SearchSegments(context.Background(), &querypb.SearchRequest{Req: &internalpb.SearchRequest{}})
	require.NoError(t, err)
	workerServer.unavailable.Store(true)

	mgr := NewShardClientMgr(nil)
	shard := mockey.Mock((*shardClientMgrImpl).GetShard).Return([]NodeInfo{
		{NodeID: 1, Serviceable: true}, {NodeID: 2, Serviceable: true},
	}, nil).Build()
	defer shard.UnPatch()
	getClient := mockey.Mock((*shardClientMgrImpl).GetClient).To(func(_ *shardClientMgrImpl, _ context.Context, node NodeInfo) (types.QueryNodeClient, error) {
		if node.NodeID == 1 {
			return badPath, nil
		}
		return goodPath, nil
	}).Build()
	defer getClient.UnPatch()

	for _, tc := range []struct {
		op     string
		legacy bool
	}{{"search", false}, {"query", false}, {"search", true}, {"query", true}} {
		name := tc.op
		if tc.legacy {
			name += "/legacy"
		}
		t.Run(name, func(t *testing.T) {
			op := tc.op
			workerServer.legacy.Store(tc.legacy)
			// Warm/reestablish a healthy transport before each failure. This
			// keeps this test about RPC failure, not missing etcd sessions.
			workerServer.unavailable.Store(false)
			_, err := worker.QuerySegments(context.Background(), &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{}})
			require.NoError(t, err)
			workerServer.unavailable.Store(true)
			lb := NewLBPolicyImpl(mgr)
			t.Cleanup(lb.blacklist.Close)
			lb.getBalancer = func() LBBalancer { return NewRoundRobinBalancer() }
			var seen []int64
			var wireErr error
			workload := ChannelWorkload{
				CollectionID: 1, Channel: "shard", PreferredNodeID: 1,
				Exec: func(ctx context.Context, id int64, client types.QueryNodeClient, channel string) error {
					seen = append(seen, id)
					ctx = retry.WithMaxAttemptsContext(ctx, 1)
					var err error
					if op == "search" {
						ret, callErr := client.Search(ctx, &querypb.SearchRequest{Req: &internalpb.SearchRequest{}, DmlChannels: []string{channel}})
						err = merr.CheckRPCCall(ret, callErr)
					} else {
						ret, callErr := client.Query(ctx, &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{}, DmlChannels: []string{channel}})
						err = merr.CheckRPCCall(ret, callErr)
					}
					if id == 1 {
						wireErr = err
					}
					return err
				},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			searches, queries := workerServer.searches.Load(), workerServer.queries.Load()
			require.NoError(t, lb.ExecuteWithRetry(ctx, workload))
			require.NoError(t, ctx.Err(), "fail over while the original caller is still live")
			require.ErrorIs(t, wireErr, merr.ErrChannelWorkerUnavailable)
			require.True(t, merr.IsRetryableErr(wireErr))
			require.Equal(t, []int64{1, 2}, seen)
			if op == "search" {
				require.Equal(t, searches+1, workerServer.searches.Load())
			} else {
				require.Equal(t, queries+1, workerServer.queries.Load())
			}
			seen = nil
			require.NoError(t, lb.ExecuteWithRetry(ctx, workload))
			require.Equal(t, []int64{2}, seen, "the next request must also bypass the failed path")
		})
	}
}
