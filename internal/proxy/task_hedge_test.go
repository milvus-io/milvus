// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to You under the Apache License, Version 2.0
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

package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type hedgeTaskClient struct {
	types.QueryNodeClient
	search func(context.Context, *querypb.SearchRequest) (*internalpb.SearchResults, error)
	query  func(context.Context, *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
}

func (c *hedgeTaskClient) Search(ctx context.Context, req *querypb.SearchRequest, _ ...grpc.CallOption) (*internalpb.SearchResults, error) {
	return c.search(ctx, req)
}

func (c *hedgeTaskClient) Query(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return c.query(ctx, req)
}

type hedgeTaskManager struct {
	shardclient.ShardClientMgr
	client        types.QueryNodeClient
	invalidations atomic.Int64
}

func (m *hedgeTaskManager) InvalidateShardLeaderCache(_ []int64) {
	m.invalidations.Add(1)
}

func (m *hedgeTaskManager) GetShard(context.Context, bool, string, string, int64, string) ([]shardclient.NodeInfo, error) {
	return []shardclient.NodeInfo{{NodeID: 1, Serviceable: true}, {NodeID: 2, Serviceable: true}}, nil
}

func (m *hedgeTaskManager) GetClient(context.Context, shardclient.NodeInfo) (types.QueryNodeClient, error) {
	return m.client, nil
}

func TestReadShardCancellationCacheInvalidation(t *testing.T) {
	for _, kind := range []string{"search", "query"} {
		for _, scenario := range []string{"caller_canceled", "deadline", "local_deadline", "remote_canceled", "unavailable"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				ctx := context.Background()
				var cancel context.CancelFunc
				switch scenario {
				case "caller_canceled":
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "deadline", "local_deadline":
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
					defer cancel()
				}
				rpcErr := status.Error(codes.Canceled, "remote request stopped")
				switch scenario {
				case "local_deadline":
					base := &grpcclient.ClientBase[types.QueryNodeClient]{}
					_, rpcErr = base.Call(ctx, func(types.QueryNodeClient) (any, error) {
						t.Fatal("expired request reached the RPC callback")
						return nil, nil
					})
					require.True(t, grpcclient.IsRequestNotDispatched(rpcErr))
				case "deadline":
					rpcErr = status.Error(codes.DeadlineExceeded, "request timed out")
				case "unavailable":
					rpcErr = status.Error(codes.Unavailable, "connection refused")
				}
				client := &hedgeTaskClient{
					search: func(context.Context, *querypb.SearchRequest) (*internalpb.SearchResults, error) { return nil, rpcErr },
					query:  func(context.Context, *querypb.QueryRequest) (*internalpb.RetrieveResults, error) { return nil, rpcErr },
				}
				manager := &hedgeTaskManager{client: client}
				var err error
				if kind == "search" {
					task := &searchTask{SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}}, shardClientMgr: manager}
					err = task.searchShard(ctx, 1, client, "ch")
				} else {
					task := &queryTask{RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}}, shardclientMgr: manager}
					err = task.queryShard(ctx, 1, client, "ch")
				}
				require.ErrorIs(t, err, rpcErr)
				if scenario == "caller_canceled" || scenario == "local_deadline" {
					require.Zero(t, manager.invalidations.Load())
				} else {
					require.EqualValues(t, 1, manager.invalidations.Load())
				}
			})
		}
	}
}

func TestReadShardPreservesStatusClassification(t *testing.T) {
	for _, kind := range []string{"search", "query"} {
		for _, sourceErr := range []error{
			merr.WrapErrParameterInvalid("BM25", "IP"),
			merr.WrapErrCollectionNotLoaded(42),
			merr.WrapErrChannelNotAvailable("ch", "delegator not ready"),
			merr.WrapErrChannelTSafeStalled("ch", "consumer stalled"),
			merr.WrapErrTooManyRequests(200, "read task queue"),
		} {
			t.Run(kind+"/"+sourceErr.Error(), func(t *testing.T) {
				wireStatus := merr.Status(sourceErr)
				client := &hedgeTaskClient{
					search: func(context.Context, *querypb.SearchRequest) (*internalpb.SearchResults, error) {
						return &internalpb.SearchResults{Status: wireStatus}, nil
					},
					query: func(context.Context, *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
						return &internalpb.RetrieveResults{Status: wireStatus}, nil
					},
				}
				manager := &hedgeTaskManager{client: client}
				var err error
				if kind == "search" {
					task := &searchTask{SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}}, shardClientMgr: manager}
					err = task.searchShard(context.Background(), 1, client, "ch")
				} else {
					task := &queryTask{RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}}, shardclientMgr: manager}
					err = task.queryShard(context.Background(), 1, client, "ch")
				}
				require.Equal(t, merr.Code(sourceErr), merr.Code(err))
				require.Equal(t, merr.GetErrorType(sourceErr), merr.GetErrorType(err))
			})
		}
	}
}

// Run the actual task publication paths through the hedged load balancer.
// A late successful primary must not overwrite the winning replica or MVCC.
func TestHedgedReadPublishesWinnerMetadata(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.ReplicaSelectionPolicy.Key, "latency_weighted")
	pt.Save(pt.ProxyCfg.HedgingEnabled.Key, "true")
	pt.Save(pt.ProxyCfg.HedgingMinDelay.Key, "1ms")
	t.Cleanup(func() {
		pt.Reset(pt.ProxyCfg.ReplicaSelectionPolicy.Key)
		pt.Reset(pt.ProxyCfg.HedgingEnabled.Key)
		pt.Reset(pt.ProxyCfg.HedgingMinDelay.Key)
	})
	for _, kind := range []string{"search", "query"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			primaryStarted, releasePrimary := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releasePrimary) }) }
			defer release()
			wait := func(node int64) error {
				if node == 1 {
					close(primaryStarted)
					select {
					case <-releasePrimary:
					case <-ctx.Done():
						return ctx.Err()
					}
				} else {
					select {
					case <-primaryStarted:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}
			client := &hedgeTaskClient{
				search: func(_ context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
					node := req.GetReq().GetBase().GetTargetID()
					return &internalpb.SearchResults{Status: merr.Success(), ChannelsMvcc: map[string]uint64{"ch": uint64(node * 100)}}, wait(node)
				},
				query: func(_ context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
					node := req.GetReq().GetBase().GetTargetID()
					return &internalpb.RetrieveResults{Status: merr.Success(), ReqID: node}, wait(node)
				},
			}
			manager := &hedgeTaskManager{client: client}
			policy := shardclient.NewLBPolicyImpl(manager)
			searchTask := &searchTask{
				SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}}, shardClientMgr: manager,
				resultBuf: typeutil.NewConcurrentSet[*internalpb.SearchResults](), queryChannelsNode: typeutil.NewConcurrentMap[string, int64](), lb: policy,
			}
			queryTask := &queryTask{
				RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}}, shardclientMgr: manager,
				resultBuf: typeutil.NewConcurrentSet[*internalpb.RetrieveResults](), lb: policy,
			}
			primaryDone := make(chan error, 1)
			exec := func(ctx context.Context, node int64, qn types.QueryNodeClient, channel string) error {
				var err error
				if kind == "search" {
					err = searchTask.searchShard(ctx, node, qn, channel)
				} else {
					err = queryTask.queryShard(ctx, node, qn, channel)
				}
				if node == 1 {
					primaryDone <- err
				}
				return err
			}
			require.NoError(t, policy.ExecuteWithRetry(ctx, shardclient.ChannelWorkload{Channel: "ch", PreferredNodeID: 1, Nq: 1, Hedgeable: true, StatsClass: kind, Exec: exec}))
			release()
			select {
			case err := <-primaryDone:
				require.ErrorIs(t, err, shardclient.ErrAttemptSuperseded)
			case <-ctx.Done():
				t.Fatal("primary did not finish")
			}
			if kind == "search" {
				resultCount := 0
				node, ok := searchTask.queryChannelsNode.Get("ch")
				require.True(t, ok)
				require.EqualValues(t, 2, node)
				searchTask.resultBuf.Range(func(result *internalpb.SearchResults) bool {
					resultCount++
					require.EqualValues(t, 200, result.GetChannelsMvcc()["ch"])
					return true
				})
				require.Equal(t, 1, resultCount)
			} else {
				resultCount := 0
				queryTask.resultBuf.Range(func(result *internalpb.RetrieveResults) bool {
					resultCount++
					require.EqualValues(t, 2, result.GetReqID())
					return true
				})
				require.Equal(t, 1, resultCount)
			}
		})
	}
}

// Exercise the actual Search/Query callback and wire status classification.
// Both RPCs are dispatched before either is released, except the single-attempt case.
func TestHedgedReadInputErrorWaitsForSibling(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.ReplicaSelectionPolicy.Key, "latency_weighted")
	pt.Save(pt.ProxyCfg.HedgingEnabled.Key, "true")
	pt.Save(pt.ProxyCfg.HedgingMinDelay.Key, "1ms")
	t.Cleanup(func() {
		pt.Reset(pt.ProxyCfg.ReplicaSelectionPolicy.Key)
		pt.Reset(pt.ProxyCfg.HedgingEnabled.Key)
		pt.Reset(pt.ProxyCfg.HedgingMinDelay.Key)
	})
	input := merr.WrapErrParameterInvalidMsg("search/query results exceed the maxOutputSize Limit")
	for _, kind := range []string{"search", "query"} {
		for _, tc := range []struct {
			name                         string
			first                        int64
			firstErr, secondErr, wantErr error
			single, cancel               bool
		}{
			{name: "hedge input then primary success", first: 2, firstErr: input},
			{name: "primary input then hedge success", first: 1, firstErr: input},
			{name: "input then system retains input", first: 2, firstErr: input, secondErr: merr.ErrServiceUnavailable, wantErr: input},
			{name: "system then input returns input", first: 1, firstErr: merr.ErrServiceUnavailable, secondErr: input, wantErr: input},
			{name: "both input errors stop retrying", first: 2, firstErr: input, secondErr: input, wantErr: input},
			{name: "caller cancels after input", first: 2, firstErr: input, wantErr: input, cancel: true},
			{name: "single input stops retrying", first: 1, firstErr: input, wantErr: input, single: true},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				started := make(chan int64, 4)
				released := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
				var calls atomic.Int64
				respond := func(node int64) *commonpb.Status {
					calls.Add(1)
					started <- node
					select {
					case <-released[node-1]:
					case <-ctx.Done():
						return merr.Status(ctx.Err())
					}
					if node == tc.first {
						return merr.Status(tc.firstErr)
					}
					return merr.Status(tc.secondErr)
				}
				client := &hedgeTaskClient{
					search: func(_ context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
						node := req.GetReq().GetBase().GetTargetID()
						return &internalpb.SearchResults{Status: respond(node), ChannelsMvcc: map[string]uint64{"ch": uint64(node * 100)}}, nil
					},
					query: func(_ context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
						node := req.GetReq().GetBase().GetTargetID()
						return &internalpb.RetrieveResults{Status: respond(node), ReqID: node}, nil
					},
				}
				manager := &hedgeTaskManager{client: client}
				policy := shardclient.NewLBPolicyImpl(manager)
				st := &searchTask{
					SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}}, shardClientMgr: manager,
					resultBuf: typeutil.NewConcurrentSet[*internalpb.SearchResults](), queryChannelsNode: typeutil.NewConcurrentMap[string, int64](), lb: policy,
				}
				qt := &queryTask{
					RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}}, shardclientMgr: manager,
					resultBuf: typeutil.NewConcurrentSet[*internalpb.RetrieveResults](), lb: policy,
				}
				exec := func(ctx context.Context, node int64, qn types.QueryNodeClient, channel string) error {
					if kind == "search" {
						return st.searchShard(ctx, node, qn, channel)
					}
					return qt.queryShard(ctx, node, qn, channel)
				}
				done := make(chan error, 1)
				go func() {
					done <- policy.ExecuteWithRetry(ctx, shardclient.ChannelWorkload{
						Channel: "ch", PreferredNodeID: 1, Nq: 1, Hedgeable: !tc.single, StatsClass: kind, Exec: exec,
					})
				}()
				attempts := 2
				if tc.single {
					attempts = 1
				}
				for i := 0; i < attempts; i++ {
					select {
					case <-started:
					case <-ctx.Done():
						t.Fatal("attempt was not dispatched")
					}
				}
				close(released[tc.first-1])
				if !tc.single {
					select {
					case err := <-done:
						t.Fatalf("request ended while sibling was pending: %v", err)
					case <-time.After(30 * time.Millisecond):
					}
					if tc.cancel {
						cancel()
					} else {
						close(released[2-tc.first])
					}
				}
				select {
				case err := <-done:
					if tc.wantErr != nil {
						require.ErrorIs(t, err, tc.wantErr)
					} else {
						require.NoError(t, err)
					}
				case <-time.After(time.Second):
					t.Fatal("request did not complete")
				}
				require.EqualValues(t, attempts, calls.Load(), "must not start another retry round")
				count := 0
				winner := 3 - tc.first
				if kind == "search" {
					st.resultBuf.Range(func(result *internalpb.SearchResults) bool {
						count++
						require.EqualValues(t, winner*100, result.GetChannelsMvcc()["ch"])
						return true
					})
					if tc.wantErr == nil {
						node, ok := st.queryChannelsNode.Get("ch")
						require.True(t, ok)
						require.Equal(t, winner, node)
					}
				} else {
					qt.resultBuf.Range(func(result *internalpb.RetrieveResults) bool {
						count++
						require.Equal(t, winner, result.GetReqID())
						return true
					})
				}
				if tc.wantErr != nil {
					require.Zero(t, count)
				} else {
					require.Equal(t, 1, count)
				}
			})
		}
	}
}

func TestFastSkipRequeryAvoidsReplicaDispatch(t *testing.T) {
	paramtable.Init()
	manager := shardclient.NewMockShardClientManager(t)
	manager.EXPECT().GetShardLeaderList(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"empty-channel"}, nil)
	policy := shardclient.NewLBPolicyImpl(manager)
	task := &queryTask{fastSkip: true, channelsMvcc: map[string]uint64{"hit-channel": 10}}
	err := policy.Execute(context.Background(), shardclient.CollectionWorkLoad{
		Nq: 1, Hedgeable: true, StatsClass: "query/Bounded",
		SkipChannel: task.skipRequeryChannel, Exec: task.queryShard,
	})
	require.NoError(t, err)
}
