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

package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type controlledPolicyCoord struct {
	types.MixCoordClient

	mu       sync.Mutex
	onList   func(context.Context, *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error)
	listCall atomic.Int32
}

func (c *controlledPolicyCoord) setListPolicy(fn func(context.Context, *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onList = fn
}

func (c *controlledPolicyCoord) ListPolicy(ctx context.Context, req *internalpb.ListPolicyRequest, _ ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	c.listCall.Add(1)
	c.mu.Lock()
	fn := c.onList
	c.mu.Unlock()
	return fn(ctx, req)
}

func TestRefreshPolicyInfoCacheCancellationAndControls(t *testing.T) {
	t.Cleanup(privilege.ResetPrivilegeCacheForTest)

	initialPolicy := funcutil.PolicyForPrivilege("reader", "Collection", "orders", "Query", "tenant_a")
	initialRole := funcutil.EncodeUserRoleCache("alice", "reader")
	initialVisibility := &internalpb.CollectionVisibilitySnapshot{
		FormatVersion: 1,
		Roles: []*internalpb.RoleCollectionVisibility{{
			Role:  "reader",
			Rules: []*internalpb.CollectionVisibilityRule{{DbName: "tenant_a", CollectionName: "orders"}},
		}},
	}

	coord := &controlledPolicyCoord{}
	coord.setListPolicy(func(context.Context, *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status:               merr.Success(),
			PolicyInfos:          []string{initialPolicy},
			UserRoles:            []string{initialRole},
			CollectionVisibility: initialVisibility,
		}, nil
	})
	require.NoError(t, privilege.InitPrivilegeCache(context.Background(), coord))

	node := &Proxy{mixCoord: coord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	cache := privilege.GetPrivilegeCache()
	require.NotNil(t, cache)

	assertInitialCacheIntact := func() {
		t.Helper()
		require.ElementsMatch(t, []string{"reader"}, cache.GetUserRole("alice"))
		require.Empty(t, cache.GetUserRole("bob"))
		visible, supported := cache.CanSeeCollection("alice", "tenant_a", "orders")
		require.True(t, supported)
		require.True(t, visible)
		visible, supported = cache.CanSeeCollection("bob", "tenant_b", "invoices")
		require.True(t, supported)
		require.False(t, visible)
	}
	assertInitialCacheIntact()

	t.Run("cancels in-flight CacheRefresh after inner ListPolicy starts", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

		innerObservedErr := make(chan error, 1)
		coord.setListPolicy(func(innerCtx context.Context, _ *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			close(entered)
			select {
			case <-innerCtx.Done():
				innerObservedErr <- innerCtx.Err()
				return nil, innerCtx.Err()
			case <-release:
				innerObservedErr <- nil
				return &internalpb.ListPolicyResponse{Status: merr.Success()}, nil
			}
		})

		reqCtx, reqCancel := context.WithCancel(context.Background())
		defer reqCancel()

		type rpcResult struct {
			status *commonpb.Status
			err    error
		}
		done := make(chan rpcResult, 1)
		go func() {
			st, err := node.RefreshPolicyInfoCache(reqCtx, &proxypb.RefreshPolicyInfoCacheRequest{
				OpType: int32(typeutil.CacheRefresh),
			})
			done <- rpcResult{status: st, err: err}
		}()

		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for inner ListPolicy entry barrier")
		}

		reqCancel()

		select {
		case err := <-innerObservedErr:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			releaseOnce.Do(func() { close(release) })
			t.Fatal("inner ListPolicy context did not observe outer request cancellation")
		}

		select {
		case res := <-done:
			require.NoError(t, res.err)
			require.Equal(t, merr.CanceledCode, res.status.GetCode())
			require.ErrorContains(t, merr.Error(res.status), context.Canceled.Error())
		case <-time.After(2 * time.Second):
			t.Fatal("RefreshPolicyInfoCache handler goroutine did not finish after cancellation")
		}

		// Aborting an in-flight refresh must preserve previously cached policies and visibility.
		assertInitialCacheIntact()
	})

	t.Run("already canceled request fails fast without mutating cache", func(t *testing.T) {
		beforeCalls := coord.listCall.Load()
		coord.setListPolicy(func(innerCtx context.Context, _ *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return nil, innerCtx.Err()
		})

		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		directErr := cache.RefreshPolicyInfoWithVisibility(canceledCtx, typeutil.CacheOp{
			OpType: typeutil.CacheRefresh,
		}, nil)
		require.ErrorIs(t, directErr, context.Canceled)

		st, err := node.RefreshPolicyInfoCache(canceledCtx, &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: int32(typeutil.CacheRefresh),
		})
		require.NoError(t, err)
		require.Equal(t, merr.CanceledCode, st.GetCode())
		require.ErrorContains(t, merr.Error(st), context.Canceled.Error())
		require.Equal(t, beforeCalls, coord.listCall.Load())
		assertInitialCacheIntact()
	})

	t.Run("maps gRPC Canceled and DeadlineExceeded from ListPolicy via caller ctx", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			grpcCode codes.Code
			setupCtx func() (context.Context, context.CancelFunc, bool)
			wantCode int32
			wantErr  error
		}{
			{
				name:     "canceled",
				grpcCode: codes.Canceled,
				setupCtx: func() (context.Context, context.CancelFunc, bool) {
					c, cancel := context.WithCancel(context.Background())
					return c, cancel, true
				},
				wantCode: merr.CanceledCode,
				wantErr:  context.Canceled,
			},
			{
				name:     "deadline exceeded",
				grpcCode: codes.DeadlineExceeded,
				setupCtx: func() (context.Context, context.CancelFunc, bool) {
					c, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
					return c, cancel, false
				},
				wantCode: merr.TimeoutCode,
				wantErr:  context.DeadlineExceeded,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				entered := make(chan struct{})
				coord.setListPolicy(func(innerCtx context.Context, _ *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
					close(entered)
					<-innerCtx.Done()
					return nil, status.Error(tc.grpcCode, tc.wantErr.Error())
				})

				reqCtx, reqCancel, triggerCancel := tc.setupCtx()
				defer reqCancel()

				type rpcResult struct {
					status *commonpb.Status
					err    error
				}
				resCh := make(chan rpcResult, 1)
				go func() {
					st, err := node.RefreshPolicyInfoCache(reqCtx, &proxypb.RefreshPolicyInfoCacheRequest{
						OpType: int32(typeutil.CacheRefresh),
					})
					resCh <- rpcResult{status: st, err: err}
				}()

				<-entered
				if triggerCancel {
					reqCancel()
				}

				res := <-resCh
				require.NoError(t, res.err)
				require.Equal(t, tc.wantCode, res.status.GetCode())
				require.ErrorContains(t, merr.Error(res.status), tc.wantErr.Error())
				assertInitialCacheIntact()
			})
		}
	})

	t.Run("normal CacheRefresh replaces policies, roles, and visibility", func(t *testing.T) {
		updatedPolicy := funcutil.PolicyForPrivilege("auditor", "Collection", "invoices", "Query", "tenant_b")
		updatedRole := funcutil.EncodeUserRoleCache("bob", "auditor")
		updatedVisibility := &internalpb.CollectionVisibilitySnapshot{
			FormatVersion: 1,
			Roles: []*internalpb.RoleCollectionVisibility{{
				Role:  "auditor",
				Rules: []*internalpb.CollectionVisibilityRule{{DbName: "tenant_b", CollectionName: "invoices"}},
			}},
		}
		coord.setListPolicy(func(innerCtx context.Context, _ *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			require.NoError(t, innerCtx.Err())
			return &internalpb.ListPolicyResponse{
				Status:               merr.Success(),
				PolicyInfos:          []string{updatedPolicy},
				UserRoles:            []string{updatedRole},
				CollectionVisibility: updatedVisibility,
			}, nil
		})

		st, err := node.RefreshPolicyInfoCache(context.Background(), &proxypb.RefreshPolicyInfoCacheRequest{
			OpType: int32(typeutil.CacheRefresh),
		})
		require.NoError(t, err)
		require.NoError(t, merr.Error(st))

		require.Empty(t, cache.GetUserRole("alice"))
		require.ElementsMatch(t, []string{"auditor"}, cache.GetUserRole("bob"))
		visible, supported := cache.CanSeeCollection("alice", "tenant_a", "orders")
		require.True(t, supported)
		require.False(t, visible)
		visible, supported = cache.CanSeeCollection("bob", "tenant_b", "invoices")
		require.True(t, supported)
		require.True(t, visible)
	})
}
