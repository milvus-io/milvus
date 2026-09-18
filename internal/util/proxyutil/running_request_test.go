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

package proxyutil

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestProxyClientManager_ListRunningRequests(t *testing.T) {
	ctx := context.Background()

	t.Run("no proxy is an error, not an empty answer", func(t *testing.T) {
		pcm := NewProxyClientManager(DefaultProxyCreator)
		requests, nodeResults, err := pcm.ListRunningRequests(ctx, &milvuspb.ListRunningRequestsRequest{})
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
		assert.Empty(t, requests)
		assert.Empty(t, nodeResults)
	})

	t.Run("merges every proxy's requests", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().ListLocalRunningRequests(mock.Anything, mock.Anything).Return(&milvuspb.ListRunningRequestsResponse{
			Status:   merr.Success(),
			Requests: []*milvuspb.RunningRequestInfo{{RequestId: 1, ProxyId: 101}},
		}, nil)
		p2 := mocks.NewMockProxyClient(t)
		p2.EXPECT().ListLocalRunningRequests(mock.Anything, mock.Anything).Return(&milvuspb.ListRunningRequestsResponse{
			Status:   merr.Success(),
			Requests: []*milvuspb.RunningRequestInfo{{RequestId: 2, ProxyId: 102}, {RequestId: 3, ProxyId: 102}},
		}, nil)

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(101, p1)
		pcm.proxyClient.Insert(102, p2)

		requests, nodeResults, err := pcm.ListRunningRequests(ctx, &milvuspb.ListRunningRequestsRequest{})
		assert.NoError(t, err)
		assert.ElementsMatch(t, []int64{1, 2, 3}, lo.Map(requests, func(r *milvuspb.RunningRequestInfo, _ int) int64 {
			return r.GetRequestId()
		}))
		assert.Len(t, nodeResults, 2)
	})

	t.Run("one failing proxy does not hide the others", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().ListLocalRunningRequests(mock.Anything, mock.Anything).Return(&milvuspb.ListRunningRequestsResponse{
			Status:   merr.Success(),
			Requests: []*milvuspb.RunningRequestInfo{{RequestId: 1, ProxyId: 101}},
		}, nil)
		p2 := mocks.NewMockProxyClient(t)
		p2.EXPECT().ListLocalRunningRequests(mock.Anything, mock.Anything).Return(nil, errors.New("proxy is down"))

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(101, p1)
		pcm.proxyClient.Insert(102, p2)

		requests, nodeResults, err := pcm.ListRunningRequests(ctx, &milvuspb.ListRunningRequestsRequest{})
		assert.NoError(t, err, "one unreachable proxy must not fail the whole call")
		require.Len(t, requests, 1, "the healthy proxy's answer must survive")
		assert.Equal(t, int64(1), requests[0].GetRequestId())
		require.Len(t, nodeResults, 2)
		failed, ok := lo.Find(nodeResults, func(r *milvuspb.RunningRequestNodeResult) bool { return r.GetNodeId() == 102 })
		require.True(t, ok)
		assert.False(t, merr.Ok(failed.GetStatus()))
		assert.False(t, failed.GetUnimplemented())
	})

	t.Run("an error only when no proxy answered", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().ListLocalRunningRequests(mock.Anything, mock.Anything).Return(nil, errors.New("proxy is down"))
		p2 := mocks.NewMockProxyClient(t)
		p2.EXPECT().ListLocalRunningRequests(mock.Anything, mock.Anything).Return(&milvuspb.ListRunningRequestsResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady("proxy", 0, "initializing")),
		}, nil)

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(101, p1)
		pcm.proxyClient.Insert(102, p2)

		requests, nodeResults, err := pcm.ListRunningRequests(ctx, &milvuspb.ListRunningRequestsRequest{})
		assert.Error(t, err, "an empty list must not be mistaken for nothing running")
		assert.Empty(t, requests)
		assert.Len(t, nodeResults, 2)
	})

	t.Run("an older proxy is flagged, not counted as a failure", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().ListLocalRunningRequests(mock.Anything, mock.Anything).Return(nil, merr.ErrServiceUnimplemented)

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(101, p1)

		requests, nodeResults, err := pcm.ListRunningRequests(ctx, &milvuspb.ListRunningRequestsRequest{})
		assert.NoError(t, err, "a rolling upgrade is not an error")
		assert.Empty(t, requests)
		require.Len(t, nodeResults, 1)
		assert.True(t, nodeResults[0].GetUnimplemented())
	})
}

func TestProxyClientManager_CancelRequests(t *testing.T) {
	ctx := context.Background()

	t.Run("no proxy is an error, not an empty answer", func(t *testing.T) {
		pcm := NewProxyClientManager(DefaultProxyCreator)
		canceled, notFound, nodeResults, err := pcm.CancelRequests(ctx, &milvuspb.CancelRequestsRequest{RequestIds: []int64{1}})
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
		assert.Empty(t, canceled)
		assert.Empty(t, notFound)
		assert.Empty(t, nodeResults)
	})

	t.Run("an id is not found only when no proxy held it", func(t *testing.T) {
		// Each proxy is asked for both ids and holds one of them: neither
		// proxy's own "not found" may become the cluster's answer.
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().CancelLocalRequests(mock.Anything, mock.Anything).Return(&milvuspb.CancelRequestsResponse{
			Status:   merr.Success(),
			Canceled: []*milvuspb.RunningRequestInfo{{RequestId: 1, ProxyId: 101, ElapsedMs: 5}},
			NotFound: []int64{2, 9},
		}, nil)
		p2 := mocks.NewMockProxyClient(t)
		p2.EXPECT().CancelLocalRequests(mock.Anything, mock.Anything).Return(&milvuspb.CancelRequestsResponse{
			Status:   merr.Success(),
			Canceled: []*milvuspb.RunningRequestInfo{{RequestId: 2, ProxyId: 102, ElapsedMs: 7}},
			NotFound: []int64{1, 9},
		}, nil)

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(101, p1)
		pcm.proxyClient.Insert(102, p2)

		canceled, notFound, nodeResults, err := pcm.CancelRequests(ctx, &milvuspb.CancelRequestsRequest{RequestIds: []int64{1, 2, 9}})
		assert.NoError(t, err)
		assert.ElementsMatch(t, []int64{1, 2}, lo.Map(canceled, func(r *milvuspb.RunningRequestInfo, _ int) int64 {
			return r.GetRequestId()
		}))
		assert.Equal(t, []int64{9}, notFound)
		assert.Len(t, nodeResults, 2)
	})

	t.Run("a failing proxy is reported and the rest still cancel", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().CancelLocalRequests(mock.Anything, mock.Anything).Return(&milvuspb.CancelRequestsResponse{
			Status:   merr.Success(),
			Canceled: []*milvuspb.RunningRequestInfo{{RequestId: 1, ProxyId: 101}},
		}, nil)
		p2 := mocks.NewMockProxyClient(t)
		p2.EXPECT().CancelLocalRequests(mock.Anything, mock.Anything).Return(nil, errors.New("proxy is down"))

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(101, p1)
		pcm.proxyClient.Insert(102, p2)

		canceled, notFound, nodeResults, err := pcm.CancelRequests(ctx, &milvuspb.CancelRequestsRequest{RequestIds: []int64{1, 2}})
		assert.NoError(t, err, "one unreachable proxy must not stop the others from canceling")
		require.Len(t, canceled, 1)
		assert.Equal(t, int64(1), canceled[0].GetRequestId())
		// id 2 may have been held by the proxy that failed to answer
		assert.Equal(t, []int64{2}, notFound)
		assert.Len(t, nodeResults, 2)
	})

	t.Run("a cancel through an unreachable proxy leaves not found unreliable", func(t *testing.T) {
		// id 2 comes back as not found only because the proxy that may hold it
		// never answered, which is what the failed node result is there to say.
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().CancelLocalRequests(mock.Anything, mock.Anything).Return(&milvuspb.CancelRequestsResponse{
			Status:   merr.Success(),
			Canceled: []*milvuspb.RunningRequestInfo{{RequestId: 1, ProxyId: 101}},
			NotFound: []int64{2},
		}, nil)
		p2 := mocks.NewMockProxyClient(t)
		p2.EXPECT().CancelLocalRequests(mock.Anything, mock.Anything).Return(nil, errors.New("proxy is down"))

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(101, p1)
		pcm.proxyClient.Insert(102, p2)

		canceled, notFound, nodeResults, err := pcm.CancelRequests(ctx, &milvuspb.CancelRequestsRequest{RequestIds: []int64{1, 2}})
		assert.NoError(t, err)
		require.Len(t, canceled, 1)
		assert.Equal(t, []int64{2}, notFound)
		failed := lo.Filter(nodeResults, func(r *milvuspb.RunningRequestNodeResult, _ int) bool {
			return !merr.Ok(r.GetStatus())
		})
		require.Len(t, failed, 1, "the unreachable proxy must be reported, or not found would look authoritative")
		assert.Equal(t, int64(102), failed[0].GetNodeId())
	})

	t.Run("a proxy status failure is surfaced", func(t *testing.T) {
		p1 := mocks.NewMockProxyClient(t)
		p1.EXPECT().CancelLocalRequests(mock.Anything, mock.Anything).Return(&milvuspb.CancelRequestsResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady("proxy", 0, "initializing")),
		}, nil)

		pcm := NewProxyClientManager(DefaultProxyCreator)
		pcm.proxyClient.Insert(101, p1)

		canceled, notFound, nodeResults, err := pcm.CancelRequests(ctx, &milvuspb.CancelRequestsRequest{RequestIds: []int64{1}})
		assert.Error(t, err)
		assert.Empty(t, canceled)
		assert.Equal(t, []int64{1}, notFound)
		require.Len(t, nodeResults, 1)
		assert.False(t, merr.Ok(nodeResults[0].GetStatus()))
	})
}
