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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/reqregistry"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestOperatorCancelWhileWaitingForAutoLoad covers the part of a DQL request
// that runs before any task exists: the retry loop asks the coordinator to
// load the collection and waits. The request must already be registered at
// that point, an operator cancel must end the wait, and the client must get
// ErrRequestCanceled rather than a plain context error.
func TestOperatorCancelWhileWaitingForAutoLoad(t *testing.T) {
	enableAutoLoad(t)
	oldRateCol := rateCol
	t.Cleanup(func() { rateCol = oldRateCol })

	for _, tc := range []struct {
		name string
		typ  string
		call func(ctx context.Context, node *Proxy) *commonpb.Status
	}{
		{"search", reqregistry.TypeSearch, func(ctx context.Context, node *Proxy) *commonpb.Status {
			rsp, err := node.Search(ctx, &milvuspb.SearchRequest{DbName: "db", CollectionName: "collection", Nq: 1})
			require.NoError(t, err)
			return rsp.GetStatus()
		}},
		{"hybrid search", reqregistry.TypeHybridSearch, func(ctx context.Context, node *Proxy) *commonpb.Status {
			rsp, err := node.HybridSearch(ctx, &milvuspb.HybridSearchRequest{DbName: "db", CollectionName: "collection"})
			require.NoError(t, err)
			return rsp.GetStatus()
		}},
		{"query", reqregistry.TypeQuery, func(ctx context.Context, node *Proxy) *commonpb.Status {
			rsp, err := node.Query(ctx, &milvuspb.QueryRequest{DbName: "db", CollectionName: "collection", Expr: "id > 0"})
			require.NoError(t, err)
			return rsp.GetStatus()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coord := mocks.NewMockMixCoordClient(t)
			waiting := make(chan struct{})
			coord.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, _ *querypb.EnsureCollectionReadyRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
					close(waiting)
					<-ctx.Done()
					return nil, ctx.Err()
				}).Once()
			node := &Proxy{
				mixCoord:  coord,
				metaCache: mockSearchCollectionMeta(t, 100, []string{"v0"}),
				requests:  newTestRequests(),
			}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			require.NoError(t, node.initRateCollector())

			done := make(chan *commonpb.Status, 1)
			go func() { done <- tc.call(context.Background(), node) }()
			awaitAutoLoadSchedulerResult(t, waiting)

			listed := node.listRunningRequests(reqregistry.Filter{})
			require.Len(t, listed, 1, "the request is listable while it waits for the load")
			require.Equal(t, tc.typ, listed[0].Type)

			canceled, notFound := node.cancelRequests(context.Background(), []string{listed[0].RequestID}, "", "root", "test")
			require.Len(t, canceled, 1)
			require.Empty(t, notFound)

			status := awaitAutoLoadSchedulerResult(t, done)
			require.Equal(t, merr.RequestCanceledCode, status.GetCode())
			require.Empty(t, node.listRunningRequests(reqregistry.Filter{}), "the entry is removed when the RPC returns")
		})
	}
}
