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

package ddl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDescribeCollectionInternalMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("alice:password")))
	ctx = metadata.AppendToOutgoingContext(ctx, util.HeaderAuthorize, crypto.Base64Encode("stale:identity"), "request-id", "original")
	for _, internal := range []bool{false, true} {
		mix := mocks.NewMockMixCoordClient(t)
		mix.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
			func(rpcCtx context.Context, _ *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
				md, _ := metadata.FromOutgoingContext(rpcCtx)
				if internal {
					require.Empty(t, md.Get(util.HeaderAuthorize))
				} else {
					require.Equal(t, []string{crypto.Base64Encode("alice:alice")}, md.Get(util.HeaderAuthorize))
				}
				require.Equal(t, []string{"original"}, md.Get("request-id"))
				return &milvuspb.DescribeCollectionResponse{
					Status: merr.Success(), DbName: "tenant_b", CollectionID: 77,
					Schema: &schemapb.CollectionSchema{Name: "orders"},
				}, nil
			}).Once()
		task := &DescribeCollectionTask{mixCoord: mix, DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{DbName: "tenant_a", CollectionName: "orders_alias"}}
		if internal {
			task.UseInternalMetadata()
		}
		require.NoError(t, task.Execute(ctx))
		require.Equal(t, "tenant_b", task.result.GetDbName())
		require.Equal(t, "orders", task.result.GetSchema().GetName())
		require.EqualValues(t, 77, task.result.GetCollectionID())
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{crypto.Base64Encode("stale:identity")}, md.Get(util.HeaderAuthorize), "parent context is unchanged")
}

func TestDescribeCollectionInternalMetadataMissingDatabase(t *testing.T) {
	mix := mocks.NewMockMixCoordClient(t)
	mix.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(
		&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(), CollectionID: 77,
			Schema: &schemapb.CollectionSchema{Name: "orders"},
		}, nil).Once()
	task := &DescribeCollectionTask{mixCoord: mix, DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{DbName: "untrusted_hint", CollectionID: 77}}
	task.UseInternalMetadata()
	require.ErrorIs(t, task.Execute(context.Background()), merr.ErrServiceUnavailable)
}
