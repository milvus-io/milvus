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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func enableCompactionTargetProxyTests(t *testing.T) {
	t.Helper()
	require.NoError(t, Params.Save(Params.DataCoordCfg.EnableTargetBasedCompaction.Key, "true"))
	require.NoError(t, Params.Save(Params.DataCoordCfg.EnableCompaction.Key, "true"))
	t.Cleanup(func() {
		Params.Reset(Params.DataCoordCfg.EnableTargetBasedCompaction.Key)
		Params.Reset(Params.DataCoordCfg.EnableCompaction.Key)
	})
}

func TestProxyCreateCompactionTargetDispatchesOnce(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *datapb.CreateCompactionTargetResponse
		rpcErr   error
		want     error
	}{
		{
			name:     "business status",
			response: &datapb.CreateCompactionTargetResponse{Status: merr.Status(merr.ErrServiceNotReady)},
			want:     merr.ErrServiceNotReady,
		},
		{
			name:   "response lost",
			rpcErr: status.Error(codes.Unavailable, "response lost"),
			want:   merr.ErrCompactionTargetCreateOutcomeUnknown,
		},
		{
			name:     "missing status",
			response: &datapb.CreateCompactionTargetResponse{TargetId: 1},
			want:     merr.ErrCompactionTargetCreateOutcomeUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enableCompactionTargetProxyTests(t)
			require.NoError(t, Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "false"))
			t.Cleanup(func() { Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key) })
			coord := mocks.NewMockMixCoordClient(t)
			identity := &milvuspb.DescribeCollectionResponse{
				Status: merr.Success(), CollectionID: 41, DbId: 2, DbName: "default",
				Schema: &schemapb.CollectionSchema{Name: "records"}, UpdateTimestamp: 10, UpdateTimestampStr: "10",
			}
			coord.EXPECT().DescribeCollection(mock.Anything, mock.MatchedBy(func(req *milvuspb.DescribeCollectionRequest) bool {
				return req.GetDbName() == "default" && req.GetCollectionName() == "records"
			})).Return(identity, nil).Once()
			coord.EXPECT().DescribeCollection(mock.Anything, mock.MatchedBy(func(req *milvuspb.DescribeCollectionRequest) bool {
				return req.GetCollectionID() == 41 && req.GetDbName() == ""
			})).Return(identity, nil).Once()
			coord.EXPECT().CreateCompactionTarget(mock.Anything, mock.MatchedBy(func(req *datapb.CreateCompactionTargetRequest) bool {
				return req.GetCollection().GetCollectionId() == 41 && req.GetCollection().GetRevision() == 10
			})).Return(tc.response, tc.rpcErr).Once()
			proxy := &Proxy{mixCoord: coord, targetCompactionEnabled: true}
			proxy.UpdateStateCode(commonpb.StateCode_Healthy)
			ctx := NewContextWithMetadata(context.Background(), "user", "another_db")
			result, err := proxy.CreateCompactionTarget(ctx, &milvuspb.CreateCompactionTargetRequest{
				CollectionName: "records",
				Target:         &milvuspb.CreateCompactionTargetRequest_Rewrite{Rewrite: &milvuspb.RewriteTargetSpec{}},
			})
			require.NoError(t, err)
			require.ErrorIs(t, merr.Error(result.GetStatus()), tc.want)
			require.Zero(t, result.GetTargetId())
		})
	}
}

type targetAuthorizationCoordinator struct {
	*metadataPrivilegeCoordinator
	resource                *datapb.CompactionTargetResource
	authorizedDescribeCalls int
}

func (c *targetAuthorizationCoordinator) GetCompactionTargetBinding(context.Context, *datapb.GetCompactionTargetBindingRequest, ...grpc.CallOption) (*datapb.GetCompactionTargetBindingResponse, error) {
	return &datapb.GetCompactionTargetBindingResponse{Status: merr.Success(), Resource: c.resource}, nil
}

func (c *targetAuthorizationCoordinator) DescribeCompactionTarget(_ context.Context, req *datapb.DescribeCompactionTargetRequest, _ ...grpc.CallOption) (*datapb.DescribeCompactionTargetResponse, error) {
	c.authorizedDescribeCalls++
	return &datapb.DescribeCompactionTargetResponse{
		Status: merr.Success(), Target: &datapb.CompactionTargetReadModel{
			TargetId: req.GetTargetId(), Resource: c.resource, Intent: datapb.TargetIntent_INTENT_REWRITE,
			Rewrite: &datapb.CompactionTargetRewrite{},
		},
	}, nil
}

func (c *targetAuthorizationCoordinator) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	resp, err := c.metadataPrivilegeCoordinator.DescribeCollection(ctx, req, opts...)
	resp.UpdateTimestamp = 10
	resp.UpdateTimestampStr = "10"
	return resp, err
}

func TestProxyDescribeCompactionTargetAuthorizesStableIdentity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		grantDatabase string
		revision      uint64
		want          error
	}{
		{name: "grant in current database", grantDatabase: "tenant_b", revision: 10},
		{name: "same name in caller database", grantDatabase: "tenant_a", revision: 10, want: merr.ErrPrivilegeNotPermitted},
		{name: "collection identity changed", grantDatabase: "tenant_b", revision: 9, want: merr.ErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enableCompactionTargetProxyTests(t)
			metadata, setPolicy := setupMetadataPrivileges(t)
			setPolicy(tc.grantDatabase, commonpb.ObjectPrivilege_PrivilegeCompaction)
			coord := &targetAuthorizationCoordinator{
				metadataPrivilegeCoordinator: metadata,
				resource: &datapb.CompactionTargetResource{
					CollectionId: 41, DbId: 1, DbName: "tenant_b", CollectionName: "records", Revision: tc.revision,
				},
			}
			proxy := &Proxy{mixCoord: coord, targetCompactionEnabled: true}
			proxy.UpdateStateCode(commonpb.StateCode_Healthy)
			ctx := NewContextWithMetadata(t.Context(), "reader", "tenant_a")
			result, err := proxy.DescribeCompactionTarget(ctx, &milvuspb.DescribeCompactionTargetRequest{TargetId: 100})
			require.NoError(t, err)
			if tc.want != nil {
				require.ErrorIs(t, merr.Error(result.GetStatus()), tc.want)
				require.Nil(t, result.GetTarget())
				return
			}
			require.NoError(t, merr.Error(result.GetStatus()))
			require.Equal(t, int64(41), result.GetTarget().GetCollectionId())
			require.Equal(t, "tenant_b", result.GetTarget().GetDbName())
		})
	}
}
