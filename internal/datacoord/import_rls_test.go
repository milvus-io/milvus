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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	internalmocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestResolveImportRLSPredicate(t *testing.T) {
	paramtable.Init()
	gate := &paramtable.Get().ProxyCfg.RLSImportEnforcementEnabled
	oldGate := gate.SwapTempValue("true")
	t.Cleanup(func() { gate.SwapTempValue(oldGate) })
	schema := &schemapb.CollectionSchema{
		Name:       "test",
		Properties: []*commonpb.KeyValuePair{{Key: common.RLSEnabledKey, Value: "true"}},
		Fields: []*schemapb.FieldSchema{{
			FieldID: 100, Name: "tenant", DataType: schemapb.DataType_VarChar,
		}},
	}
	request := &internalpb.ImportRequestInternal{
		CollectionID: 10,
		Schema:       schema,
		RlsPrincipal: "alice",
	}

	t.Run("compile ordered metadata snapshot", func(t *testing.T) {
		mixCoord := internalmocks.NewMixCoord(t)
		mixCoord.EXPECT().GetRLSMetadata(mock.Anything, mock.MatchedBy(func(req *rootcoordpb.GetRLSMetadataRequest) bool {
			return req.GetCollectionId() == 10 && req.GetKind() == rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES
		})).Return(&rootcoordpb.GetRLSMetadataResponse{
			Status:       merr.Success(),
			CollectionId: 10,
			Policies: []*rootcoordpb.RLSPolicyInfo{{
				CollectionId: 10,
				PolicyId:     20,
				PolicyName:   "tenant_check",
				PolicyType:   milvuspb.RowPolicyType_RowPolicyTypePermissive,
				Actions:      []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Insert},
				CheckExpr:    "tenant == $current_principal_tags['tenant']",
			}},
		}, nil).Once()
		mixCoord.EXPECT().GetRLSMetadata(mock.Anything, mock.MatchedBy(func(req *rootcoordpb.GetRLSMetadataRequest) bool {
			return req.GetCollectionId() == 10 && req.GetKind() == rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_PRINCIPALS && req.GetPrincipalName() == "alice"
		})).Return(&rootcoordpb.GetRLSMetadataResponse{
			Status:       merr.Success(),
			CollectionId: 10,
			Principals: []*rootcoordpb.RLSPrincipalInfo{{
				CollectionId:  10,
				PrincipalName: "alice",
				Tags:          `{"tenant":"acme"}`,
			}},
		}, nil).Once()

		predicate, err := (&Server{mixCoord: mixCoord}).resolveImportRLSPredicate(context.Background(), request)
		require.NoError(t, err)
		require.NotNil(t, predicate)
		field := func(value string) []*schemapb.FieldData {
			return []*schemapb.FieldData{{
				Type:    schemapb.DataType_VarChar,
				FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{value}}},
				}},
			}}
		}
		require.NoError(t, rlsutil.ValidateRowsByPredicate(context.Background(), field("acme"), 1, predicate, "import", "check"))
		require.ErrorIs(t, rlsutil.ValidateRowsByPredicate(context.Background(), field("other"), 1, predicate, "import", "check"), merr.ErrPrivilegeNotPermitted)
	})

	t.Run("metadata failure is classified retryable", func(t *testing.T) {
		mixCoord := internalmocks.NewMixCoord(t)
		mixCoord.EXPECT().GetRLSMetadata(mock.MatchedBy(func(ctx context.Context) bool {
			_, ok := ctx.Deadline()
			return ok
		}), mock.Anything).Return(nil, status.Error(codes.Unavailable, "unavailable")).Once()

		predicate, err := (&Server{mixCoord: mixCoord}).resolveImportRLSPredicate(context.Background(), request)
		require.Nil(t, predicate)
		require.Error(t, err)
		require.True(t, merr.IsRetryableErr(err))
	})

	t.Run("metadata storage failure is classified retryable", func(t *testing.T) {
		mixCoord := internalmocks.NewMixCoord(t)
		mixCoord.EXPECT().GetRLSMetadata(mock.Anything, mock.Anything).Return(&rootcoordpb.GetRLSMetadataResponse{
			Status: merr.Status(merr.WrapErrIoFailedReason("etcd read failed")),
		}, nil).Once()

		predicate, err := (&Server{mixCoord: mixCoord}).resolveImportRLSPredicate(context.Background(), request)
		require.Nil(t, predicate)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.ErrorIs(t, err, merr.ErrIoFailed)
		require.True(t, merr.IsRetryableErr(err))
	})

	t.Run("metadata integrity failure is terminal", func(t *testing.T) {
		mixCoord := internalmocks.NewMixCoord(t)
		mixCoord.EXPECT().GetRLSMetadata(mock.Anything, mock.Anything).Return(&rootcoordpb.GetRLSMetadataResponse{
			Status: merr.Status(merr.WrapErrDataIntegrityMsg("corrupt RLS metadata")),
		}, nil).Once()

		predicate, err := (&Server{mixCoord: mixCoord}).resolveImportRLSPredicate(context.Background(), request)
		require.Nil(t, predicate)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		require.False(t, merr.IsRetryableErr(err))
	})

	t.Run("authorized skip avoids metadata", func(t *testing.T) {
		skipped := proto.Clone(request).(*internalpb.ImportRequestInternal)
		skipped.SkipRls = true

		predicate, err := (&Server{}).resolveImportRLSPredicate(context.Background(), skipped)
		require.Nil(t, predicate)
		require.NoError(t, err)
	})

	t.Run("force rejects stale authorized skip", func(t *testing.T) {
		skipped := proto.Clone(request).(*internalpb.ImportRequestInternal)
		skipped.SkipRls = true
		skipped.Schema.Properties = append(skipped.Schema.Properties,
			&commonpb.KeyValuePair{Key: common.RLSForceKey, Value: "true"})

		predicate, err := (&Server{}).resolveImportRLSPredicate(context.Background(), skipped)
		require.Nil(t, predicate)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})

	t.Run("principal-only predicate skips tag lookup", func(t *testing.T) {
		mixCoord := internalmocks.NewMixCoord(t)
		mixCoord.EXPECT().GetRLSMetadata(mock.Anything, mock.MatchedBy(func(req *rootcoordpb.GetRLSMetadataRequest) bool {
			return req.GetCollectionId() == 10 && req.GetKind() == rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES
		})).Return(&rootcoordpb.GetRLSMetadataResponse{
			Status:       merr.Success(),
			CollectionId: 10,
			Policies: []*rootcoordpb.RLSPolicyInfo{{
				CollectionId: 10,
				PolicyId:     20,
				PolicyName:   "principal_check",
				PolicyType:   milvuspb.RowPolicyType_RowPolicyTypePermissive,
				Actions:      []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Insert},
				CheckExpr:    "tenant == $current_principal",
			}},
		}, nil).Once()

		predicate, err := (&Server{mixCoord: mixCoord}).resolveImportRLSPredicate(context.Background(), request)
		require.NotNil(t, predicate)
		require.NoError(t, err)
	})
}

func TestResolveImportRLSPredicateRejectsBeforeClusterUpgrade(t *testing.T) {
	paramtable.Init()
	gate := &paramtable.Get().ProxyCfg.RLSImportEnforcementEnabled
	oldGate := gate.SwapTempValue("false")
	t.Cleanup(func() { gate.SwapTempValue(oldGate) })

	_, err := (&Server{}).resolveImportRLSPredicate(context.Background(), &internalpb.ImportRequestInternal{
		CollectionID: 10,
		Schema: &schemapb.CollectionSchema{
			Properties: []*commonpb.KeyValuePair{{Key: common.RLSEnabledKey, Value: "true"}},
		},
		RlsPrincipal: "alice",
		SkipRls:      true,
	})
	require.ErrorIs(t, err, merr.ErrImportSysFailed)
	require.False(t, merr.IsRetryableErr(err))
}
