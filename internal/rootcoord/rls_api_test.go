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

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRLSProtoRequestConversion(t *testing.T) {
	req := createRowPolicyRequestFromProto(&milvuspb.CreateRowPolicyRequest{
		DbName:         "db",
		CollectionName: "coll",
		PolicyName:     "tenant",
		PolicyType:     milvuspb.RowPolicyType_RowPolicyTypeRestrictive.Enum(),
		Actions: []milvuspb.RowPolicyAction{
			milvuspb.RowPolicyAction_Query,
			milvuspb.RowPolicyAction_Upsert,
		},
		UsingExpr:   "tenant == $current_principal",
		CheckExpr:   "tenant == $current_principal",
		Description: "tenant isolation",
	})

	require.Equal(t, "db", req.GetDbName())
	require.Equal(t, "coll", req.GetCollectionName())
	require.Equal(t, "tenant", req.GetPolicyName())
	require.Equal(t, rlsutil.PolicyTypeRestrictive, req.GetPolicyType())
	require.Equal(t, []rlsutil.PolicyAction{rlsutil.PolicyActionQuery, rlsutil.PolicyActionUpsert}, req.GetActions())
	defaultTypeReq := createRowPolicyRequestFromProto(&milvuspb.CreateRowPolicyRequest{})
	require.Equal(t, rlsutil.PolicyTypePermissive, defaultTypeReq.GetPolicyType())
	require.Nil(t, createRowPolicyRequestFromProto(nil))
	require.Nil(t, updateRowPolicyRequestFromProto(nil))
}

func TestRLSPolicyActionValuesMatchPublicProto(t *testing.T) {
	require.Equal(t, int32(milvuspb.RowPolicyAction_Query), int32(rlsutil.PolicyActionQuery))
	require.Equal(t, int32(milvuspb.RowPolicyAction_Search), int32(rlsutil.PolicyActionSearch))
	require.Equal(t, int32(milvuspb.RowPolicyAction_Insert), int32(rlsutil.PolicyActionInsert))
	require.Equal(t, int32(milvuspb.RowPolicyAction_Delete), int32(rlsutil.PolicyActionDelete))
	require.Equal(t, int32(milvuspb.RowPolicyAction_Upsert), int32(rlsutil.PolicyActionUpsert))
	require.Equal(t, int32(milvuspb.RowPolicyAction_QueryIterator), int32(rlsutil.PolicyActionQueryIterator))
	require.Equal(t, int32(milvuspb.RowPolicyAction_SearchIterator), int32(rlsutil.PolicyActionSearchIterator))
	require.Equal(t, int32(milvuspb.RowPolicyAction_HybridSearch), int32(rlsutil.PolicyActionHybridSearch))
}

func TestCoreRejectsLegacyRowPolicyRoles(t *testing.T) {
	core := newTestCore()

	status, err := core.CreateRowPolicy(context.Background(), &milvuspb.CreateRowPolicyRequest{Roles: []string{"reader"}})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrParameterInvalid)

	status, err = core.UpdateRowPolicy(context.Background(), &milvuspb.UpdateRowPolicyRequest{Roles: []string{"reader"}})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrParameterInvalid)
}

func TestCoreListRowPoliciesProtoConversion(t *testing.T) {
	ctx := context.Background()
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().ListRLSPolicies(mock.Anything, mock.MatchedBy(func(req *rlsutil.ListRowPoliciesRequest) bool {
		return req.GetDbName() == "db" && req.GetCollectionName() == "coll"
	})).Return([]*rlsutil.RowPolicy{
		{
			PolicyName:  "tenant",
			PolicyType:  rlsutil.PolicyTypePermissive,
			Actions:     []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:   "tenant == $current_principal",
			Description: "tenant isolation",
			PolicyId:    100,
		},
	}, nil).Once()

	core := newTestCore(withHealthyCode(), withMeta(meta))
	resp, err := core.ListRowPolicies(ctx, &milvuspb.ListRowPoliciesRequest{
		DbName:         "db",
		CollectionName: "coll",
	})
	require.NoError(t, err)
	require.True(t, merr.Ok(resp.GetStatus()))
	require.Equal(t, "db", resp.GetDbName())
	require.Equal(t, "coll", resp.GetCollectionName())
	require.Len(t, resp.GetPolicies(), 1)
	require.Equal(t, milvuspb.RowPolicyType_RowPolicyTypePermissive, resp.GetPolicies()[0].GetPolicyType())
	require.Equal(t, []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query}, resp.GetPolicies()[0].GetActions())
	require.Empty(t, resp.GetPolicies()[0].GetRoles())
	require.Zero(t, resp.GetPolicies()[0].GetCreatedAt())

	nilResp, err := core.ListRowPolicies(ctx, nil)
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(nilResp.GetStatus()), merr.ErrParameterInvalid)
}
