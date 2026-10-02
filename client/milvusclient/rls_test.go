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

package milvusclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type RLSSuite struct {
	MockSuiteBase
}

func (s *RLSSuite) TestOptions() {
	createReq := NewCreateRowPolicyOption(
		"collection", "policy", milvuspb.RowPolicyType_RowPolicyTypePermissive,
		milvuspb.RowPolicyAction_Query, milvuspb.RowPolicyAction_Insert,
	).WithDbName("db").
		WithUsingExpr("tenant == $current_principal").
		WithCheckExpr("tenant == $current_principal").
		WithDescription("description").Request()
	s.NotNil(createReq.GetBase())
	s.Equal("db", createReq.GetDbName())
	s.Equal("collection", createReq.GetCollectionName())
	s.Equal("policy", createReq.GetPolicyName())
	s.Equal([]milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Query, milvuspb.RowPolicyAction_Insert}, createReq.GetActions())
	s.Equal("tenant == $current_principal", createReq.GetUsingExpr())
	s.Equal("tenant == $current_principal", createReq.GetCheckExpr())
	s.Equal("description", createReq.GetDescription())
	s.Equal(milvuspb.RowPolicyType_RowPolicyTypePermissive, createReq.GetPolicyType())

	updateReq := NewUpdateRowPolicyOption(
		"collection", "policy", milvuspb.RowPolicyType_RowPolicyTypeRestrictive,
		milvuspb.RowPolicyAction_Search,
	).WithDbName("db").
		WithUsingExpr("owner == $current_principal").
		WithCheckExpr("owner == $current_principal").
		WithDescription("updated").Request()
	s.NotNil(updateReq.GetBase())
	s.Equal("db", updateReq.GetDbName())
	s.Equal("collection", updateReq.GetCollectionName())
	s.Equal("policy", updateReq.GetPolicyName())
	s.Equal([]milvuspb.RowPolicyAction{milvuspb.RowPolicyAction_Search}, updateReq.GetActions())
	s.Equal("owner == $current_principal", updateReq.GetUsingExpr())
	s.Equal("owner == $current_principal", updateReq.GetCheckExpr())
	s.Equal("updated", updateReq.GetDescription())
	s.Equal(milvuspb.RowPolicyType_RowPolicyTypeRestrictive, updateReq.GetPolicyType())

	dropReq := NewDropRowPolicyOption("collection", "policy").WithDbName("db").Request()
	s.NotNil(dropReq.GetBase())
	s.Equal("db", dropReq.GetDbName())
	s.Equal("collection", dropReq.GetCollectionName())
	s.Equal("policy", dropReq.GetPolicyName())

	listReq := NewListRowPoliciesOption("collection").WithDbName("db").Request()
	s.NotNil(listReq.GetBase())
	s.Equal("db", listReq.GetDbName())
	s.Equal("collection", listReq.GetCollectionName())

	setReq := NewSetRLSPrincipalTagsOption("collection", "alice", `{"department":"sales"}`).WithDbName("db").Request()
	s.NotNil(setReq.GetBase())
	s.Equal("db", setReq.GetDbName())
	s.Equal("collection", setReq.GetCollectionName())
	s.Equal("alice", setReq.GetPrincipalName())
	s.Equal(`{"department":"sales"}`, setReq.GetTags())

	getReq := NewGetRLSPrincipalTagsOption("collection", "alice").WithDbName("db").Request()
	s.NotNil(getReq.GetBase())
	s.Equal("db", getReq.GetDbName())
	s.Equal("collection", getReq.GetCollectionName())
	s.Equal("alice", getReq.GetPrincipalName())

	listPrincipalsReq := NewListRLSPrincipalsOption("collection").WithDbName("db").Request()
	s.NotNil(listPrincipalsReq.GetBase())
	s.Equal("db", listPrincipalsReq.GetDbName())
	s.Equal("collection", listPrincipalsReq.GetCollectionName())

	deleteReq := NewDeleteRLSPrincipalTagsOption("collection", "alice", "department", "region").WithDbName("db").Request()
	s.NotNil(deleteReq.GetBase())
	s.Equal("db", deleteReq.GetDbName())
	s.Equal("collection", deleteReq.GetCollectionName())
	s.Equal("alice", deleteReq.GetPrincipalName())
	s.Equal([]string{"department", "region"}, deleteReq.GetTagKeys())
}

func (s *RLSSuite) TestAPIs() {
	ctx := context.Background()

	s.mock.EXPECT().CreateRowPolicy(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *milvuspb.CreateRowPolicyRequest) (*commonpb.Status, error) {
			s.Equal("create", req.GetPolicyName())
			return merr.Success(), nil
		}).Once()
	s.NoError(s.client.CreateRowPolicy(ctx, NewCreateRowPolicyOption(
		"collection", "create", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query,
	)))

	s.mock.EXPECT().UpdateRowPolicy(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *milvuspb.UpdateRowPolicyRequest) (*commonpb.Status, error) {
			s.Equal("update", req.GetPolicyName())
			return merr.Success(), nil
		}).Once()
	s.NoError(s.client.UpdateRowPolicy(ctx, NewUpdateRowPolicyOption(
		"collection", "update", milvuspb.RowPolicyType_RowPolicyTypeRestrictive, milvuspb.RowPolicyAction_Search,
	)))

	s.mock.EXPECT().DropRowPolicy(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *milvuspb.DropRowPolicyRequest) (*commonpb.Status, error) {
			s.Equal("drop", req.GetPolicyName())
			return merr.Success(), nil
		}).Once()
	s.NoError(s.client.DropRowPolicy(ctx, NewDropRowPolicyOption("collection", "drop")))

	s.mock.EXPECT().ListRowPolicies(mock.Anything, mock.Anything).Return(&milvuspb.ListRowPoliciesResponse{
		Status:   merr.Success(),
		Policies: []*milvuspb.RowPolicy{{PolicyName: "policy"}},
	}, nil).Once()
	policies, err := s.client.ListRowPolicies(ctx, NewListRowPoliciesOption("collection"))
	s.NoError(err)
	s.Require().Len(policies, 1)
	s.Equal("policy", policies[0].GetPolicyName())

	s.mock.EXPECT().SetRLSPrincipalTags(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *milvuspb.SetRLSPrincipalTagsRequest) (*commonpb.Status, error) {
			s.Equal("alice", req.GetPrincipalName())
			return merr.Success(), nil
		}).Once()
	s.NoError(s.client.SetRLSPrincipalTags(ctx, NewSetRLSPrincipalTagsOption("collection", "alice", `{"department":"sales"}`)))

	s.mock.EXPECT().GetRLSPrincipalTags(mock.Anything, mock.Anything).Return(&milvuspb.GetRLSPrincipalTagsResponse{
		Status: merr.Success(),
		Tags:   `{"department":"sales"}`,
	}, nil).Once()
	tags, err := s.client.GetRLSPrincipalTags(ctx, NewGetRLSPrincipalTagsOption("collection", "alice"))
	s.NoError(err)
	s.Equal(`{"department":"sales"}`, tags)

	s.mock.EXPECT().ListRLSPrincipals(mock.Anything, mock.Anything).Return(&milvuspb.ListRLSPrincipalsResponse{
		Status:         merr.Success(),
		PrincipalNames: []string{"alice", "bob"},
	}, nil).Once()
	principals, err := s.client.ListRLSPrincipals(ctx, NewListRLSPrincipalsOption("collection"))
	s.NoError(err)
	s.Equal([]string{"alice", "bob"}, principals)

	s.mock.EXPECT().DeleteRLSPrincipalTags(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *milvuspb.DeleteRLSPrincipalTagsRequest) (*commonpb.Status, error) {
			s.Equal([]string{"department"}, req.GetTagKeys())
			return merr.Success(), nil
		}).Once()
	s.NoError(s.client.DeleteRLSPrincipalTags(ctx, NewDeleteRLSPrincipalTagsOption("collection", "alice", "department")))
}

func (s *RLSSuite) TestErrors() {
	ctx := context.Background()

	nilOptionCalls := map[string]func() error{
		"create policy": func() error { return s.client.CreateRowPolicy(ctx, nil) },
		"update policy": func() error { return s.client.UpdateRowPolicy(ctx, nil) },
		"drop policy":   func() error { return s.client.DropRowPolicy(ctx, nil) },
		"list policies": func() error {
			_, err := s.client.ListRowPolicies(ctx, nil)
			return err
		},
		"set tags": func() error { return s.client.SetRLSPrincipalTags(ctx, nil) },
		"get tags": func() error {
			_, err := s.client.GetRLSPrincipalTags(ctx, nil)
			return err
		},
		"list principals": func() error {
			_, err := s.client.ListRLSPrincipals(ctx, nil)
			return err
		},
		"delete tags": func() error { return s.client.DeleteRLSPrincipalTags(ctx, nil) },
	}
	for name, call := range nilOptionCalls {
		s.Run(name+" nil option", func() {
			s.ErrorIs(call(), merr.ErrParameterInvalid)
		})
	}

	s.Run("rpc error", func() {
		s.mock.EXPECT().CreateRowPolicy(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()
		s.Error(s.client.CreateRowPolicy(ctx, NewCreateRowPolicyOption(
			"collection", "policy", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Query,
		)))
	})

	s.Run("status error", func() {
		s.mock.EXPECT().ListRowPolicies(mock.Anything, mock.Anything).Return(&milvuspb.ListRowPoliciesResponse{
			Status: merr.Status(merr.ErrParameterInvalid),
		}, nil).Once()
		policies, err := s.client.ListRowPolicies(ctx, NewListRowPoliciesOption("collection"))
		s.Error(err)
		s.Nil(policies)
	})
}

func TestRLS(t *testing.T) {
	suite.Run(t, new(RLSSuite))
}
