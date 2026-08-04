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
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (c *Core) CreateRowPolicy(ctx context.Context, req *milvuspb.CreateRowPolicyRequest) (*commonpb.Status, error) {
	if req != nil {
		if err := rlsutil.ValidatePolicyRoles(req.GetRoles()); err != nil {
			return merr.Status(err), nil
		}
	}
	return c.createRowPolicy(ctx, createRowPolicyRequestFromProto(req))
}

func (c *Core) UpdateRowPolicy(ctx context.Context, req *milvuspb.UpdateRowPolicyRequest) (*commonpb.Status, error) {
	if req != nil {
		if err := rlsutil.ValidatePolicyRoles(req.GetRoles()); err != nil {
			return merr.Status(err), nil
		}
	}
	return c.updateRowPolicy(ctx, updateRowPolicyRequestFromProto(req))
}

func (c *Core) DropRowPolicy(ctx context.Context, req *milvuspb.DropRowPolicyRequest) (*commonpb.Status, error) {
	if req == nil {
		return c.dropRowPolicy(ctx, nil)
	}
	return c.dropRowPolicy(ctx, &rlsutil.DropRowPolicyRequest{
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
		PolicyName:     req.GetPolicyName(),
	})
}

func (c *Core) ListRowPolicies(ctx context.Context, req *milvuspb.ListRowPoliciesRequest) (*milvuspb.ListRowPoliciesResponse, error) {
	var internalReq *rlsutil.ListRowPoliciesRequest
	if req != nil {
		internalReq = &rlsutil.ListRowPoliciesRequest{
			DbName:         req.GetDbName(),
			CollectionName: req.GetCollectionName(),
		}
	}
	resp, err := c.listRowPolicies(ctx, internalReq)
	if resp == nil {
		return nil, err
	}
	policies := make([]*milvuspb.RowPolicy, 0, len(resp.Policies))
	for _, policy := range resp.Policies {
		if policy == nil {
			continue
		}
		policies = append(policies, &milvuspb.RowPolicy{
			PolicyName:  policy.PolicyName,
			PolicyType:  milvuspb.RowPolicyType(policy.PolicyType),
			Actions:     policyActionsToProto(policy.Actions),
			UsingExpr:   policy.UsingExpr,
			CheckExpr:   policy.CheckExpr,
			Description: policy.Description,
		})
	}
	return &milvuspb.ListRowPoliciesResponse{
		Status:         resp.Status,
		Policies:       policies,
		DbName:         resp.DbName,
		CollectionName: resp.CollectionName,
	}, err
}

func (c *Core) SetRLSPrincipalTags(ctx context.Context, req *milvuspb.SetRLSPrincipalTagsRequest) (*commonpb.Status, error) {
	if req == nil {
		return c.setRLSPrincipalTags(ctx, nil)
	}
	internalReq, err := setRLSPrincipalTagsRequestFromProto(req)
	if err != nil {
		return merr.Status(err), nil
	}
	return c.setRLSPrincipalTags(ctx, internalReq)
}

func setRLSPrincipalTagsRequestFromProto(req *milvuspb.SetRLSPrincipalTagsRequest) (*rlsutil.SetRLSPrincipalTagsRequest, error) {
	if req == nil {
		return nil, nil
	}
	tags, err := rlsutil.TagsFromJSON(req.GetTags())
	if err != nil {
		return nil, err
	}
	return &rlsutil.SetRLSPrincipalTagsRequest{
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
		PrincipalName:  req.GetPrincipalName(),
		Tags:           tags,
	}, nil
}

func (c *Core) GetRLSPrincipalTags(ctx context.Context, req *milvuspb.GetRLSPrincipalTagsRequest) (*milvuspb.GetRLSPrincipalTagsResponse, error) {
	var internalReq *rlsutil.GetRLSPrincipalTagsRequest
	if req != nil {
		internalReq = &rlsutil.GetRLSPrincipalTagsRequest{
			DbName:         req.GetDbName(),
			CollectionName: req.GetCollectionName(),
			PrincipalName:  req.GetPrincipalName(),
		}
	}
	resp, err := c.getRLSPrincipalTags(ctx, internalReq)
	if resp == nil {
		return nil, err
	}
	tags, encodeErr := rlsutil.TagsToJSON(resp.Tags)
	if encodeErr != nil {
		return &milvuspb.GetRLSPrincipalTagsResponse{
			Status:         merr.Status(encodeErr),
			DbName:         resp.DbName,
			CollectionName: resp.CollectionName,
			PrincipalName:  resp.PrincipalName,
		}, nil
	}
	return &milvuspb.GetRLSPrincipalTagsResponse{
		Status:         resp.Status,
		Tags:           tags,
		DbName:         resp.DbName,
		CollectionName: resp.CollectionName,
		PrincipalName:  resp.PrincipalName,
	}, err
}

func (c *Core) ListRLSPrincipals(ctx context.Context, req *milvuspb.ListRLSPrincipalsRequest) (*milvuspb.ListRLSPrincipalsResponse, error) {
	var internalReq *rlsutil.ListRLSPrincipalsRequest
	if req != nil {
		internalReq = &rlsutil.ListRLSPrincipalsRequest{
			DbName:         req.GetDbName(),
			CollectionName: req.GetCollectionName(),
		}
	}
	resp, err := c.listRLSPrincipals(ctx, internalReq)
	if resp == nil {
		return nil, err
	}
	return &milvuspb.ListRLSPrincipalsResponse{
		Status:         resp.Status,
		PrincipalNames: slices.Clone(resp.PrincipalNames),
		DbName:         resp.DbName,
		CollectionName: resp.CollectionName,
	}, err
}

func (c *Core) DeleteRLSPrincipalTags(ctx context.Context, req *milvuspb.DeleteRLSPrincipalTagsRequest) (*commonpb.Status, error) {
	if req == nil {
		return c.deleteRLSPrincipalTags(ctx, nil)
	}
	return c.deleteRLSPrincipalTags(ctx, &rlsutil.DeleteRLSPrincipalTagsRequest{
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
		PrincipalName:  req.GetPrincipalName(),
		TagKeys:        slices.Clone(req.GetTagKeys()),
	})
}

func createRowPolicyRequestFromProto(req *milvuspb.CreateRowPolicyRequest) *rlsutil.CreateRowPolicyRequest {
	if req == nil {
		return nil
	}
	policyType := rlsutil.PolicyTypePermissive
	if req.PolicyType != nil {
		policyType = rlsutil.PolicyType(req.GetPolicyType())
	}
	return &rlsutil.CreateRowPolicyRequest{
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
		PolicyName:     req.GetPolicyName(),
		PolicyType:     policyType,
		Actions:        policyActionsFromProto(req.GetActions()),
		UsingExpr:      req.GetUsingExpr(),
		CheckExpr:      req.GetCheckExpr(),
		Description:    req.GetDescription(),
	}
}

func updateRowPolicyRequestFromProto(req *milvuspb.UpdateRowPolicyRequest) *rlsutil.UpdateRowPolicyRequest {
	if req == nil {
		return nil
	}
	return &rlsutil.UpdateRowPolicyRequest{
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
		PolicyName:     req.GetPolicyName(),
		PolicyType:     rlsutil.PolicyType(req.GetPolicyType()),
		Actions:        policyActionsFromProto(req.GetActions()),
		UsingExpr:      req.GetUsingExpr(),
		CheckExpr:      req.GetCheckExpr(),
		Description:    req.GetDescription(),
	}
}

func policyActionsFromProto(actions []milvuspb.RowPolicyAction) []rlsutil.PolicyAction {
	converted := make([]rlsutil.PolicyAction, len(actions))
	for i, action := range actions {
		converted[i] = rlsutil.PolicyAction(action)
	}
	return converted
}

func policyActionsToProto(actions []rlsutil.PolicyAction) []milvuspb.RowPolicyAction {
	converted := make([]milvuspb.RowPolicyAction, len(actions))
	for i, action := range actions {
		converted[i] = milvuspb.RowPolicyAction(action)
	}
	return converted
}
