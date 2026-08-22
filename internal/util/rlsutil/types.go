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

package rlsutil

import "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

type PolicyType int32

const (
	PolicyTypeUnknown     PolicyType = 0
	PolicyTypePermissive  PolicyType = 1
	PolicyTypeRestrictive PolicyType = 2
)

func (policyType PolicyType) String() string {
	switch policyType {
	case PolicyTypePermissive:
		return "RowPolicyTypePermissive"
	case PolicyTypeRestrictive:
		return "RowPolicyTypeRestrictive"
	default:
		return "RowPolicyTypeUnknown"
	}
}

type PolicyAction int32

const (
	PolicyActionQuery          PolicyAction = 0
	PolicyActionSearch         PolicyAction = 1
	PolicyActionInsert         PolicyAction = 2
	PolicyActionDelete         PolicyAction = 3
	PolicyActionUpsert         PolicyAction = 4
	PolicyActionQueryIterator  PolicyAction = 5
	PolicyActionSearchIterator PolicyAction = 6
	PolicyActionHybridSearch   PolicyAction = 7
)

func (action PolicyAction) String() string {
	switch action {
	case PolicyActionQuery:
		return "Query"
	case PolicyActionSearch:
		return "Search"
	case PolicyActionInsert:
		return "Insert"
	case PolicyActionDelete:
		return "Delete"
	case PolicyActionUpsert:
		return "Upsert"
	case PolicyActionQueryIterator:
		return "QueryIterator"
	case PolicyActionSearchIterator:
		return "SearchIterator"
	case PolicyActionHybridSearch:
		return "HybridSearch"
	default:
		return "Unknown"
	}
}

type CreateRowPolicyRequest struct {
	DbName         string
	CollectionName string
	PolicyName     string
	PolicyType     PolicyType
	Actions        []PolicyAction
	UsingExpr      string
	CheckExpr      string
	Description    string
}

func (request *CreateRowPolicyRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *CreateRowPolicyRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *CreateRowPolicyRequest) GetPolicyName() string {
	if request == nil {
		return ""
	}
	return request.PolicyName
}

func (request *CreateRowPolicyRequest) GetPolicyType() PolicyType {
	if request == nil {
		return PolicyTypeUnknown
	}
	return request.PolicyType
}

func (request *CreateRowPolicyRequest) GetActions() []PolicyAction {
	if request == nil {
		return nil
	}
	return request.Actions
}

func (request *CreateRowPolicyRequest) GetUsingExpr() string {
	if request == nil {
		return ""
	}
	return request.UsingExpr
}

func (request *CreateRowPolicyRequest) GetCheckExpr() string {
	if request == nil {
		return ""
	}
	return request.CheckExpr
}

func (request *CreateRowPolicyRequest) GetDescription() string {
	if request == nil {
		return ""
	}
	return request.Description
}

type UpdateRowPolicyRequest = CreateRowPolicyRequest

type DropRowPolicyRequest struct {
	DbName         string
	CollectionName string
	PolicyName     string
}

func (request *DropRowPolicyRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *DropRowPolicyRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *DropRowPolicyRequest) GetPolicyName() string {
	if request == nil {
		return ""
	}
	return request.PolicyName
}

type ListRowPoliciesRequest struct {
	DbName         string
	CollectionName string
}

func (request *ListRowPoliciesRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *ListRowPoliciesRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

type RowPolicy struct {
	PolicyName  string
	PolicyType  PolicyType
	Actions     []PolicyAction
	UsingExpr   string
	CheckExpr   string
	Description string
	PolicyId    int64
}

func (policy *RowPolicy) GetPolicyName() string {
	if policy == nil {
		return ""
	}
	return policy.PolicyName
}

func (policy *RowPolicy) GetPolicyType() PolicyType {
	if policy == nil {
		return PolicyTypeUnknown
	}
	return policy.PolicyType
}

func (policy *RowPolicy) GetActions() []PolicyAction {
	if policy == nil {
		return nil
	}
	return policy.Actions
}

func (policy *RowPolicy) GetUsingExpr() string {
	if policy == nil {
		return ""
	}
	return policy.UsingExpr
}

func (policy *RowPolicy) GetCheckExpr() string {
	if policy == nil {
		return ""
	}
	return policy.CheckExpr
}

type ListRowPoliciesResponse struct {
	Status         *commonpb.Status
	Policies       []*RowPolicy
	DbName         string
	CollectionName string
}

type SetRLSPrincipalTagsRequest struct {
	DbName         string
	CollectionName string
	PrincipalName  string
	Tags           map[string]string
}

func (request *SetRLSPrincipalTagsRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *SetRLSPrincipalTagsRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *SetRLSPrincipalTagsRequest) GetPrincipalName() string {
	if request == nil {
		return ""
	}
	return request.PrincipalName
}

func (request *SetRLSPrincipalTagsRequest) GetTags() map[string]string {
	if request == nil {
		return nil
	}
	return request.Tags
}

type GetRLSPrincipalTagsRequest struct {
	DbName         string
	CollectionName string
	PrincipalName  string
}

func (request *GetRLSPrincipalTagsRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *GetRLSPrincipalTagsRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *GetRLSPrincipalTagsRequest) GetPrincipalName() string {
	if request == nil {
		return ""
	}
	return request.PrincipalName
}

type GetRLSPrincipalTagsResponse struct {
	Status         *commonpb.Status
	Tags           map[string]string
	DbName         string
	CollectionName string
	PrincipalName  string
}

type ListRLSPrincipalsRequest struct {
	DbName         string
	CollectionName string
}

func (request *ListRLSPrincipalsRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *ListRLSPrincipalsRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

type ListRLSPrincipalsResponse struct {
	Status         *commonpb.Status
	PrincipalNames []string
	DbName         string
	CollectionName string
}

type DeleteRLSPrincipalTagsRequest struct {
	DbName         string
	CollectionName string
	PrincipalName  string
	TagKeys        []string
}

func (request *DeleteRLSPrincipalTagsRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *DeleteRLSPrincipalTagsRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *DeleteRLSPrincipalTagsRequest) GetPrincipalName() string {
	if request == nil {
		return ""
	}
	return request.PrincipalName
}

func (request *DeleteRLSPrincipalTagsRequest) GetTagKeys() []string {
	if request == nil {
		return nil
	}
	return request.TagKeys
}
