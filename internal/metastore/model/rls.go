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

package model

import (
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

type RLSPolicy struct {
	DBID         int64
	CollectionID int64
	PolicyID     int64
	PolicyName   string
	PolicyType   rlsutil.PolicyType
	Actions      []rlsutil.PolicyAction
	UsingExpr    string
	CheckExpr    string
	Description  string
}

func MarshalRLSPolicyModel(policy *RLSPolicy) *rootcoordpb.RLSPolicyInfo {
	if policy == nil {
		return nil
	}
	return &rootcoordpb.RLSPolicyInfo{
		DbId:         policy.DBID,
		CollectionId: policy.CollectionID,
		PolicyId:     policy.PolicyID,
		PolicyName:   policy.PolicyName,
		PolicyType:   milvuspb.RowPolicyType(policy.PolicyType),
		Actions:      policyActionsToProto(policy.Actions),
		UsingExpr:    policy.UsingExpr,
		CheckExpr:    policy.CheckExpr,
		Description:  policy.Description,
	}
}

func UnmarshalRLSPolicyModel(policy *rootcoordpb.RLSPolicyInfo) *RLSPolicy {
	if policy == nil {
		return nil
	}
	return &RLSPolicy{
		DBID:         policy.GetDbId(),
		CollectionID: policy.GetCollectionId(),
		PolicyID:     policy.GetPolicyId(),
		PolicyName:   policy.GetPolicyName(),
		PolicyType:   rlsutil.PolicyType(policy.GetPolicyType()),
		Actions:      policyActionsFromProto(policy.GetActions()),
		UsingExpr:    policy.GetUsingExpr(),
		CheckExpr:    policy.GetCheckExpr(),
		Description:  policy.GetDescription(),
	}
}

func (policy *RLSPolicy) ToRowPolicy() *rlsutil.RowPolicy {
	if policy == nil {
		return nil
	}
	return &rlsutil.RowPolicy{
		PolicyName:  policy.PolicyName,
		PolicyType:  policy.PolicyType,
		Actions:     cloneRowPolicyActions(policy.Actions),
		UsingExpr:   policy.UsingExpr,
		CheckExpr:   policy.CheckExpr,
		Description: policy.Description,
		PolicyId:    policy.PolicyID,
	}
}

func CloneRLSPolicy(policy *RLSPolicy) *RLSPolicy {
	if policy == nil {
		return nil
	}
	return &RLSPolicy{
		DBID:         policy.DBID,
		CollectionID: policy.CollectionID,
		PolicyID:     policy.PolicyID,
		PolicyName:   policy.PolicyName,
		PolicyType:   policy.PolicyType,
		Actions:      cloneRowPolicyActions(policy.Actions),
		UsingExpr:    policy.UsingExpr,
		CheckExpr:    policy.CheckExpr,
		Description:  policy.Description,
	}
}

func CloneRLSPolicies(policies []*RLSPolicy) []*RLSPolicy {
	if policies == nil {
		return nil
	}
	cloned := make([]*RLSPolicy, len(policies))
	for i, policy := range policies {
		cloned[i] = CloneRLSPolicy(policy)
	}
	return cloned
}

func CloneRLSPolicyMap(policies map[string]*RLSPolicy) map[string]*RLSPolicy {
	if policies == nil {
		return nil
	}
	cloned := make(map[string]*RLSPolicy, len(policies))
	for name, policy := range policies {
		cloned[name] = CloneRLSPolicy(policy)
	}
	return cloned
}

func RLSPolicyMapFromSlice(policies []*RLSPolicy) map[string]*RLSPolicy {
	if policies == nil {
		return nil
	}
	policyMap := make(map[string]*RLSPolicy, len(policies))
	for _, policy := range policies {
		if policy != nil {
			policyMap[policy.PolicyName] = CloneRLSPolicy(policy)
		}
	}
	return policyMap
}

func RLSPolicyMapToSlice(policies map[string]*RLSPolicy) []*RLSPolicy {
	if policies == nil {
		return nil
	}
	policyList := make([]*RLSPolicy, 0, len(policies))
	for _, policy := range policies {
		if policy != nil {
			policyList = append(policyList, CloneRLSPolicy(policy))
		}
	}
	sort.Slice(policyList, func(i, j int) bool {
		if policyList[i].PolicyName != policyList[j].PolicyName {
			return policyList[i].PolicyName < policyList[j].PolicyName
		}
		return policyList[i].PolicyID < policyList[j].PolicyID
	})
	return policyList
}

type RLSPrincipal struct {
	DBID          int64
	CollectionID  int64
	PrincipalName string
	Tags          map[string]string
}

func MarshalRLSPrincipalModel(principal *RLSPrincipal) *rootcoordpb.RLSPrincipalInfo {
	if principal == nil {
		return nil
	}
	return &rootcoordpb.RLSPrincipalInfo{
		DbId:          principal.DBID,
		CollectionId:  principal.CollectionID,
		PrincipalName: principal.PrincipalName,
		Tags:          cloneStringMap(principal.Tags),
	}
}

func UnmarshalRLSPrincipalModel(principal *rootcoordpb.RLSPrincipalInfo) *RLSPrincipal {
	if principal == nil {
		return nil
	}
	return &RLSPrincipal{
		DBID:          principal.GetDbId(),
		CollectionID:  principal.GetCollectionId(),
		PrincipalName: principal.GetPrincipalName(),
		Tags:          cloneStringMap(principal.GetTags()),
	}
}

func CloneRLSPrincipal(principal *RLSPrincipal) *RLSPrincipal {
	if principal == nil {
		return nil
	}
	return &RLSPrincipal{
		DBID:          principal.DBID,
		CollectionID:  principal.CollectionID,
		PrincipalName: principal.PrincipalName,
		Tags:          cloneStringMap(principal.Tags),
	}
}

func CloneRLSPrincipals(principals []*RLSPrincipal) []*RLSPrincipal {
	if principals == nil {
		return nil
	}
	cloned := make([]*RLSPrincipal, len(principals))
	for i, principal := range principals {
		cloned[i] = CloneRLSPrincipal(principal)
	}
	return cloned
}

func cloneRowPolicyActions(actions []rlsutil.PolicyAction) []rlsutil.PolicyAction {
	if actions == nil {
		return nil
	}
	cloned := make([]rlsutil.PolicyAction, len(actions))
	copy(cloned, actions)
	return cloned
}

func policyActionsToProto(actions []rlsutil.PolicyAction) []milvuspb.RowPolicyAction {
	if actions == nil {
		return nil
	}
	converted := make([]milvuspb.RowPolicyAction, len(actions))
	for i, action := range actions {
		converted[i] = milvuspb.RowPolicyAction(action)
	}
	return converted
}

func policyActionsFromProto(actions []milvuspb.RowPolicyAction) []rlsutil.PolicyAction {
	if actions == nil {
		return nil
	}
	converted := make([]rlsutil.PolicyAction, len(actions))
	for i, action := range actions {
		converted[i] = rlsutil.PolicyAction(action)
	}
	return converted
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
