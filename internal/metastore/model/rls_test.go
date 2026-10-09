// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/rlsutil"
)

func TestRLSPolicyModelCopiesActions(t *testing.T) {
	policy := &RLSPolicy{
		DBID: 10, CollectionID: 20, PolicyID: 100, PolicyName: "tenant",
		PolicyType: rlsutil.PolicyTypeRestrictive,
		Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery, rlsutil.PolicyActionUpsert},
		UsingExpr:  "tenant == $current_principal", CheckExpr: "tenant == $current_principal",
	}
	encoded := MarshalRLSPolicyModel(policy)
	decoded := UnmarshalRLSPolicyModel(encoded)
	require.Equal(t, policy, decoded)
	encoded.Actions[0] = rlsutil.PolicyActionDelete
	require.Equal(t, rlsutil.PolicyActionQuery, policy.Actions[0])
	require.Equal(t, rlsutil.PolicyActionQuery, decoded.Actions[0])
	decoded.Actions[1] = rlsutil.PolicyActionInsert
	require.Equal(t, rlsutil.PolicyActionUpsert, policy.Actions[1])

	require.Nil(t, MarshalRLSPolicyModel(nil))
	require.Nil(t, UnmarshalRLSPolicyModel(nil))
	for _, actions := range [][]rlsutil.PolicyAction{nil, {}} {
		policy.Actions = actions
		require.Equal(t, actions, UnmarshalRLSPolicyModel(MarshalRLSPolicyModel(policy)).Actions)
	}
}

func TestCollectionClonePreservesDeferredRLSPolicies(t *testing.T) {
	collection := &Collection{CollectionID: 20, RLSPoliciesUnloaded: true}
	require.True(t, collection.Clone().RLSPoliciesUnloaded)
	require.True(t, collection.ShallowClone().RLSPoliciesUnloaded)
}

func TestRLSPolicyMapToSlice(t *testing.T) {
	policyMap := map[string]*RLSPolicy{
		"policy_b": {PolicyID: 2, PolicyName: "policy_b", Actions: []rlsutil.PolicyAction{rlsutil.PolicyActionSearch}},
		"nil":      nil,
		"policy_a": {PolicyID: 1, PolicyName: "policy_a", Actions: []rlsutil.PolicyAction{rlsutil.PolicyActionQuery}},
	}

	policyList := RLSPolicyMapToSlice(policyMap)
	require.Len(t, policyList, 2)
	require.Equal(t, []string{"policy_a", "policy_b"}, []string{policyList[0].PolicyName, policyList[1].PolicyName})
	require.NotSame(t, policyMap["policy_a"], policyList[0])

	policyList[0].Actions[0] = rlsutil.PolicyActionDelete
	require.Equal(t, rlsutil.PolicyActionQuery, policyMap["policy_a"].Actions[0])
}
