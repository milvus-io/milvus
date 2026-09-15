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

package rls

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CoordClient interface {
	GetRLSMetadata(ctx context.Context, in *rootcoordpb.GetRLSMetadataRequest, opts ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error)
}

func (m *manager) ensurePoliciesFresh(ctx context.Context, collectionID UniqueID) error {
	if m == nil || collectionID == 0 {
		return merr.WrapErrServiceInternalMsg("failed to validate RLS policy freshness with invalid manager or collection id")
	}
	coord, refreshCtx := m.dependencies()
	if coord == nil || refreshCtx == nil {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS policies without required dependencies")
	}
	refreshTTL := paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.GetAsDuration(time.Second)
	if refreshTTL <= 0 {
		return merr.WrapErrServiceInternalMsg("failed to validate RLS policy freshness with invalid TTL %s", refreshTTL)
	}
	if !m.policyRefreshDue(collectionID, refreshTTL, time.Now()) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.policyRefreshBackoffError(collectionID, time.Now()); err != nil {
		return err
	}

	resultCh := m.policyRefreshes.DoChan(policyRefreshKey(collectionID), func() (any, error) {
		if !m.policyRefreshDue(collectionID, refreshTTL, time.Now()) {
			return struct{}{}, nil
		}
		if err := m.policyRefreshBackoffError(collectionID, time.Now()); err != nil {
			return struct{}{}, err
		}
		if err := m.refreshPolicies(collectionID); err != nil {
			return struct{}{}, merr.Wrap(err, "failed to refresh expired RLS policies")
		}
		return struct{}{}, nil
	})
	select {
	case result := <-resultCh:
		if err := ctx.Err(); err != nil {
			return err
		}
		if m.isCollectionDropped(collectionID) {
			return merr.WrapErrServiceUnavailableMsg("RLS collection %d was removed during policy refresh", collectionID)
		}
		return result.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *manager) policyRefreshDue(collectionID UniqueID, refreshTTL time.Duration, now time.Time) bool {
	state := m.getCollectionState(collectionID)
	if state == nil {
		return true
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.policyRefreshedAt.IsZero() || !state.policyRefreshedAt.Add(refreshTTL).After(now)
}

func (m *manager) policyRefreshBackoffError(collectionID UniqueID, now time.Time) error {
	state := m.getCollectionState(collectionID)
	if state == nil {
		return nil
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	if state.policyBackoff == nil || !now.Before(state.policyBackoff.NextInstant()) {
		return nil
	}
	return merr.WrapErrServiceUnavailableMsg("RLS policy metadata refresh is backing off for collection %d", collectionID)
}

func (m *manager) refreshPolicies(collectionID UniqueID) error {
	if m == nil {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS policy snapshot without manager")
	}
	if collectionID == 0 {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS policy snapshot with empty collection id")
	}
	coord, refreshCtx := m.dependencies()
	if coord == nil || refreshCtx == nil {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS policies without required dependencies")
	}
	state, generation := m.beginPolicyRefresh(collectionID)
	if state == nil {
		return merr.WrapErrServiceUnavailableMsg("RLS collection %d was removed before policy refresh", collectionID)
	}
	finished := false
	defer func() {
		if !finished {
			m.finishPolicyRefresh(collectionID, state, generation, nil)
		}
	}()

	resp, err := coord.GetRLSMetadata(refreshCtx, &rootcoordpb.GetRLSMetadataRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionId: collectionID,
		Kind:         rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return merr.Wrap(err, "failed to get RLS metadata")
	}
	if resp.GetCollectionId() != collectionID {
		return merr.WrapErrServiceInternalMsg("RLS metadata collection id mismatch: requested %d, received %d", collectionID, resp.GetCollectionId())
	}
	policies, err := rowPoliciesFromInfo(collectionID, resp.GetPolicies())
	if err != nil {
		return err
	}
	snapshot := policySnapshot{
		RefreshedAt: time.Now(),
		Policies:    policies,
	}
	current := m.finishPolicyRefresh(collectionID, state, generation, &snapshot)
	finished = true
	if !current {
		return merr.WrapErrServiceUnavailableMsg("RLS policy metadata changed during refresh for collection %d", collectionID)
	}
	return nil
}

func (m *manager) ensurePrincipalTags(ctx context.Context, collectionID UniqueID, principalName string) (map[string]rlsutil.TagValue, error) {
	if m == nil || collectionID == 0 || principalName == "" {
		return nil, merr.WrapErrPrivilegeNotPermitted("RLS principal is required")
	}

	key := principalKey{collectionID: collectionID, principalName: principalName}
	coord, refreshCtx := m.dependencies()
	if coord == nil || refreshCtx == nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to refresh RLS principal tags without coord client")
	}
	if entry := m.getPrincipalTagsEntry(key); entry != nil {
		return entry.tags, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cacheKey := principalRefreshKey(key)
	resultCh := m.principalRefreshes.DoChan(cacheKey, func() (any, error) {
		coord, refreshCtx := m.dependencies()
		if coord == nil || refreshCtx == nil {
			return nil, merr.WrapErrServiceInternalMsg("failed to refresh RLS principal tags without coord client")
		}
		if entry := m.getPrincipalTagsEntry(key); entry != nil {
			return entry.tags, nil
		}
		state, token := m.beginPrincipalRefresh(key)
		if state == nil {
			return nil, merr.WrapErrServiceUnavailableMsg("RLS collection %d was removed during principal refresh", collectionID)
		}
		finished := false
		defer func() {
			if !finished {
				m.finishPrincipalRefresh(key, state, token, nil)
			}
		}()

		resp, err := coord.GetRLSMetadata(refreshCtx, &rootcoordpb.GetRLSMetadataRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			CollectionId:  collectionID,
			Kind:          rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_PRINCIPALS,
			PrincipalName: principalName,
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return nil, merr.Wrapf(err, "failed to get RLS principal %q tags", principalName)
		}
		if resp.GetCollectionId() != collectionID {
			return nil, merr.WrapErrServiceInternalMsg("RLS metadata collection id mismatch: requested %d, received %d", collectionID, resp.GetCollectionId())
		}
		var tags map[string]rlsutil.TagValue
		for _, principal := range resp.GetPrincipals() {
			if principal.GetPrincipalName() == principalName {
				tags, err = rlsutil.TagsFromJSON(principal.GetTags())
				break
			}
		}
		if err != nil {
			return nil, merr.WrapErrDataIntegrity(err, "decode RLS principal %q tags", principalName)
		}
		var entry *principalTagsEntry
		if tags != nil {
			entry = &principalTagsEntry{
				refreshedAt: time.Now(),
				tags:        rlsutil.CloneTags(tags),
			}
		}
		current := m.finishPrincipalRefresh(key, state, token, entry)
		finished = true
		if !current {
			return nil, merr.WrapErrServiceUnavailableMsg("RLS principal %q metadata changed during refresh", principalName)
		}
		if tags == nil {
			return map[string]rlsutil.TagValue{}, nil
		}
		return entry.tags, nil
	})
	select {
	case result := <-resultCh:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if m.isCollectionDropped(collectionID) {
			return nil, merr.WrapErrServiceUnavailableMsg("RLS collection %d was removed during principal refresh", collectionID)
		}
		if result.Err != nil {
			return nil, result.Err
		}
		tags, ok := result.Val.(map[string]rlsutil.TagValue)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("RLS principal refresh returned an invalid result")
		}
		return tags, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func rowPoliciesFromInfo(collectionID UniqueID, policies []*rootcoordpb.RLSPolicyInfo) ([]*rlsutil.RowPolicy, error) {
	converted := make([]*rlsutil.RowPolicy, 0, len(policies))
	names := make(map[string]struct{}, len(policies))
	for i, policy := range policies {
		if policy == nil {
			return nil, merr.WrapErrDataIntegrityMsg("RLS policy metadata at index %d is nil", i)
		}
		actions := make([]rlsutil.PolicyAction, len(policy.GetActions()))
		for i, action := range policy.GetActions() {
			actions[i] = rlsutil.PolicyAction(action)
		}
		convertedPolicy := &rlsutil.RowPolicy{
			PolicyName:  policy.GetPolicyName(),
			PolicyType:  rlsutil.PolicyType(policy.GetPolicyType()),
			Actions:     actions,
			UsingExpr:   policy.GetUsingExpr(),
			CheckExpr:   policy.GetCheckExpr(),
			Description: policy.GetDescription(),
			PolicyId:    policy.GetPolicyId(),
		}
		if policy.GetCollectionId() != collectionID {
			return nil, merr.WrapErrDataIntegrityMsg(
				"RLS policy %q collection id mismatch: expected %d, received %d",
				convertedPolicy.GetPolicyName(), collectionID, policy.GetCollectionId())
		}
		if convertedPolicy.PolicyId <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("RLS policy %q has invalid id %d", convertedPolicy.GetPolicyName(), convertedPolicy.PolicyId)
		}
		if err := rlsutil.ValidateStoredPolicy(
			convertedPolicy.GetPolicyName(), convertedPolicy.GetPolicyType(), convertedPolicy.GetActions(),
			convertedPolicy.GetUsingExpr(), convertedPolicy.GetCheckExpr(),
		); err != nil {
			return nil, merr.WrapErrDataIntegrity(err, "invalid RLS policy %q metadata", convertedPolicy.GetPolicyName())
		}
		if _, ok := names[convertedPolicy.GetPolicyName()]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("duplicated RLS policy name %q", convertedPolicy.GetPolicyName())
		}
		names[convertedPolicy.GetPolicyName()] = struct{}{}
		converted = append(converted, convertedPolicy)
	}
	return converted, nil
}
