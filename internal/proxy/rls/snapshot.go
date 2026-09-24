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

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CoordClient interface {
	GetRLSMetadata(ctx context.Context, in *rootcoordpb.GetRLSMetadataRequest, opts ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error)
}

func wrapMetadataRefreshError(err error, format string, args ...any) error {
	if merr.IsMilvusError(err) {
		return merr.Wrapf(err, format, args...)
	}
	code := status.Code(err)
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) ||
		code == codes.Canceled || code == codes.DeadlineExceeded || code == codes.Unavailable {
		return merr.WrapErrServiceUnavailableErr(err, format, args...)
	}
	return merr.WrapErrServiceInternalErr(err, format, args...)
}

func (m *manager) ensurePoliciesFresh(ctx context.Context, collectionID UniqueID) error {
	if m == nil || collectionID == 0 {
		return merr.WrapErrServiceInternalMsg("failed to validate RLS policy freshness with invalid manager or collection id")
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
	state, generation := m.beginPolicyRefresh(collectionID)
	if state == nil {
		return merr.WrapErrServiceUnavailableMsg("RLS collection %d was removed before policy refresh", collectionID)
	}

	resultCh := m.policyRefreshes.DoChan(policyRefreshKey(collectionID, generation), func() (any, error) {
		if !m.policyRefreshDue(collectionID, refreshTTL, time.Now()) {
			return struct{}{}, nil
		}
		if err := m.policyRefreshBackoffError(collectionID, time.Now()); err != nil {
			return struct{}{}, err
		}
		if err := m.refreshPoliciesAtGeneration(collectionID, state, generation); err != nil {
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
	state, generation := m.beginPolicyRefresh(collectionID)
	if state == nil {
		return merr.WrapErrServiceUnavailableMsg("RLS collection %d was removed before policy refresh", collectionID)
	}
	return m.refreshPoliciesAtGeneration(collectionID, state, generation)
}

func (m *manager) refreshPoliciesAtGeneration(collectionID UniqueID, state *collectionState, generation uint64) error {
	if !m.policyRefreshCurrent(collectionID, state, generation) {
		return merr.WrapErrServiceUnavailableMsg("RLS policy metadata changed before refresh for collection %d", collectionID)
	}
	coord, refreshCtx := m.coord, m.refreshCtx
	if coord == nil || refreshCtx == nil {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS policies without required dependencies")
	}
	finished := false
	defer func() {
		if !finished {
			m.finishPolicyRefresh(collectionID, state, generation, nil)
		}
	}()

	rpcCtx, cancel := context.WithTimeout(refreshCtx, metadataRefreshTimeout)
	defer cancel()
	resp, err := coord.GetRLSMetadata(rpcCtx, &rootcoordpb.GetRLSMetadataRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionId: collectionID,
		Kind:         rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return wrapMetadataRefreshError(err, "failed to get RLS metadata")
	}
	if resp.GetCollectionId() != collectionID {
		return merr.WrapErrServiceInternalMsg("RLS metadata collection id mismatch: requested %d, received %d", collectionID, resp.GetCollectionId())
	}
	policies, err := rowPoliciesFromInfo(collectionID, resp.GetPolicies())
	if err != nil {
		return err
	}
	current := m.finishPolicyRefresh(collectionID, state, generation, policies)
	finished = true
	if !current {
		return merr.WrapErrServiceUnavailableMsg("RLS policy metadata changed during refresh for collection %d", collectionID)
	}
	return nil
}

func (m *manager) ensurePrincipalTags(ctx context.Context, collectionID UniqueID, principalName string) (map[string]rlsutil.TagValue, error) {
	if m == nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to validate RLS principal tags without metadata manager")
	}
	if collectionID == 0 {
		return nil, merr.WrapErrServiceInternalMsg("failed to validate RLS principal tags with empty collection id")
	}
	if principalName == "" {
		return nil, merr.WrapErrPrivilegeNotPermitted("RLS principal is required")
	}

	key := principalKey{collectionID: collectionID, principalName: principalName}
	refreshTTL := paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.GetAsDuration(time.Second)
	if refreshTTL <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("failed to validate RLS principal freshness with invalid TTL %s", refreshTTL)
	}
	now := time.Now()
	if entry := m.getPrincipalTagsEntry(key); principalTagsEntryFresh(entry, refreshTTL, now) {
		return entry.tags, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	coord, refreshCtx := m.coord, m.refreshCtx
	if coord == nil || refreshCtx == nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to refresh RLS principal tags without coord client")
	}
	entry, resultCh, err := m.startPrincipalRefresh(key, refreshTTL, func(state *collectionState, token principalRefreshToken) (any, error) {
		if !m.principalRefreshCurrent(key, state, token) {
			return nil, merr.WrapErrServiceUnavailableMsg("RLS principal %q metadata changed before refresh", principalName)
		}
		finished := false
		defer func() {
			if !finished {
				_, _ = m.finishPrincipalRefresh(key, state, token, nil, false)
			}
		}()

		rpcCtx, cancel := context.WithTimeout(refreshCtx, metadataRefreshTimeout)
		defer cancel()
		resp, err := coord.GetRLSMetadata(rpcCtx, &rootcoordpb.GetRLSMetadataRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			CollectionId:  collectionID,
			Kind:          rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_PRINCIPALS,
			PrincipalName: principalName,
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return nil, wrapMetadataRefreshError(err, "failed to get RLS principal %q tags", principalName)
		}
		if resp.GetCollectionId() != collectionID {
			return nil, merr.WrapErrServiceInternalMsg("RLS metadata collection id mismatch: requested %d, received %d", collectionID, resp.GetCollectionId())
		}
		var tags map[string]rlsutil.TagValue
		found := false
		for _, principal := range resp.GetPrincipals() {
			if principal.GetPrincipalName() == principalName {
				found = true
				tags, err = rlsutil.TagsFromJSON(principal.GetTags())
				break
			}
		}
		if err != nil {
			return nil, merr.WrapErrDataIntegrity(err, "decode RLS principal %q tags", principalName)
		}
		if tags == nil {
			tags = map[string]rlsutil.TagValue{}
		}
		entry := &principalTagsEntry{
			tags:    tags,
			missing: !found,
		}
		current, err := m.finishPrincipalRefresh(key, state, token, entry, true)
		finished = true
		if err != nil {
			return nil, err
		}
		if !current {
			return nil, merr.WrapErrServiceUnavailableMsg("RLS principal %q metadata changed during refresh", principalName)
		}
		return entry.tags, nil
	})
	if err != nil {
		return nil, err
	}
	if entry != nil {
		return entry.tags, nil
	}
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

func rowPoliciesFromInfo(collectionID UniqueID, policies []*rootcoordpb.RLSPolicyInfo) (map[string]*rlsutil.RowPolicy, error) {
	converted := make(map[string]*rlsutil.RowPolicy, len(policies))
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
		if _, ok := converted[convertedPolicy.GetPolicyName()]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("duplicated RLS policy name %q", convertedPolicy.GetPolicyName())
		}
		converted[convertedPolicy.GetPolicyName()] = convertedPolicy
	}
	return converted, nil
}
