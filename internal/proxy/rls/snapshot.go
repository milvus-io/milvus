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
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CoordClient interface {
	GetRLSMetadata(ctx context.Context, in *rootcoordpb.GetRLSMetadataRequest, opts ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error)
}

func (m *manager) Init(ctx context.Context, coord CoordClient, allocVersion SnapshotVersionAllocator) error {
	if m == nil || coord == nil || allocVersion == nil {
		return merr.WrapErrServiceInternalMsg("failed to initialize RLS metadata manager without required dependencies")
	}
	m.configure(ctx, coord, allocVersion)
	return nil
}

func (m *manager) RefreshPolicySnapshot(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64) error {
	return m.refreshPolicySnapshot(ctx, coord, dbName, collectionName, collectionID, version)
}

func (m *manager) refreshPolicySnapshot(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64) error {
	if m == nil || coord == nil {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS policy snapshot without manager or coord client")
	}
	if collectionID == 0 {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS policy snapshot with empty collection id")
	}
	m.refreshLocks.RLock(collectionID)
	defer m.refreshLocks.RUnlock(collectionID)
	return m.refreshPolicySnapshotUnlocked(ctx, coord, dbName, collectionName, collectionID, version)
}

func (m *manager) refreshPolicySnapshotUnlocked(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64) error {
	// Capture one collection-state incarnation before the RPC. Cache invalidation
	// removes this pointer from the manager map. A response that started before
	// invalidation may still update the detached object, but can never recreate
	// or overwrite the new incarnation installed by a later request.
	state := m.getOrCreateCollectionState(newCollectionKey(collectionID))

	resp, err := coord.GetRLSMetadata(ctx, &rootcoordpb.GetRLSMetadataRequest{
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
	if resp.GetDbName() != "" {
		dbName = resp.GetDbName()
	}
	if resp.GetCollectionName() != "" {
		collectionName = resp.GetCollectionName()
	}

	updated := state.setRLSPolicySnapshot(policySnapshot{
		Version:        int64(version),
		RefreshedAt:    time.Now(),
		DBName:         dbName,
		CollectionName: collectionName,
		Policies:       rowPoliciesFromInfo(resp.GetPolicies()),
	})
	if !updated {
		mlog.Debug(ctx, "skip stale RLS policy snapshot",
			mlog.FieldDbName(dbName),
			mlog.FieldCollectionName(collectionName),
			mlog.FieldCollectionID(collectionID),
			mlog.Uint64("version", version))
	}
	return nil
}

func (m *manager) ensurePrincipalTags(ctx context.Context, collectionID UniqueID, principalName string) (map[string]rlsutil.TagValue, error) {
	if m == nil || collectionID == 0 || principalName == "" {
		return nil, merr.WrapErrPrivilegeNotPermitted("RLS principal is required")
	}

	key := principalKey{collectionID: collectionID, principalName: principalName}
	coord, _, validateFreshness := m.refreshDependencies()
	if entry := m.getPrincipalTagsEntry(key); entry != nil {
		return clonePrincipalTags(entry.tags), nil
	}
	if !validateFreshness {
		return nil, merr.WrapErrPrivilegeNotPermitted("RLS principal %q does not exist", principalName)
	}
	if coord == nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to refresh RLS principal tags without coord client")
	}

	m.refreshLocks.RLock(collectionID)
	defer m.refreshLocks.RUnlock(collectionID)
	m.principalRefreshLocks.RLock(key)
	defer m.principalRefreshLocks.RUnlock(key)

	if entry := m.getPrincipalTagsEntry(key); entry != nil {
		return clonePrincipalTags(entry.tags), nil
	}

	cacheKey := fmt.Sprintf("%d/%s", collectionID, principalName)
	tags, err, _ := m.principalRefreshes.Do(cacheKey, func() (map[string]rlsutil.TagValue, error) {
		coord, _, _ := m.refreshDependencies()
		if coord == nil {
			return nil, merr.WrapErrServiceInternalMsg("failed to refresh RLS principal tags without coord client")
		}
		if entry := m.getPrincipalTagsEntry(key); entry != nil {
			return clonePrincipalTags(entry.tags), nil
		}

		resp, err := coord.GetRLSMetadata(ctx, &rootcoordpb.GetRLSMetadataRequest{
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
			return nil, merr.Wrapf(err, "failed to decode RLS principal %q tags", principalName)
		}
		if tags == nil {
			return nil, merr.WrapErrParameterInvalidMsg("RLS principal [%s] does not exist", principalName)
		}
		if !m.setPrincipalTags(key, &principalTagsEntry{
			refreshedAt: time.Now(),
			tags:        clonePrincipalTags(tags),
		}) {
			return nil, merr.WrapErrServiceInternalMsg("RLS collection %d was removed during principal refresh", collectionID)
		}
		return tags, nil
	})
	return clonePrincipalTags(tags), err
}

func rowPoliciesFromInfo(policies []*rootcoordpb.RLSPolicyInfo) []*rlsutil.RowPolicy {
	converted := make([]*rlsutil.RowPolicy, 0, len(policies))
	for _, policy := range policies {
		if policy == nil {
			continue
		}
		actions := make([]rlsutil.PolicyAction, len(policy.GetActions()))
		for i, action := range policy.GetActions() {
			actions[i] = rlsutil.PolicyAction(action)
		}
		converted = append(converted, &rlsutil.RowPolicy{
			PolicyName:  policy.GetPolicyName(),
			PolicyType:  rlsutil.PolicyType(policy.GetPolicyType()),
			Actions:     actions,
			UsingExpr:   policy.GetUsingExpr(),
			CheckExpr:   policy.GetCheckExpr(),
			Description: policy.GetDescription(),
			PolicyId:    policy.GetPolicyId(),
		})
	}
	return converted
}
