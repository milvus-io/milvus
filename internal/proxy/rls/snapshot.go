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
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CoordClient interface {
	GetRLSMetadata(ctx context.Context, in *rootcoordpb.GetRLSMetadataRequest, opts ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error)
}

func (m *manager) Init(_ context.Context, coord CoordClient, allocVersion SnapshotVersionAllocator) error {
	if m == nil || coord == nil || allocVersion == nil {
		return merr.WrapErrServiceInternalMsg("failed to initialize RLS metadata manager without required dependencies")
	}
	m.configure(coord, allocVersion)
	return nil
}

func (m *manager) RefreshPolicySnapshot(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64) error {
	return m.refreshSnapshots(ctx, coord, dbName, collectionName, collectionID, version, true, false)
}

func (m *manager) RefreshPrincipalTagsSnapshot(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64) error {
	return m.refreshSnapshots(ctx, coord, dbName, collectionName, collectionID, version, false, true)
}

func (m *manager) refreshSnapshots(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64, refreshPolicies bool, refreshPrincipalTags bool) error {
	if m == nil || coord == nil {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS snapshots without manager or coord client")
	}
	if collectionID == 0 {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS snapshots with empty collection id")
	}
	if !refreshPolicies && !refreshPrincipalTags {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS snapshots without a target")
	}
	m.refreshLocks.RLock(collectionID)
	defer m.refreshLocks.RUnlock(collectionID)
	return m.refreshSnapshotsUnlocked(ctx, coord, dbName, collectionName, collectionID, version, refreshPolicies, refreshPrincipalTags)
}

func (m *manager) refreshSnapshotsUnlocked(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64, refreshPolicies bool, refreshPrincipalTags bool) error {
	// Capture one collection-state incarnation before the RPC. Cache invalidation
	// removes this pointer from the manager map. A response that started before
	// invalidation may still update the detached object, but can never recreate
	// or overwrite the new incarnation installed by a later request.
	state := m.getOrCreateCollectionState(newCollectionKey(collectionID))

	kind := rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_ALL
	if refreshPolicies && !refreshPrincipalTags {
		kind = rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES
	} else if !refreshPolicies && refreshPrincipalTags {
		kind = rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_PRINCIPALS
	}
	resp, err := coord.GetRLSMetadata(ctx, &rootcoordpb.GetRLSMetadataRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionId: collectionID,
		Kind:         kind,
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

	refreshedAt := time.Now()
	if refreshPolicies {
		updated := state.setRLSPolicySnapshot(policySnapshot{
			Version:     int64(version),
			RefreshedAt: refreshedAt,
			Policies:    rowPoliciesFromInfo(resp.GetPolicies()),
		})
		if !updated {
			mlog.Debug(ctx, "skip stale RLS policy snapshot",
				mlog.FieldDbName(dbName),
				mlog.FieldCollectionName(collectionName),
				mlog.FieldCollectionID(collectionID),
				mlog.Uint64("version", version))
		}
	}
	if refreshPrincipalTags {
		principalTags := make(map[string]map[string]string, len(resp.GetPrincipals()))
		for _, principal := range resp.GetPrincipals() {
			if principal == nil || principal.GetPrincipalName() == "" {
				continue
			}
			principalTags[principal.GetPrincipalName()] = principal.GetTags()
		}
		updated := state.setRLSPrincipalTagsSnapshot(principalTagsSnapshot{
			Version:       int64(version),
			RefreshedAt:   refreshedAt,
			PrincipalTags: principalTags,
		})
		if !updated {
			mlog.Debug(ctx, "skip stale RLS principal tags snapshot",
				mlog.FieldDbName(dbName),
				mlog.FieldCollectionName(collectionName),
				mlog.FieldCollectionID(collectionID),
				mlog.Uint64("version", version))
		}
	}
	return nil
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
