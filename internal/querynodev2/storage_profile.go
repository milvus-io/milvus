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

package querynodev2

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (node *QueryNode) beginRequestStorageContribution(
	ctx context.Context,
	level int32,
	scopeID string,
	requestID, collectionID int64,
	workload storageprofile.WorkloadKind,
) (context.Context, *storageprofile.Scope) {
	attribution := storageprofile.Attribution{
		ScopeType:     storageprofile.ScopeTypeRequest,
		RequestID:     scopeID,
		TraceID:       trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		Component:     "querynode",
		NodeID:        node.GetNodeID(),
		CollectionID:  collectionID,
		WorkloadClass: storageprofile.WorkloadClassRequestPath,
		WorkloadKind:  workload,
		StorageRole:   storageprofile.StorageRolePersistent,
	}
	if attribution.RequestID == "" {
		attribution.RequestID = fmt.Sprint(requestID)
	}
	scope := storageprofile.NewContributionScope(ctx, attribution, storageprofile.StorageProfileLevel(level))
	return scope.Bind(storageprofile.WithDefaultAttribution(ctx, attribution)), scope
}

func finishRequestStorageContribution(
	ctx context.Context,
	scope *storageprofile.Scope,
	scopeID, executionID string,
	nodeID int64,
	scannedColdBytes, scannedTotalBytes int64,
) []byte {
	if scope == nil {
		return nil
	}
	if paramtable.Get().QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() {
		total := uint64(max(scannedTotalBytes, 0))
		cold := uint64(max(scannedColdBytes, 0))
		served := total
		if cold < total {
			served = total - cold
		} else if cold >= total {
			served = 0
		}
		storageprofile.ObserveCache(ctx, storageprofile.CacheEvent{Tier: storageprofile.CacheTierTiered, Action: storageprofile.CacheActionRequested, Bytes: total})
		storageprofile.ObserveCache(ctx, storageprofile.CacheEvent{Tier: storageprofile.CacheTierTiered, Action: storageprofile.CacheActionCold, Bytes: cold})
		storageprofile.ObserveCache(ctx, storageprofile.CacheEvent{Tier: storageprofile.CacheTierTiered, Action: storageprofile.CacheActionServed, Bytes: served})
	}
	profile := scope.Snapshot()
	scope.Finish()
	if profile == nil {
		return nil
	}
	data, err := storageprofile.MarshalContribution(storageprofile.Contribution{
		Identity: storageprofile.ContributionIdentity{
			ClusterID:   paramtable.Get().CommonCfg.ClusterID.GetValue(),
			NodeID:      nodeID,
			ScopeID:     scopeID,
			ExecutionID: executionID,
		},
		Profile: profile,
	})
	if err != nil {
		metrics.StorageProfileDroppedSummaries.WithLabelValues(storageprofile.ProfileReasonSerialization.String()).Inc()
		return nil
	}
	return data
}
