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

package proxy

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func beginProxyStorageProfile(ctx context.Context, workload storageprofile.WorkloadKind) (context.Context, *storageprofile.Scope, storageprofile.StorageProfileLevel, string) {
	requested, explicit := storageprofile.RequestedLevelFromContext(ctx)
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID().String()
	if traceID == "00000000000000000000000000000000" || traceID == "" {
		traceID = fmt.Sprintf("%d/%d", paramtable.GetNodeID(), time.Now().UnixNano())
	}
	user, _ := GetCurUserFromContext(ctx)
	attribution := storageprofile.Attribution{
		ScopeType:     storageprofile.ScopeTypeRequest,
		UserID:        user,
		RequestID:     traceID,
		TraceID:       traceID,
		Component:     "proxy",
		NodeID:        paramtable.GetNodeID(),
		WorkloadClass: storageprofile.WorkloadClassRequestPath,
		WorkloadKind:  workload,
	}
	scope, decision := storageprofile.NewRequestScope(ctx, attribution, requested, explicit, true)
	return scope.Bind(storageprofile.WithDefaultAttribution(ctx, attribution)), scope, decision.Effective, traceID
}

func publishMergedStorageProfiles(ctx context.Context, payloads ...[]byte) {
	mergedPayload, err := storageprofile.MergeContributionPayloads(payloads...)
	if err != nil {
		metrics.StorageProfileDroppedSummaries.WithLabelValues(storageprofile.ProfileReasonSerialization.String()).Inc()
		return
	}
	contributions, err := storageprofile.UnmarshalContributions(mergedPayload)
	if err != nil {
		metrics.StorageProfileDroppedSummaries.WithLabelValues(storageprofile.ProfileReasonSerialization.String()).Inc()
		return
	}
	profile := storageprofile.MergeContributions(contributions)
	if profile != nil {
		_ = (storageprofile.NoopSummarySink{}).Publish(ctx, profile)
	}
}
