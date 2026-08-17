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
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// wrapPlanCreationError adds request context to a plan-creation failure and
// keeps the Proxy's existing ParameterInvalid (1100) projection for expression
// parsing errors, while preserving the cause in the chain -- errors.Is still
// matches merr.ErrQueryPlan.
//
// The projection is deliberate and must not be "fixed" by dropping the
// Combine. This helper is on the shared path for every Query, Search and
// Delete expression, not just membership filters, so returning the parser's
// ErrQueryPlan (2201) verbatim changes a long-standing public contract: it
// breaks REST/SDK/Delete cases that assert 1100, and drops the
// parameter/argument keywords that the geometry error cases match on. Moving
// this boundary to 2201 is a worthwhile cross-cutting change, but it needs its
// own PR: REST, Go SDK and Delete expectations updated together, plus a
// release note.
func wrapPlanCreationError(err error, context string) error {
	return merr.Combine(merr.Wrap(err, context), merr.ErrParameterInvalid)
}

// marshalPlanWithMembershipFilterSizeLimit serializes a plan after accounting
// for its size against the request-wide membership-filter plan budget. Plans
// carrying no client pre-built membership blob do not consume it. For
// HybridSearch, callers pass the returned accumulated size into the next
// sub-plan so all sub-searches share one limit. Complex Delete uses the same
// gate before its shard fan-out.
//
// bloom_match and roaring_match share one budget because they consume one
// resource: both embed a client-built blob into the serialized plan that is
// then fanned out to every QueryNode. Splitting the budget would let a request
// carrying one of each spend twice what either alone is allowed. The generic
// config key retains the legacy Bloom-specific key as a compatibility fallback.
//
// proto.Size is intentionally evaluated before proto.Marshal: repeated uses of
// one client blob create independent blob fields in the compiled plan, and
// rejecting here avoids allocating the amplified serialized buffer.
func marshalPlanWithMembershipFilterSizeLimit(plan *planpb.PlanNode, accumulatedSize int64) ([]byte, int64, error) {
	nextSize := accumulatedSize
	if planparserv2.PlanContainsMembershipFilter(plan) {
		planSize := int64(proto.Size(plan))
		maxSize := paramtable.Get().ProxyCfg.MaxMembershipFilterPlanSize.GetAsInt64()
		if accumulatedSize > maxSize || planSize > maxSize-accumulatedSize {
			return nil, accumulatedSize, merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"aggregate membership-filter plan size exceeds proxy.maxMembershipFilterPlanSize: %d + %d > %d bytes",
				accumulatedSize, planSize, maxSize))
		}
		nextSize += planSize
	}

	serialized, err := proto.Marshal(plan)
	if err != nil {
		return nil, accumulatedSize, err
	}
	return serialized, nextSize, nil
}
