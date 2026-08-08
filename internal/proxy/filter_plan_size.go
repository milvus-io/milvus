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

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
)

// wrapPlanCreationError preserves resource-limit errors produced by the parser
// while retaining the Proxy's existing ParameterInvalid projection for all
// other expression parsing failures.
func wrapPlanCreationError(err error, context string) error {
	if errors.Is(err, merr.ErrParameterTooLarge) {
		return merr.Wrap(err, context)
	}
	// Keep the established client-visible rendering for parser failures. SDK
	// and e2e compatibility checks rely on the trailing "invalid parameter"
	// sentinel text. Combining the contextual cause with the sentinel preserves
	// the original error chain without flattening the cause into a format string.
	return merr.Combine(merr.Wrap(err, context), merr.ErrParameterInvalid)
}

// marshalPlanWithFilterSizeLimit serializes a plan after accounting for its
// size against the request-wide filter-plan budget. Plans carrying no client
// pre-built filter blob do not consume it. For HybridSearch, callers pass the
// returned accumulated size into the next sub-plan so all sub-searches share
// one limit. Complex Delete uses the same gate before its shard fan-out.
//
// bloom_match and roaring_match share one budget because they consume one
// resource: both embed a client-built blob into the serialized plan that is
// then fanned out to every QueryNode. Splitting the budget would let a request
// carrying one of each spend twice what either alone is allowed. The config key
// keeps its original bloom-specific name to avoid breaking deployments that
// already set it.
//
// proto.Size is intentionally evaluated before proto.Marshal: repeated uses of
// one client blob create independent blob fields in the compiled plan, and
// rejecting here avoids allocating the amplified serialized buffer.
func marshalPlanWithFilterSizeLimit(plan *planpb.PlanNode, accumulatedSize int64) ([]byte, int64, error) {
	serialized, nextSize, _, err := marshalPlanWithFilterSizeLimits(plan, accumulatedSize, 0)
	return serialized, nextSize, err
}

// marshalPlanWithFilterSizeLimits extends the serialized plan budget with a
// request-wide decoded-memory budget for roaring_match. HybridSearch passes
// both accumulated values from one sub-plan to the next; single-plan callers
// use marshalPlanWithFilterSizeLimit, which starts the decoded budget at zero.
func marshalPlanWithFilterSizeLimits(
	plan *planpb.PlanNode,
	accumulatedSize int64,
	accumulatedRoaringDecodedBytes uint64,
) ([]byte, int64, uint64, error) {
	nextSize := accumulatedSize
	if planparserv2.PlanContainsBloomFilter(plan) || planparserv2.PlanContainsRoaringFilter(plan) {
		planSize := int64(proto.Size(plan))
		maxSize := paramtable.Get().ProxyCfg.MaxBloomFilterPlanSize.GetAsInt64()
		if accumulatedSize > maxSize || planSize > maxSize-accumulatedSize {
			return nil, accumulatedSize, accumulatedRoaringDecodedBytes, merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"aggregate membership-filter plan size exceeds proxy.maxBloomFilterPlanSize: %d + %d > %d bytes",
				accumulatedSize, planSize, maxSize))
		}
		nextSize += planSize
	}

	nextRoaringDecodedBytes := accumulatedRoaringDecodedBytes
	if planparserv2.PlanContainsRoaringFilter(plan) {
		planDecodedBytes, err := planparserv2.EstimateRoaringFilterPlanDecodedBytes(plan)
		if err != nil {
			return nil, accumulatedSize, accumulatedRoaringDecodedBytes, err
		}
		if accumulatedRoaringDecodedBytes > roaringfilter.MaxEstimatedDecodedBytes ||
			planDecodedBytes > roaringfilter.MaxEstimatedDecodedBytes-accumulatedRoaringDecodedBytes {
			return nil, accumulatedSize, accumulatedRoaringDecodedBytes, merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"aggregate roaring_match estimated decoded size exceeds maximum: %d + %d > %d bytes",
				accumulatedRoaringDecodedBytes, planDecodedBytes, roaringfilter.MaxEstimatedDecodedBytes))
		}
		nextRoaringDecodedBytes += planDecodedBytes
	}

	serialized, err := proto.Marshal(plan)
	if err != nil {
		return nil, accumulatedSize, accumulatedRoaringDecodedBytes, err
	}
	return serialized, nextSize, nextRoaringDecodedBytes, nil
}
