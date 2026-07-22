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

// marshalPlanWithBloomFilterSizeLimit serializes a plan after accounting for
// its size against the request-wide bloom-plan budget. Non-bloom plans do not
// consume this Bloom-specific budget. For HybridSearch, callers pass the
// returned accumulated size into the next sub-plan so all sub-searches share
// one limit.
//
// proto.Size is intentionally evaluated before proto.Marshal: repeated uses of
// one client blob create independent FilterBlob fields in the compiled plan,
// and rejecting here avoids allocating the amplified serialized buffer.
func marshalPlanWithBloomFilterSizeLimit(plan *planpb.PlanNode, accumulatedSize int64) ([]byte, int64, error) {
	nextSize := accumulatedSize
	if planparserv2.PlanContainsBloomFilter(plan) {
		planSize := int64(proto.Size(plan))
		maxSize := paramtable.Get().ProxyCfg.MaxBloomFilterPlanSize.GetAsInt64()
		if accumulatedSize > maxSize || planSize > maxSize-accumulatedSize {
			return nil, accumulatedSize, merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"aggregate bloom_match plan size exceeds proxy.maxBloomFilterPlanSize: %d + %d > %d bytes",
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
