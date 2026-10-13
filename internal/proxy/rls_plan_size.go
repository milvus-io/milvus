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

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// 2.6 has no membership filters; only RLS-bearing HybridSearch plans consume
// this request-wide budget.
func marshalPlanWithFilterSizeLimit(plan *planpb.PlanNode, accumulatedSize int64, accountWholePlan bool) ([]byte, int64, error) {
	nextSize := accumulatedSize
	if accountWholePlan {
		planSize := int64(proto.Size(plan))
		maxSize := paramtable.Get().ProxyCfg.MaxMembershipFilterPlanSize.GetAsInt64()
		if accumulatedSize > maxSize || planSize > maxSize-accumulatedSize {
			return nil, accumulatedSize, merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"aggregate filter plan size exceeds proxy.maxMembershipFilterPlanSize: %d + %d > %d bytes",
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
