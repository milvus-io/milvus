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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// checkSegmentFilter checks whether the plan contains a PK predicate
// optimizable by segment filtering.
func checkSegmentFilter(plan *planpb.PlanNode) int32 {
	predicates, err := exprutil.ParseExprFromPlan(plan)
	if err != nil {
		log.Warn("checkSegmentFilter: failed to parse expr from plan, fallback to no PK filter",
			zap.Error(err))
		return common.PkFilterNoPkFilter
	}
	if predicates != nil && exprutil.HasOptimizablePkPredicate(predicates) {
		return common.PkFilterHasPkFilter
	}
	return common.PkFilterNoPkFilter
}
