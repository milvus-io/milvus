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

package tasks

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const unknownTakeForOutputResultCount int64 = -1

func saturatingMultiplyNonNegative(left, right int64) int64 {
	if left < 0 || right < 0 {
		return unknownTakeForOutputResultCount
	}
	if left == 0 || right == 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}

func searchTakeForOutputResultCount(nq, topK, groupSize int64) int64 {
	if groupSize <= 0 {
		groupSize = 1
	}
	return saturatingMultiplyNonNegative(
		saturatingMultiplyNonNegative(nq, topK),
		groupSize,
	)
}

func retrieveTakeForOutputResultCount(req *internalpb.RetrieveRequest, plan *planpb.PlanNode) int64 {
	// Zero is the protobuf default and is left unset by internal requests such
	// as Delete QueryStream, so only a positive limit is a known upper bound.
	if plan != nil {
		term := plan.GetQuery().GetPredicates().GetTermExpr()
		if term != nil &&
			term.GetColumnInfo().GetIsPrimaryKey() &&
			!term.GetIsInField() {
			termCount := int64(len(term.GetValues()))
			if req != nil && req.GetLimit() > 0 {
				return min(termCount, req.GetLimit())
			}
			return termCount
		}
	}

	if req != nil && req.GetLimit() > 0 {
		return req.GetLimit()
	}
	return unknownTakeForOutputResultCount
}

func takeForOutputAllowed(resultCount, limit int64) bool {
	if limit == 0 {
		return true
	}
	return resultCount >= 0 && resultCount <= limit
}

func requestAllowsTakeForOutput(resultCount int64) bool {
	limit := paramtable.Get().QueryNodeCfg.TakeForOutputResultCountLimit.GetAsInt64()
	return takeForOutputAllowed(resultCount, limit)
}
