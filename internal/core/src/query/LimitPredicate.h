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

#pragma once

#include <cstdint>
#include <vector>

#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"

namespace milvus::query {

// Limit-bounded predicate evaluation for Query.
//
// A Query with a finite limit, no ORDER BY and no aggregation only needs the
// first `limit` rows, in primary-key order, that satisfy its filter. The plan
// builder removes expensive predicates from the top-level AND of the filter
// (RetrievePlanNode::limit_predicate_); the filter and visibility operators
// then produce candidates for the remaining filter, and this function
// evaluates the removed predicate on those candidates in batches, stopping as
// soon as enough rows passed.
//
// `offsets` holds the candidates in primary-key order and is reduced in place
// to the rows that passed, in the same order. For an element-level query,
// `element_indices` is the companion list of selected element indices per
// candidate; a passing row then contributes its list length to the quota and a
// failing row is dropped together with its list. The row that crosses the
// quota keeps its complete list.
//
// `limit` follows segcore conventions: Unlimited / NoLimit evaluate every
// candidate. UNKNOWN (NULL) results do not pass.
//
// Returns true when evaluation stopped before the last candidate, i.e. more
// passing rows may exist. Reduce relies on this to stop merging at a source
// that is drained but not exhausted.
bool
EvalLimitPredicate(exec::QueryContext* query_context,
                   const expr::TypedExprPtr& predicate,
                   int64_t limit,
                   std::vector<int64_t>& offsets,
                   std::vector<std::vector<int32_t>>* element_indices);

}  // namespace milvus::query
