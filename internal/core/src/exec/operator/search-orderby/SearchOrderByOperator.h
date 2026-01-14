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

#include <vector>
#include <optional>
#include <functional>
#include "common/Types.h"
#include "common/QueryResult.h"
#include "knowhere/comp/index_param.h"
#include "plan/PlanNode.h"
#include "segcore/SegmentInterface.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"

namespace milvus {
namespace exec {

void
SearchOrderBy(milvus::OpContext* op_ctx,
              const std::vector<plan::OrderByField>& order_by_fields,
              const segcore::SegmentInternalInterface& segment,
              std::vector<int64_t>& seg_offsets,
              std::vector<float>& distances,
              std::optional<std::vector<GroupByValueType>>& group_by_values,
              std::vector<size_t>& topk_per_nq_prefix_sum,
              const knowhere::MetricType& metric_type);

}  // namespace exec
}  // namespace milvus
