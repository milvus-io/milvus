// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <memory>
#include <vector>

#include "common/Types.h"
#include "Aggregate.h"
#include "plan/PlanNode.h"
#include "exec/operator/Operator.h"

namespace milvus {
namespace exec {

/// Information needed to evaluate an aggregate function.
struct AggregateInfo {
    /// Instance of the Aggregate class.
    std::unique_ptr<Aggregate> function_;

    /// Indices of the input columns in the input RowVector.
    std::vector<column_index_t> input_column_idxes_;

    /// Index of the result column in the output RowVector.
    column_index_t output_;
};

std::vector<AggregateInfo>
toAggregateInfo(const plan::AggregationNode& aggregationNode,
                const milvus::exec::OperatorContext& operatorCtx,
                uint32_t numKeys);

}  // namespace exec
}  // namespace milvus
