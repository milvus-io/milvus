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

#include <stdint.h>
#include <deque>
#include <memory>
#include <optional>
#include <utility>

#include "common/QueryResult.h"
#include "exec/QueryContext.h"
#include "exec/expression/Expr.h"
#include "folly/FBVector.h"

namespace milvus {

class ElementFilterIterator : public VectorIterator {
 public:
    ElementFilterIterator(std::shared_ptr<VectorIterator> base_iterator,
                          exec::ExecContext* exec_context,
                          exec::ExprSet* expr_set);

    bool
    HasNext() override;

    std::optional<std::pair<int64_t, float>>
    Next() override;

 private:
    // Fetch a batch from base iterator, evaluate expression, and cache results
    // Steps:
    //  1. Fetch up to batch_size elements from base_iterator
    //  2. Batch evaluate element_expr on fetched elements
    //  3. Filter elements based on evaluation results
    //  4. Cache passing elements in filtered_buffer_
    void
    FetchAndFilterBatch();

    // Base iterator to fetch elements from
    std::shared_ptr<VectorIterator> base_iterator_;

    // Execution context for expression evaluation
    exec::ExecContext* exec_context_;

    // Expression set containing element-level filter expression
    exec::ExprSet* expr_set_;

    // Cache of filtered elements ready to be consumed
    std::deque<std::pair<int64_t, float>> filtered_buffer_;

    // Reusable buffers for batch fetching (avoid repeated allocations)
    FixedVector<int32_t> element_ids_buffer_;
    FixedVector<float> distances_buffer_;
};

}  // namespace milvus
