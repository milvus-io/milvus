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

#include <memory>
#include <string>
#include <vector>

#include "exec/operator/Operator.h"
#include "exec/SortBuffer.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {

/**
 * @brief Physical operator for Query ORDER BY operations
 *
 * This is a BLOCKING operator that:
 * 1. Accumulates all input rows (AddInput phase)
 * 2. Sorts them by specified keys when no more input (NoMoreInput)
 * 3. Returns sorted results in batches (GetOutput phase)
 *
 * Pipeline position:
 *   FilterBitsNode → MvccNode → ProjectNode → QueryOrderByNode
 *
 * For GROUP BY + ORDER BY:
 *   FilterBitsNode → MvccNode → ProjectNode → AggregationNode → QueryOrderByNode
 *
 * The operator delegates all sorting logic to SortBuffer, keeping the
 * operator itself thin and focused on the Operator interface contract.
 */
class PhyQueryOrderByNode : public Operator {
 public:
    /**
     * @brief Construct a QueryOrderByNode operator
     *
     * @param operator_id Unique operator ID within the pipeline
     * @param ctx Driver context for execution
     * @param order_by_node The logical plan node containing sort specifications
     */
    PhyQueryOrderByNode(
        int32_t operator_id,
        DriverContext* ctx,
        const std::shared_ptr<const plan::OrderByNode>& order_by_node);

    ~PhyQueryOrderByNode() override = default;

    // Operator interface
    bool
    NeedInput() const override {
        return !no_more_input_;
    }

    void
    AddInput(RowVectorPtr& input) override;

    void
    NoMoreInput() override;

    RowVectorPtr
    GetOutput() override;

    bool
    IsFinished() override {
        return no_more_input_ && !sort_buffer_->HasOutput();
    }

    bool
    IsFilter() const override {
        return false;
    }

    BlockingReason
    IsBlocked(ContinueFuture* /* unused */) override {
        return BlockingReason::kNotBlocked;
    }

    bool
    PreserveOrder() const override {
        return true;
    }

    std::string
    ToString() const override {
        return "QueryOrderBy Operator";
    }

 private:
    // Sort buffer that handles all sorting logic
    std::unique_ptr<SortBuffer> sort_buffer_;

    // Output batch size (max rows per GetOutput call)
    int64_t output_batch_size_;

    // Data types of columns in sort buffer
    std::vector<DataType> column_types_;
};

}  // namespace exec
}  // namespace milvus
