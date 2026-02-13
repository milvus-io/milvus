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

    //=========================================================================
    // Operator Interface Implementation
    //=========================================================================

    /**
     * @brief This operator always needs input until NoMoreInput is called
     */
    bool
    NeedInput() const override {
        return !no_more_input_;
    }

    /**
     * @brief Add input rows to the sort buffer
     *
     * Extracts column data from input RowVector and adds to SortBuffer.
     * Can be called multiple times before NoMoreInput().
     *
     * @param input RowVector containing rows to sort
     */
    void
    AddInput(RowVectorPtr& input) override;

    /**
     * @brief Signal that all input has been received, trigger sorting
     *
     * After this call:
     * - NeedInput() returns false
     * - GetOutput() starts returning sorted results
     */
    void
    NoMoreInput() override;

    /**
     * @brief Get next batch of sorted output
     *
     * Returns nullptr if:
     * - NoMoreInput() hasn't been called yet
     * - All sorted rows have been returned
     *
     * @return RowVector with sorted rows, or nullptr if no more output
     */
    RowVectorPtr
    GetOutput() override;

    /**
     * @brief Check if operator has finished producing output
     *
     * @return true if NoMoreInput was called and all output has been returned
     */
    bool
    IsFinished() override {
        return no_more_input_ && !sort_buffer_->HasOutput();
    }

    /**
     * @brief This is not a filter operator
     */
    bool
    IsFilter() const override {
        return false;
    }

    /**
     * @brief This operator is never blocked
     */
    BlockingReason
    IsBlocked(ContinueFuture* /* unused */) override {
        return BlockingReason::kNotBlocked;
    }

    /**
     * @brief ORDER BY preserves order (it defines the order)
     */
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

    // Column indices to extract from input RowVector
    // Maps sort buffer column index to input column index
    std::vector<int32_t> input_column_indices_;

    // Data types of columns in sort buffer
    std::vector<DataType> column_types_;
};

}  // namespace exec
}  // namespace milvus
