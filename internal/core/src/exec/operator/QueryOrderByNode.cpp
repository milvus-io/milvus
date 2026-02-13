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

#include "QueryOrderByNode.h"

#include <utility>

#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

namespace {

// Default output batch size for GetOutput
constexpr int64_t kDefaultOutputBatchSize = 1024;

}  // namespace

PhyQueryOrderByNode::PhyQueryOrderByNode(
    int32_t operator_id,
    DriverContext* ctx,
    const std::shared_ptr<const plan::OrderByNode>& order_by_node)
    : Operator(ctx,
               order_by_node->output_type(),
               operator_id,
               order_by_node->id(),
               "QueryOrderBy"),
      output_batch_size_(kDefaultOutputBatchSize) {
    // Extract sorting keys and orders from plan node
    const auto& sorting_keys = order_by_node->SortingKeys();
    const auto& sorting_orders = order_by_node->SortingOrders();

    AssertInfo(!sorting_keys.empty(),
               "QueryOrderByNode requires at least one sorting key");
    AssertInfo(sorting_keys.size() == sorting_orders.size(),
               "Number of sorting keys must match number of sort orders");

    auto output_type = order_by_node->output_type();
    AssertInfo(output_type != nullptr && output_type->column_count() > 0,
               "QueryOrderByNode requires non-empty output type");

    // Get column types from output type
    for (size_t i = 0; i < output_type->column_count(); ++i) {
        column_types_.push_back(output_type->column_type(i));
    }

    // Build input column indices (identity mapping)
    input_column_indices_.resize(output_type->column_count());
    for (size_t i = 0; i < output_type->column_count(); ++i) {
        input_column_indices_[i] = static_cast<int32_t>(i);
    }

    // Build sort key infos with proper column index mapping
    // Use field name to find the column index in output_type
    std::vector<SortKeyInfo> sort_key_infos;
    sort_key_infos.reserve(sorting_keys.size());

    for (size_t i = 0; i < sorting_keys.size(); ++i) {
        const auto& key = sorting_keys[i];
        const auto& order = sorting_orders[i];

        // Find column index by name in output_type
        int32_t col_idx =
            static_cast<int32_t>(output_type->GetChildIndex(key->name()));

        sort_key_infos.emplace_back(
            col_idx, order.ascending, order.nulls_first);

        LOG_DEBUG(
            "Sort key {}: field='{}', column_index={}, asc={}, nulls_first={}",
            i,
            key->name(),
            col_idx,
            order.ascending,
            order.nulls_first);
    }

    // Create SortBuffer
    sort_buffer_ = std::make_unique<SortBuffer>(
        column_types_, sort_key_infos, order_by_node->Limit());

    LOG_DEBUG(
        "PhyQueryOrderByNode created with {} sort keys, {} columns, limit={}",
        sorting_keys.size(),
        column_types_.size(),
        order_by_node->Limit());
}

void
PhyQueryOrderByNode::AddInput(RowVectorPtr& input) {
    if (input == nullptr || input->size() == 0) {
        return;
    }

    // Extract column vectors from input RowVector
    const auto& children = input->childrens();
    std::vector<ColumnVectorPtr> columns;
    columns.reserve(children.size());

    for (size_t i = 0; i < children.size(); ++i) {
        auto col_vec = std::dynamic_pointer_cast<ColumnVector>(children[i]);
        if (col_vec) {
            columns.push_back(col_vec);
        } else {
            // Handle case where child is not a ColumnVector
            // This shouldn't happen in normal operation
            ThrowInfo(Unsupported,
                      "QueryOrderByNode input child {} is not a ColumnVector",
                      i);
        }
    }

    // Verify column count matches expected
    if (columns.size() != column_types_.size()) {
        LOG_WARN(
            "QueryOrderByNode: input column count ({}) differs from expected "
            "({})",
            columns.size(),
            column_types_.size());
    }

    // Add rows to sort buffer
    auto num_rows = static_cast<vector_size_t>(input->size());
    sort_buffer_->AddRows(columns, num_rows);

    LOG_DEBUG("QueryOrderByNode: added {} rows, total={}",
              num_rows,
              sort_buffer_->NumInputRows());
}

void
PhyQueryOrderByNode::NoMoreInput() {
    Operator::NoMoreInput();  // Sets no_more_input_ = true

    // Trigger sorting
    sort_buffer_->NoMoreInput();

    LOG_DEBUG("QueryOrderByNode: NoMoreInput called, {} rows sorted",
              sort_buffer_->NumInputRows());
}

RowVectorPtr
PhyQueryOrderByNode::GetOutput() {
    if (!no_more_input_) {
        // Haven't received all input yet
        return nullptr;
    }

    if (!sort_buffer_->HasOutput()) {
        // No more output available
        return nullptr;
    }

    // Get next batch of sorted output
    auto output_columns = sort_buffer_->GetOutput(output_batch_size_);

    if (output_columns.empty()) {
        return nullptr;
    }

    // Wrap columns in RowVector
    auto row_vector = std::make_shared<RowVector>(std::move(output_columns));

    LOG_DEBUG("QueryOrderByNode: GetOutput returned {} rows, total output={}",
              row_vector->size(),
              sort_buffer_->NumOutputRows());

    return row_vector;
}

}  // namespace exec
}  // namespace milvus
