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

#include "ComputeProjectNode.h"

#include <cmath>
#include <cstdlib>

#include "log/Log.h"

namespace milvus {
namespace exec {

PhyComputeProjectNode::PhyComputeProjectNode(
    int32_t operator_id,
    DriverContext* ctx,
    const std::shared_ptr<const plan::ComputeProjectNode>& projectNode)
    : Operator(ctx,
               projectNode->output_type(),
               operator_id,
               projectNode->id(),
               "PhyComputeProjectNode"),
      project_items_(projectNode->items()),
      input_type_(projectNode->output_type()) {
}

namespace {

// Try to parse a string argument as a double literal.
// Returns true if successful, false if it's a column reference.
bool
TryParseLiteral(const std::string& arg, double& value) {
    char* end = nullptr;
    value = std::strtod(arg.c_str(), &end);
    return end != arg.c_str() && *end == '\0';
}

// Resolve an argument to column values (as doubles).
// If it's a literal, fills the vector with that constant.
// If it's a column name, extracts values from the RowVector.
bool
ResolveArgToDoubles(const std::string& arg,
                    const RowVectorPtr& input,
                    const RowTypePtr& input_type,
                    size_t num_rows,
                    std::vector<double>& out) {
    out.resize(num_rows);

    // Try as literal first
    double lit_val;
    if (TryParseLiteral(arg, lit_val)) {
        std::fill(out.begin(), out.end(), lit_val);
        return true;
    }

    // Try as column reference
    if (!input_type || input_type == RowType::None) {
        LOG_WARN(
            "ComputeProject: cannot resolve arg '{}' without column names",
            arg);
        return false;
    }

    int col_idx = -1;
    try {
        col_idx = input_type->GetChildIndex(arg);
    } catch (...) {
        LOG_WARN("ComputeProject: column '{}' not found", arg);
        return false;
    }

    auto col = std::dynamic_pointer_cast<ColumnVector>(input->child(col_idx));
    if (!col) {
        return false;
    }

    auto dtype = col->type();
    for (size_t i = 0; i < num_rows; i++) {
        switch (dtype) {
            case DataType::INT8:
                out[i] = static_cast<double>(col->ValueAt<int8_t>(i));
                break;
            case DataType::INT16:
                out[i] = static_cast<double>(col->ValueAt<int16_t>(i));
                break;
            case DataType::INT32:
                out[i] = static_cast<double>(col->ValueAt<int32_t>(i));
                break;
            case DataType::INT64:
                out[i] = static_cast<double>(col->ValueAt<int64_t>(i));
                break;
            case DataType::FLOAT:
                out[i] = static_cast<double>(col->ValueAt<float>(i));
                break;
            case DataType::DOUBLE:
                out[i] = col->ValueAt<double>(i);
                break;
            default:
                out[i] = 0.0;
                break;
        }
    }
    return true;
}

}  // namespace

void
PhyComputeProjectNode::AddInput(RowVectorPtr& input) {
    if (!input || input->size() == 0) {
        output_ = input;
        return;
    }

    auto num_rows = static_cast<size_t>(input->size());

    // Start with all existing columns from input
    std::vector<VectorPtr> output_columns;
    for (auto& child : input->childrens()) {
        output_columns.push_back(child);
    }

    // Evaluate each project item and append as a new column
    for (auto& item : project_items_) {
        auto& func = item.function_name;
        auto& args = item.args;

        // Binary arithmetic operations: add, sub, mul, div
        if ((func == "add" || func == "sub" || func == "mul" ||
             func == "div") &&
            args.size() == 2) {
            std::vector<double> left, right;
            if (!ResolveArgToDoubles(
                    args[0], input, input_type_, num_rows, left) ||
                !ResolveArgToDoubles(
                    args[1], input, input_type_, num_rows, right)) {
                LOG_WARN("ComputeProject: failed to resolve args for '{}'",
                         func);
                continue;
            }

            auto result_col =
                std::make_shared<ColumnVector>(DataType::DOUBLE, num_rows);
            for (size_t i = 0; i < num_rows; i++) {
                double val = 0.0;
                if (func == "add")
                    val = left[i] + right[i];
                else if (func == "sub")
                    val = left[i] - right[i];
                else if (func == "mul")
                    val = left[i] * right[i];
                else if (func == "div")
                    val = (right[i] != 0.0) ? left[i] / right[i] : 0.0;
                result_col->SetValueAt<double>(i, val);
            }
            output_columns.push_back(result_col);
        } else {
            // Unsupported function: log and pass through without adding column
            LOG_WARN(
                "ComputeProject: unsupported function '{}' with {} args, "
                "skipping",
                func,
                args.size());
        }
    }

    output_ = std::make_shared<RowVector>(std::move(output_columns));
}

RowVectorPtr
PhyComputeProjectNode::GetOutput() {
    if (is_finished_) {
        return nullptr;
    }
    is_finished_ = true;
    return output_;
}

}  // namespace exec
}  // namespace milvus
