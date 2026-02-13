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

#include "exec/SortBuffer.h"

#include <algorithm>
#include <functional>

#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

SortBuffer::SortBuffer(const std::vector<DataType>& column_types,
                       const std::vector<SortKeyInfo>& sort_keys,
                       int64_t limit)
    : column_types_(column_types), sort_keys_(sort_keys), limit_(limit) {
    AssertInfo(!sort_keys_.empty(),
               "SortBuffer requires at least one sort key");
    AssertInfo(!column_types_.empty(),
               "SortBuffer requires at least one column");

    // Validate sort key column indices
    for (const auto& sort_key : sort_keys_) {
        AssertInfo(sort_key.column_index >= 0 &&
                       sort_key.column_index <
                           static_cast<int32_t>(column_types_.size()),
                   "Sort key column_index {} out of range [0, {})",
                   sort_key.column_index,
                   column_types_.size());
    }

    // Create RowContainer with no accumulators (pure data storage)
    std::vector<Accumulator> empty_accumulators;
    data_ = std::make_unique<RowContainer>(column_types_, empty_accumulators);
}

void
SortBuffer::AddRow(const std::vector<ColumnVectorPtr>& columns,
                   vector_size_t row_index) {
    AssertInfo(!sorted_, "Cannot add input after NoMoreInput()");
    AssertInfo(columns.size() == column_types_.size(),
               "Column count mismatch: expected {}, got {}",
               column_types_.size(),
               columns.size());

    // Allocate a new row in RowContainer
    char* row = data_->newRow();

    // Store each column value into the row
    for (size_t col = 0; col < columns.size(); ++col) {
        data_->store(columns[col], row_index, row, static_cast<int32_t>(col));
    }

    num_input_rows_++;
}

void
SortBuffer::AddRows(const std::vector<ColumnVectorPtr>& columns,
                    vector_size_t num_rows) {
    AssertInfo(!sorted_, "Cannot add input after NoMoreInput()");
    AssertInfo(columns.size() == column_types_.size(),
               "Column count mismatch: expected {}, got {}",
               column_types_.size(),
               columns.size());

    for (vector_size_t i = 0; i < num_rows; ++i) {
        AddRow(columns, i);
    }
}

void
SortBuffer::NoMoreInput() {
    if (sorted_) {
        return;  // Already sorted
    }

    if (num_input_rows_ == 0) {
        sorted_ = true;
        return;
    }

    // Collect all row pointers from RowContainer
    const auto& all_rows = data_->allRows();
    sorted_rows_.reserve(all_rows.size());
    for (char* row : all_rows) {
        sorted_rows_.push_back(row);
    }

    // Perform sorting
    Sort();

    // Apply limit to sorted_rows_ if specified
    if (limit_ > 0) {
        int64_t keep =
            std::min(static_cast<int64_t>(sorted_rows_.size()), limit_);
        sorted_rows_.resize(keep);
    }

    sorted_ = true;

    LOG_DEBUG("SortBuffer: sorted {} rows, keeping {} after limit",
              num_input_rows_,
              sorted_rows_.size());
}

void
SortBuffer::Sort() {
    if (sorted_rows_.empty()) {
        return;
    }

    // Comparator lambda that calls our Compare method
    auto comparator = [this](const char* lhs, const char* rhs) {
        return Compare(lhs, rhs) < 0;
    };

    // TopK optimization using partial_sort vs full sort:
    //
    // - partial_sort: O(n log k) - only guarantees first k elements are sorted
    // - full sort:    O(n log n) - sorts entire array
    //
    // When limit (k) is small, partial_sort is significantly faster.
    // When k approaches n, full sort is faster due to better cache locality
    // (introsort vs heap-based algorithm).
    //
    // Threshold: use partial_sort when k < n/2. This is a practical balance -
    // partial_sort wins clearly when k << n, and the overhead is acceptable
    // when k is moderately large.
    if (limit_ > 0 && limit_ < static_cast<int64_t>(sorted_rows_.size()) / 2) {
        int64_t k = std::min(limit_, static_cast<int64_t>(sorted_rows_.size()));
        std::partial_sort(sorted_rows_.begin(),
                          sorted_rows_.begin() + k,
                          sorted_rows_.end(),
                          comparator);
        LOG_DEBUG("SortBuffer: used partial_sort for top-{}", k);
    } else {
        std::sort(sorted_rows_.begin(), sorted_rows_.end(), comparator);
        LOG_DEBUG("SortBuffer: used full sort for {} rows",
                  sorted_rows_.size());
    }
}

int
SortBuffer::Compare(const char* lhs, const char* rhs) const {
    for (const auto& sort_key : sort_keys_) {
        const auto& row_column = data_->columnAt(sort_key.column_index);
        const auto data_type = column_types_[sort_key.column_index];

        // Check for nulls first
        bool lhs_null = RowContainer::isNullAt(
            lhs, row_column.nullByte(), row_column.nullMask());
        bool rhs_null = RowContainer::isNullAt(
            rhs, row_column.nullByte(), row_column.nullMask());

        // Handle null cases before reading values to avoid reading garbage
        if (lhs_null || rhs_null) {
            int result = CompareNulls(
                lhs_null, rhs_null, sort_key.ascending, sort_key.nulls_first);
            if (result != 0) {
                return result;
            }
            // Both are null, continue to next sort key
            continue;
        }

        // Both values are non-null, safe to read
        int result = 0;
        switch (data_type) {
            case DataType::BOOL: {
                auto lhs_val =
                    RowContainer::valueAt<bool>(lhs, row_column.offset());
                auto rhs_val =
                    RowContainer::valueAt<bool>(rhs, row_column.offset());
                result = CompareNonNullValues<bool>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::INT8: {
                auto lhs_val =
                    RowContainer::valueAt<int8_t>(lhs, row_column.offset());
                auto rhs_val =
                    RowContainer::valueAt<int8_t>(rhs, row_column.offset());
                result = CompareNonNullValues<int8_t>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::INT16: {
                auto lhs_val =
                    RowContainer::valueAt<int16_t>(lhs, row_column.offset());
                auto rhs_val =
                    RowContainer::valueAt<int16_t>(rhs, row_column.offset());
                result = CompareNonNullValues<int16_t>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::INT32: {
                auto lhs_val =
                    RowContainer::valueAt<int32_t>(lhs, row_column.offset());
                auto rhs_val =
                    RowContainer::valueAt<int32_t>(rhs, row_column.offset());
                result = CompareNonNullValues<int32_t>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::INT64: {
                auto lhs_val =
                    RowContainer::valueAt<int64_t>(lhs, row_column.offset());
                auto rhs_val =
                    RowContainer::valueAt<int64_t>(rhs, row_column.offset());
                result = CompareNonNullValues<int64_t>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::FLOAT: {
                auto lhs_val =
                    RowContainer::valueAt<float>(lhs, row_column.offset());
                auto rhs_val =
                    RowContainer::valueAt<float>(rhs, row_column.offset());
                result = CompareNonNullValues<float>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::DOUBLE: {
                auto lhs_val =
                    RowContainer::valueAt<double>(lhs, row_column.offset());
                auto rhs_val =
                    RowContainer::valueAt<double>(rhs, row_column.offset());
                result = CompareNonNullValues<double>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::VARCHAR:
            case DataType::STRING: {
                // Use string_view to avoid copying strings on every comparison
                const std::string* lhs_str =
                    RowContainer::strAt(lhs, row_column.offset());
                const std::string* rhs_str =
                    RowContainer::strAt(rhs, row_column.offset());
                std::string_view lhs_val =
                    lhs_str ? std::string_view(*lhs_str) : std::string_view();
                std::string_view rhs_val =
                    rhs_str ? std::string_view(*rhs_str) : std::string_view();
                result = CompareNonNullValues<std::string_view>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            default:
                ThrowInfo(DataTypeInvalid,
                          "Unsupported data type for ORDER BY: {}",
                          static_cast<int>(data_type));
        }

        if (result != 0) {
            return result;
        }
        // Continue to next sort key if equal
    }

    return 0;  // All sort keys are equal
}

bool
SortBuffer::HasOutput() const {
    if (!sorted_) {
        return false;
    }
    if (sorted_rows_.empty()) {
        return false;
    }

    return output_cursor_ < static_cast<int64_t>(sorted_rows_.size());
}

std::vector<VectorPtr>
SortBuffer::GetOutput(int64_t max_rows) {
    AssertInfo(sorted_, "Must call NoMoreInput() before GetOutput()");

    if (sorted_rows_.empty()) {
        return {};
    }

    // Check if we've exhausted output
    if (output_cursor_ >= static_cast<int64_t>(sorted_rows_.size())) {
        return {};
    }

    // Calculate batch size
    int64_t remaining =
        static_cast<int64_t>(sorted_rows_.size()) - output_cursor_;
    int64_t batch_size = std::min(max_rows, remaining);

    // Apply limit if set
    if (limit_ > 0) {
        int64_t limit_remaining = limit_ - num_output_rows_;
        batch_size = std::min(batch_size, limit_remaining);
    }

    if (batch_size <= 0) {
        return {};
    }

    // Extract output
    auto output = ExtractOutput(batch_size);

    output_cursor_ += batch_size;
    num_output_rows_ += batch_size;

    return output;
}

std::vector<VectorPtr>
SortBuffer::ExtractOutput(int64_t num_rows) {
    std::vector<VectorPtr> result;
    result.reserve(column_types_.size());

    // Get row pointers for this batch
    const char* const* rows_ptr = sorted_rows_.data() + output_cursor_;

    // Extract each column
    for (size_t col = 0; col < column_types_.size(); ++col) {
        auto column_type = column_types_[col];

        // Create output ColumnVector with proper size
        // ColumnVector(DataType, size_t length) creates an empty vector with pre-allocated storage
        auto output_vec = std::make_shared<ColumnVector>(column_type, num_rows);

        // Use RowContainer's extractColumn to fill the output
        data_->extractColumn(rows_ptr,
                             static_cast<int32_t>(num_rows),
                             static_cast<int32_t>(col),
                             output_vec);

        result.push_back(std::move(output_vec));
    }

    return result;
}

}  // namespace exec
}  // namespace milvus
