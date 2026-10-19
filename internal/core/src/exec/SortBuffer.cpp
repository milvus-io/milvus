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
#include <limits>
#include <string_view>

#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

namespace {

// With a limit, up to kCandidateBufferFactor * limit candidate rows are kept
// before shrinking back to limit. The slack amortizes each O(n) selection
// over the (factor - 1) * limit rows accepted in between.
constexpr int64_t kCandidateBufferFactor = 2;

// Left-hand side of SortBuffer::CompareImpl(): a row stored in the
// RowContainer.
struct StoredRow {
    const char* row;

    bool
    IsNull(int32_t, const RowColumn& column) const {
        return RowContainer::isNullAt(
            row, column.nullByte(), column.nullMask());
    }
    template <typename T>
    T
    ValueAt(int32_t, const RowColumn& column) const {
        return RowContainer::valueAt<T>(row, column.offset());
    }
    const std::string*
    StrAt(int32_t, const RowColumn& column) const {
        return RowContainer::strAt(row, column.offset());
    }
};

// Left-hand side of SortBuffer::CompareImpl(): an input row that has not been
// stored, read straight from its column vectors. Values are read exactly as
// RowContainer::storeWithNull()/storeNoNulls() read them (ValidAt and
// RawValueAt), so comparing in place equals comparing the stored row, and
// strings are viewed without being copied.
struct InputRow {
    const std::vector<ColumnVectorPtr>& columns;
    vector_size_t index;

    bool
    IsNull(int32_t column_index, const RowColumn&) const {
        return !columns[column_index]->ValidAt(index);
    }
    template <typename T>
    T
    ValueAt(int32_t column_index, const RowColumn&) const {
        return *static_cast<const T*>(
            columns[column_index]->RawValueAt(index, sizeof(T)));
    }
    const std::string*
    StrAt(int32_t column_index, const RowColumn&) const {
        return static_cast<const std::string*>(
            columns[column_index]->RawValueAt(index, sizeof(std::string)));
    }
};

}  // namespace

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

    if (limit_ > 0) {
        candidate_capacity_ = limit_ > std::numeric_limits<int64_t>::max() /
                                           kCandidateBufferFactor
                                  ? std::numeric_limits<int64_t>::max()
                                  : limit_ * kCandidateBufferFactor;
    }
}

void
SortBuffer::AddRow(const std::vector<ColumnVectorPtr>& columns,
                   vector_size_t row_index) {
    AssertInfo(!sorted_, "Cannot add input after NoMoreInput()");
    AssertInfo(columns.size() == column_types_.size(),
               "Column count mismatch: expected {}, got {}",
               column_types_.size(),
               columns.size());

    // With a limit, only rows that can still make the top limit_ are stored
    if (limit_ > 0) {
        OfferRow(columns, row_index);
        num_input_rows_++;
        return;
    }

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

    if (num_rows == 0) {
        return;
    }

    // With a limit, rows go through OfferRow() instead of being stored in bulk
    if (limit_ > 0) {
        for (vector_size_t i = 0; i < num_rows; ++i) {
            OfferRow(columns, i);
        }
        num_input_rows_ += num_rows;
        return;
    }

    // Without a limit
    // Phase 1: Batch-allocate all rows upfront
    std::vector<char*> new_rows(num_rows);
    for (vector_size_t i = 0; i < num_rows; ++i) {
        new_rows[i] = data_->newRow();
    }

    // Phase 2: Store row-by-row (row-major). RowContainer rows are contiguous
    // in memory, so writing all columns of the same row is cache-friendly.
    for (vector_size_t i = 0; i < num_rows; ++i) {
        for (size_t col = 0; col < columns.size(); ++col) {
            data_->store(
                columns[col], i, new_rows[i], static_cast<int32_t>(col));
        }
    }

    num_input_rows_ += num_rows;
}

// Bounded TopK (threshold + periodic selection): stores the input row as a
// candidate unless it can no longer make the top limit_; when the candidates
// are full, first shrinks them back to limit_ with a selection algorithm.
void
SortBuffer::OfferRow(const std::vector<ColumnVectorPtr>& columns,
                     vector_size_t row_index) {
    // A row that is not strictly better than the threshold can never make the
    // top limit_: at least limit_ candidates already compare <= it, and the
    // threshold only improves. Such rows are compared in place and never
    // stored. Ties are rejected too; as with the (unstable) sort, no
    // particular row among equal keys is guaranteed to be kept.
    if (threshold_row_ != nullptr &&
        CompareInput(columns, row_index, threshold_row_) >= 0) {
        return;
    }

    // Only a row that is kept needs room, so shrink just before storing it
    if (static_cast<int64_t>(sorted_rows_.size()) >= candidate_capacity_) {
        ShrinkCandidates();
    }

    // Reuse a row released by the last shrink before allocating a new one
    char* row;
    if (!free_rows_.empty()) {
        row = free_rows_.back();
        free_rows_.pop_back();
    } else {
        row = data_->newRow();
    }
    for (size_t col = 0; col < columns.size(); ++col) {
        data_->store(columns[col], row_index, row, static_cast<int32_t>(col));
    }
    sorted_rows_.push_back(row);
}

void
SortBuffer::ShrinkCandidates() {
    // Select the best limit_ candidates with a selection algorithm
    // (nth_element, O(n) on average), release the rest for reuse and remember
    // the worst kept row as the rejection threshold.
    std::nth_element(sorted_rows_.begin(),
                     sorted_rows_.begin() + (limit_ - 1),
                     sorted_rows_.end(),
                     [this](const char* lhs, const char* rhs) {
                         return Compare(lhs, rhs) < 0;
                     });
    for (size_t i = limit_; i < sorted_rows_.size(); ++i) {
        data_->resetRow(sorted_rows_[i]);
        free_rows_.push_back(sorted_rows_[i]);
    }
    sorted_rows_.resize(limit_);
    threshold_row_ = sorted_rows_[limit_ - 1];
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

    // Collect all row pointers from RowContainer. With a limit, sorted_rows_
    // already holds the candidate rows (a superset of the top limit_).
    if (limit_ <= 0) {
        const auto& all_rows = data_->allRows();
        sorted_rows_.reserve(all_rows.size());
        for (char* row : all_rows) {
            sorted_rows_.push_back(row);
        }
    }
    const auto num_sorted = sorted_rows_.size();

    // Perform sorting
    Sort();

    // Apply limit to sorted_rows_ if specified
    if (limit_ > 0) {
        int64_t keep =
            std::min(static_cast<int64_t>(sorted_rows_.size()), limit_);
        sorted_rows_.resize(keep);
    }

    sorted_ = true;

    LOG_DEBUG(
        "SortBuffer: {} input rows, sorted {} rows, keeping {} after limit",
        num_input_rows_,
        num_sorted,
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

// TODO(performance): The switch-case on data_type runs on every comparison
// (sorting, plus one CompareInput() per input row with a limit). Since sort
// key types are fixed at construction time,
// we could pre-bind typed comparators to eliminate the runtime dispatch.
// In practice, branch prediction handles this well (type never changes),
// so this is low-priority unless profiling shows it as a bottleneck.
template <typename Lhs>
int
SortBuffer::CompareImpl(const Lhs& lhs, const char* rhs) const {
    for (const auto& sort_key : sort_keys_) {
        const auto col = sort_key.column_index;
        const auto& row_column = data_->columnAt(col);
        const auto data_type = column_types_[col];

        // Check for nulls first
        bool lhs_null = lhs.IsNull(col, row_column);
        bool rhs_null = RowContainer::isNullAt(
            rhs, row_column.nullByte(), row_column.nullMask());

        // Handle null cases before reading values to avoid reading garbage
        if (lhs_null || rhs_null) {
            int result = CompareNulls(lhs_null, rhs_null, sort_key.nulls_first);
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
                auto lhs_val = lhs.template ValueAt<bool>(col, row_column);
                auto rhs_val =
                    RowContainer::valueAt<bool>(rhs, row_column.offset());
                result = CompareNonNullValues<bool>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::INT8: {
                auto lhs_val = lhs.template ValueAt<int8_t>(col, row_column);
                auto rhs_val =
                    RowContainer::valueAt<int8_t>(rhs, row_column.offset());
                result = CompareNonNullValues<int8_t>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::INT16: {
                auto lhs_val = lhs.template ValueAt<int16_t>(col, row_column);
                auto rhs_val =
                    RowContainer::valueAt<int16_t>(rhs, row_column.offset());
                result = CompareNonNullValues<int16_t>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::INT32: {
                auto lhs_val = lhs.template ValueAt<int32_t>(col, row_column);
                auto rhs_val =
                    RowContainer::valueAt<int32_t>(rhs, row_column.offset());
                result = CompareNonNullValues<int32_t>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::INT64:
            case DataType::TIMESTAMPTZ: {
                auto lhs_val = lhs.template ValueAt<int64_t>(col, row_column);
                auto rhs_val =
                    RowContainer::valueAt<int64_t>(rhs, row_column.offset());
                result = CompareNonNullValues<int64_t>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::FLOAT: {
                auto lhs_val = lhs.template ValueAt<float>(col, row_column);
                auto rhs_val =
                    RowContainer::valueAt<float>(rhs, row_column.offset());
                result = CompareNonNullValues<float>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::DOUBLE: {
                auto lhs_val = lhs.template ValueAt<double>(col, row_column);
                auto rhs_val =
                    RowContainer::valueAt<double>(rhs, row_column.offset());
                result = CompareNonNullValues<double>(
                    lhs_val, rhs_val, sort_key.ascending);
                break;
            }
            case DataType::VARCHAR:
            case DataType::STRING: {
                // Use string_view to avoid copying strings on every comparison
                const std::string* lhs_str = lhs.StrAt(col, row_column);
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
                ThrowInfo(UnexpectedError,
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

int
SortBuffer::Compare(const char* lhs, const char* rhs) const {
    return CompareImpl(StoredRow{lhs}, rhs);
}

int
SortBuffer::CompareInput(const std::vector<ColumnVectorPtr>& columns,
                         vector_size_t row_index,
                         const char* row) const {
    return CompareImpl(InputRow{columns, row_index}, row);
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
        auto output_vec =
            data_->extractColumnVector(rows_ptr,
                                       static_cast<int32_t>(num_rows),
                                       static_cast<int32_t>(col));
        result.push_back(std::move(output_vec));
    }

    return result;
}

}  // namespace exec
}  // namespace milvus
