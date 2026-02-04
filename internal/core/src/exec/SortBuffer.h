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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/operator/query-agg/RowContainer.h"

namespace milvus {
namespace exec {

/// Sort key specification for ORDER BY operations
struct SortKeyInfo {
    int32_t column_index;  // Index of the column in column_types (0-based)
    bool ascending;        // true = ASC (default), false = DESC
    bool nulls_first;      // true = NULLS FIRST, false = NULLS LAST (default)

    SortKeyInfo(int32_t col_idx, bool asc = true, bool nulls_first_val = false)
        : column_index(col_idx), ascending(asc), nulls_first(nulls_first_val) {
        AssertInfo(col_idx >= 0, "column_index must be non-negative");
    }
};

/**
 * @brief SortBuffer - A self-contained sorting component for ORDER BY operations
 *
 * Inspired by Velox's SortBuffer design, this class provides memory-efficient
 * sorting using pointer-based sorting with RowContainer storage.
 *
 * Key features:
 * - Pointer-based sorting: Only sorts pointers (8 bytes each), not row data
 * - Multi-field sorting: Supports compound ORDER BY (field1 ASC, field2 DESC)
 * - NULL handling: Configurable NULLS FIRST/LAST per sort key
 * - TopK optimization: Uses partial_sort when limit is small relative to input
 * - Batched output: Returns results in configurable batch sizes
 *
 * Usage pattern:
 *   1. Create SortBuffer with schema and sort keys
 *   2. Call AddInput() to accumulate rows (can be called multiple times)
 *   3. Call NoMoreInput() to trigger sorting
 *   4. Call GetOutput() repeatedly until HasOutput() returns false
 *
 * Memory model:
 *   - Current implementation is memory-only (no disk spilling)
 *   - Memory usage: O(rows * row_size) for data + O(rows * 8) for pointers
 *
 * @note This class is NOT thread-safe. External synchronization is required
 *       if used from multiple threads.
 */
class SortBuffer {
 public:
    /**
     * @brief Construct a SortBuffer for ORDER BY operations
     *
     * @param column_types Data types of ALL columns to store in the buffer
     * @param sort_keys Sort key specifications (column_index into column_types,
     *                  direction, nulls handling). Only these columns are used
     *                  for sorting; other columns are just stored and returned.
     * @param limit Maximum rows to output (-1 for unlimited)
     *
     * @note Offset is NOT supported at segment level. In distributed queries,
     *       offset must be applied at the proxy reduce level after k-way merge.
     *       Segments should use (offset + limit) as the limit parameter.
     */
    SortBuffer(const std::vector<DataType>& column_types,
               const std::vector<SortKeyInfo>& sort_keys,
               int64_t limit = -1);

    ~SortBuffer() = default;

    // Disable copy
    SortBuffer(const SortBuffer&) = delete;
    SortBuffer&
    operator=(const SortBuffer&) = delete;

    // Enable move
    SortBuffer(SortBuffer&&) = default;
    SortBuffer&
    operator=(SortBuffer&&) = default;

    //=========================================================================
    // Phase 1: Input Accumulation
    //=========================================================================

    /**
     * @brief Add a row to the buffer
     *
     * Stores column values from the provided vectors into RowContainer.
     * Must be called before NoMoreInput().
     *
     * @param columns Column vectors containing row data (one per key_type)
     * @param row_index Index of the row within the column vectors
     */
    void
    AddRow(const std::vector<ColumnVectorPtr>& columns,
           vector_size_t row_index);

    /**
     * @brief Add multiple rows from column vectors
     *
     * @param columns Column vectors containing row data
     * @param num_rows Number of rows to add (starting from index 0)
     */
    void
    AddRows(const std::vector<ColumnVectorPtr>& columns,
            vector_size_t num_rows);

    //=========================================================================
    // Phase 2: Finalization
    //=========================================================================

    /**
     * @brief Signal no more input, trigger sorting
     *
     * Must be called after all AddRow/AddRows calls and before GetOutput().
     * This method sorts the row pointers (not the row data itself).
     */
    void
    NoMoreInput();

    //=========================================================================
    // Phase 3: Output Extraction
    //=========================================================================

    /**
     * @brief Get next batch of sorted rows
     *
     * Returns column vectors containing the next batch of sorted rows.
     * Returns up to max_rows per call until all rows are exhausted.
     *
     * @param max_rows Maximum rows to return in this batch
     * @return Vector of column vectors, or empty if no more output
     */
    std::vector<VectorPtr>
    GetOutput(int64_t max_rows);

    /**
     * @brief Check if more output is available
     *
     * @return true if GetOutput() will return more data
     */
    bool
    HasOutput() const;

    //=========================================================================
    // State Queries
    //=========================================================================

    /// Number of input rows added
    int64_t
    NumInputRows() const {
        return num_input_rows_;
    }

    /// Number of output rows already returned
    int64_t
    NumOutputRows() const {
        return num_output_rows_;
    }

    /// Whether sorting has been performed
    bool
    IsSorted() const {
        return sorted_;
    }

    /// Number of columns in each row
    size_t
    NumColumns() const {
        return column_types_.size();
    }

 private:
    //=========================================================================
    // Internal Methods
    //=========================================================================

    /**
     * @brief Perform in-memory sort of row pointers
     *
     * Uses std::sort or std::partial_sort depending on limit.
     */
    void
    Sort();

    /**
     * @brief Compare two rows by sort keys
     *
     * @param lhs Pointer to first row in RowContainer
     * @param rhs Pointer to second row in RowContainer
     * @return negative if lhs < rhs, positive if lhs > rhs, 0 if equal
     */
    int
    Compare(const char* lhs, const char* rhs) const;

    /**
     * @brief Compare null flags only (used when at least one value is null)
     *
     * @param lhs_null Whether left value is null
     * @param rhs_null Whether right value is null
     * @param ascending Sort direction (affects null comparison when mixed)
     * @param nulls_first Null placement preference
     * @return Comparison result: negative if lhs < rhs, positive if lhs > rhs, 0 if equal
     */
    static int
    CompareNulls(bool lhs_null,
                 bool rhs_null,
                 bool ascending,
                 bool nulls_first);

    /**
     * @brief Compare two non-null values of the same type
     *
     * @tparam T Native type of the values
     * @param lhs_val Left value (must not be null)
     * @param rhs_val Right value (must not be null)
     * @param ascending Sort direction (true = ASC, false = DESC)
     * @return Comparison result
     */
    template <typename T>
    static int
    CompareNonNullValues(T lhs_val, T rhs_val, bool ascending);

    /**
     * @brief Extract output columns from sorted row pointers
     *
     * @param num_rows Number of rows to extract
     * @return Vector of column vectors
     */
    std::vector<VectorPtr>
    ExtractOutput(int64_t num_rows);

    //=========================================================================
    // Data Members
    //=========================================================================

    // Schema and configuration
    std::vector<DataType> column_types_;
    std::vector<SortKeyInfo> sort_keys_;
    int64_t limit_;

    // Data storage (contiguous row storage)
    std::unique_ptr<RowContainer> data_;

    // Sorted row pointers (8 bytes per row)
    // After Sort(), these point to rows in data_ in sorted order
    std::vector<char*> sorted_rows_;

    // State tracking
    bool sorted_ = false;
    int64_t num_input_rows_ = 0;
    int64_t num_output_rows_ = 0;
    int64_t output_cursor_ = 0;  // Current position in sorted_rows_
};

//=============================================================================
// Inline Implementation
//=============================================================================

inline int
SortBuffer::CompareNulls(bool lhs_null,
                         bool rhs_null,
                         bool ascending,
                         bool nulls_first) {
    if (lhs_null && rhs_null) {
        return 0;  // Both null, equal
    }
    // One is null, one is not
    if (nulls_first) {
        return lhs_null ? -1 : 1;
    } else {
        return lhs_null ? 1 : -1;
    }
}

template <typename T>
int
SortBuffer::CompareNonNullValues(T lhs_val, T rhs_val, bool ascending) {
    int result = 0;
    if constexpr (std::is_same_v<T, std::string> ||
                  std::is_same_v<T, std::string_view>) {
        if (lhs_val < rhs_val) {
            result = -1;
        } else if (lhs_val > rhs_val) {
            result = 1;
        }
    } else {
        // Numeric types
        result = milvus::comparePrimitiveAsc(lhs_val, rhs_val);
    }

    // Apply sort direction (ascending keeps result, descending inverts)
    return ascending ? result : -result;
}

}  // namespace exec
}  // namespace milvus
