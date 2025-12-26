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
#include <folly/Range.h>
#include "common/Types.h"
#include "common/Vector.h"
#include "common/Utils.h"
#include "Aggregate.h"
#include "storage/Util.h"

namespace milvus {
namespace exec {

class Accumulator {
 public:
    Accumulator(bool isFixedSize, int32_t fixedSize, int32_t alignment);

    explicit Accumulator(Aggregate* aggregate);

    bool
    isFixedSize() const {
        return isFixedSize_;
    }

    int32_t
    alignment() const {
        return alignment_;
    }

    int32_t
    fixedWidthSize() const {
        return fixedSize_;
    }

 private:
    bool isFixedSize_;
    int32_t fixedSize_;
    int32_t alignment_;
};

/// Packed representation of offset, null byte offset and null mask for
/// a column inside a RowContainer.
class RowColumn {
 public:
    RowColumn(int32_t offset, int32_t nullOffset)
        : packedOffsets_(PackOffsets(offset, nullOffset)) {
    }

    int32_t
    offset() const {
        return packedOffsets_ >> 32;
    }

    int32_t
    nullByte() const {
        return static_cast<uint32_t>(packedOffsets_) >> 8;
    }

    uint8_t
    nullMask() const {
        return packedOffsets_ & 0xff;
    }

 private:
    static uint64_t
    PackOffsets(int32_t offset, int32_t nullOffset) {
        return (1UL << (nullOffset & 7)) | ((nullOffset & ~7UL) << 5) |
               static_cast<uint64_t>(offset) << 32;
    }

    const uint64_t packedOffsets_;
};

class RowContainer {
 public:
    RowContainer(const std::vector<DataType>& keyTypes,
                 const std::vector<Accumulator>& accumulators);

    ~RowContainer();

    /// Allocates a new row and initializes possible aggregates to null.
    char*
    newRow();

    const std::vector<DataType>&
    KeyTypes() const {
        return keyTypes_;
    }

    const RowColumn&
    columnAt(int32_t column_idx) const {
        return rowColumns_[column_idx];
    }

    static int32_t
    combineAlignments(int32_t a, int32_t b) {
        AssertInfo(__builtin_popcount(a) == 1,
                   "Alignment can only be power of 2, but got{}",
                   a);
        AssertInfo(__builtin_popcount(b) == 1,
                   "Alignment can only be power of 2, but got{}",
                   b);
        return std::max(a, b);
    }

    int32_t
    rowSizeOffset() const {
        return rowSizeOffset_;
    }

    static inline bool
    isNullAt(const char* row, int32_t nullByte, uint8_t nullMask) {
        return (row[nullByte] & nullMask) != 0;
    }

    static inline const std::string*&
    strAt(const char* group, int32_t offset) {
        return *reinterpret_cast<const std::string**>(
            const_cast<char*>(group + offset));
    }

    template <typename T>
    static inline T
    valueAt(const char* group, int32_t offset) {
        return *reinterpret_cast<const T*>(group + offset);
    }

    template <DataType Type>
    inline bool
    equalsNoNulls(const char* row,
                  int32_t offset,
                  const ColumnVectorPtr& column,
                  vector_size_t index) {
        if constexpr (Type == DataType::NONE || Type == DataType::ROW ||
                      Type == DataType::JSON || Type == DataType::ARRAY) {
            ThrowInfo(DataTypeInvalid,
                      "Cannot support complex data type:[ROW/JSON/ARRAY] in "
                      "rows container for now");
        } else {
            using T = typename TypeTraits<Type>::NativeType;
            T raw_value = column->ValueAt<T>(index);
            bool equal = false;
            if constexpr (std::is_same_v<T, std::string>) {
                equal = (raw_value == *(strAt(row, offset)));
            } else {
                equal = (milvus::comparePrimitiveAsc(
                             raw_value, valueAt<T>(row, offset)) == 0);
            }
            return equal;
        }
    }

    template <DataType Type>
    inline bool
    equalsWithNulls(const char* row,
                    int32_t offset,
                    int32_t nullByte,
                    uint8_t nullMask,
                    const ColumnVectorPtr& column,
                    vector_size_t index) {
        bool rowIsNull = isNullAt(row, nullByte, nullMask);
        bool columnIsNull = !column->ValidAt(index);
        if (rowIsNull || columnIsNull) {
            return rowIsNull == columnIsNull;
        }
        return equalsNoNulls<Type>(row, offset, column, index);
    }

    inline bool
    equals(const char* row,
           RowColumn column,
           const ColumnVectorPtr& column_data,
           vector_size_t index) {
        return MILVUS_DYNAMIC_TYPE_DISPATCH(equalsWithNulls,
                                            column_data->type(),
                                            row,
                                            column.offset(),
                                            column.nullByte(),
                                            column.nullMask(),
                                            column_data,
                                            index);
    }

    /// Stores the 'index'th value in 'columnVector' into 'row' at 'columnIndex'.
    void
    store(const ColumnVectorPtr& column_data,
          vector_size_t index,
          char* row,
          int32_t column_index);

    template <DataType Type>
    inline void
    storeWithNull(const ColumnVectorPtr& column,
                  vector_size_t index,
                  char* row,
                  int32_t offset,
                  int32_t nullByte,
                  uint8_t nullMask) {
        static std::string null_string_val = "";
        static std::string* null_string_val_ptr = &null_string_val;
        if constexpr (Type == DataType::NONE || Type == DataType::ROW ||
                      Type == DataType::JSON || Type == DataType::ARRAY) {
            ThrowInfo(DataTypeInvalid,
                      "Cannot support complex data type:[ROW/JSON/ARRAY] in "
                      "rows container for now");
        } else {
            using T = typename milvus::TypeTraits<Type>::NativeType;
            if (!column->ValidAt(index)) {
                row[nullByte] |= nullMask;
                if constexpr (std::is_same_v<T, std::string>) {
                    *reinterpret_cast<std::string**>(row + offset) =
                        null_string_val_ptr;
                } else {
                    *reinterpret_cast<T*>(row + offset) = T();
                }
                return;
            }
            storeNoNulls<Type>(column, index, row, offset);
        }
    }

    template <DataType Type>
    inline void
    storeNoNulls(const ColumnVectorPtr& column,
                 vector_size_t index,
                 char* group,
                 int32_t offset) {
        using T = typename milvus::TypeTraits<Type>::NativeType;
        if constexpr (Type == DataType::NONE || Type == DataType::ROW ||
                      Type == DataType::JSON || Type == DataType::ARRAY) {
            ThrowInfo(DataTypeInvalid,
                      "Cannot support complex data type:[ROW/JSON/ARRAY] in "
                      "rows container for now");
        } else {
            auto raw_val_ptr = column->RawValueAt(index, sizeof(T));
            if constexpr (std::is_same_v<T, std::string>) {
                // the string object and also the underlying char array are both allocated on the heap
                // must call clear method to deallocate these memory allocated for varchar type to avoid memory leak
                *reinterpret_cast<std::string**>(group + offset) =
                    new std::string(*static_cast<std::string*>(raw_val_ptr));
            } else {
                *reinterpret_cast<T*>(group + offset) =
                    *(static_cast<T*>(raw_val_ptr));
            }
        }
    }

    template <typename T>
    static void
    extractValuesWithNulls(const char* const* rows,
                           int32_t numRows,
                           int32_t offset,
                           int32_t nullByte,
                           uint8_t nullMask,
                           const VectorPtr& result) {
        AssertInfo(numRows == result->size(),
                   "extracted rows number should be equal to the size of "
                   "result vector");
        auto result_column_vec =
            std::dynamic_pointer_cast<ColumnVector>(result);
        AssertInfo(
            result_column_vec != nullptr,
            "Input column to extract result must be of ColumnVector type");
        for (auto i = 0; i < numRows; i++) {
            const char* row = rows[i];
            auto resultIndex = i;
            if (row == nullptr || isNullAt(row, nullByte, nullMask)) {
                result_column_vec->nullAt(resultIndex);
            } else {
                if constexpr (std::is_same_v<T, std::string> ||
                              std::is_same_v<T, std::string_view>) {
                    auto* str_ptr = strAt(row, offset);
                    result_column_vec->SetValueAt<T>(resultIndex, *str_ptr);
                } else {
                    result_column_vec->SetValueAt<T>(resultIndex,
                                                     valueAt<T>(row, offset));
                }
            }
        }
    }

    template <typename T>
    static void
    extractValuesNoNulls(const char* const* rows,
                         int32_t numRows,
                         int32_t offset,
                         const VectorPtr& result) {
        AssertInfo(numRows == result->size(),
                   "extracted rows number should be equal to the size of "
                   "result vector");
        auto result_column_vec =
            std::dynamic_pointer_cast<ColumnVector>(result);
        AssertInfo(
            result_column_vec != nullptr,
            "Input column to extract result must be of ColumnVector type");
        for (auto i = 0; i < numRows; i++) {
            const char* row = rows[i];
            if (row == nullptr) {
                result_column_vec->nullAt(i);
            } else {
                if constexpr (std::is_same_v<T, std::string> ||
                              std::is_same_v<T, std::string_view>) {
                    auto* str_ptr = strAt(row, offset);
                    result_column_vec->SetValueAt<T>(i, *str_ptr);
                } else {
                    result_column_vec->SetValueAt<T>(i,
                                                     valueAt<T>(row, offset));
                }
            }
        }
    }

    template <DataType Type>
    static void
    extractColumnTypedInternal(const char* const* rows,
                               int32_t numRows,
                               RowColumn column,
                               const VectorPtr& result) {
        result->resize(numRows);
        if constexpr (Type == DataType::ROW || Type == DataType::JSON ||
                      Type == DataType::ARRAY || Type == DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "Not Support Extract types:[ROW/JSON/ARRAY/NONE]");
        } else {
            using T = typename milvus::TypeTraits<Type>::NativeType;
            auto nullMask = column.nullMask();
            auto offset = column.offset();
            if (nullMask) {
                extractValuesWithNulls<T>(
                    rows, numRows, offset, column.nullByte(), nullMask, result);
            } else {
                extractValuesNoNulls<T>(rows, numRows, offset, result);
            }
        }
    }

    template <DataType Type>
    static void
    extractColumnTyped(const char* const* rows,
                       int32_t numRows,
                       RowColumn column,
                       const VectorPtr& result) {
        extractColumnTypedInternal<Type>(rows, numRows, column, result);
    }

    static void
    extractColumn(const char* const* rows,
                  int32_t num_rows,
                  RowColumn column,
                  const VectorPtr& result);

    void
    extractColumn(const char* const* rows,
                  int32_t numRows,
                  int32_t column_idx,
                  const VectorPtr& result) {
        extractColumn(rows, numRows, columnAt(column_idx), result);
    }

    const std::vector<char*>&
    allRows() const {
        return rows_;
    }

    static inline int32_t
    nullByte(int32_t nullOffset) {
        return nullOffset / 8;
    }

    static inline uint8_t
    nullMask(int32_t nullOffset) {
        return 1 << (nullOffset & 7);
    }

    void
    clear() {
        for (auto row : rows_) {
            for (auto i = 0; i < variable_offsets_.size(); i++) {
                auto& off = variable_offsets_[i];
                auto& row_col = columnAt(variable_idxes_[i]);
                bool isStrNull =
                    isNullAt(row, row_col.nullByte(), row_col.nullMask());
                auto str = *reinterpret_cast<std::string**>(row + off);
                if (!isStrNull && str) {
                    delete str;
                    str = nullptr;
                    *reinterpret_cast<std::string**>(row + off) = nullptr;
                }
            }
            delete[] row;
        }
        rows_.clear();
        numRows_ = 0;
    }

    char*
    initializeRow(char* row);

 private:
    const std::vector<DataType> keyTypes_;
    std::vector<int> variable_offsets_{};
    std::vector<int> variable_idxes_{};
    std::vector<uint32_t> offsets_;
    std::vector<RowColumn> rowColumns_;

    // How many bytes do the flags (null, free) occupy.
    uint32_t fixedRowSize_;
    uint32_t flagBytes_;

    // for rows containing variable width fields, we store row size at the end of the row
    uint32_t rowSizeOffset_ = 0;
    int alignment_ = 1;
    std::vector<Accumulator> accumulators_;
    uint64_t numRows_ = 0;
    std::vector<char*> rows_{};
};

inline void
RowContainer::extractColumn(const char* const* rows,
                            int32_t num_rows,
                            milvus::exec::RowColumn column,
                            const milvus::VectorPtr& result) {
    MILVUS_DYNAMIC_TYPE_DISPATCH(
        extractColumnTyped, result->type(), rows, num_rows, column, result);
}
}  // namespace exec
}  // namespace milvus
