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

#pragma once

#include <sys/types.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>
#include "arrow/array/array_base.h"
#include "arrow/record_batch.h"
#include "common/Array.h"
#include "common/ChunkTarget.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/Span.h"
#include "knowhere/sparse_utils.h"
#include "simdjson/common_defs.h"
#include "sys/mman.h"
#include "common/Types.h"

namespace milvus {
constexpr uint64_t MMAP_STRING_PADDING = 1;
constexpr uint64_t MMAP_ARRAY_PADDING = 1;
class Chunk {
 public:
    Chunk() = default;
    Chunk(int64_t row_nums, char* data, uint64_t size, bool nullable)
        : data_(data), row_nums_(row_nums), size_(size), nullable_(nullable) {
        if (nullable) {
            valid_.reserve(row_nums);
            for (int i = 0; i < row_nums; i++) {
                valid_.push_back((data[i >> 3] >> (i & 0x07)) & 1);
            }
        }
    }
    virtual ~Chunk() {
        munmap(data_, size_);
    }

    uint64_t
    Size() const {
        return size_;
    }

    size_t
    CellByteSize() const {
        return size_;
    }

    int64_t
    RowNums() const {
        return row_nums_;
    }

    virtual const char*
    ValueAt(int64_t idx) const = 0;

    virtual const char*
    Data() const {
        return data_;
    }

    const char*
    RawData() const {
        return data_;
    }

    virtual bool
    isValid(int offset) {
        if (nullable_) {
            return valid_[offset];
        }
        return true;
    };

 protected:
    char* data_;
    int64_t row_nums_;
    uint64_t size_;
    bool nullable_;
    FixedVector<bool>
        valid_;  // parse null bitmap to valid_ to be compatible with SpanBase
};
// for fixed size data, includes fixed size array
class FixedWidthChunk : public Chunk {
 public:
    FixedWidthChunk(int32_t row_nums,
                    int32_t dim,
                    char* data,
                    uint64_t size,
                    uint64_t element_size,
                    bool nullable)
        : Chunk(row_nums, data, size, nullable),
          dim_(dim),
          element_size_(element_size) {
        auto null_bitmap_bytes_num = nullable_ ? (row_nums_ + 7) / 8 : 0;
        data_start_ = data_ + null_bitmap_bytes_num;
    };

    milvus::SpanBase
    Span() const {
        return milvus::SpanBase(data_start_,
                                nullable_ ? valid_.data() : nullptr,
                                row_nums_,
                                element_size_ * dim_);
    }

    const char*
    ValueAt(int64_t idx) const override {
        return data_start_ + idx * element_size_ * dim_;
    }

    const char*
    Data() const override {
        return data_start_;
    }

 private:
    int dim_;
    int element_size_;
    const char* data_start_;
};
// A StringChunk is a class that represents a collection of strings stored in a contiguous memory block.
// It is initialized with the number of rows, a pointer to the data, the size of the data, and a boolean
// indicating whether the data can contain null values. The data is accessed using offsets, which are
// stored after an optional null bitmap. Each string is represented by a range in the data block, defined
// by these offsets.
//
// Example of a valid StringChunk:
//
// Suppose we have a data block containing the strings "apple", "banana", and "cherry", and we want to
// create a StringChunk for these strings. The data block might look like this:
//
// [null_bitmap][offsets][string_data]
// [00000000] [17, 22, 28, 34]  ["apple", "banana", "cherry"]
//
// Here, the null_bitmap is empty (indicating no nulls), the offsets array indicates the start of each
// string in the data block, and the string_data contains the actual string content.
//
// StringChunk exampleChunk(3, dataPointer, dataSize, false);
//
// In this example, 'exampleChunk' is a StringChunk with 3 rows, a pointer to the data stored in 'dataPointer',
// a total data size of 'dataSize', and it does not support nullability.

class StringChunk : public Chunk {
 public:
    StringChunk() = default;
    StringChunk(int32_t row_nums, char* data, uint64_t size, bool nullable)
        : Chunk(row_nums, data, size, nullable) {
        auto null_bitmap_bytes_num = nullable_ ? (row_nums_ + 7) / 8 : 0;
        offsets_ = reinterpret_cast<uint32_t*>(data + null_bitmap_bytes_num);
    }

    std::string_view
    operator[](const int i) const {
        if (i < 0 || i > row_nums_) {
            PanicInfo(ErrorCode::OutOfRange, "index out of range");
        }

        return {data_ + offsets_[i], offsets_[i + 1] - offsets_[i]};
    }

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    StringViews(std::optional<std::pair<int64_t, int64_t>> offset_len);

    int
    binary_search_string(std::string_view target) {
        // only supported sorted pk
        int left = 0;
        int right = row_nums_ - 1;  // `right` should be num_rows_ - 1
        int result =
            -1;  // Initialize result to store the first occurrence index

        while (left <= right) {
            int mid = left + (right - left) / 2;
            std::string_view midString = (*this)[mid];
            if (midString == target) {
                result = mid;     // Store the index of match
                right = mid - 1;  // Continue searching in the left half
            } else if (midString < target) {
                // midString < target
                left = mid + 1;
            } else {
                // midString > target
                right = mid - 1;
            }
        }
        return result;
    }

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    ViewsByOffsets(const FixedVector<int32_t>& offsets);

    const char*
    ValueAt(int64_t idx) const override {
        return (*this)[idx].data();
    }

    uint32_t*
    Offsets() {
        return offsets_;
    }

 protected:
    uint32_t* offsets_;
};

using JSONChunk = StringChunk;

// An ArrayChunk is a class that represents a collection of arrays stored in a contiguous memory block.
// It is initialized with the number of rows, a pointer to the data, the size of the data, the element type,
// and a boolean indicating whether the data can contain null values. The data is accessed using offsets and lengths,
// which are stored after an optional null bitmap. Each array is represented by a range in the data block.
//
// Example of a valid ArrayChunk:
//
// Suppose we have a data block containing arrays of integers [1, 2, 3], [4, 5], and [6, 7, 8, 9], and we want to
// create an ArrayChunk for these arrays. The data block might look like this:
//
// [null_bitmap][offsets_lens][array_data]
// [00000000] [24, 3, 36, 2, 44, 4, 60] [1, 2, 3, 4, 5, 6, 7, 8, 9]
//
// For string arrays, the structure is more complex as each string element needs its own offset:
// [null_bitmap][offsets_lens][array1_offsets][array1_data][array2_offsets][array2_data][array3_offsets][array3_data]
// [00000000] [24, 3, 48, 2, 64, 4, 96] [0, 5, 11, 16] ["hello", "world", "!"] [0, 3, 6] ["foo", "bar"] [0, 6, 12, 18, 24] ["apple", "orange", "banana", "grape"]
//
// Here, the null_bitmap is empty (indicating no nulls), the offsets_lens array contains pairs of (offset, length)
// for each array, and the array_data contains the actual array elements.
//
// ArrayChunk exampleChunk(3, dataPointer, dataSize, DataType::INT32, false);
//
// In this example, 'exampleChunk' is an ArrayChunk with 3 rows, a pointer to the data stored in 'dataPointer',
// a total data size of 'dataSize', element type INT32, and it does not support nullability.

class ArrayChunk : public Chunk {
 public:
    ArrayChunk(int32_t row_nums,
               char* data,
               uint64_t size,
               milvus::DataType element_type,
               bool nullable)
        : Chunk(row_nums, data, size, nullable), element_type_(element_type) {
        auto null_bitmap_bytes_num = 0;
        if (nullable) {
            null_bitmap_bytes_num = (row_nums + 7) / 8;
        }
        offsets_lens_ =
            reinterpret_cast<uint32_t*>(data + null_bitmap_bytes_num);
    }

    ArrayView
    View(int idx) const {
        int idx_off = 2 * idx;
        auto offset = offsets_lens_[idx_off];
        auto len = offsets_lens_[idx_off + 1];
        auto next_offset = offsets_lens_[idx_off + 2];
        auto data_ptr = data_ + offset;
        uint32_t offsets_bytes_len = 0;
        uint32_t* offsets_ptr = nullptr;
        if (IsStringDataType(element_type_)) {
            offsets_bytes_len = len * sizeof(uint32_t);
            offsets_ptr = reinterpret_cast<uint32_t*>(data_ptr);
        }

        return ArrayView(data_ptr + offsets_bytes_len,
                         len,
                         next_offset - offset - offsets_bytes_len,
                         element_type_,
                         offsets_ptr);
    }

    std::pair<std::vector<ArrayView>, FixedVector<bool>>
    Views(std::optional<std::pair<int64_t, int64_t>> offset_len =
              std::nullopt) const {
        auto start_offset = 0;
        auto len = row_nums_;
        if (offset_len.has_value()) {
            start_offset = offset_len->first;
            len = offset_len->second;
            AssertInfo(start_offset >= 0 && start_offset < row_nums_,
                       "Retrieve array views with out-of-bound offset:{}, "
                       "len:{}, wrong",
                       start_offset,
                       len);
            AssertInfo(len > 0 && len <= row_nums_,
                       "Retrieve array views with out-of-bound offset:{}, "
                       "len:{}, wrong",
                       start_offset,
                       len);
            AssertInfo(start_offset + len <= row_nums_,
                       "Retrieve array views with out-of-bound offset:{}, "
                       "len:{}, wrong",
                       start_offset,
                       len);
        }
        std::vector<ArrayView> views;
        views.reserve(len);
        auto end_offset = start_offset + len;
        for (auto i = start_offset; i < end_offset; i++) {
            views.emplace_back(View(i));
        }
        if (nullable_) {
            FixedVector<bool> res_valid(valid_.begin() + start_offset,
                                        valid_.begin() + end_offset);
            return {std::move(views), std::move(res_valid)};
        }
        return {std::move(views), {}};
    }

    const char*
    ValueAt(int64_t idx) const override {
        PanicInfo(ErrorCode::Unsupported,
                  "ArrayChunk::ValueAt is not supported");
    }

 private:
    milvus::DataType element_type_;
    uint32_t* offsets_lens_;
};

class SparseFloatVectorChunk : public Chunk {
 public:
    SparseFloatVectorChunk(int32_t row_nums,
                           char* data,
                           uint64_t size,
                           bool nullable)
        : Chunk(row_nums, data, size, nullable) {
        vec_.resize(row_nums);
        auto null_bitmap_bytes_num = (row_nums + 7) / 8;
        auto offsets_ptr =
            reinterpret_cast<uint64_t*>(data + null_bitmap_bytes_num);
        for (int i = 0; i < row_nums; i++) {
            vec_[i] = {(offsets_ptr[i + 1] - offsets_ptr[i]) /
                           knowhere::sparse::SparseRow<float>::element_size(),
                       reinterpret_cast<uint8_t*>(data + offsets_ptr[i]),
                       false};
            dim_ = std::max(dim_, vec_[i].dim());
        }
    }

    const char*
    Data() const override {
        return static_cast<const char*>(static_cast<const void*>(vec_.data()));
    }

    const char*
    ValueAt(int64_t i) const override {
        return static_cast<const char*>(
            static_cast<const void*>(vec_.data() + i));
    }

    // only for test
    std::vector<knowhere::sparse::SparseRow<float>>&
    Vec() {
        return vec_;
    }

    int64_t
    Dim() {
        return dim_;
    }

 private:
    int64_t dim_ = 0;
    std::vector<knowhere::sparse::SparseRow<float>> vec_;
};
}  // namespace milvus