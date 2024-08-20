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
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/Span.h"
#include "knowhere/sparse_utils.h"
#include "simdjson/common_defs.h"
#include "sys/mman.h"
namespace milvus {
constexpr size_t MMAP_STRING_PADDING = 1;
constexpr size_t MMAP_ARRAY_PADDING = 1;
class Chunk {
 public:
    Chunk() = default;
    Chunk(int64_t row_nums, char* data, size_t size)
        : row_nums_(row_nums), data_(data), size_(size) {
    }
    virtual ~Chunk() {
        munmap(data_, size_);
    }

 protected:
    char* data_;
    int64_t row_nums_;
    size_t size_;
};

// for fixed size data, includes fixed size array
template <typename T>
class FixedWidthChunk : public Chunk {
 public:
    FixedWidthChunk(int32_t row_nums, int32_t dim, char* data, size_t size)
        : Chunk(row_nums, data, size), dim_(dim){};

    milvus::SpanBase
    Span() const {
        auto null_bitmap_bytes_num = (row_nums_ + 7) / 8;
        return milvus::SpanBase(
            data_ + null_bitmap_bytes_num, row_nums_, sizeof(T) * dim_);
    }

 private:
    int dim_;
};

class StringChunk : public Chunk {
 public:
    StringChunk() = default;
    StringChunk(int32_t row_nums, char* data, size_t size)
        : Chunk(row_nums, data, size) {
        auto null_bitmap_bytes_num = (row_nums + 7) / 8;
        offsets_ = reinterpret_cast<uint64_t*>(data + null_bitmap_bytes_num);
    }

    std::vector<std::string_view>
    StringViews() const;

 protected:
    uint64_t* offsets_;
};

using JSONChunk = StringChunk;

class ArrayChunk : public Chunk {
 public:
    ArrayChunk(int32_t row_nums,
               char* data,
               size_t size,
               milvus::DataType element_type)
        : Chunk(row_nums, data, size), element_type_(element_type) {
        auto null_bitmap_bytes_num = (row_nums + 7) / 8;
        offsets_ = reinterpret_cast<uint64_t*>(data + null_bitmap_bytes_num);
        lens_ = offsets_ + row_nums;
        ConstructViews();
    }

    SpanBase
    Span() const;

    void
    ConstructViews();

 private:
    milvus::DataType element_type_;
    uint64_t* offsets_;
    uint64_t* lens_;
    std::vector<ArrayView> views_;
};

class SparseFloatVectorChunk : public Chunk {
 public:
    SparseFloatVectorChunk(int32_t row_nums, char* data, size_t size)
        : Chunk(row_nums, data, size) {
        vec_.resize(row_nums);
        auto null_bitmap_bytes_num = (row_nums + 7) / 8;
        auto offsets_ptr =
            reinterpret_cast<uint64_t*>(data + null_bitmap_bytes_num);
        for (int i = 0; i < row_nums; i++) {
            int vec_size = 0;
            if (i == row_nums - 1) {
                vec_size = size - offsets_ptr[i];
            } else {
                vec_size = offsets_ptr[i + 1] - offsets_ptr[i];
            }

            vec_[i] = {
                vec_size / knowhere::sparse::SparseRow<float>::element_size(),
                (uint8_t*)(data + offsets_ptr[i]),
                false};
        }
    }

    const char*
    Data() const {
        return static_cast<const char*>(static_cast<const void*>(vec_.data()));
    }

    // only for test
    std::vector<knowhere::sparse::SparseRow<float>>&
    Vec() {
        return vec_;
    }

 private:
    std::vector<knowhere::sparse::SparseRow<float>> vec_;
};
}  // namespace milvus