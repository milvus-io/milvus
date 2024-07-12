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
    SparseFloatVectorChunk(int32_t row_nums,
                           char* data,
                           size_t size,
                           std::vector<uint64_t>& offsets)
        : Chunk(row_nums, data, size), offsets_(offsets) {
    }

 private:
    std::vector<uint64_t>& offsets_;
};
}  // namespace milvus