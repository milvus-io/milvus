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

#include <unistd.h>
#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "cachinglayer/Utils.h"
#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Span.h"
#include "common/TypeTraits.h"
#include "common/Types.h"
#include "common/VectorArray.h"
#include "folly/FBVector.h"
#include "knowhere/sparse_utils.h"
#include "sys/mman.h"

namespace milvus {
constexpr uint64_t MMAP_STRING_PADDING = 1;
constexpr uint64_t MMAP_GEOMETRY_PADDING = 1;
constexpr uint64_t MMAP_ARRAY_PADDING = 1;

// Shared mmap region manager for group chunks
class ChunkMmapGuard {
 public:
    ChunkMmapGuard(char* mmap_ptr, size_t mmap_size, std::string file_path)
        : mmap_ptr_(mmap_ptr), mmap_size_(mmap_size), file_path_(file_path) {
    }

    ~ChunkMmapGuard() {
        if (mmap_ptr_ != nullptr) {
            munmap(mmap_ptr_, mmap_size_);
        }
        if (!file_path_.empty()) {
            unlink(file_path_.c_str());
        }
    }

    char*
    get_ptr() const {
        return mmap_ptr_;
    }

    bool
    is_file_backed() const {
        return !file_path_.empty();
    }

 private:
    char* mmap_ptr_;
    size_t mmap_size_;
    const std::string file_path_;
};

class Chunk {
 public:
    Chunk() = default;
    Chunk(int64_t row_nums,
          char* data,
          uint64_t size,
          bool nullable,
          std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard)
        : data_(data),
          row_nums_(row_nums),
          size_(size),
          nullable_(nullable),
          chunk_mmap_guard_(chunk_mmap_guard) {
    }
    virtual ~Chunk() {
        // The ChunkMmapGuard will handle the unmapping and unlinking of the file if it is file backed
    }

    uint64_t
    Size() const {
        return size_;
    }

    cachinglayer::ResourceUsage
    CellByteSize() const {
        if (chunk_mmap_guard_ && chunk_mmap_guard_->is_file_backed()) {
            return cachinglayer::ResourceUsage(0, static_cast<int64_t>(size_));
        }
        return cachinglayer::ResourceUsage(static_cast<int64_t>(size_), 0);
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
    isValid(int offset) const {
        if (nullable_) {
            return (static_cast<uint8_t>(data_[offset >> 3]) >>
                    (offset & 0x07)) &
                   1;
        }
        return true;
    };

    ValidityView
    Validity(int64_t offset = 0) const {
        if (!nullable_) {
            return {};
        }
        return ValidityView::FromPacked(reinterpret_cast<const uint8_t*>(data_))
            .Subview(offset);
    }

    void
    ApplyValidityMask(int64_t offset,
                      int64_t count,
                      TargetBitmapView target) const {
        if (!nullable_ || count == 0) {
            return;
        }
        AssertInfo(offset >= 0 && count >= 0 && offset + count <= row_nums_,
                   "Invalid validity range, offset: {}, count: {}, rows: {}",
                   offset,
                   count,
                   row_nums_);

        const auto validity = Validity(offset);
        const auto* packed = validity.packed_data();
        AssertInfo(packed != nullptr, "Packed validity data is missing");
        const auto read_word = [packed](int64_t bit_offset, int bit_count) {
            const auto byte_offset = bit_offset >> 3;
            const auto shift = static_cast<int>(bit_offset & 0x07);
            const auto bytes = (shift + bit_count + 7) >> 3;
            uint64_t word = 0;
            const auto low_bytes = std::min(bytes, 8);
            for (int i = 0; i < low_bytes; ++i) {
                word |= uint64_t(packed[byte_offset + i]) << (i * 8);
            }
            word >>= shift;
            if (bytes > 8) {
                word |= uint64_t(packed[byte_offset + 8]) << (64 - shift);
            }
            if (bit_count < 64) {
                word &= (uint64_t{1} << bit_count) - 1;
            }
            return word;
        };

        for (int64_t i = 0; i < count; i += 64) {
            const auto bit_count =
                static_cast<int>(std::min<int64_t>(count - i, 64));
            auto word = read_word(validity.bit_offset() + i, bit_count);
            TargetBitmapView validity_word(&word, bit_count);
            target.view(i).inplace_and(validity_word, bit_count);
        }
    }

    // Map a logical row offset to its compact physical row slot. This does
    // not map logical ARRAY elements to compact vector payload positions.
    int64_t
    PhysicalOffsetOf(int64_t logical_offset) const {
        AssertInfo(logical_offset >= 0 && logical_offset < row_nums_,
                   "Logical offset {} out of range, row nums {}",
                   logical_offset,
                   row_nums_);
        if (!nullable_) {
            return logical_offset;
        }
        AssertInfo(isValid(logical_offset),
                   "Logical offset {} is null",
                   logical_offset);
        BuildValidRankBlocks();
        const auto block_id = logical_offset / kValidRankBlockSize;
        int64_t physical_offset = valid_rank_blocks_[block_id];
        const auto block_start = block_id * kValidRankBlockSize;
        return physical_offset + CountValidBits(block_start, logical_offset);
    }

 protected:
    int64_t
    CountValidBits(int64_t begin, int64_t end) const {
        int64_t count = 0;
        while (begin < end && (begin & 0x07) != 0) {
            count += isValid(begin) ? 1 : 0;
            ++begin;
        }
        while (begin + 8 <= end) {
            count += std::popcount(static_cast<unsigned int>(
                static_cast<uint8_t>(data_[begin >> 3])));
            begin += 8;
        }
        while (begin < end) {
            count += isValid(begin) ? 1 : 0;
            ++begin;
        }
        return count;
    }

    void
    BuildValidRankBlocks() const {
        std::call_once(valid_rank_blocks_once_, [&]() {
            const auto num_blocks =
                (row_nums_ + kValidRankBlockSize - 1) / kValidRankBlockSize;
            valid_rank_blocks_.resize(num_blocks + 1);
            int64_t valid_count = 0;
            for (int64_t block_id = 0; block_id < num_blocks; ++block_id) {
                valid_rank_blocks_[block_id] = valid_count;
                const auto block_start = block_id * kValidRankBlockSize;
                const auto block_end =
                    std::min(block_start + kValidRankBlockSize, row_nums_);
                valid_count += CountValidBits(block_start, block_end);
            }
            valid_rank_blocks_[num_blocks] = valid_count;
        });
    }

    static constexpr int64_t kValidRankBlockSize = 256;
    char* data_;
    int64_t row_nums_;
    uint64_t size_;
    bool nullable_;

    std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard_{nullptr};
    mutable std::once_flag valid_rank_blocks_once_;
    mutable std::vector<int64_t> valid_rank_blocks_;
};

// for fixed size data, includes fixed size array
class FixedWidthChunk : public Chunk {
 public:
    FixedWidthChunk(int32_t row_nums,
                    int32_t dim,
                    char* data,
                    uint64_t size,
                    uint64_t element_size,
                    bool nullable,
                    std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard,
                    size_t payload_offset = 0)
        : Chunk(row_nums, data, size, nullable, chunk_mmap_guard),
          dim_(dim),
          element_size_(element_size) {
        auto null_bitmap_bytes_num = nullable_ ? (row_nums_ + 7) / 8 : 0;
        data_start_ = data_ + null_bitmap_bytes_num + payload_offset;
    };

    milvus::SpanBase
    Span() const {
        return milvus::SpanBase(
            data_start_, Validity(), row_nums_, element_size_ * dim_);
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
    StringChunk(int32_t row_nums,
                char* data,
                uint64_t size,
                bool nullable,
                std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard,
                size_t payload_offset = 0)
        : Chunk(row_nums, data, size, nullable, chunk_mmap_guard) {
        auto null_bitmap_bytes_num = nullable_ ? (row_nums_ + 7) / 8 : 0;
        offsets_ = reinterpret_cast<uint32_t*>(data + null_bitmap_bytes_num +
                                               payload_offset);
    }

    std::string_view
    operator[](const int i) const {
        if (i < 0 || i >= row_nums_) {
            ThrowInfo(ErrorCode::OutOfRange,
                      "index out of range {} at {}",
                      i,
                      row_nums_);
        }

        return {data_ + offsets_[i], offsets_[i + 1] - offsets_[i]};
    }

    std::pair<std::vector<std::string_view>, ValidityView>
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
            auto cmp = midString.compare(target);
            if (cmp == 0) {
                result = mid;     // Store the index of match
                right = mid - 1;  // Continue searching in the left half
            } else if (cmp < 0) {
                // midString < target
                left = mid + 1;
            } else {
                // midString > target
                right = mid - 1;
            }
        }
        return result;
    }

    int
    lower_bound_string(std::string_view target) {
        int left = 0;
        int right = row_nums_;
        while (left < right) {
            int mid = left + (right - left) / 2;
            std::string_view midString = (*this)[mid];

            if (midString < target) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    int
    upper_bound_string(std::string_view target) {
        int left = 0;
        int right = row_nums_;
        while (left < right) {
            int mid = left + (right - left) / 2;
            std::string_view midString = (*this)[mid];

            if (midString <= target) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
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
using GeometryChunk = StringChunk;

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
// [00000000] [29, 3, 41, 2, 49, 4, 65] [1, 2, 3, 4, 5, 6, 7, 8, 9]
//
// For string arrays, the structure is more complex as each string element needs its own offset:
// [null_bitmap][offsets_lens][array1_offsets][array1_data][array2_offsets][array2_data][array3_offsets][array3_data]
// [00000000] [29, 3, 53, 2, 69, 4, 101] [0, 5, 11, 16] ["hello", "world", "!"] [0, 3, 6] ["foo", "bar"] [0, 6, 12, 18, 24] ["apple", "orange", "banana", "grape"]
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
               bool nullable,
               std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard)
        : Chunk(row_nums, data, size, nullable, chunk_mmap_guard),
          element_type_(element_type) {
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
    ViewsByOffsets(const FixedVector<int32_t>& offsets) {
        std::vector<ArrayView> views;
        FixedVector<bool> valid_res;
        size_t size = offsets.size();
        views.reserve(size);
        valid_res.reserve(size);
        for (auto i = 0; i < size; ++i) {
            views.emplace_back(View(offsets[i]));
            valid_res.emplace_back(isValid(offsets[i]));
        }
        return {std::move(views), std::move(valid_res)};
    }

    std::pair<std::vector<ArrayView>, ValidityView>
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
        return {std::move(views), Validity(start_offset)};
    }

    const char*
    ValueAt(int64_t idx) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "ArrayChunk::ValueAt is not supported");
    }

    // Element count of row idx, read from the offsets table without
    // touching the element data.
    uint32_t
    LengthAt(int64_t idx) const {
        return offsets_lens_[2 * idx + 1];
    }

 private:
    milvus::DataType element_type_;
    uint32_t* offsets_lens_;
};

// A VectorArrayChunk is similar to an ArrayChunk but is specialized for storing arrays of vectors.
// Key differences and characteristics:
// - Nullable fields use a null bitmap plus compact payload for valid rows.
// - Fixed Vector Dimensions: All vectors within a VectorArrayChunk have the same, fixed dimension, specified at creation.
//   However, each row (array of vectors) can contain a variable number of these fixed-dimension vectors.
//
// Due to these characteristics, the data layout is simpler:
// Legacy: [row_bitmap?][(byte_offset, physical_count)*rows][final_offset]
//         [all_vector_data_concatenated].
// Element-nullable: the same header, followed by [logical_count*rows]
//         [element_bitmap for all logical elements][compact valid vectors].
// Null rows have zero logical and physical elements. The offsets exposed by
// Offsets() count physical vectors, not logical elements.
// offsets_ and logical_offsets_ are prefix sums computed on the heap when the
// chunk is constructed; neither vector is stored in the mmap file.
// Stored byte offsets and counts are uint32_t; byte offsets are from the chunk
// start. Bitmaps are LSB-first (1 = valid). No alignment padding is inserted.
// One zero byte (MMAP_ARRAY_PADDING) follows the payload, outside final_offset.
//
// Legacy example (FloatVector, dim=3, nullable=false, element_nullable=false):
// Rows: [[1, 2, 3], [4, 5, 6], [7, 8, 9]], [[10, 11, 12]],
//       [[13, 14, 15], [16, 17, 18]].
// [offsets_lens][all_vector_data_concatenated][padding]
// [28, 3, 64, 1, 76, 2, 100] [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18] [0x00]
// Header=28 bytes, each vector=12 bytes, payload=72 bytes, total size=101 bytes.
// Offsets() = [0, 3, 4, 6].
//
// Element-nullable example (FloatVector, dim=2, nullable=true, element_nullable=true):
// Rows: [[1, 2], null, [3, 4]], null, [], [null], [[5, 6]].
// [row_bitmap][offsets_lens][logical_count][element_bitmap][compact_vector_data][padding]
// [0xFD] [66, 2, 82, 0, 82, 0, 82, 0, 82, 1, 90] [3, 0, 0, 1, 1] [0x15] [1, 2, 3, 4, 5, 6] [0x00]
// Row bits 0..4: [1, 0, 1, 1, 1]; unused high bits remain 1 in the row bitmap.
// Element bits 0..4: [1, 0, 1, 0, 1]; unused high bits are 0 in the element bitmap.
// Byte starts: row_bitmap=0, offsets_lens=1, logical_count=45,
// element_bitmap=65, payload=66, padding=90; total size=91 bytes.
// Sizes: 1 + 44 + 20 + 1 + (3 vectors * 8 bytes) + 1 bytes, respectively.
// Offsets() = [0, 2, 2, 2, 2, 3]; logical_offsets_ = [0, 3, 3, 3, 4, 5].
// Row 1 (null) and row 2 (empty) differ only in the row bitmap.
// Row 3 has logical_count=1, physical_count=0, and one invalid element bit.
// Row 4 starts at byte 82, right after row 0's two vectors: rows 1-3 own no
// payload, so their byte offsets repeat 82 and Offsets() stays at 2 until row 4.
class VectorArrayChunk : public Chunk {
 public:
    VectorArrayChunk(int64_t dim,
                     int32_t row_nums,
                     char* data,
                     uint64_t size,
                     milvus::DataType element_type,
                     std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard,
                     bool nullable,
                     bool element_nullable = false)
        : Chunk(row_nums, data, size, nullable, chunk_mmap_guard),
          dim_(dim),
          element_type_(element_type),
          element_nullable_(element_nullable) {
        const uint64_t rows = row_nums_;
        const uint64_t null_bitmap_bytes_num = nullable_ ? (rows + 7) / 8 : 0;
        // Header: [row_bitmap?][(byte_offset, physical_count)*rows]
        // [final_offset], then [logical_count*rows] when element-nullable.
        uint64_t header_end =
            null_bitmap_bytes_num + (rows * 2 + 1) * sizeof(uint32_t);
        if (element_nullable_) {
            header_end += rows * sizeof(uint32_t);
        }
        if (row_nums_ < 0 || size < header_end) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "VectorArrayChunk size {} is too small for the header "
                      "of {} rows ({} bytes)",
                      size,
                      row_nums_,
                      header_end);
        }
        offsets_lens_ =
            reinterpret_cast<uint32_t*>(data + null_bitmap_bytes_num);
        uint64_t payload_begin = header_end;
        if (element_nullable_) {
            const auto* logical_lengths = offsets_lens_ + rows * 2 + 1;
            uint64_t logical_count = 0;
            for (uint64_t i = 0; i < rows; ++i) {
                logical_count += logical_lengths[i];
            }
            payload_begin += (logical_count + 7) / 8;
        }
        const uint64_t payload_start = offsets_lens_[0];
        const uint64_t payload_end = offsets_lens_[rows * 2];
        if (payload_start < payload_begin || payload_end < payload_start ||
            payload_end > size) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "VectorArrayChunk of {} rows has payload [{}, {}) that "
                      "does not fit between metadata end {} and chunk size {}",
                      row_nums_,
                      payload_start,
                      payload_end,
                      payload_begin,
                      size);
        }

        auto offset = 0;
        offsets_.reserve(row_nums_ + 1);
        offsets_.push_back(offset);
        for (int64_t i = 0; i < row_nums_; i++) {
            offset += offsets_lens_[i * 2 + 1];
            offsets_.push_back(offset);
        }
        if (element_nullable_) {
            logical_lengths_ = offsets_lens_ + row_nums_ * 2 + 1;
            element_bitmap_ =
                reinterpret_cast<uint8_t*>(logical_lengths_ + row_nums_);
            logical_offsets_.reserve(row_nums_ + 1);
            logical_offsets_.push_back(0);
            for (int64_t i = 0; i < row_nums_; ++i) {
                logical_offsets_.push_back(logical_offsets_.back() +
                                           logical_lengths_[i]);
            }
        }
    }

    VectorArrayView
    View(int64_t idx) const {
        AssertInfo(idx >= 0 && idx < row_nums_,
                   "VectorArrayChunk::View offset {} out of range, "
                   "rows {}",
                   idx,
                   row_nums_);
        AssertInfo(!nullable_ || isValid(idx),
                   "VectorArrayChunk::View offset {} is null",
                   idx);
        return ViewUnchecked(idx);
    }

    std::pair<std::vector<VectorArrayView>, ValidityView>
    Views(std::optional<std::pair<int64_t, int64_t>> offset_len =
              std::nullopt) const {
        auto start_offset = 0;
        auto len = row_nums_;
        if (offset_len.has_value()) {
            start_offset = offset_len->first;
            len = offset_len->second;
            AssertInfo(
                start_offset >= 0 && start_offset < row_nums_,
                "Retrieve vector array views with out-of-bound offset:{}, "
                "len:{}, wrong",
                start_offset,
                len);
            AssertInfo(
                len > 0 && len <= row_nums_,
                "Retrieve vector array views with out-of-bound offset:{}, "
                "len:{}, wrong",
                start_offset,
                len);
            AssertInfo(
                start_offset + len <= row_nums_,
                "Retrieve vector array views with out-of-bound offset:{}, "
                "len:{}, wrong",
                start_offset,
                len);
        }

        std::vector<VectorArrayView> views;
        views.reserve(len);
        auto end_offset = start_offset + len;
        const auto validity = Validity(start_offset);
        for (int64_t i = start_offset; i < end_offset; i++) {
            if (nullable_ && !validity[i - start_offset]) {
                views.emplace_back();
            } else {
                views.emplace_back(ViewUnchecked(i));
            }
        }
        return {std::move(views), validity};
    }

    const char*
    ValueAt(int64_t idx) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "VectorArrayChunk::ValueAt is not supported");
    }

    const char*
    Data() const override {
        return data_ + offsets_lens_[0];
    }

    const size_t*
    Offsets() const {
        // Prefix counts of physically stored vectors (null elements excluded).
        return offsets_.data();
    }

    bool
    IsElementNullable() const {
        return element_nullable_;
    }

    // Number of physically stored vectors in this chunk.
    int64_t
    PhysicalElementCount() const {
        return static_cast<int64_t>(offsets_.back());
    }

    // Number of logical elements in this chunk, null elements included.
    int64_t
    LogicalElementCount() const {
        return element_nullable_ ? static_cast<int64_t>(logical_offsets_.back())
                                 : PhysicalElementCount();
    }

    // Element validity bitmap over the chunk's logical elements (LSB-first,
    // 1 = valid). nullptr when the chunk is not element-nullable.
    const uint8_t*
    ElementValidity() const {
        return element_bitmap_;
    }

 private:
    VectorArrayView
    ViewUnchecked(int64_t idx) const {
        int idx_off = 2 * idx;
        auto offset = offsets_lens_[idx_off];
        auto len = offsets_lens_[idx_off + 1];
        auto next_offset = offsets_lens_[idx_off + 2];
        auto data_ptr = data_ + offset;
        if (element_nullable_) {
            return VectorArrayView(data_ptr,
                                   dim_,
                                   logical_lengths_[idx],
                                   next_offset - offset,
                                   element_type_,
                                   TargetBitmapView(element_bitmap_,
                                                    logical_offsets_[idx],
                                                    logical_lengths_[idx]),
                                   true);
        }
        return VectorArrayView(
            data_ptr, dim_, len, next_offset - offset, element_type_);
    }
    int64_t dim_;
    uint32_t* offsets_lens_;
    uint32_t* logical_lengths_{nullptr};
    uint8_t* element_bitmap_{nullptr};
    milvus::DataType element_type_;
    bool element_nullable_{false};
    std::vector<size_t> offsets_;
    std::vector<size_t> logical_offsets_;
};

class SparseFloatVectorChunk : public Chunk {
 public:
    SparseFloatVectorChunk(int32_t row_nums,
                           char* data,
                           uint64_t size,
                           bool nullable,
                           std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard)
        : Chunk(row_nums, data, size, nullable, chunk_mmap_guard) {
        auto null_bitmap_bytes_num = nullable ? (row_nums + 7) / 8 : 0;
        auto offsets_ptr =
            reinterpret_cast<uint64_t*>(data + null_bitmap_bytes_num);

        if (nullable_) {
            for (int i = 0; i < row_nums; i++) {
                if (isValid(i)) {
                    vec_.emplace_back(
                        (offsets_ptr[i + 1] - offsets_ptr[i]) /
                            knowhere::sparse::SparseRow<
                                SparseValueType>::element_size(),
                        reinterpret_cast<uint8_t*>(data + offsets_ptr[i]),
                        false);
                    dim_ = std::max(dim_, vec_.back().dim());
                }
            }
        } else {
            vec_.resize(row_nums);
            for (int i = 0; i < row_nums; i++) {
                vec_[i] = {(offsets_ptr[i + 1] - offsets_ptr[i]) /
                               knowhere::sparse::SparseRow<
                                   SparseValueType>::element_size(),
                           reinterpret_cast<uint8_t*>(data + offsets_ptr[i]),
                           false};
                dim_ = std::max(dim_, vec_[i].dim());
            }
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
    std::vector<knowhere::sparse::SparseRow<SparseValueType>>&
    Vec() {
        return vec_;
    }

    int64_t
    Dim() {
        return dim_;
    }

 private:
    int64_t dim_ = 0;
    std::vector<knowhere::sparse::SparseRow<SparseValueType>> vec_;
};
}  // namespace milvus
