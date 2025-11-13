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
#include <memory>
#include <utility>
#include <vector>
#include <string>
#include <map>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>
#include <folly/small_vector.h>

#include "index/StringIndex.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileWriter.h"
#include "common/File.h"

namespace milvus::index {

// Forward declaration
class StringIndexSortImpl;

// Main StringIndexSort class using pImpl pattern
class StringIndexSort : public StringIndex {
 public:
    static constexpr uint32_t SERIALIZATION_VERSION = 1;
    static constexpr uint64_t MAGIC_CODE =
        0x5354524E47534F52;  // "STRNGSOR" in hex

    explicit StringIndexSort(
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    virtual ~StringIndexSort();

    int64_t
    Count() override;

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::STLSORT;
    }

    const bool
    HasRawData() const override {
        return true;
    }

    void
    Build(size_t n,
          const std::string* values,
          const bool* valid_data = nullptr) override;

    void
    Build(const Config& config = {}) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

    // See detailed format in StringIndexSortMemoryImpl::SerializeToBinary
    BinarySet
    Serialize(const Config& config) override;

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    void
    Load(const BinarySet& index_binary, const Config& config = {}) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    void
    LoadWithoutAssemble(const BinarySet& binary_set,
                        const Config& config) override;

    // Query methods - delegated to impl
    const TargetBitmap
    In(size_t n, const std::string* values) override;

    const TargetBitmap
    NotIn(size_t n, const std::string* values) override;

    const TargetBitmap
    IsNull() override;

    TargetBitmap
    IsNotNull() override;

    const TargetBitmap
    Range(std::string value, OpType op) override;

    const TargetBitmap
    Range(std::string lower_bound_value,
          bool lb_inclusive,
          std::string upper_bound_value,
          bool ub_inclusive) override;

    const TargetBitmap
    PrefixMatch(const std::string_view prefix) override;

    std::optional<std::string>
    Reverse_Lookup(size_t offset) const override;

    int64_t
    Size() override;

 protected:
    int64_t
    CalculateTotalSize() const;

    // Common fields
    int64_t field_id_ = 0;
    bool is_built_ = false;
    Config config_;
    std::shared_ptr<storage::MemFileManagerImpl> file_manager_;
    size_t total_num_rows_{0};
    TargetBitmap valid_bitset_;
    std::vector<int32_t> idx_to_offsets_;
    std::chrono::time_point<std::chrono::system_clock> index_build_begin_;

    int64_t total_size_{0};
    std::unique_ptr<StringIndexSortImpl> impl_;
};

// Abstract interface for implementations
class StringIndexSortImpl {
 public:
    virtual ~StringIndexSortImpl() = default;

    virtual void
    LoadFromBinary(const BinarySet& binary_set,
                   size_t total_num_rows,
                   TargetBitmap& valid_bitset,
                   std::vector<int32_t>& idx_to_offsets) = 0;

    struct ParsedData {
        uint32_t unique_count;
        const uint32_t* string_offsets;
        const uint8_t* string_data_start;
        const uint32_t* post_list_offsets;
        const uint8_t* post_list_data_start;
    };

    static ParsedData
    ParseBinaryData(const uint8_t* data, size_t data_size);

    virtual const TargetBitmap
    In(size_t n, const std::string* values, size_t total_num_rows) = 0;

    virtual const TargetBitmap
    NotIn(size_t n,
          const std::string* values,
          size_t total_num_rows,
          const TargetBitmap& valid_bitset) = 0;

    virtual const TargetBitmap
    IsNull(size_t total_num_rows, const TargetBitmap& valid_bitset) = 0;

    virtual TargetBitmap
    IsNotNull(const TargetBitmap& valid_bitset) = 0;

    virtual const TargetBitmap
    Range(std::string value, OpType op, size_t total_num_rows) = 0;

    virtual const TargetBitmap
    Range(std::string lower_bound_value,
          bool lb_inclusive,
          std::string upper_bound_value,
          bool ub_inclusive,
          size_t total_num_rows) = 0;

    virtual const TargetBitmap
    PrefixMatch(const std::string_view prefix, size_t total_num_rows) = 0;

    virtual std::optional<std::string>
    Reverse_Lookup(size_t offset,
                   size_t total_num_rows,
                   const TargetBitmap& valid_bitset,
                   const std::vector<int32_t>& idx_to_offsets) const = 0;

    virtual int64_t
    Size() = 0;
};

class StringIndexSortMemoryImpl : public StringIndexSortImpl {
 public:
    using PostingList = folly::small_vector<uint32_t, 4>;

    void
    BuildFromRawData(size_t n,
                     const std::string* values,
                     const bool* valid_data,
                     TargetBitmap& valid_bitset,
                     std::vector<int32_t>& idx_to_offsets);

    void
    BuildFromFieldData(const std::vector<FieldDataPtr>& field_datas,
                       size_t total_num_rows,
                       TargetBitmap& valid_bitset,
                       std::vector<int32_t>& idx_to_offsets);

    // Serialize to binary format
    // The binary format is : [unique_count][string_offsets][string_data][post_list_offsets][post_list_data][magic_code]
    // string_offsets: array of offsets into string_data section
    // string_data: str_len1, str1, str_len2, str2, ...
    // post_list_offsets: array of offsets into post_list_data section
    // post_list_data: post_list_len1, row_id1, row_id2, ..., post_list_len2, row_id1, row_id2, ...
    void
    SerializeToBinary(uint8_t* ptr, size_t& offset) const;

    size_t
    GetSerializedSize() const;

    void
    LoadFromBinary(const BinarySet& binary_set,
                   size_t total_num_rows,
                   TargetBitmap& valid_bitset,
                   std::vector<int32_t>& idx_to_offsets) override;

    const TargetBitmap
    In(size_t n, const std::string* values, size_t total_num_rows) override;

    const TargetBitmap
    NotIn(size_t n,
          const std::string* values,
          size_t total_num_rows,
          const TargetBitmap& valid_bitset) override;

    const TargetBitmap
    IsNull(size_t total_num_rows, const TargetBitmap& valid_bitset) override;

    TargetBitmap
    IsNotNull(const TargetBitmap& valid_bitset) override;

    const TargetBitmap
    Range(std::string value, OpType op, size_t total_num_rows) override;

    const TargetBitmap
    Range(std::string lower_bound_value,
          bool lb_inclusive,
          std::string upper_bound_value,
          bool ub_inclusive,
          size_t total_num_rows) override;

    const TargetBitmap
    PrefixMatch(const std::string_view prefix, size_t total_num_rows) override;

    std::optional<std::string>
    Reverse_Lookup(size_t offset,
                   size_t total_num_rows,
                   const TargetBitmap& valid_bitset,
                   const std::vector<int32_t>& idx_to_offsets) const override;

    int64_t
    Size() override;

 private:
    // Helper method for binary search
    size_t
    FindValueIndex(const std::string& value) const;

    void
    BuildFromMap(std::map<std::string, PostingList>&& unique_map,
                 size_t total_num_rows,
                 std::vector<int32_t>& idx_to_offsets);

    // Keep unique_values_ and posting_lists_ separated for cache efficiency
    // Sorted unique values
    std::vector<std::string> unique_values_;
    // Corresponding posting lists
    std::vector<PostingList> posting_lists_;
};

class StringIndexSortMmapImpl : public StringIndexSortImpl {
 public:
    ~StringIndexSortMmapImpl();

    // Helper struct to access separated string and posting list data
    struct MmapEntry {
        const char* str_data_ptr;            // Pointer to string data
        const uint32_t* post_list_data_ptr;  // Pointer to posting list data
        uint32_t str_len;                    // String length
        uint32_t post_list_len;              // Posting list length

        MmapEntry() = default;

        MmapEntry(const uint8_t* str_ptr, const uint8_t* post_list_ptr) {
            // Read string length and data pointer
            str_len = *reinterpret_cast<const uint32_t*>(str_ptr);
            str_data_ptr =
                reinterpret_cast<const char*>(str_ptr + sizeof(uint32_t));

            // Read posting list length and data pointer
            post_list_len = *reinterpret_cast<const uint32_t*>(post_list_ptr);
            post_list_data_ptr = reinterpret_cast<const uint32_t*>(
                post_list_ptr + sizeof(uint32_t));
        }

        std::string_view
        get_string_view() const {
            return std::string_view(str_data_ptr, str_len);
        }

        size_t
        get_posting_list_len() const {
            return post_list_len;
        }

        uint32_t
        get_row_id(size_t idx) const {
            return post_list_data_ptr[idx];
        }

        template <typename Func>
        void
        for_each_row_id(Func func) const {
            for (uint32_t i = 0; i < post_list_len; ++i) {
                func(post_list_data_ptr[i]);
            }
        }
    };

    void
    LoadFromBinary(const BinarySet& binary_set,
                   size_t total_num_rows,
                   TargetBitmap& valid_bitset,
                   std::vector<int32_t>& idx_to_offsets) override;

    void
    SetMmapFilePath(const std::string& filepath) {
        mmap_filepath_ = filepath;
    }

    const TargetBitmap
    In(size_t n, const std::string* values, size_t total_num_rows) override;

    const TargetBitmap
    NotIn(size_t n,
          const std::string* values,
          size_t total_num_rows,
          const TargetBitmap& valid_bitset) override;

    const TargetBitmap
    IsNull(size_t total_num_rows, const TargetBitmap& valid_bitset) override;

    TargetBitmap
    IsNotNull(const TargetBitmap& valid_bitset) override;

    const TargetBitmap
    Range(std::string value, OpType op, size_t total_num_rows) override;

    const TargetBitmap
    Range(std::string lower_bound_value,
          bool lb_inclusive,
          std::string upper_bound_value,
          bool ub_inclusive,
          size_t total_num_rows) override;

    const TargetBitmap
    PrefixMatch(const std::string_view prefix, size_t total_num_rows) override;

    std::optional<std::string>
    Reverse_Lookup(size_t offset,
                   size_t total_num_rows,
                   const TargetBitmap& valid_bitset,
                   const std::vector<int32_t>& idx_to_offsets) const override;

    int64_t
    Size() override;

 private:
    // Binary search for a value
    size_t
    FindValueIndex(const std::string& value) const;

    // Binary search helpers
    size_t
    LowerBound(const std::string_view& value) const;

    size_t
    UpperBound(const std::string_view& value) const;

    MmapEntry
    GetEntry(size_t idx) const {
        const uint8_t* str_ptr = string_data_start_ + string_offsets_[idx];
        const uint8_t* post_list_ptr =
            post_list_data_start_ + post_list_offsets_[idx];
        return MmapEntry(str_ptr, post_list_ptr);
    }

 private:
    char* mmap_data_ = nullptr;
    size_t mmap_size_ = 0;
    size_t data_size_ = 0;  // Actual data size without padding
    std::string mmap_filepath_;
    size_t unique_count_ = 0;

    // Pointers to different sections in mmap'd data
    const uint32_t* string_offsets_ = nullptr;
    const uint8_t* string_data_start_ = nullptr;
    const uint32_t* post_list_offsets_ = nullptr;
    const uint8_t* post_list_data_start_ = nullptr;
};

using StringIndexSortPtr = std::unique_ptr<StringIndexSort>;

inline StringIndexSortPtr
CreateStringIndexSort(const storage::FileManagerContext& file_manager_context =
                          storage::FileManagerContext()) {
    return std::make_unique<StringIndexSort>(file_manager_context);
}

}  // namespace milvus::index