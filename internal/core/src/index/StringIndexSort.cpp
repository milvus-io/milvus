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

#include "index/StringIndexSort.h"

#include <assert.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iterator>
#include <limits>
#include <memory>
#include <utility>

#include "bitset/bitset.h"
#include "bitset/detail/element_vectorized.h"
#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/RegexQuery.h"
#include "common/Slice.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "fmt/core.h"
#include "folly/small_vector.h"
#include "glog/logging.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "knowhere/binaryset.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "storage/FileWriter.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"

namespace milvus::index {

StringIndexSortImpl::ParsedData
StringIndexSortImpl::ParseBinaryData(const uint8_t* data, size_t data_size) {
    ParsedData result;
    const uint8_t* ptr = data;

    // Verify magic code at the end first
    uint64_t magic_at_end;
    memcpy(
        &magic_at_end, data + data_size - sizeof(uint64_t), sizeof(uint64_t));
    if (magic_at_end != StringIndexSort::MAGIC_CODE) {
        ThrowInfo(DataFormatBroken,
                  fmt::format("Invalid magic code: expected 0x{:X}, got 0x{:X}",
                              StringIndexSort::MAGIC_CODE,
                              magic_at_end));
    }

    // Read unique count
    memcpy(&result.unique_count, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // Handle all-null case where unique_count is 0
    if (result.unique_count == 0) {
        result.string_offsets = nullptr;
        result.string_data_start = ptr;
        result.post_list_offsets = nullptr;
        result.post_list_data_start = ptr;
        return result;
    }

    // Read string offsets
    result.string_offsets = reinterpret_cast<const uint32_t*>(ptr);
    ptr += result.unique_count * sizeof(uint32_t);

    result.string_data_start = ptr;

    // Calculate total string section size
    auto total_string_size = 0;
    const uint8_t* last_str_ptr =
        result.string_data_start +
        result.string_offsets[result.unique_count - 1];
    uint32_t last_str_len;
    memcpy(&last_str_len, last_str_ptr, sizeof(uint32_t));
    total_string_size = result.string_offsets[result.unique_count - 1] +
                        sizeof(uint32_t) + last_str_len;

    // Skip past string section to posting list offsets
    ptr = result.string_data_start + total_string_size;
    result.post_list_offsets = reinterpret_cast<const uint32_t*>(ptr);
    ptr += result.unique_count * sizeof(uint32_t);

    result.post_list_data_start = ptr;

    return result;
}

const std::string STRING_INDEX_SORT_FILE = "string_index_sort";

constexpr size_t ALIGNMENT = 32;  // 32-byte alignment

const uint64_t MMAP_INDEX_PADDING = 1;

StringIndexSort::StringIndexSort(
    const storage::FileManagerContext& file_manager_context,
    bool is_nested_index)
    : StringIndex(ASCENDING_SORT),
      is_built_(false),
      is_nested_index_(is_nested_index) {
    if (file_manager_context.Valid()) {
        field_id_ = file_manager_context.fieldDataMeta.field_id;
        file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
    }
}

StringIndexSort::~StringIndexSort() = default;

int64_t
StringIndexSort::Count() {
    return total_num_rows_;
}

void
StringIndexSort::Build(size_t n,
                       const std::string* values,
                       const bool* valid_data) {
    if (is_built_)
        return;
    if (n == 0) {
        ThrowInfo(DataIsEmpty, "StringIndexSort cannot build null values!");
    }

    index_build_begin_ = std::chrono::system_clock::now();
    total_num_rows_ = n;
    valid_bitset_ = TargetBitmap(total_num_rows_, false);
    idx_to_offsets_.clear();

    // Create MemoryImpl and delegate building to it
    impl_ = std::make_unique<StringIndexSortMemoryImpl>();
    auto* memory_impl = static_cast<StringIndexSortMemoryImpl*>(impl_.get());

    // Let MemoryImpl handle the building process
    memory_impl->BuildFromRawData(
        n, values, valid_data, valid_bitset_, idx_to_offsets_);

    is_built_ = true;
    total_size_ = CalculateTotalSize();
    ComputeByteSize();
}

void
StringIndexSort::Build(const Config& config) {
    if (is_built_) {
        return;
    }
    config_ = config;
    auto field_datas =
        storage::CacheRawDataAndFillMissing(file_manager_, config);
    BuildWithFieldData(field_datas);
}

void
StringIndexSort::BuildWithFieldData(
    const std::vector<FieldDataPtr>& field_datas) {
    if (is_built_)
        return;

    index_build_begin_ = std::chrono::system_clock::now();

    // Calculate total number of rows
    total_num_rows_ = 0;
    if (is_nested_index_) {
        for (const auto& data : field_datas) {
            auto n = data->get_num_rows();
            auto array_column = static_cast<const Array*>(data->Data());
            for (int64_t i = 0; i < n; i++) {
                if (data->is_valid(i)) {
                    total_num_rows_ += array_column[i].length();
                }
            }
        }
    } else {
        for (const auto& data : field_datas) {
            total_num_rows_ += data->get_num_rows();
        }
    }

    if (total_num_rows_ == 0) {
        ThrowInfo(DataIsEmpty, "StringIndexSort cannot build null values!");
    }

    // Initialize structures
    valid_bitset_ = TargetBitmap(total_num_rows_, false);
    idx_to_offsets_.clear();

    // Create MemoryImpl and build directly from field data
    impl_ = std::make_unique<StringIndexSortMemoryImpl>();
    if (is_nested_index_) {
        static_cast<StringIndexSortMemoryImpl*>(impl_.get())
            ->BuildFromArrayDataNested(
                field_datas, total_num_rows_, valid_bitset_, idx_to_offsets_);
    } else {
        static_cast<StringIndexSortMemoryImpl*>(impl_.get())
            ->BuildFromFieldData(
                field_datas, total_num_rows_, valid_bitset_, idx_to_offsets_);
    }

    is_built_ = true;
    total_size_ = CalculateTotalSize();
    ComputeByteSize();
}

BinarySet
StringIndexSort::Serialize(const Config& config) {
    AssertInfo(is_built_, "index has not been built");
    AssertInfo(impl_ != nullptr, "impl_ is null, cannot serialize");

    BinarySet res_set;

    std::shared_ptr<uint8_t[]> version_buf(new uint8_t[sizeof(uint32_t)]);
    uint32_t version = SERIALIZATION_VERSION;
    memcpy(version_buf.get(), &version, sizeof(uint32_t));
    res_set.Append("version", version_buf, sizeof(uint32_t));

    // Use MemoryImpl to serialize
    auto* memory_impl = static_cast<StringIndexSortMemoryImpl*>(impl_.get());
    size_t total_size = memory_impl->GetSerializedSize();

    std::shared_ptr<uint8_t[]> data_buffer(new uint8_t[total_size]);
    size_t offset = 0;
    memory_impl->SerializeToBinary(data_buffer.get(), offset);

    res_set.Append("index_data", data_buffer, total_size);

    // Serialize total number of rows (use same key as ScalarIndexSort)
    std::shared_ptr<uint8_t[]> index_num_rows(new uint8_t[sizeof(size_t)]);
    memcpy(index_num_rows.get(), &total_num_rows_, sizeof(size_t));
    res_set.Append("index_num_rows", index_num_rows, sizeof(size_t));

    // Serialize valid_bitset
    size_t valid_bitset_size =
        (total_num_rows_ + 7) / 8;  // Round up to byte boundary
    std::shared_ptr<uint8_t[]> valid_bitset_data(
        new uint8_t[valid_bitset_size]);
    memset(valid_bitset_data.get(), 0, valid_bitset_size);
    for (size_t i = 0; i < total_num_rows_; ++i) {
        if (valid_bitset_[i]) {
            valid_bitset_data[i / 8] |= (1 << (i % 8));
        }
    }
    res_set.Append("valid_bitset", valid_bitset_data, valid_bitset_size);

    // Serialize is_nested_index
    std::shared_ptr<uint8_t[]> is_nested_data(new uint8_t[sizeof(bool)]);
    memcpy(is_nested_data.get(), &is_nested_index_, sizeof(bool));
    res_set.Append("is_nested_index", is_nested_data, sizeof(bool));

    milvus::Disassemble(res_set);
    return res_set;
}

IndexStatsPtr
StringIndexSort::Upload(const Config& config) {
    auto index_build_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - index_build_begin_)
            .count();
    LOG_INFO(
        "index build done for StringIndexSort, field_id: {}, duration: {}ms",
        field_id_,
        index_build_duration);

    auto binary_set = Serialize(config);
    file_manager_->AddFile(binary_set);

    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
    return IndexStats::NewFromSizeMap(file_manager_->GetAddedTotalMemSize(),
                                      remote_paths_to_size);
}

void
StringIndexSort::Load(const BinarySet& index_binary, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(index_binary));
    LoadWithoutAssemble(index_binary, config);
}

void
StringIndexSort::Load(milvus::tracer::TraceContext ctx, const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value() && !index_files.value().empty(),
               "index_files not found in config");

    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);

    auto index_datas =
        file_manager_->LoadIndexToMemory(index_files.value(), load_priority);

    BinarySet binary_set;
    AssembleIndexDatas(index_datas, binary_set);

    index_datas.clear();

    LoadWithoutAssemble(binary_set, config);
}

void
StringIndexSort::LoadWithoutAssemble(const BinarySet& binary_set,
                                     const Config& config) {
    config_ = config;

    auto index_num_rows = binary_set.GetByName("index_num_rows");
    AssertInfo(index_num_rows != nullptr,
               "Failed to find 'index_num_rows' in binary_set");
    memcpy(&total_num_rows_, index_num_rows->data.get(), sizeof(size_t));

    // Initialize idx_to_offsets - it will be rebuilt by LoadFromBinary
    idx_to_offsets_.resize(total_num_rows_);

    auto valid_bitset_data = binary_set.GetByName("valid_bitset");
    AssertInfo(valid_bitset_data != nullptr,
               "Failed to find 'valid_bitset' in binary_set");
    valid_bitset_ = TargetBitmap(total_num_rows_, false);
    for (size_t i = 0; i < total_num_rows_; ++i) {
        uint8_t byte = valid_bitset_data->data[i / 8];
        if (byte & (1 << (i % 8))) {
            valid_bitset_.set(i);
        }
    }

    // Deserialize is_nested_index (optional for backward compatibility)
    auto is_nested_data = binary_set.GetByName("is_nested_index");
    if (is_nested_data != nullptr) {
        memcpy(&is_nested_index_, is_nested_data->data.get(), sizeof(bool));
    }

    auto version_data = binary_set.GetByName("version");
    AssertInfo(version_data != nullptr,
               "Failed to find 'version' in binary_set");

    uint32_t version;
    memcpy(&version, version_data->data.get(), sizeof(uint32_t));

    if (version != SERIALIZATION_VERSION) {
        ThrowInfo(milvus::ErrorCode::Unsupported,
                  fmt::format("Unsupported StringIndexSort serialization "
                              "version: {}, expected: {}",
                              version,
                              SERIALIZATION_VERSION));
    }

    // Check if mmap is enabled
    if (config.contains(MMAP_FILE_PATH)) {
        LOG_INFO("StringIndexSort: loading with mmap strategy");
        auto mmap_impl = std::make_unique<StringIndexSortMmapImpl>();

        auto mmap_path =
            GetValueFromConfig<std::string>(config, MMAP_FILE_PATH).value();
        mmap_impl->SetMmapFilePath(mmap_path);
        mmap_impl->LoadFromBinary(
            binary_set, total_num_rows_, valid_bitset_, idx_to_offsets_);
        impl_ = std::move(mmap_impl);
    } else {
        LOG_INFO("StringIndexSort: loading with memory strategy");
        impl_ = std::make_unique<StringIndexSortMemoryImpl>();
        impl_->LoadFromBinary(
            binary_set, total_num_rows_, valid_bitset_, idx_to_offsets_);
    }

    is_built_ = true;
    total_size_ = CalculateTotalSize();
    ComputeByteSize();
}

const TargetBitmap
StringIndexSort::In(size_t n, const std::string* values) {
    assert(impl_ != nullptr);
    return impl_->In(n, values, total_num_rows_);
}

const TargetBitmap
StringIndexSort::NotIn(size_t n, const std::string* values) {
    assert(impl_ != nullptr);
    return impl_->NotIn(n, values, total_num_rows_, valid_bitset_);
}

const TargetBitmap
StringIndexSort::IsNull() {
    assert(impl_ != nullptr);
    return impl_->IsNull(total_num_rows_, valid_bitset_);
}

TargetBitmap
StringIndexSort::IsNotNull() {
    assert(impl_ != nullptr);
    return impl_->IsNotNull(valid_bitset_);
}

const TargetBitmap
StringIndexSort::Range(std::string value, OpType op) {
    assert(impl_ != nullptr);
    return impl_->Range(value, op, total_num_rows_);
}

const TargetBitmap
StringIndexSort::Range(std::string lower_bound_value,
                       bool lb_inclusive,
                       std::string upper_bound_value,
                       bool ub_inclusive) {
    assert(impl_ != nullptr);
    return impl_->Range(lower_bound_value,
                        lb_inclusive,
                        upper_bound_value,
                        ub_inclusive,
                        total_num_rows_);
}

const TargetBitmap
StringIndexSort::PrefixMatch(const std::string_view prefix) {
    assert(impl_ != nullptr);
    return impl_->PrefixMatch(prefix, total_num_rows_);
}

const TargetBitmap
StringIndexSort::PatternMatch(const std::string& pattern,
                              proto::plan::OpType op) {
    assert(impl_ != nullptr);

    if (op == proto::plan::OpType::PrefixMatch) {
        return PrefixMatch(pattern);
    }

    // Support Match, PostfixMatch, InnerMatch
    // All can benefit from unique value deduplication
    if (op != proto::plan::OpType::Match &&
        op != proto::plan::OpType::PostfixMatch &&
        op != proto::plan::OpType::InnerMatch) {
        ThrowInfo(Unsupported,
                  "StringIndexSort::PatternMatch only supports Match, "
                  "PrefixMatch, PostfixMatch, InnerMatch, got op: {}",
                  static_cast<int>(op));
    }

    return impl_->PatternMatch(pattern, op, total_num_rows_);
}

std::optional<std::string>
StringIndexSort::Reverse_Lookup(size_t offset) const {
    assert(impl_ != nullptr);
    return impl_->Reverse_Lookup(
        offset, total_num_rows_, valid_bitset_, idx_to_offsets_);
}

int64_t
StringIndexSort::Size() {
    return total_size_;
}

int64_t
StringIndexSort::CalculateTotalSize() const {
    int64_t size = 0;

    size += impl_->Size();
    // Add common structures (always present)
    size += idx_to_offsets_.size() * sizeof(int32_t);
    size += valid_bitset_.size() / 8;

    // Add object overhead
    size += sizeof(*this);

    return size;
}

void
StringIndexSort::ComputeByteSize() {
    StringIndex::ComputeByteSize();
    int64_t total = cached_byte_size_;

    // Common structures (always in memory)
    // idx_to_offsets_: vector<int32_t>
    total += idx_to_offsets_.capacity() * sizeof(int32_t);

    // valid_bitset_: TargetBitmap
    total += valid_bitset_.size_in_bytes();

    // Add impl-specific memory usage
    if (impl_) {
        total += impl_->ByteSize();
    }

    cached_byte_size_ = total;
}

void
StringIndexSortMemoryImpl::BuildFromMap(
    std::map<std::string, PostingList>&& map,
    size_t total_num_rows,
    std::vector<int32_t>& idx_to_offsets) {
    unique_values_.clear();
    posting_lists_.clear();
    unique_values_.reserve(map.size());
    posting_lists_.reserve(map.size());

    // Initialize idx_to_offsets
    idx_to_offsets.resize(total_num_rows);
    std::fill(idx_to_offsets.begin(), idx_to_offsets.end(), -1);

    // Convert map to vectors (map is already sorted)
    size_t unique_idx = 0;
    for (auto& [value, posting_list] : map) {
        // Map each row_id to its unique value index
        for (uint32_t row_id : posting_list) {
            idx_to_offsets[row_id] = unique_idx;
        }
        unique_values_.push_back(std::move(value));
        posting_lists_.push_back(std::move(posting_list));
        unique_idx++;
    }
}

void
StringIndexSortMemoryImpl::BuildFromRawData(
    size_t n,
    const std::string* values,
    const bool* valid_data,
    TargetBitmap& valid_bitset,
    std::vector<int32_t>& idx_to_offsets) {
    // Use map to collect unique values and their posting lists
    std::map<std::string, PostingList> map;

    for (size_t i = 0; i < n; ++i) {
        if (!valid_data || valid_data[i]) {
            map[values[i]].push_back(static_cast<uint32_t>(i));
            valid_bitset.set(i);
        }
    }

    BuildFromMap(std::move(map), n, idx_to_offsets);
}

void
StringIndexSortMemoryImpl::BuildFromFieldData(
    const std::vector<FieldDataPtr>& field_datas,
    size_t total_num_rows,
    TargetBitmap& valid_bitset,
    std::vector<int32_t>& idx_to_offsets) {
    // Use map to collect unique values and their posting lists
    // std::map is sorted
    std::map<std::string, PostingList> map;

    size_t row_id = 0;
    for (const auto& field_data : field_datas) {
        auto slice_num = field_data->get_num_rows();
        for (size_t i = 0; i < slice_num; ++i) {
            if (field_data->is_valid(i)) {
                auto value = reinterpret_cast<const std::string*>(
                    field_data->RawValue(i));
                map[*value].push_back(static_cast<int32_t>(row_id));
                valid_bitset.set(row_id);
            }
            row_id++;
        }
    }

    BuildFromMap(std::move(map), total_num_rows, idx_to_offsets);
}

void
StringIndexSortMemoryImpl::BuildFromArrayDataNested(
    const std::vector<FieldDataPtr>& field_datas,
    size_t total_num_rows,
    TargetBitmap& valid_bitset,
    std::vector<int32_t>& idx_to_offsets) {
    // Use map to collect unique values and their posting lists
    // std::map is sorted
    std::map<std::string, PostingList> map;
    size_t element_id = 0;
    for (const auto& field_data : field_datas) {
        auto n = field_data->get_num_rows();
        auto array_column = static_cast<const Array*>(field_data->Data());
        for (int64_t i = 0; i < n; i++) {
            if (!field_data->is_valid(i)) {
                continue;
            }
            for (int64_t j = 0; j < array_column[i].length(); j++) {
                auto value = array_column[i].get_data<std::string>(j);
                map[value].push_back(static_cast<int32_t>(element_id));
                valid_bitset.set(element_id);
                element_id++;
            }
        }
    }
    BuildFromMap(std::move(map), total_num_rows, idx_to_offsets);
}

size_t
StringIndexSortMemoryImpl::GetSerializedSize() const {
    size_t total_size = sizeof(uint32_t);  // unique_count

    // String offsets array
    total_size += unique_values_.size() * sizeof(uint32_t);

    // String data section
    for (size_t i = 0; i < unique_values_.size(); ++i) {
        total_size += sizeof(uint32_t);          // str_len
        total_size += unique_values_[i].size();  // string content
    }

    // Posting list offsets array
    total_size += posting_lists_.size() * sizeof(uint32_t);

    // Posting list data section
    for (size_t i = 0; i < posting_lists_.size(); ++i) {
        total_size += sizeof(uint32_t);  // post_list_len
        total_size += posting_lists_[i].size() * sizeof(uint32_t);  // row_ids
    }

    // Magic code at end
    total_size += sizeof(uint64_t);

    return total_size;
}

void
StringIndexSortMemoryImpl::SerializeToBinary(uint8_t* ptr,
                                             size_t& offset) const {
    size_t start_offset = offset;

    // Write unique count as uint32_t
    uint32_t unique_count = static_cast<uint32_t>(unique_values_.size());
    memcpy(ptr + offset, &unique_count, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // Calculate and write string offsets
    size_t string_offsets_start = offset;
    offset += unique_count * sizeof(uint32_t);  // Reserve space for offsets

    size_t string_data_start = offset;
    std::vector<uint32_t> string_offsets;
    string_offsets.reserve(unique_count);

    // Write string data section
    for (size_t i = 0; i < unique_values_.size(); ++i) {
        string_offsets.push_back(
            static_cast<uint32_t>(offset - string_data_start));

        // Write string length and content
        uint32_t str_len = static_cast<uint32_t>(unique_values_[i].size());
        memcpy(ptr + offset, &str_len, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy(ptr + offset, unique_values_[i].data(), str_len);
        offset += str_len;
    }

    // Write string offsets back
    memcpy(ptr + string_offsets_start,
           string_offsets.data(),
           string_offsets.size() * sizeof(uint32_t));

    // Calculate and write posting list offsets
    size_t post_list_offsets_start = offset;
    offset += unique_count * sizeof(uint32_t);  // Reserve space for offsets

    size_t post_list_data_start = offset;
    std::vector<uint32_t> post_list_offsets;
    post_list_offsets.reserve(unique_count);

    // Write posting list data section
    for (size_t i = 0; i < posting_lists_.size(); ++i) {
        post_list_offsets.push_back(
            static_cast<uint32_t>(offset - post_list_data_start));

        // Write posting list length and content
        uint32_t post_list_len =
            static_cast<uint32_t>(posting_lists_[i].size());
        memcpy(ptr + offset, &post_list_len, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        for (uint32_t row_id : posting_lists_[i]) {
            memcpy(ptr + offset, &row_id, sizeof(uint32_t));
            offset += sizeof(uint32_t);
        }
    }

    // Write posting list offsets back
    memcpy(ptr + post_list_offsets_start,
           post_list_offsets.data(),
           post_list_offsets.size() * sizeof(uint32_t));

    // Write magic code at the very end
    uint64_t magic = StringIndexSort::MAGIC_CODE;
    memcpy(ptr + offset, &magic, sizeof(uint64_t));
    offset += sizeof(uint64_t);
}

void
StringIndexSortMemoryImpl::LoadFromBinary(
    const BinarySet& binary_set,
    size_t total_num_rows,
    TargetBitmap& valid_bitset,
    std::vector<int32_t>& idx_to_offsets) {
    auto index_data = binary_set.GetByName("index_data");
    AssertInfo(index_data != nullptr,
               "Failed to find 'index_data' in binary_set");

    auto parsed = ParseBinaryData(index_data->data.get(), index_data->size);
    unique_values_.clear();
    posting_lists_.clear();
    unique_values_.reserve(parsed.unique_count);
    posting_lists_.reserve(parsed.unique_count);

    std::fill(idx_to_offsets.begin(), idx_to_offsets.end(), -1);

    // Read strings and posting lists
    for (uint32_t unique_idx = 0; unique_idx < parsed.unique_count;
         ++unique_idx) {
        // Read string
        const uint8_t* str_ptr =
            parsed.string_data_start + parsed.string_offsets[unique_idx];
        uint32_t str_len;
        memcpy(&str_len, str_ptr, sizeof(uint32_t));
        str_ptr += sizeof(uint32_t);
        std::string value(reinterpret_cast<const char*>(str_ptr), str_len);

        // Read posting list
        const uint8_t* post_list_ptr =
            parsed.post_list_data_start + parsed.post_list_offsets[unique_idx];
        uint32_t post_list_len;
        memcpy(&post_list_len, post_list_ptr, sizeof(uint32_t));
        post_list_ptr += sizeof(uint32_t);

        PostingList posting_list;
        posting_list.reserve(post_list_len);
        const uint32_t* row_ids =
            reinterpret_cast<const uint32_t*>(post_list_ptr);

        for (uint32_t j = 0; j < post_list_len; ++j) {
            uint32_t row_id = row_ids[j];
            posting_list.push_back(row_id);

            // Map each row_id to its unique value index
            if (static_cast<size_t>(row_id) >= idx_to_offsets.size()) {
                ThrowInfo(
                    milvus::ErrorCode::UnexpectedError,
                    fmt::format("row_id {} exceeds idx_to_offsets size {}",
                                row_id,
                                idx_to_offsets.size()));
            }
            idx_to_offsets[row_id] = unique_idx;
        }

        unique_values_.push_back(std::move(value));
        posting_lists_.push_back(std::move(posting_list));
    }
}

size_t
StringIndexSortMemoryImpl::FindValueIndex(const std::string& value) const {
    auto it =
        std::lower_bound(unique_values_.begin(), unique_values_.end(), value);
    if (it != unique_values_.end() && *it == value) {
        return std::distance(unique_values_.begin(), it);
    }
    return std::numeric_limits<size_t>::max();
}

const TargetBitmap
StringIndexSortMemoryImpl::In(size_t n,
                              const std::string* values,
                              size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    for (size_t i = 0; i < n; ++i) {
        size_t idx = FindValueIndex(values[i]);
        if (idx != std::numeric_limits<size_t>::max()) {
            const auto& posting_list = posting_lists_[idx];
            for (uint32_t row_id : posting_list) {
                bitset[row_id] = true;
            }
        }
    }

    return bitset;
}

const TargetBitmap
StringIndexSortMemoryImpl::NotIn(size_t n,
                                 const std::string* values,
                                 size_t total_num_rows,
                                 const TargetBitmap& valid_bitset) {
    auto in_bitset = In(n, values, total_num_rows);
    in_bitset.flip();

    // Reset null values
    for (size_t i = 0; i < total_num_rows; ++i) {
        if (!valid_bitset[i]) {
            in_bitset.reset(i);
        }
    }

    return in_bitset;
}

const TargetBitmap
StringIndexSortMemoryImpl::IsNull(size_t total_num_rows,
                                  const TargetBitmap& valid_bitset) {
    auto result = valid_bitset.clone();
    result.flip();
    return result;
}

TargetBitmap
StringIndexSortMemoryImpl::IsNotNull(const TargetBitmap& valid_bitset) {
    return valid_bitset.clone();
}

const TargetBitmap
StringIndexSortMemoryImpl::Range(std::string value,
                                 OpType op,
                                 size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    size_t start_idx = 0;
    size_t end_idx = unique_values_.size();

    switch (op) {
        case OpType::GreaterThan: {
            auto it = std::upper_bound(
                unique_values_.begin(), unique_values_.end(), value);
            start_idx = std::distance(unique_values_.begin(), it);
            break;
        }
        case OpType::GreaterEqual: {
            auto it = std::lower_bound(
                unique_values_.begin(), unique_values_.end(), value);
            start_idx = std::distance(unique_values_.begin(), it);
            break;
        }
        case OpType::LessThan: {
            auto it = std::lower_bound(
                unique_values_.begin(), unique_values_.end(), value);
            end_idx = std::distance(unique_values_.begin(), it);
            break;
        }
        case OpType::LessEqual: {
            auto it = std::upper_bound(
                unique_values_.begin(), unique_values_.end(), value);
            end_idx = std::distance(unique_values_.begin(), it);
            break;
        }
        default:
            ThrowInfo(
                milvus::OpTypeInvalid,
                fmt::format("Invalid OperatorType: {}", static_cast<int>(op)));
    }

    // Set bits for all posting lists in range
    for (size_t i = start_idx; i < end_idx; ++i) {
        const auto& posting_list = posting_lists_[i];
        for (uint32_t row_id : posting_list) {
            bitset[row_id] = true;
        }
    }

    return bitset;
}

const TargetBitmap
StringIndexSortMemoryImpl::Range(std::string lower_bound_value,
                                 bool lb_inclusive,
                                 std::string upper_bound_value,
                                 bool ub_inclusive,
                                 size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    auto start_it = lb_inclusive ? std::lower_bound(unique_values_.begin(),
                                                    unique_values_.end(),
                                                    lower_bound_value)
                                 : std::upper_bound(unique_values_.begin(),
                                                    unique_values_.end(),
                                                    lower_bound_value);

    auto end_it = ub_inclusive ? std::upper_bound(unique_values_.begin(),
                                                  unique_values_.end(),
                                                  upper_bound_value)
                               : std::lower_bound(unique_values_.begin(),
                                                  unique_values_.end(),
                                                  upper_bound_value);

    size_t start_idx = std::distance(unique_values_.begin(), start_it);
    size_t end_idx = std::distance(unique_values_.begin(), end_it);

    for (size_t i = start_idx; i < end_idx; ++i) {
        const auto& posting_list = posting_lists_[i];
        for (uint32_t row_id : posting_list) {
            bitset[row_id] = true;
        }
    }

    return bitset;
}

const TargetBitmap
StringIndexSortMemoryImpl::PrefixMatch(const std::string_view prefix,
                                       size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    // Use FindPrefixRange for O(log n) lookup of both start and end
    auto [start_idx, end_idx] = FindPrefixRange(std::string(prefix));

    for (size_t idx = start_idx; idx < end_idx; ++idx) {
        const auto& posting_list = posting_lists_[idx];
        for (uint32_t row_id : posting_list) {
            bitset[row_id] = true;
        }
    }

    return bitset;
}

std::pair<size_t, size_t>
StringIndexSortMemoryImpl::FindPrefixRange(const std::string& prefix) const {
    if (prefix.empty()) {
        return {0, unique_values_.size()};
    }

    // Binary search for start: first value >= prefix
    auto start_it =
        std::lower_bound(unique_values_.begin(), unique_values_.end(), prefix);
    size_t start_idx = std::distance(unique_values_.begin(), start_it);

    // Compute "next prefix" for end boundary: "abc" -> "abd"
    // Range is [prefix, next_prefix), all strings starting with prefix
    std::string next_prefix = prefix;
    bool has_next = false;
    // Find rightmost char that can be incremented (not 0xFF)
    for (int i = next_prefix.size() - 1; i >= 0; --i) {
        if (static_cast<unsigned char>(next_prefix[i]) < 255) {
            ++next_prefix[i];
            next_prefix.resize(i + 1);
            has_next = true;
            break;
        }
    }

    size_t end_idx;
    if (has_next) {
        // Binary search for end: first value >= next_prefix
        auto end_it = std::lower_bound(
            unique_values_.begin(), unique_values_.end(), next_prefix);
        end_idx = std::distance(unique_values_.begin(), end_it);
    } else {
        // All chars are 0xFF, no upper bound
        end_idx = unique_values_.size();
    }

    return {start_idx, end_idx};
}

bool
StringIndexSortMemoryImpl::MatchValue(const std::string& value,
                                      const std::string& pattern,
                                      proto::plan::OpType op) const {
    switch (op) {
        case proto::plan::OpType::PostfixMatch:
            // Suffix match: value ends with pattern
            if (pattern.size() > value.size()) {
                return false;
            }
            return value.compare(value.size() - pattern.size(),
                                 pattern.size(),
                                 pattern) == 0;
        case proto::plan::OpType::InnerMatch:
            // Contains match: value contains pattern
            return value.find(pattern) != std::string::npos;
        default:
            // For Match op, use Pattern matcher (handled separately)
            return false;
    }
}

const TargetBitmap
StringIndexSortMemoryImpl::PatternMatch(const std::string& pattern,
                                        proto::plan::OpType op,
                                        size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    // For PostfixMatch and InnerMatch, no prefix optimization possible
    // Still benefits from unique value deduplication
    if (op == proto::plan::OpType::PostfixMatch ||
        op == proto::plan::OpType::InnerMatch) {
        // Iterate over all unique values
        for (size_t idx = 0; idx < unique_values_.size(); ++idx) {
            if (MatchValue(unique_values_[idx], pattern, op)) {
                const auto& posting_list = posting_lists_[idx];
                for (uint32_t row_id : posting_list) {
                    bitset[row_id] = true;
                }
            }
        }
        return bitset;
    }

    // For Match op, use prefix optimization + LIKE matcher
    std::string prefix = extract_fixed_prefix_from_pattern(pattern);

    // Find the range of unique values to check
    auto [start_idx, end_idx] = FindPrefixRange(prefix);

    // Build matcher for LIKE pattern
    LikePatternMatcher matcher(pattern);

    // Iterate over unique values in range (each value checked only once)
    for (size_t idx = start_idx; idx < end_idx; ++idx) {
        if (matcher(unique_values_[idx])) {
            // Match found, set all row IDs in posting list
            const auto& posting_list = posting_lists_[idx];
            for (uint32_t row_id : posting_list) {
                bitset[row_id] = true;
            }
        }
    }

    return bitset;
}

std::optional<std::string>
StringIndexSortMemoryImpl::Reverse_Lookup(
    size_t offset,
    size_t total_num_rows,
    const TargetBitmap& valid_bitset,
    const std::vector<int32_t>& idx_to_offsets) const {
    if (offset >= total_num_rows || !valid_bitset[offset]) {
        return std::nullopt;
    }

    if (offset < idx_to_offsets.size()) {
        size_t unique_idx = idx_to_offsets[offset];
        if (unique_idx < unique_values_.size()) {
            return unique_values_[unique_idx];
        }
    }

    return std::nullopt;
}

int64_t
StringIndexSortMemoryImpl::Size() {
    size_t size = 0;

    // Size of unique values (actual string data)
    for (const auto& str : unique_values_) {
        size += str.size();
    }

    // Size of posting lists (actual ID data)
    for (const auto& list : posting_lists_) {
        size += list.size() * sizeof(uint32_t);
    }

    return size;
}

int64_t
StringIndexSortMemoryImpl::ByteSize() const {
    int64_t total = 0;

    // unique_values_: vector<string>
    // sizeof(std::string) includes the SSO buffer
    // For heap-allocated strings (capacity > SSO threshold), we need to add external buffer
    const size_t sso_threshold = GetStringSSOThreshold();
    total += unique_values_.capacity() * sizeof(std::string);
    for (const auto& str : unique_values_) {
        // Only add capacity for heap-allocated strings (non-SSO)
        if (str.capacity() > sso_threshold) {
            total += str.capacity();
        }
    }

    // posting_lists_: vector<PostingList>
    // PostingList is folly::small_vector<uint32_t, 4>
    // sizeof(PostingList) includes the inline buffer for 4 elements
    total += posting_lists_.capacity() * sizeof(PostingList);
    for (const auto& list : posting_lists_) {
        // If the capacity exceeds inline capacity (4), it allocates on heap
        if (list.capacity() > 4) {
            total += list.capacity() * sizeof(uint32_t);
        }
    }

    return total;
}

StringIndexSortMmapImpl::~StringIndexSortMmapImpl() {
    if (mmap_data_ != nullptr && mmap_data_ != MAP_FAILED) {
        munmap(mmap_data_, mmap_size_);
        if (!mmap_filepath_.empty()) {
            unlink(mmap_filepath_.c_str());
        }
    }
}

void
StringIndexSortMmapImpl::LoadFromBinary(const BinarySet& binary_set,
                                        size_t total_num_rows,
                                        TargetBitmap& valid_bitset,
                                        std::vector<int32_t>& idx_to_offsets) {
    auto index_data = binary_set.GetByName("index_data");

    AssertInfo(!mmap_filepath_.empty(), "mmap filepath is not set");

    std::filesystem::create_directories(
        std::filesystem::path(mmap_filepath_).parent_path());

    auto aligned_size =
        ((index_data->size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT;
    {
        auto file_writer = storage::FileWriter(mmap_filepath_);
        file_writer.Write(index_data->data.get(), index_data->size);

        if (aligned_size > index_data->size) {
            std::vector<uint8_t> padding(aligned_size - index_data->size, 0);
            file_writer.Write(padding.data(), padding.size());
        }
        // write padding in case of all null values
        std::vector<uint8_t> padding(MMAP_INDEX_PADDING, 0);
        file_writer.Write(padding.data(), padding.size());
        file_writer.Finish();
    }

    auto fd = open(mmap_filepath_.c_str(), O_RDONLY);
    if (fd == -1) {
        ThrowInfo(DataFormatBroken, "Failed to open mmap file");
    }

    mmap_size_ = aligned_size + MMAP_INDEX_PADDING;
    data_size_ = index_data->size;
    mmap_data_ = static_cast<char*>(
        mmap(nullptr, mmap_size_, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);

    if (mmap_data_ == MAP_FAILED) {
        ThrowInfo(DataFormatBroken, "Failed to mmap file");
    }

    const uint8_t* data_start = reinterpret_cast<const uint8_t*>(mmap_data_);

    auto parsed = ParseBinaryData(data_start, data_size_);
    unique_count_ = parsed.unique_count;
    string_offsets_ = parsed.string_offsets;
    string_data_start_ = parsed.string_data_start;
    post_list_offsets_ = parsed.post_list_offsets;
    post_list_data_start_ = parsed.post_list_data_start;

    // Initialize idx_to_offsets
    std::fill(idx_to_offsets.begin(), idx_to_offsets.end(), -1);
    // Rebuild idx_to_offsets by iterating through entries
    for (uint32_t unique_idx = 0; unique_idx < unique_count_; ++unique_idx) {
        MmapEntry entry = GetEntry(unique_idx);

        // Map each row_id in posting list to this unique index
        entry.for_each_row_id([&idx_to_offsets, unique_idx](uint32_t row_id) {
            idx_to_offsets[row_id] = unique_idx;
        });
    }
}

size_t
StringIndexSortMmapImpl::FindValueIndex(const std::string& value) const {
    std::string_view search_value(value);
    size_t left = 0;
    size_t right = unique_count_;

    while (left < right) {
        size_t mid = left + (right - left) / 2;
        MmapEntry entry = GetEntry(mid);
        std::string_view entry_sv = entry.get_string_view();

        int cmp = entry_sv.compare(search_value);
        if (cmp < 0) {
            left = mid + 1;
        } else if (cmp > 0) {
            right = mid;
        } else {
            return mid;
        }
    }

    return unique_count_;
}

size_t
StringIndexSortMmapImpl::LowerBound(const std::string_view& value) const {
    size_t left = 0, right = unique_count_;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (GetEntry(mid).get_string_view() < value) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

size_t
StringIndexSortMmapImpl::UpperBound(const std::string_view& value) const {
    size_t left = 0, right = unique_count_;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (GetEntry(mid).get_string_view() <= value) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

const TargetBitmap
StringIndexSortMmapImpl::In(size_t n,
                            const std::string* values,
                            size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    for (size_t i = 0; i < n; ++i) {
        size_t idx = FindValueIndex(values[i]);
        if (idx < unique_count_) {
            MmapEntry entry = GetEntry(idx);
            // Set bits for all row_ids in posting list
            entry.for_each_row_id(
                [&bitset](uint32_t row_id) { bitset.set(row_id); });
        }
    }

    return bitset;
}

const TargetBitmap
StringIndexSortMmapImpl::NotIn(size_t n,
                               const std::string* values,
                               size_t total_num_rows,
                               const TargetBitmap& valid_bitset) {
    auto in_bitset = In(n, values, total_num_rows);
    in_bitset.flip();

    for (size_t i = 0; i < total_num_rows; ++i) {
        if (!valid_bitset[i]) {
            in_bitset.reset(i);
        }
    }

    return in_bitset;
}

const TargetBitmap
StringIndexSortMmapImpl::IsNull(size_t total_num_rows,
                                const TargetBitmap& valid_bitset) {
    auto null_bitset = valid_bitset.clone();
    null_bitset.flip();
    return null_bitset;
}

TargetBitmap
StringIndexSortMmapImpl::IsNotNull(const TargetBitmap& valid_bitset) {
    return valid_bitset.clone();
}

const TargetBitmap
StringIndexSortMmapImpl::Range(std::string value,
                               OpType op,
                               size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    size_t start_idx = 0;
    size_t end_idx = unique_count_;

    switch (op) {
        case OpType::GreaterThan:
            start_idx = UpperBound(value);
            break;
        case OpType::GreaterEqual:
            start_idx = LowerBound(value);
            break;
        case OpType::LessThan:
            end_idx = LowerBound(value);
            break;
        case OpType::LessEqual:
            end_idx = UpperBound(value);
            break;
        default:
            ThrowInfo(
                OpTypeInvalid,
                fmt::format("Invalid OperatorType: {}", static_cast<int>(op)));
    }

    // Set bits for all posting lists in range
    for (size_t i = start_idx; i < end_idx; ++i) {
        MmapEntry entry = GetEntry(i);
        entry.for_each_row_id(
            [&bitset](uint32_t row_id) { bitset.set(row_id); });
    }

    return bitset;
}

const TargetBitmap
StringIndexSortMmapImpl::Range(std::string lower_bound_value,
                               bool lb_inclusive,
                               std::string upper_bound_value,
                               bool ub_inclusive,
                               size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    size_t start_idx = lb_inclusive ? LowerBound(lower_bound_value)
                                    : UpperBound(lower_bound_value);
    size_t end_idx = ub_inclusive ? UpperBound(upper_bound_value)
                                  : LowerBound(upper_bound_value);

    // Set bits for all posting lists in range
    for (size_t i = start_idx; i < end_idx; ++i) {
        MmapEntry entry = GetEntry(i);
        entry.for_each_row_id(
            [&bitset](uint32_t row_id) { bitset.set(row_id); });
    }

    return bitset;
}

const TargetBitmap
StringIndexSortMmapImpl::PrefixMatch(const std::string_view prefix,
                                     size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    // Use FindPrefixRange for O(log n) lookup of both start and end
    auto [start_idx, end_idx] = FindPrefixRange(std::string(prefix));

    for (size_t idx = start_idx; idx < end_idx; ++idx) {
        MmapEntry entry = GetEntry(idx);
        entry.for_each_row_id(
            [&bitset](uint32_t row_id) { bitset.set(row_id); });
    }

    return bitset;
}

std::pair<size_t, size_t>
StringIndexSortMmapImpl::FindPrefixRange(const std::string& prefix) const {
    if (prefix.empty()) {
        return {0, unique_count_};
    }

    // Binary search for start
    size_t start_idx = LowerBound(prefix);

    // Compute "next prefix" for end boundary: "abc" -> "abd"
    std::string next_prefix = prefix;
    bool has_next = false;
    for (int i = next_prefix.size() - 1; i >= 0; --i) {
        if (static_cast<unsigned char>(next_prefix[i]) < 255) {
            ++next_prefix[i];
            next_prefix.resize(i + 1);
            has_next = true;
            break;
        }
    }

    size_t end_idx;
    if (has_next) {
        end_idx = LowerBound(next_prefix);
    } else {
        end_idx = unique_count_;
    }

    return {start_idx, end_idx};
}

bool
StringIndexSortMmapImpl::MatchValue(const std::string& value,
                                    const std::string& pattern,
                                    proto::plan::OpType op) const {
    switch (op) {
        case proto::plan::OpType::PostfixMatch:
            // Suffix match: value ends with pattern
            if (pattern.size() > value.size()) {
                return false;
            }
            return value.compare(value.size() - pattern.size(),
                                 pattern.size(),
                                 pattern) == 0;
        case proto::plan::OpType::InnerMatch:
            // Contains match: value contains pattern
            return value.find(pattern) != std::string::npos;
        default:
            return false;
    }
}

const TargetBitmap
StringIndexSortMmapImpl::PatternMatch(const std::string& pattern,
                                      proto::plan::OpType op,
                                      size_t total_num_rows) {
    TargetBitmap bitset(total_num_rows, false);

    // For PostfixMatch and InnerMatch, no prefix optimization possible
    // Still benefits from unique value deduplication
    if (op == proto::plan::OpType::PostfixMatch ||
        op == proto::plan::OpType::InnerMatch) {
        for (size_t idx = 0; idx < unique_count_; ++idx) {
            MmapEntry entry = GetEntry(idx);
            std::string_view sv = entry.get_string_view();

            if (MatchValue(std::string(sv), pattern, op)) {
                entry.for_each_row_id(
                    [&bitset](uint32_t row_id) { bitset.set(row_id); });
            }
        }
        return bitset;
    }

    // For Match op, use prefix optimization + LIKE matcher
    std::string prefix = extract_fixed_prefix_from_pattern(pattern);

    // Find the range of unique values to check
    auto [start_idx, end_idx] = FindPrefixRange(prefix);

    // Build matcher for LIKE pattern
    LikePatternMatcher matcher(pattern);

    // Iterate over unique values in range (each value checked only once)
    for (size_t idx = start_idx; idx < end_idx; ++idx) {
        MmapEntry entry = GetEntry(idx);
        std::string_view sv = entry.get_string_view();

        if (matcher(sv)) {
            // Match found, set all row IDs in posting list
            entry.for_each_row_id(
                [&bitset](uint32_t row_id) { bitset.set(row_id); });
        }
    }

    return bitset;
}

std::optional<std::string>
StringIndexSortMmapImpl::Reverse_Lookup(
    size_t offset,
    size_t total_num_rows,
    const TargetBitmap& valid_bitset,
    const std::vector<int32_t>& idx_to_offsets) const {
    if (offset >= total_num_rows || !valid_bitset[offset]) {
        return std::nullopt;
    }

    if (offset < idx_to_offsets.size()) {
        int32_t unique_idx = idx_to_offsets[offset];
        if (unique_idx >= 0 &&
            static_cast<size_t>(unique_idx) < unique_count_) {
            MmapEntry entry = GetEntry(unique_idx);
            // Convert string_view to string for return
            std::string_view sv = entry.get_string_view();
            return std::string(sv);
        }
    }

    return std::nullopt;
}

int64_t
StringIndexSortMmapImpl::Size() {
    return mmap_size_;
}

int64_t
StringIndexSortMmapImpl::ByteSize() const {
    // mmap size (O(n) - the mapped index data)
    return mmap_size_;
}

}  // namespace milvus::index
