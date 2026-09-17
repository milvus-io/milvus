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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/OffsetMapping.h"
#include "index/Meta.h"
#include "index/vector/VectorValidData.h"

namespace milvus::index {

// Sealed vector-validity wire helpers. The legacy entries remain an exact
// native uint64 row count plus an LSB-first bitmap. Readers accept trailing
// bitmap bytes for compatibility but require the count entry to be exact and
// validate the coordinate domain before allocating the decoded bool array.
//
// The stateful part lives in VectorValidData. These helpers never expose the
// mutable SealedOffsetMapping used during construction.

// Entry names of the valid-data payload inside a nullable vector artifact.
constexpr const char* VALID_DATA_KEY = "valid_data";
constexpr const char* VALID_DATA_COUNT_KEY = "valid_data_count";

inline bool
IsValidDataBinary(const std::string& name) {
    return name == VALID_DATA_COUNT_KEY || name == VALID_DATA_KEY;
}

inline std::string
GetIndexFileName(const std::string& file) {
    auto pos = file.find_last_of("/\\");
    if (pos == std::string::npos) {
        return file;
    }
    return file.substr(pos + 1);
}

inline bool
IsValidDataDiskFileSlice(const std::string& file) {
    const auto file_name = GetIndexFileName(file);
    const std::string prefix = std::string(VALID_DATA_KEY) + "_";
    if (file_name.size() <= prefix.size() ||
        file_name.compare(0, prefix.size(), prefix) != 0) {
        return false;
    }
    return std::all_of(file_name.begin() + prefix.size(),
                       file_name.end(),
                       [](char c) { return c >= '0' && c <= '9'; });
}

inline std::vector<std::string>
FilterValidDataDiskFileSlices(const std::vector<std::string>& files) {
    std::vector<std::string> valid_data_files;
    for (const auto& file : files) {
        if (IsValidDataDiskFileSlice(file)) {
            valid_data_files.emplace_back(file);
        }
    }
    return valid_data_files;
}

inline std::vector<std::string>
GetCacheFilesForDiskIndexLoad(const std::vector<std::string>& index_files,
                              bool load_index_with_stream) {
    return load_index_with_stream ? FilterValidDataDiskFileSlices(index_files)
                                  : index_files;
}

inline bool
ContainsOnlyValidData(const BinarySet& binary_set) {
    if (!binary_set.Contains(VALID_DATA_COUNT_KEY) ||
        !binary_set.Contains(VALID_DATA_KEY)) {
        return false;
    }
    for (const auto& [name, _] : binary_set.binary_map_) {
        if (!IsValidDataBinary(name)) {
            return false;
        }
    }
    return true;
}

inline bool
IsAllNullNullable(const OffsetMapping& offset_mapping) {
    return offset_mapping.IsEnabled() && offset_mapping.GetValidCount() == 0;
}

inline size_t
GetValidDataBitmapSize(size_t count) {
    return count / 8 + (count % 8 != 0);
}

inline void
ValidatePersistedValidDataCount(size_t count) {
    constexpr size_t max_count =
        static_cast<size_t>(std::numeric_limits<int32_t>::max()) + 1;
    if (count > max_count) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector valid_data count {} exceeds offset mapping "
                  "domain",
                  count);
    }
}

inline uint64_t
ToValidDataCount(size_t count) {
    AssertInfo(
        count <= static_cast<size_t>(std::numeric_limits<int32_t>::max()) + 1,
        "nullable vector valid_data count exceeds offset mapping domain");
    return static_cast<uint64_t>(count);
}

inline size_t
FromValidDataCount(uint64_t count) {
    constexpr uint64_t max_count =
        static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) + 1;
    if (count > std::numeric_limits<size_t>::max() || count > max_count) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector valid_data count {} exceeds offset mapping "
                  "domain",
                  count);
    }
    return static_cast<size_t>(count);
}

namespace detail {

struct EmptyEmbeddingListState {
    int64_t dim{0};
    std::vector<size_t> offsets;
};

inline constexpr size_t kEmptyEmbeddingListHeaderSize =
    sizeof(int64_t) + sizeof(uint64_t);

// The caller checks that the header is present, then validates the decoded
// dimension and offsets in its original format-specific order.
inline EmptyEmbeddingListState
ReadEmptyEmbeddingListPayload(const uint8_t* cursor, uint64_t byte_size) {
    EmptyEmbeddingListState result;
    std::memcpy(&result.dim, cursor, sizeof(result.dim));
    cursor += sizeof(result.dim);
    uint64_t wire_count = 0;
    std::memcpy(&wire_count, cursor, sizeof(wire_count));
    cursor += sizeof(wire_count);
    const auto count = FromValidDataCount(wire_count);
    if (count == 0 || count > (std::numeric_limits<size_t>::max() -
                               kEmptyEmbeddingListHeaderSize) /
                                  sizeof(size_t)) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset count is invalid");
    }
    const auto required =
        kEmptyEmbeddingListHeaderSize + count * sizeof(size_t);
    if (byte_size < required) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset entry is truncated");
    }
    result.offsets.resize(count);
    std::memcpy(result.offsets.data(), cursor, count * sizeof(size_t));
    return result;
}

}  // namespace detail

inline size_t
CountValidDataBitmap(size_t count, const uint8_t* bitmap) {
    ValidatePersistedValidDataCount(count);
    if (count > 0 && bitmap == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector valid_data bitmap is null for {} rows",
                  count);
    }
    size_t valid_count = 0;
    for (size_t i = 0; i < count; ++i) {
        if ((bitmap[i / 8] >> (i % 8)) & 1) {
            ++valid_count;
        }
    }
    return valid_count;
}

inline constexpr const char* OFFSET_MAPPING_MMAP_DIR = "id_mapping_mmap";

inline std::string
GetOffsetMappingMmapDir(const std::string& local_index_path_prefix) {
    return (std::filesystem::path(local_index_path_prefix) /
            OFFSET_MAPPING_MMAP_DIR)
        .string();
}

inline bool
NeedOffsetMappingMmap(const OffsetMappingBuildOptions& options,
                      size_t total_count,
                      size_t valid_count) {
    return (options.enable_mmap_o2i_map && total_count > 0) ||
           (options.enable_mmap_i2o_map && valid_count > 0);
}

OffsetMappingBuildOptions
GetOffsetMappingMmapOptions(const Config& config);

inline std::vector<uint8_t>
PackValidDataBitmap(const OffsetMapping& offset_mapping) {
    const auto total_count = offset_mapping.GetTotalCount();
    AssertInfo(total_count >= 0,
               "nullable vector offset mapping has a negative row count");
    const auto count = static_cast<size_t>(total_count);
    (void)ToValidDataCount(count);
    std::vector<uint8_t> data(GetValidDataBitmapSize(count), 0);
    for (size_t i = 0; i < count; ++i) {
        if (offset_mapping.IsValid(i)) {
            data[i / 8] |= (1 << (i % 8));
        }
    }
    return data;
}

inline void
BuildValidDataFromBitmap(VectorValidData& valid,
                         size_t count,
                         const uint8_t* bitmap,
                         const OffsetMappingBuildOptions& options = {}) {
    ValidatePersistedValidDataCount(count);
    if (count > 0 && bitmap == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector valid_data bitmap is null for {} rows",
                  count);
    }

    std::unique_ptr<bool[]> valid_data(count == 0 ? nullptr : new bool[count]);
    for (size_t i = 0; i < count; ++i) {
        valid_data[i] = (bitmap[i / 8] >> (i % 8)) & 1;
    }
    valid.Build(valid_data.get(), static_cast<int64_t>(count), options);
}

inline void
AppendValidDataToBinarySet(const OffsetMapping& offset_mapping,
                           BinarySet& binary_set) {
    if (!offset_mapping.IsEnabled()) {
        return;
    }

    auto count = static_cast<size_t>(offset_mapping.GetTotalCount());
    auto wire_count = ToValidDataCount(count);
    std::shared_ptr<uint8_t[]> count_buf(new uint8_t[sizeof(uint64_t)]);
    milvus::fastmem::FastMemcpy(count_buf.get(), &wire_count, sizeof(uint64_t));
    binary_set.Append(VALID_DATA_COUNT_KEY, count_buf, sizeof(uint64_t));

    auto packed_data = PackValidDataBitmap(offset_mapping);
    std::shared_ptr<uint8_t[]> data(new uint8_t[packed_data.size()]);
    if (!packed_data.empty()) {
        milvus::fastmem::FastMemcpy(
            data.get(), packed_data.data(), packed_data.size());
    }
    binary_set.Append(VALID_DATA_KEY, data, packed_data.size());
}

inline bool
LoadValidDataFromBinarySet(const BinarySet& binary_set,
                           VectorValidData& valid,
                           const OffsetMappingBuildOptions& options = {}) {
    bool has_count = binary_set.Contains(VALID_DATA_COUNT_KEY);
    bool has_data = binary_set.Contains(VALID_DATA_KEY);
    if (!has_count && !has_data) {
        return false;
    }
    if (!has_count || !has_data) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector index valid_data files are incomplete");
    }

    auto count_ptr = binary_set.GetByName(VALID_DATA_COUNT_KEY);
    if (count_ptr == nullptr || count_ptr->size != sizeof(uint64_t) ||
        count_ptr->data == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector index valid_data count file is invalid");
    }
    uint64_t wire_count = 0;
    milvus::fastmem::FastMemcpy(
        &wire_count, count_ptr->data.get(), sizeof(uint64_t));
    auto count = FromValidDataCount(wire_count);

    auto data_ptr = binary_set.GetByName(VALID_DATA_KEY);
    const auto required_bytes = GetValidDataBitmapSize(count);
    if (data_ptr == nullptr || data_ptr->size < required_bytes ||
        (required_bytes > 0 && data_ptr->data == nullptr)) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector index valid_data bitmap file is invalid");
    }
    BuildValidDataFromBitmap(valid, count, data_ptr->data.get(), options);
    return true;
}

}  // namespace milvus::index
