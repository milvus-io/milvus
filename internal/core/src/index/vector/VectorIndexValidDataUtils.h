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
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/ValidityView.h"
#include "index/Meta.h"
#include "knowhere/expected.h"
#include "knowhere/id_map.h"
#include "knowhere/index/index_node.h"

namespace milvus::index {

// Sealed vector-validity wire helpers. The legacy entries remain an exact
// native uint64 row count plus an LSB-first bitmap. Readers accept trailing
// bitmap bytes for compatibility but require the count entry to be exact and
// validate the coordinate domain before handing the bitmap to knowhere.
//
// The stateful part lives in knowhere's IdMap (#50524): loaders and builders
// hand it the public-row validity bitmap and knowhere owns both mapping
// directions, so no Milvus-side offset mapping exists for an indexed nullable
// vector field.

// Entry names of the valid-data payload inside a nullable vector artifact.
constexpr const char* VALID_DATA_KEY = "valid_data";
constexpr const char* VALID_DATA_COUNT_KEY = "valid_data_count";
constexpr const char* EMPTY_EMB_LIST_OFFSETS_KEY = "empty_emb_list_offsets";

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

// A nullable index whose public domain is non-empty but holds no vector.
inline bool
IsAllNullNullable(const knowhere::IdMap& id_map) {
    return !id_map.ValidBitmap().empty() && id_map.InCount() == 0;
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
                  "nullable vector valid_data count {} exceeds the id map "
                  "domain",
                  count);
    }
}

inline uint64_t
ToValidDataCount(size_t count) {
    AssertInfo(
        count <= static_cast<size_t>(std::numeric_limits<int32_t>::max()) + 1,
        "nullable vector valid_data count exceeds the id map domain");
    return static_cast<uint64_t>(count);
}

inline size_t
FromValidDataCount(uint64_t count) {
    constexpr uint64_t max_count =
        static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) + 1;
    if (count > std::numeric_limits<size_t>::max() || count > max_count) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector valid_data count {} exceeds the id map "
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

inline bool
IsValidEmptyEmbeddingListDimension(int64_t dim) {
    return dim > 0;
}

inline bool
HasValidEmptyEmbeddingListBounds(const std::vector<size_t>& offsets) {
    return !offsets.empty() && offsets.front() == 0 && offsets.back() == 0;
}

inline bool
IsValidEmptyEmbeddingListOffsets(const std::vector<size_t>& offsets) {
    return HasValidEmptyEmbeddingListBounds(offsets) &&
           std::is_sorted(offsets.begin(), offsets.end());
}

inline size_t
GetEmptyEmbeddingListPayloadSize(size_t offset_count) {
    if (offset_count >
        (std::numeric_limits<size_t>::max() - kEmptyEmbeddingListHeaderSize) /
            sizeof(size_t)) {
        ThrowInfo(UnexpectedError,
                  "empty embedding-list offset payload size overflows");
    }
    return kEmptyEmbeddingListHeaderSize + offset_count * sizeof(size_t);
}

inline void
EncodeEmptyEmbeddingListPayload(uint8_t* cursor,
                                int64_t dim,
                                uint64_t wire_count,
                                const std::vector<size_t>& offsets) {
    std::memcpy(cursor, &dim, sizeof(dim));
    cursor += sizeof(dim);
    std::memcpy(cursor, &wire_count, sizeof(wire_count));
    cursor += sizeof(wire_count);
    std::memcpy(cursor, offsets.data(), offsets.size() * sizeof(size_t));
}

// The caller checks that the header is present, then validates the decoded
// dimension and offsets in its original format-specific order.
inline EmptyEmbeddingListState
DecodeEmptyEmbeddingListPayload(const uint8_t* cursor, uint64_t byte_size) {
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

// --- knowhere IdMap input views ------------------------------------------

// Borrowed public-row validity bitmap. `count` is the number of public rows,
// not bytes; the bitmap is LSB-first over that range.
struct ValidDataView {
    bool found{false};
    size_t count{0};
    const uint8_t* bitmap{nullptr};
};

struct OwnedValidData {
    bool found{false};
    size_t count{0};
    std::vector<uint8_t> bitmap;

    ValidDataView
    View() const {
        return {found, count, bitmap.data()};
    }
};

// Materialize one LSB-first public-row validity bitmap the IdMap can consume.
// knowhere only views the buffer during Build/Add, so the returned vector must
// outlive that call.
inline std::vector<uint8_t>
PackValidityBitmap(ValidityView validity, int64_t total_count) {
    AssertInfo(total_count >= 0,
               "nullable vector row count {} is negative",
               total_count);
    const auto count = static_cast<size_t>(total_count);
    ValidatePersistedValidDataCount(count);
    std::vector<uint8_t> bitmap(GetValidDataBitmapSize(count), 0);
    if (count == 0) {
        return bitmap;
    }
    AssertInfo(static_cast<bool>(validity),
               "nullable vector validity view is empty for {} rows",
               total_count);
    for (int64_t row = 0; row < total_count; ++row) {
        if (validity[row]) {
            bitmap[static_cast<size_t>(row) >> 3] |=
                static_cast<uint8_t>(1U << (static_cast<size_t>(row) & 7U));
        }
    }
    return bitmap;
}

inline std::vector<uint8_t>
PackValidityBitmap(const bool* valid_data, int64_t total_count) {
    return PackValidityBitmap(total_count == 0
                                  ? ValidityView{}
                                  : ValidityView::FromExpanded(valid_data),
                              total_count);
}

inline knowhere::IdMapData
MakeIdMapData(const ValidDataView& valid_data) {
    AssertInfo(valid_data.found, "nullable vector valid_data is empty");
    ValidatePersistedValidDataCount(valid_data.count);
    if (valid_data.count > 0 && valid_data.bitmap == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector valid_data bitmap is null for {} rows",
                  valid_data.count);
    }
    return knowhere::IdMapData::FromValidBitmap(valid_data.bitmap,
                                                valid_data.count);
}

// --- knowhere IdMap mmap configuration -----------------------------------

// Derived dense id arrays may be file-backed. The name is kept from the
// pre-#50524 layout so an already deployed local staging directory keeps
// working.
inline constexpr const char* ID_MAP_MMAP_DIR = "id_mapping_mmap";

struct IdMapMmapFlags {
    // i2o: compact vector id -> public row id.
    bool enable_i2o{false};
    // o2i: public row id -> compact vector id.
    bool enable_o2i{false};

    bool
    Any() const {
        return enable_i2o || enable_o2i;
    }
};

inline bool
ReadIdMapMmapFlag(const Config& config, const char* key) {
    if (!config.contains(key) || config.at(key).is_null()) {
        return false;
    }

    const auto& encoded = config.at(key);
    if (encoded.is_boolean()) {
        return encoded.get<bool>();
    }
    if (encoded.is_string()) {
        const auto& value = encoded.get_ref<const std::string&>();
        const bool is_true = value.size() == 4 &&
                             (value[0] == 't' || value[0] == 'T') &&
                             (value[1] == 'r' || value[1] == 'R') &&
                             (value[2] == 'u' || value[2] == 'U') &&
                             (value[3] == 'e' || value[3] == 'E');
        const bool is_false = value.size() == 5 &&
                              (value[0] == 'f' || value[0] == 'F') &&
                              (value[1] == 'a' || value[1] == 'A') &&
                              (value[2] == 'l' || value[2] == 'L') &&
                              (value[3] == 's' || value[3] == 'S') &&
                              (value[4] == 'e' || value[4] == 'E');
        if (is_true || is_false) {
            return is_true;
        }
    }
    ThrowInfo(
        DataTypeInvalid, "nullable vector parameter {} must be boolean", key);
}

inline IdMapMmapFlags
GetIdMapMmapFlags(const Config& config) {
    IdMapMmapFlags flags;
    flags.enable_i2o = ReadIdMapMmapFlag(config, ENABLE_MMAP_I2O_MAP);
    flags.enable_o2i = ReadIdMapMmapFlag(config, ENABLE_MMAP_O2I_MAP);
    return flags;
}

inline std::string
GetIdMapMmapDir(const std::string& local_index_path_prefix) {
    return (std::filesystem::path(local_index_path_prefix) / ID_MAP_MMAP_DIR)
        .string();
}

// Mmap applies only to the derived dense id arrays. The validity bitmap stays
// heap-backed and is consumed once by AddFromData. knowhere removes each
// backing file together with its mapping, so only the (empty) directory is
// left behind.
inline void
ConfigureIdMapMmap(knowhere::IdMap& id_map,
                   const IdMapMmapFlags& flags,
                   const std::string& local_index_path_prefix) {
    if (!flags.Any()) {
        return;
    }
    AssertInfo(!local_index_path_prefix.empty(),
               "nullable vector id map mmap requires a staging parent");
    if (!id_map.IsEnabled()) {
        id_map.SetType(knowhere::IdMap::Type::SEALED);
    }
    AssertInfo(id_map.type() == knowhere::IdMap::Type::SEALED,
               "nullable vector id map mmap requires sealed storage");

    knowhere::IdMapMmapOptions options;
    options.enable_in_to_out_ids = flags.enable_i2o;
    options.enable_out_to_in_ids = flags.enable_o2i;
    options.mmap_dir_path = GetIdMapMmapDir(local_index_path_prefix);
    id_map.ConfigureMmap(std::move(options));
}

inline void
ConfigureIdMapMmap(knowhere::IdMap& id_map,
                   const Config& config,
                   const std::string& local_index_path_prefix) {
    ConfigureIdMapMmap(
        id_map, GetIdMapMmapFlags(config), local_index_path_prefix);
}

// --- restore / persist ----------------------------------------------------

struct RestoredIdMap {
    bool has_valid_data{false};
    bool all_null_nullable{false};

    bool
    IsAllNullNullable() const {
        return has_valid_data && all_null_nullable;
    }
};

// knowhere derives both mapping directions inside Build / Add / Deserialize.
// A metadata-only artifact never reaches any of those, so its restore has to
// finalize the map explicitly.
inline void
FinalizeRestoredIdMap(knowhere::IndexNode* index_node,
                      const std::string& context) {
    AssertInfo(index_node != nullptr, "index node is null");
    auto status = index_node->FinalizeIdMap();
    if (status != knowhere::Status::success) {
        // Route the knowhere status through the shared mapper rather than
        // taking a fixed code from the caller: every caller passed
        // UnexpectedError, which discarded the retriability verdict knowhere
        // had already made (e.g. malloc_error -> retriable MemAllocateFailed)
        // and left the failure in the "unclassified internal bug" bucket.
        ThrowInfo(KnowhereStatusToErrorCode(status),
                  "failed to finalize the nullable vector id map for {}: "
                  "status {} ({})",
                  context,
                  static_cast<int>(status),
                  knowhere::Status2String(status));
    }
}

// Publish the borrowed validity bitmap into a sealed id map. Must run before
// the engine is deserialized, because deserialization is what derives the
// dense id arrays from this bitmap.
inline RestoredIdMap
RestoreIdMapFromValidData(knowhere::IdMap& id_map,
                          const ValidDataView& valid_data,
                          const IdMapMmapFlags& mmap_flags = {},
                          const std::string& mmap_path_prefix = {}) {
    if (!valid_data.found) {
        return {};
    }
    if (!id_map.IsEnabled()) {
        id_map.SetType(knowhere::IdMap::Type::SEALED);
    }
    AssertInfo(id_map.type() == knowhere::IdMap::Type::SEALED,
               "a nullable sealed vector index requires sealed id map "
               "storage");
    ConfigureIdMapMmap(id_map, mmap_flags, mmap_path_prefix);
    id_map.AddFromData(MakeIdMapData(valid_data));
    return {true, IsAllNullNullable(id_map)};
}

inline ValidDataView
LoadValidDataViewFromPayload(const uint8_t* count_data,
                             int64_t count_size,
                             const uint8_t* bitmap_data,
                             int64_t bitmap_size) {
    if (count_data == nullptr ||
        count_size != static_cast<int64_t>(sizeof(uint64_t))) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector index valid_data count file is invalid");
    }
    uint64_t wire_count = 0;
    milvus::fastmem::FastMemcpy(&wire_count, count_data, sizeof(uint64_t));
    const auto count = FromValidDataCount(wire_count);

    const auto required_bytes =
        static_cast<int64_t>(GetValidDataBitmapSize(count));
    if (bitmap_size < required_bytes ||
        (required_bytes > 0 && bitmap_data == nullptr)) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector index valid_data bitmap file is invalid");
    }
    return {true, count, bitmap_data};
}

inline RestoredIdMap
RestoreIdMapFromValidDataPayload(knowhere::IdMap& id_map,
                                 const uint8_t* count_data,
                                 int64_t count_size,
                                 const uint8_t* bitmap_data,
                                 int64_t bitmap_size,
                                 const IdMapMmapFlags& mmap_flags = {},
                                 const std::string& mmap_path_prefix = {}) {
    return RestoreIdMapFromValidData(
        id_map,
        LoadValidDataViewFromPayload(
            count_data, count_size, bitmap_data, bitmap_size),
        mmap_flags,
        mmap_path_prefix);
}

inline RestoredIdMap
RestoreIdMapFromBinarySet(const BinarySet& binary_set,
                          knowhere::IdMap& id_map,
                          const IdMapMmapFlags& mmap_flags = {},
                          const std::string& mmap_path_prefix = {}) {
    const bool has_count = binary_set.Contains(VALID_DATA_COUNT_KEY);
    const bool has_data = binary_set.Contains(VALID_DATA_KEY);
    if (!has_count && !has_data) {
        return {};
    }
    if (!has_count || !has_data) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector index valid_data files are incomplete");
    }

    auto count_ptr = binary_set.GetByName(VALID_DATA_COUNT_KEY);
    auto data_ptr = binary_set.GetByName(VALID_DATA_KEY);
    if (count_ptr == nullptr || data_ptr == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector index valid_data files are incomplete");
    }
    return RestoreIdMapFromValidDataPayload(id_map,
                                            count_ptr->data.get(),
                                            count_ptr->size,
                                            data_ptr->data.get(),
                                            data_ptr->size,
                                            mmap_flags,
                                            mmap_path_prefix);
}

inline std::vector<uint8_t>
PackValidDataBitmap(const knowhere::IdMap& id_map) {
    const auto valid_bitmap = id_map.ValidBitmap();
    if (valid_bitmap.empty()) {
        return {};
    }
    const auto count = valid_bitmap.size();
    (void)ToValidDataCount(count);
    const auto bytes = GetValidDataBitmapSize(count);
    std::vector<uint8_t> data(bytes, 0);
    // A growing id map holds its bitmap in append-only chunks, so read it
    // byte-wise instead of assuming one contiguous buffer.
    if (const auto* contiguous = valid_bitmap.data(); contiguous != nullptr) {
        milvus::fastmem::FastMemcpy(data.data(), contiguous, bytes);
    } else {
        for (size_t i = 0; i < bytes; ++i) {
            data[i] = valid_bitmap[i];
        }
    }
    return data;
}

inline void
AppendValidDataToBinarySet(const knowhere::IdMap& id_map,
                           BinarySet& binary_set) {
    const auto valid_bitmap = id_map.ValidBitmap();
    if (valid_bitmap.empty()) {
        return;
    }

    const auto count = valid_bitmap.size();
    auto wire_count = ToValidDataCount(count);
    std::shared_ptr<uint8_t[]> count_buf(new uint8_t[sizeof(uint64_t)]);
    milvus::fastmem::FastMemcpy(count_buf.get(), &wire_count, sizeof(uint64_t));
    binary_set.Append(VALID_DATA_COUNT_KEY, count_buf, sizeof(uint64_t));

    auto packed_data = PackValidDataBitmap(id_map);
    std::shared_ptr<uint8_t[]> data(new uint8_t[packed_data.size()]);
    if (!packed_data.empty()) {
        milvus::fastmem::FastMemcpy(
            data.get(), packed_data.data(), packed_data.size());
    }
    binary_set.Append(VALID_DATA_KEY, data, packed_data.size());
}

}  // namespace milvus::index
