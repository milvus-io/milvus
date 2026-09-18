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

#include "index/scalar/sort/SortedIndexLoader.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <optional>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include "index/ParamUtils.h"
#include "common/EasyAssert.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "storage/artifact/LocalFileUtils.h"
#include "index/Families.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "index/scalar/sort/SortedIndexFormat.h"
#include "index/scalar/sort/SortedIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

struct RuntimeParams {
    DataType field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
    bool nested{false};
    bool value_lookup{true};
    bool one_value_per_coordinate{true};
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    RuntimeParams result;
    result.field_type =
        ReadDataTypeParam(params, "field_type").value_or(DataType::NONE);
    const auto element_type =
        ReadCompatibleScalarArrayElementType(params, "sorted");
    const auto configured =
        ReadDataTypeParam(params, "value_type").value_or(DataType::NONE);
    if (result.field_type == DataType::NONE && element_type != DataType::NONE) {
        result.field_type = DataType::ARRAY;
    }
    result.nested = ReadRequiredNestedParam(params, "sorted loader");

    if (result.field_type == DataType::JSON) {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested sorted input requires ARRAY field_type");
        }
        if (element_type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "sorted JSON field conflicts with element type {}",
                      static_cast<int>(element_type));
        }
        if (configured == DataType::NONE || configured == DataType::ARRAY ||
            configured == DataType::JSON) {
            ThrowInfo(DataTypeInvalid,
                      "sorted JSON input requires a concrete cast "
                      "value_type");
        }
        result.value_type = configured;
    } else if (result.field_type == DataType::ARRAY) {
        if (configured != DataType::NONE && configured != DataType::ARRAY &&
            element_type != DataType::NONE &&
            !ScalarValueTypesMatch(configured, element_type)) {
            ThrowInfo(DataTypeInvalid,
                      "sorted ARRAY value_type {} conflicts with element type "
                      "{}",
                      static_cast<int>(configured),
                      static_cast<int>(element_type));
        }
        result.value_type =
            element_type != DataType::NONE
                ? element_type
                : (configured != DataType::ARRAY ? configured : DataType::NONE);
    } else {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested sorted input requires ARRAY field_type");
        }
        if (element_type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "sorted scalar field_type {} conflicts with element "
                      "type {}",
                      static_cast<int>(result.field_type),
                      static_cast<int>(element_type));
        }
        if (configured == DataType::ARRAY) {
            ThrowInfo(DataTypeInvalid,
                      "sorted scalar input cannot use ARRAY value_type");
        }
        if (configured != DataType::NONE &&
            result.field_type != DataType::NONE &&
            !ScalarValueTypesMatch(configured, result.field_type)) {
            ThrowInfo(DataTypeInvalid,
                      "sorted value_type {} conflicts with field_type {}",
                      static_cast<int>(configured),
                      static_cast<int>(result.field_type));
        }
        result.value_type =
            configured != DataType::NONE ? configured : result.field_type;
    }
    if (result.value_type == DataType::NONE ||
        result.value_type == DataType::ARRAY) {
        ThrowInfo(DataTypeInvalid,
                  "sorted loader requires a concrete value type");
    }
    result.one_value_per_coordinate =
        result.field_type != DataType::ARRAY || result.nested;
    result.value_lookup = result.one_value_per_coordinate;
    return result;
}

template <typename T>
T
ReadRequiredPod(storage::FileSource& source, std::string_view name) {
    if (!source.HasEntry(name)) {
        ThrowInfo(
            DataFormatBroken, "sorted artifact entry {} is missing", name);
    }
    const auto bytes = source.ReadEntry(name);
    if (bytes.size() != sizeof(T)) {
        ThrowInfo(DataFormatBroken,
                  "sorted artifact entry {} has size {}, expected {}",
                  name,
                  bytes.size(),
                  sizeof(T));
    }
    T result;
    std::memcpy(&result, bytes.data(), sizeof(result));
    return result;
}

template <typename T>
std::optional<T>
ReadOptionalPod(storage::FileSource& source, std::string_view name) {
    if (!source.HasEntry(name)) {
        return std::nullopt;
    }
    return ReadRequiredPod<T>(source, name);
}

template <typename T>
T
GetRequiredMeta(storage::FileSource& source, std::string_view name) {
    const auto value = source.GetMeta(name);
    if (!value.has_value()) {
        ThrowInfo(
            DataFormatBroken, "sorted artifact metadata {} is missing", name);
    }
    try {
        return value->get<T>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid sorted metadata {}: {}",
                  name,
                  error.what());
    }
}

template <typename T>
std::optional<T>
GetOptionalMeta(storage::FileSource& source, std::string_view name) {
    const auto value = source.GetMeta(name);
    if (!value.has_value()) {
        return std::nullopt;
    }
    try {
        return value->get<T>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid sorted metadata {}: {}",
                  name,
                  error.what());
    }
}

struct CommonMeta {
    size_t count{0};
    bool nested{false};
    bool has_nested{false};
};

struct NumericMeta : CommonMeta {
    size_t index_length{0};
};

NumericMeta
ReadNumericMeta(storage::FileSource& source) {
    NumericMeta result;
    if (source.Gen() == storage::Generation::V1V2) {
        result.index_length =
            ReadRequiredPod<size_t>(source, sort_format::kIndexLength);
        result.count =
            ReadOptionalPod<size_t>(source, sort_format::kLegacyNumRows)
                .value_or(result.index_length);
        if (const auto nested =
                ReadOptionalPod<bool>(source, sort_format::kLegacyNested);
            nested.has_value()) {
            result.nested = *nested;
            result.has_nested = true;
        }
        return result;
    }
    result.index_length =
        GetRequiredMeta<size_t>(source, sort_format::kIndexLength);
    result.count = GetRequiredMeta<size_t>(source, sort_format::kNumRows);
    if (const auto nested = GetOptionalMeta<bool>(source, sort_format::kNested);
        nested.has_value()) {
        result.nested = *nested;
        result.has_nested = true;
    }
    return result;
}

CommonMeta
ReadStringMeta(storage::FileSource& source) {
    CommonMeta result;
    uint32_t version = 0;
    if (source.Gen() == storage::Generation::V1V2) {
        version = ReadRequiredPod<uint32_t>(source, sort_format::kVersion);
        result.count =
            ReadRequiredPod<size_t>(source, sort_format::kLegacyNumRows);
        if (const auto nested =
                ReadOptionalPod<bool>(source, sort_format::kLegacyNested);
            nested.has_value()) {
            result.nested = *nested;
            result.has_nested = true;
        }
    } else {
        version = GetRequiredMeta<uint32_t>(source, sort_format::kVersion);
        result.count = GetRequiredMeta<size_t>(source, sort_format::kNumRows);
        if (const auto nested =
                GetOptionalMeta<bool>(source, sort_format::kNested);
            nested.has_value()) {
            result.nested = *nested;
            result.has_nested = true;
        }
    }
    if (version != sort_format::kStringVersion) {
        ThrowInfo(Unsupported,
                  "unsupported sorted string version {}, expected {}",
                  version,
                  sort_format::kStringVersion);
    }
    return result;
}

void
ResolveNested(CommonMeta& meta, bool runtime_nested) {
    if (meta.has_nested && meta.nested != runtime_nested) {
        ThrowInfo(DataFormatBroken,
                  "sorted persisted nested value {} disagrees with runtime "
                  "value {}",
                  meta.nested,
                  runtime_nested);
    }
    meta.nested = runtime_nested;
}

void
CheckCount(size_t count) {
    if (count > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "sorted coordinate count {} exceeds int32 domain",
                  count);
    }
}

size_t
CheckedMultiply(size_t left, size_t right, std::string_view label) {
    if (right != 0 && left > std::numeric_limits<size_t>::max() / right) {
        ThrowInfo(DataFormatBroken, "sorted {} byte size overflows", label);
    }
    return left * right;
}

storage::LocalEntryGuard
CreateLocalFile(const std::string& configured_dir, std::string_view prefix) {
    const auto directory = configured_dir.empty()
                               ? std::filesystem::temp_directory_path()
                               : std::filesystem::path(configured_dir);
    std::error_code error;
    std::filesystem::create_directories(directory, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create sorted staging directory {}: {}",
                  directory.string(),
                  error.message());
    }
    storage::LocalEntryGuard file(
        (directory / (std::string(prefix) + "_XXXXXX")).string());
    const auto fd = mkstemp(file.MutablePath());
    if (fd == -1) {
        const auto error = errno;
        static_cast<void>(file.Release());
        ThrowInfo(FileCreateFailed,
                  "failed to create sorted staging file in {}: {}",
                  directory.string(),
                  std::strerror(error));
    }
    storage::FileDescriptorGuard descriptor(fd);
    return file;
}

template <typename T>
std::shared_ptr<std::vector<T>>
ReadEntryVector(storage::FileSource& source,
                std::string_view name,
                size_t expected_bytes,
                const std::string& staging_dir) {
    if (!source.HasEntry(name)) {
        ThrowInfo(
            DataFormatBroken, "sorted artifact entry {} is missing", name);
    }
    if (expected_bytes % sizeof(T) != 0) {
        ThrowInfo(DataFormatBroken,
                  "sorted entry {} size is not element aligned",
                  name);
    }
    auto file = CreateLocalFile(staging_dir, "sorted_heap");
    source.ReadEntryToLocalFile(name, file.Path());
    const auto actual = storage::LocalFileSize(
        file.Path(), "failed to size sorted staging file");
    if (actual != expected_bytes) {
        ThrowInfo(DataFormatBroken,
                  "sorted entry {} has size {}, expected {}",
                  name,
                  actual,
                  expected_bytes);
    }
    auto result = std::make_shared<std::vector<T>>(expected_bytes / sizeof(T));
    if (expected_bytes == 0) {
        return result;
    }
    const auto fd = open(file.Path().c_str(), O_RDONLY);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open sorted staging file {}: {}",
                  file.Path(),
                  std::strerror(errno));
    }
    storage::FileDescriptorGuard descriptor(fd);
    storage::ReadAll(descriptor.Get(),
                     result->data(),
                     expected_bytes,
                     file.Path(),
                     "sorted staging file");
    return result;
}

std::shared_ptr<SortedMmapOwner>
MapEntry(storage::FileSource& source,
         std::string_view name,
         const std::string& staging_dir,
         std::optional<size_t> expected_bytes = std::nullopt) {
    if (!source.HasEntry(name)) {
        ThrowInfo(
            DataFormatBroken, "sorted artifact entry {} is missing", name);
    }
    auto file = CreateLocalFile(staging_dir, "sorted_mmap");
    source.ReadEntryToLocalFile(name, file.Path());
    const auto size = storage::LocalFileSize(
        file.Path(), "failed to size sorted staging file");
    if (expected_bytes.has_value() && size != *expected_bytes) {
        ThrowInfo(DataFormatBroken,
                  "sorted entry {} has size {}, expected {}",
                  name,
                  size,
                  *expected_bytes);
    }
    if (size == 0) {
        return nullptr;
    }
    storage::MappedRegionGuard mapping;
    {
        const auto fd = open(file.Path().c_str(), O_RDONLY);
        if (fd == -1) {
            ThrowInfo(FileOpenFailed,
                      "failed to open sorted mmap file {}: {}",
                      file.Path(),
                      std::strerror(errno));
        }
        storage::FileDescriptorGuard descriptor(fd);
        auto* mapped = static_cast<char*>(
            mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
        const auto saved_errno = errno;
        if (mapped == MAP_FAILED) {
            ThrowInfo(MmapError,
                      "failed to mmap sorted file {}: {}",
                      file.Path(),
                      std::strerror(saved_errno));
        }
        mapping = storage::MappedRegionGuard(mapped, size);
    }
    auto owner = std::make_shared<SortedMmapOwner>(
        mapping.Data(), size, size, file.Path());
    mapping.Release();
    static_cast<void>(file.Release());
    return owner;
}

TargetBitmap
DecodePackedValidity(const std::vector<uint8_t>& bytes, size_t count) {
    const auto expected = (count + 7) / 8;
    if (bytes.size() != expected) {
        ThrowInfo(DataFormatBroken,
                  "sorted packed validity has size {}, expected {}",
                  bytes.size(),
                  expected);
    }
    TargetBitmap result(count, false);
    for (size_t i = 0; i < count; ++i) {
        if ((bytes[i / 8] & static_cast<uint8_t>(1U << (i % 8))) != 0) {
            result.set(i);
        }
    }
    return result;
}

TargetBitmap
DecodeRawValidity(const std::vector<uint8_t>& bytes, size_t count) {
    TargetBitmap result(count, false);
    const auto expected = result.size_in_bytes();
    if (bytes.size() != expected) {
        ThrowInfo(DataFormatBroken,
                  "sorted raw validity has size {}, expected {}",
                  bytes.size(),
                  expected);
    }
    if (expected != 0) {
        std::memcpy(result.data(), bytes.data(), expected);
    }
    return result;
}

template <typename T>
void
ValidateNumericData(const IndexStructure<T>* data, size_t size, size_t count) {
    for (size_t i = 0; i < size; ++i) {
        if (i != 0 && data[i] < data[i - 1]) {
            ThrowInfo(DataFormatBroken, "sorted numeric data is not ordered");
        }
        if (data[i].idx_ < 0 || static_cast<size_t>(data[i].idx_) >= count) {
            ThrowInfo(DataFormatBroken,
                      "sorted numeric row {} exceeds count {}",
                      data[i].idx_,
                      count);
        }
    }
}

template <typename T>
std::pair<TargetBitmap, std::shared_ptr<std::vector<int32_t>>>
RebuildNumericAux(const IndexStructure<T>* data,
                  size_t size,
                  size_t count,
                  const RuntimeParams& params) {
    TargetBitmap validity(count, false);
    auto offsets = std::make_shared<std::vector<int32_t>>(count, -1);
    for (size_t i = 0; i < size; ++i) {
        const auto row = static_cast<size_t>(data[i].idx_);
        if (params.one_value_per_coordinate && validity[row]) {
            ThrowInfo(DataFormatBroken,
                      "sorted scalar row {} has multiple values",
                      row);
        }
        validity.set(row);
        (*offsets)[row] = static_cast<int32_t>(i);
    }
    if (params.nested && !validity.all()) {
        ThrowInfo(DataFormatBroken,
                  "sorted nested data does not cover every element "
                  "coordinate");
    }
    return {std::move(validity), std::move(offsets)};
}

template <typename T>
void
ValidateNumericAux(const IndexStructure<T>* data,
                   size_t size,
                   const TargetBitmap& validity,
                   const int32_t* offsets,
                   size_t count,
                   const RuntimeParams& params) {
    TargetBitmap seen(count, false);
    for (size_t i = 0; i < size; ++i) {
        const auto row = static_cast<size_t>(data[i].idx_);
        if (!validity[row]) {
            ThrowInfo(DataFormatBroken,
                      "sorted numeric posting references null row {}",
                      row);
        }
        if (params.one_value_per_coordinate && seen[row]) {
            ThrowInfo(DataFormatBroken,
                      "sorted scalar row {} has multiple values",
                      row);
        }
        seen.set(row);
    }
    for (size_t row = 0; row < count; ++row) {
        if (!validity[row]) {
            if (params.nested) {
                ThrowInfo(DataFormatBroken,
                          "sorted nested data does not cover element "
                          "coordinate {}",
                          row);
            }
            // Legacy JSON numeric V3 writers did not define reverse offsets
            // for null rows, so those unused slots can contain stale values.
            continue;
        }
        const auto offset = offsets[row];
        if (offset < -1 ||
            (offset >= 0 && static_cast<size_t>(offset) >= size)) {
            ThrowInfo(DataFormatBroken,
                      "invalid sorted numeric offset {} for row {}",
                      offset,
                      row);
        }
        if (offset >= 0 && data[static_cast<size_t>(offset)].idx_ !=
                               static_cast<int32_t>(row)) {
            ThrowInfo(DataFormatBroken,
                      "sorted numeric offset {} does not map row {}",
                      offset,
                      row);
        }
        if (params.value_lookup && validity[row] && offset < 0) {
            ThrowInfo(DataFormatBroken,
                      "sorted scalar valid row {} has no reverse offset",
                      row);
        }
        if (params.nested && (!validity[row] || !seen[row])) {
            ThrowInfo(DataFormatBroken,
                      "sorted nested data does not cover element coordinate "
                      "{}",
                      row);
        }
    }
}

template <typename T>
typename SortedIndexReader<T>::OpenArgs
LoadNumericState(storage::FileSource& source,
                 const storage::LoadOptions& opts,
                 NumericMeta meta,
                 const RuntimeParams& params) {
    static_assert(std::is_trivially_copyable_v<IndexStructure<T>>);
    ResolveNested(meta, params.nested);
    CheckCount(meta.count);
    if (meta.index_length >
        static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "sorted numeric entry count {} exceeds int32 domain",
                  meta.index_length);
    }
    const auto data_bytes = CheckedMultiply(
        meta.index_length, sizeof(IndexStructure<T>), "numeric data");

    typename SortedIndexReader<T>::OpenArgs args;
    args.storage.size = meta.index_length;
    if (opts.enable_mmap && data_bytes != 0) {
        auto owner = MapEntry(
            source, sort_format::kIndexData, opts.mmap_dir_path, data_bytes);
        args.storage.data =
            reinterpret_cast<const IndexStructure<T>*>(owner->Data());
        args.storage.data_owner = owner;
        args.storage.data_file_bytes = owner->MappedSize();
    } else {
        auto owner = ReadEntryVector<IndexStructure<T>>(
            source, sort_format::kIndexData, data_bytes, opts.mmap_dir_path);
        args.storage.data = owner->data();
        args.storage.data_owner = owner;
        args.storage.data_heap_bytes =
            owner->capacity() * sizeof(IndexStructure<T>);
    }
    ValidateNumericData(args.storage.data, args.storage.size, meta.count);

    const bool has_offsets = source.Gen() == storage::Generation::V3 &&
                             source.HasEntry(sort_format::kIdxToOffsets);
    const bool has_validity = source.Gen() == storage::Generation::V3 &&
                              source.HasEntry(sort_format::kValidBitset);
    if (has_offsets != has_validity) {
        ThrowInfo(DataFormatBroken,
                  "sorted numeric V3 auxiliary entries are incomplete");
    }

    if (has_offsets) {
        const auto validity = source.ReadEntry(sort_format::kValidBitset);
        args.state.valid_bitset = std::make_shared<const TargetBitmap>(
            DecodeRawValidity(validity, meta.count));
        const auto offset_bytes =
            CheckedMultiply(meta.count, sizeof(int32_t), "reverse offsets");
        if (opts.enable_mmap && offset_bytes != 0) {
            auto owner = MapEntry(source,
                                  sort_format::kIdxToOffsets,
                                  opts.mmap_dir_path,
                                  offset_bytes);
            args.state.idx_to_offsets =
                reinterpret_cast<const int32_t*>(owner->Data());
            args.state.idx_to_offsets_owner = owner;
            args.state.idx_to_offsets_file_bytes = owner->MappedSize();
        } else {
            auto owner = ReadEntryVector<int32_t>(source,
                                                  sort_format::kIdxToOffsets,
                                                  offset_bytes,
                                                  opts.mmap_dir_path);
            args.state.idx_to_offsets = owner->data();
            args.state.idx_to_offsets_owner = owner;
            args.state.idx_to_offsets_heap_bytes =
                owner->capacity() * sizeof(int32_t);
        }
        args.state.idx_to_offsets_size = meta.count;
        ValidateNumericAux(args.storage.data,
                           args.storage.size,
                           *args.state.valid_bitset,
                           args.state.idx_to_offsets,
                           meta.count,
                           params);
    } else {
        auto rebuilt = RebuildNumericAux(
            args.storage.data, args.storage.size, meta.count, params);
        args.state.valid_bitset =
            std::make_shared<const TargetBitmap>(std::move(rebuilt.first));
        args.state.idx_to_offsets = rebuilt.second->data();
        args.state.idx_to_offsets_size = rebuilt.second->size();
        args.state.idx_to_offsets_heap_bytes =
            rebuilt.second->capacity() * sizeof(int32_t);
        args.state.idx_to_offsets_owner = std::move(rebuilt.second);
    }

    args.state.total_num_rows = meta.count;
    args.state.value_type = params.value_type;
    args.state.nested = meta.nested;
    args.state.value_lookup = params.value_lookup;
    return args;
}

void
ValidateStringAux(const SortedStringLayout& layout,
                  const TargetBitmap& validity,
                  const int32_t* offsets,
                  size_t count,
                  const RuntimeParams& params) {
    TargetBitmap seen(count, false);
    for (size_t value = 0; value < layout.UniqueCount(); ++value) {
        const auto posting = layout.Posting(value);
        for (size_t i = 0; i < posting.size; ++i) {
            const auto row = posting.At(i);
            if (!validity[row]) {
                ThrowInfo(DataFormatBroken,
                          "sorted string posting references null row {}",
                          row);
            }
            if (params.one_value_per_coordinate && seen[row]) {
                ThrowInfo(DataFormatBroken,
                          "sorted string row {} has multiple values",
                          row);
            }
            seen.set(row);
            if (params.one_value_per_coordinate &&
                offsets[row] != static_cast<int32_t>(value)) {
                ThrowInfo(DataFormatBroken,
                          "sorted string reverse offset disagrees for row {}",
                          row);
            }
        }
    }
    for (size_t row = 0; row < count; ++row) {
        const auto value = offsets[row];
        if (value < -1 || (value >= 0 && static_cast<size_t>(value) >=
                                             layout.UniqueCount())) {
            ThrowInfo(DataFormatBroken,
                      "invalid sorted string offset {} for row {}",
                      value,
                      row);
        }
        if (params.value_lookup && validity[row] && value < 0) {
            ThrowInfo(DataFormatBroken,
                      "sorted string valid row {} has no reverse offset",
                      row);
        }
        if (params.nested && (!validity[row] || !seen[row])) {
            ThrowInfo(DataFormatBroken,
                      "sorted nested string data does not cover element "
                      "coordinate {}",
                      row);
        }
    }
}

SortedIndexReader<std::string_view>::OpenArgs
LoadStringState(storage::FileSource& source,
                const storage::LoadOptions& opts,
                CommonMeta meta,
                const RuntimeParams& params) {
    ResolveNested(meta, params.nested);
    CheckCount(meta.count);
    if (!source.HasEntry(sort_format::kIndexData) ||
        !source.HasEntry(sort_format::kValidBitset)) {
        ThrowInfo(DataFormatBroken,
                  "sorted string artifact requires index_data and "
                  "valid_bitset");
    }

    SortedIndexReader<std::string_view>::OpenArgs args;
    if (opts.enable_mmap) {
        auto owner =
            MapEntry(source, sort_format::kIndexData, opts.mmap_dir_path);
        if (owner == nullptr) {
            ThrowInfo(DataFormatBroken, "sorted string index_data is empty");
        }
        args.storage.layout = SortedStringLayout::FromPackedMmap(
            std::move(owner), meta.count);
    } else {
        auto packed = source.ReadEntry(sort_format::kIndexData);
        args.storage.layout = SortedStringLayout::FromPackedHeap(
            std::move(packed), meta.count);
    }
    const auto validity = source.ReadEntry(sort_format::kValidBitset);
    args.state.valid_bitset = std::make_shared<const TargetBitmap>(
        DecodePackedValidity(validity, meta.count));

    const bool has_offsets = source.Gen() == storage::Generation::V3 &&
                             source.HasEntry(sort_format::kIdxToOffsets);
    if (has_offsets) {
        const auto bytes =
            CheckedMultiply(meta.count, sizeof(int32_t), "reverse offsets");
        if (opts.enable_mmap && bytes != 0) {
            auto owner = MapEntry(
                source, sort_format::kIdxToOffsets, opts.mmap_dir_path, bytes);
            args.state.idx_to_offsets =
                reinterpret_cast<const int32_t*>(owner->Data());
            args.state.idx_to_offsets_owner = owner;
            args.state.idx_to_offsets_file_bytes = owner->MappedSize();
        } else {
            auto owner = ReadEntryVector<int32_t>(
                source, sort_format::kIdxToOffsets, bytes, opts.mmap_dir_path);
            args.state.idx_to_offsets = owner->data();
            args.state.idx_to_offsets_owner = owner;
            args.state.idx_to_offsets_heap_bytes =
                owner->capacity() * sizeof(int32_t);
        }
        args.state.idx_to_offsets_size = meta.count;
        ValidateStringAux(*args.storage.layout,
                          *args.state.valid_bitset,
                          args.state.idx_to_offsets,
                          meta.count,
                          params);
    } else {
        auto offsets = std::make_shared<std::vector<int32_t>>(
            args.storage.layout->BuildOffsets(meta.count));
        args.state.idx_to_offsets = offsets->data();
        args.state.idx_to_offsets_size = offsets->size();
        args.state.idx_to_offsets_heap_bytes =
            offsets->capacity() * sizeof(int32_t);
        args.state.idx_to_offsets_owner = std::move(offsets);
        ValidateStringAux(*args.storage.layout,
                          *args.state.valid_bitset,
                          args.state.idx_to_offsets,
                          meta.count,
                          params);
    }

    args.state.total_num_rows = meta.count;
    args.state.value_type = params.value_type;
    args.state.nested = meta.nested;
    args.state.value_lookup = params.value_lookup;
    return args;
}

}  // namespace

ReaderCaps
SortedIndexLoader::DeriveCaps(const Config& index_meta) {
    const auto params = ParseRuntimeParams(index_meta);
    if (params.value_type == DataType::NONE ||
        params.value_type == DataType::ARRAY) {
        ThrowInfo(DataTypeInvalid,
                  "sorted loader requires value_type or array_element_type");
    }
    return DeriveJsonProjectedCaps(
        families::kSort,
        index_meta,
        ReaderCaps{.predicate = true,
                   .pattern_match = IsStringDataType(params.value_type),
                   .nested = params.nested,
                   .value_lookup = params.value_lookup,
                   .cheap_value_lookup = params.value_lookup,
                   .exact = !params.nested});
}

IIndexReaderBasePtr
SortedIndexLoader::Open(storage::FileSource& source,
                        const storage::LoadOptions& opts) {
    auto projection = PrepareJsonProjectedOpen(families::kSort, source, opts);
    const auto params = ParseRuntimeParams(opts.params);
    if (params.value_type == DataType::NONE ||
        params.value_type == DataType::ARRAY) {
        ThrowInfo(DataTypeInvalid,
                  "sorted loader requires value_type or array_element_type");
    }
    if (IsStringDataType(params.value_type)) {
        return FinishJsonProjectedOpen(
            std::move(projection),
            source,
            std::make_unique<SortedIndexReader<std::string_view>>(
                LoadStringState(source, opts, ReadStringMeta(source), params)));
    }

    const auto meta = ReadNumericMeta(source);
    std::unique_ptr<IIndexReaderBase> inner;
    switch (params.value_type) {
        case DataType::BOOL:
            inner = std::make_unique<SortedIndexReader<bool>>(
                LoadNumericState<bool>(source, opts, meta, params));
            break;
        case DataType::INT8:
            inner = std::make_unique<SortedIndexReader<int8_t>>(
                LoadNumericState<int8_t>(source, opts, meta, params));
            break;
        case DataType::INT16:
            inner = std::make_unique<SortedIndexReader<int16_t>>(
                LoadNumericState<int16_t>(source, opts, meta, params));
            break;
        case DataType::INT32:
            inner = std::make_unique<SortedIndexReader<int32_t>>(
                LoadNumericState<int32_t>(source, opts, meta, params));
            break;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            inner = std::make_unique<SortedIndexReader<int64_t>>(
                LoadNumericState<int64_t>(source, opts, meta, params));
            break;
        case DataType::FLOAT:
            inner = std::make_unique<SortedIndexReader<float>>(
                LoadNumericState<float>(source, opts, meta, params));
            break;
        case DataType::DOUBLE:
            inner = std::make_unique<SortedIndexReader<double>>(
                LoadNumericState<double>(source, opts, meta, params));
            break;
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported sorted value type {}",
                      static_cast<int>(params.value_type));
    }
    return FinishJsonProjectedOpen(
        std::move(projection), source, std::move(inner));
}

namespace {

const bool kSortedLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<SortedIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
