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

#include "folly/coro/WithCancellation.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LocalFileIOPool.h"
#include "common/OpContext.h"
#include "index/scalar/sort/SortedIndexLoader.h"
#include "index/IndexLoaderFactory.h"
#include "index/PackedIndexLoad.h"
#include "index/LegacyIndexLoad.h"

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
#include "index/IndexLoadUtils.h"
#include "common/EasyAssert.h"
#include "common/OpContext.h"
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

/**
 * @brief Normalized runtime field semantics used to interpret persisted data.
 */
struct RuntimeParams {
    DataType field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
    bool nested{false};
    bool value_lookup{true};
    bool one_value_per_coordinate{true};
};

// Normalize field/value types and row-versus-element semantics before
// decoding.
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

// Read one native-layout scalar and require an exact sizeof(T) entry.
template <typename T>
folly::coro::Task<T>
ReadRequiredPod(bool use_async,
                storage::FileSource& source,
                std::string_view name) {
    if (!source.HasEntry(name)) {
        ThrowInfo(
            DataFormatBroken, "sorted artifact entry {} is missing", name);
    }
    const auto bytes = co_await source.ReadEntryAsync(name, use_async);
    if (bytes.size() != sizeof(T)) {
        ThrowInfo(DataFormatBroken,
                  "sorted artifact entry {} has size {}, expected {}",
                  name,
                  bytes.size(),
                  sizeof(T));
    }
    T result;
    std::memcpy(&result, bytes.data(), sizeof(result));
    co_return result;
}

// Treat a missing legacy metadata entry as absent; validate present entries
// strictly.
template <typename T>
folly::coro::Task<std::optional<T>>
ReadOptionalPod(bool use_async,
                storage::FileSource& source,
                std::string_view name) {
    if (!source.HasEntry(name)) {
        co_return std::nullopt;
    }
    co_return (co_await ReadRequiredPod<T>(use_async, source, name));
}

/** @brief Persisted coordinate count and optional legacy nesting metadata. */
struct CommonMeta {
    size_t count{0};
    bool nested{false};
    bool has_nested{false};
};

/** @brief Numeric posting count alongside the shared coordinate-domain metadata. */
struct NumericMeta : CommonMeta {
    size_t index_length{0};
};

// Read numeric metadata, falling back to entry count for legacy artifacts
// without row count.
folly::coro::Task<NumericMeta>
ReadNumericMeta(bool use_async, storage::FileSource& source) {
    NumericMeta result;
    result.index_length = (co_await ReadRequiredPod<size_t>(
        use_async, source, sort_format::kIndexLength));
    result.count = (co_await ReadOptionalPod<size_t>(
                        use_async, source, sort_format::kLegacyNumRows))
                       .value_or(result.index_length);
    if (auto nested = (co_await ReadOptionalPod<bool>(
            use_async, source, sort_format::kLegacyNested))) {
        result.nested = *nested;
        result.has_nested = true;
    }
    co_return result;
}

// Read string metadata and reject unsupported persisted layout versions.
folly::coro::Task<CommonMeta>
ReadStringMeta(bool use_async, storage::FileSource& source) {
    CommonMeta result;
    auto version = (co_await ReadRequiredPod<uint32_t>(
        use_async, source, sort_format::kVersion));
    result.count = (co_await ReadRequiredPod<size_t>(
        use_async, source, sort_format::kLegacyNumRows));
    if (auto nested = (co_await ReadOptionalPod<bool>(
            use_async, source, sort_format::kLegacyNested))) {
        result.nested = *nested;
        result.has_nested = true;
    }
    if (version != sort_format::kStringVersion) {
        ThrowInfo(Unsupported,
                  "unsupported sorted string version {}, expected {}",
                  version,
                  sort_format::kStringVersion);
    }
    co_return result;
}

// Reject conflicting persisted/runtime domains; use runtime metadata when
// legacy data omits it.
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

// Enforce the int32 coordinate domain used by sorted reverse offsets.
void
CheckCount(size_t count) {
    if (count > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "sorted coordinate count {} exceeds int32 domain",
                  count);
    }
}

// Compute persisted payload sizes without wrapping before allocation or
// mapping.
size_t
CheckedMultiply(size_t left, size_t right, std::string_view label) {
    if (right != 0 && left > std::numeric_limits<size_t>::max() / right) {
        ThrowInfo(DataFormatBroken, "sorted {} byte size overflows", label);
    }
    return left * right;
}

// Create a closed staging file whose path guard removes it until ownership
// transfers.
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

// Stage and check the exact entry size before allocating its final typed
// vector.
template <typename T>
folly::coro::Task<std::shared_ptr<std::vector<T>>>
ReadEntryVector(bool use_async,
                const storage::LoadOptions& opts,
                storage::FileSource& source,
                std::string_view name,
                size_t expected_bytes,
                const std::string& staging_dir) {
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    auto run_io = [&]() -> folly::coro::Task<std::shared_ptr<std::vector<T>>> {
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
        co_await source.ReadEntryToLocalFileAsync(name, file.Path(), use_async);
        const auto actual = storage::LocalFileSize(
            file.Path(), "failed to size sorted staging file");
        if (actual != expected_bytes) {
            ThrowInfo(DataFormatBroken,
                      "sorted entry {} has size {}, expected {}",
                      name,
                      actual,
                      expected_bytes);
        }
        auto result =
            std::make_shared<std::vector<T>>(expected_bytes / sizeof(T));
        if (expected_bytes == 0) {
            co_return result;
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
        co_return result;
    };
    if (!use_async)
        co_return co_await run_io();
    co_return co_await folly::coro::co_withExecutor(
        storage::ResolveAsyncLoadExecutor(
            storage::LocalFileIOPool::GetInstance().GetExecutor(), priority),
        run_io());
}

// Stage and map an entry with a retained file owner; an empty entry returns
// null.
folly::coro::Task<std::shared_ptr<SortedMmapOwner>>
MapEntry(bool use_async,
         const storage::LoadOptions& opts,
         storage::FileSource& source,
         std::string_view name,
         const std::string& staging_dir,
         std::optional<size_t> expected_bytes = std::nullopt) {
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    auto run_io = [&]() -> folly::coro::Task<std::shared_ptr<SortedMmapOwner>> {
        if (!source.HasEntry(name)) {
            ThrowInfo(
                DataFormatBroken, "sorted artifact entry {} is missing", name);
        }
        auto file = CreateLocalFile(staging_dir, "sorted_mmap");
        co_await source.ReadEntryToLocalFileAsync(name, file.Path(), use_async);
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
            co_return nullptr;
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
        co_return owner;
    };
    if (!use_async)
        co_return co_await run_io();
    co_return co_await folly::coro::co_withExecutor(
        storage::ResolveAsyncLoadExecutor(
            storage::LocalFileIOPool::GetInstance().GetExecutor(), priority),
        run_io());
}

// Decode byte-packed validity only after checking its length against the row
// count.
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

// Check sort order and coordinate bounds before auxiliary arrays index those
// coordinates.
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

// Rebuild validity and reverse offsets; require unique coordinates where the
// domain demands it.
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

// Cross-check supplied validity and reverse offsets against already
// range-checked postings.
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

// Load heap or mmap numeric data and rebuild the auxiliary state omitted by
// legacy artifacts.
template <typename T>
folly::coro::Task<typename SortedIndexReader<T>::OpenArgs>
LoadNumericState(bool use_async,
                 storage::FileSource& source,
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
        auto owner = (co_await MapEntry(use_async,
                                        opts,
                                        source,
                                        sort_format::kIndexData,
                                        opts.mmap_dir_path,
                                        data_bytes));
        args.storage.data =
            reinterpret_cast<const IndexStructure<T>*>(owner->Data());
        args.storage.data_owner = owner;
        args.storage.data_file_bytes = owner->MappedSize();
    } else {
        auto owner = (co_await ReadEntryVector<IndexStructure<T>>(
            use_async,
            opts,
            source,
            sort_format::kIndexData,
            data_bytes,
            opts.mmap_dir_path));
        args.storage.data = owner->data();
        args.storage.data_owner = owner;
        args.storage.data_heap_bytes =
            owner->capacity() * sizeof(IndexStructure<T>);
    }
    ValidateNumericData(args.storage.data, args.storage.size, meta.count);

    auto rebuilt = RebuildNumericAux(
        args.storage.data, args.storage.size, meta.count, params);
    args.state.valid_bitset =
        std::make_shared<const TargetBitmap>(std::move(rebuilt.first));
    args.state.idx_to_offsets = rebuilt.second->data();
    args.state.idx_to_offsets_size = rebuilt.second->size();
    args.state.idx_to_offsets_heap_bytes =
        rebuilt.second->capacity() * sizeof(int32_t);
    args.state.idx_to_offsets_owner = std::move(rebuilt.second);

    args.state.total_num_rows = meta.count;
    args.state.value_type = params.value_type;
    args.state.nested = meta.nested;
    args.state.value_lookup = params.value_lookup;
    co_return args;
}

// Cross-check validity and reverse lookup against the validated string
// layout.
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

// Load the string layout and validity, then rebuild and validate reverse
// offsets.
folly::coro::Task<SortedIndexReader<std::string_view>::OpenArgs>
LoadStringState(bool use_async,
                storage::FileSource& source,
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
        auto owner = (co_await MapEntry(use_async,
                                        opts,
                                        source,
                                        sort_format::kIndexData,
                                        opts.mmap_dir_path));
        if (owner == nullptr) {
            ThrowInfo(DataFormatBroken, "sorted string index_data is empty");
        }
        args.storage.layout =
            SortedStringLayout::FromPackedMmap(std::move(owner), meta.count);
    } else {
        auto packed =
            co_await source.ReadEntryAsync(sort_format::kIndexData, use_async);
        args.storage.layout =
            SortedStringLayout::FromPackedHeap(std::move(packed), meta.count);
    }
    const auto validity =
        co_await source.ReadEntryAsync(sort_format::kValidBitset, use_async);
    args.state.valid_bitset = std::make_shared<const TargetBitmap>(
        DecodePackedValidity(validity, meta.count));

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

    args.state.total_num_rows = meta.count;
    args.state.value_type = params.value_type;
    args.state.nested = meta.nested;
    args.state.value_lookup = params.value_lookup;
    co_return args;
}

/**
 * @brief Per-load payload owners and decoding state, retained by IndexLoadPlan.
 */
struct PackedSortedState {
    RuntimeParams params;
    NumericMeta meta;
    JsonProjectedOpenPlan projection;
    std::shared_ptr<TargetBitmap> validity;
    std::shared_ptr<void> data_owner;
    void* data{nullptr};
    size_t data_bytes{0};
    size_t data_heap_bytes{0};
    std::shared_ptr<std::vector<uint8_t>> string_data;
    std::shared_ptr<std::vector<int32_t>> offsets;
    std::shared_ptr<storage::IndexFileTarget> data_file;
    std::shared_ptr<storage::IndexFileTarget> offsets_file;
    bool has_offsets{false};
};

// Select the typed numeric decoder; TIMESTAMPTZ shares the int64
// representation.
template <typename F>
decltype(auto)
DispatchPackedNumeric(DataType type, F&& function) {
    switch (type) {
        case DataType::BOOL:
            return function.template operator()<bool>();
        case DataType::INT8:
            return function.template operator()<int8_t>();
        case DataType::INT16:
            return function.template operator()<int16_t>();
        case DataType::INT32:
            return function.template operator()<int32_t>();
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return function.template operator()<int64_t>();
        case DataType::FLOAT:
            return function.template operator()<float>();
        case DataType::DOUBLE:
            return function.template operator()<double>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported sorted value type {}",
                      static_cast<int>(type));
    }
}

// Register a prepared mmap destination, transferring temporary-path cleanup
// to the target.
std::shared_ptr<storage::IndexFileTarget>
PlanSortedFile(IndexLoadPlan& plan,
               std::string_view name,
               size_t bytes,
               const storage::LoadOptions& opts) {
    auto local = CreateLocalFile(opts.mmap_dir_path, "sorted_mmap");
    auto file =
        std::make_shared<storage::IndexFileTarget>(local.Path(), bytes, true);
    file->Prepare(storage::io::GetPriorityFromLoadPriority(
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH));
    static_cast<void>(local.Release());
    plan.entries.push_back(
        {std::string(name), storage::FileEntryTarget{file, 0, bytes}});
    return file;
}

// Map a completed nonempty target; its owner retains the view and path for
// the reader.
std::shared_ptr<SortedMmapOwner>
MapSortedTarget(const storage::IndexFileTarget& file) {
    AssertInfo(file.Prepared() && file.file_size != 0,
               "sorted mmap target is not prepared");
    const auto fd = ::open(file.path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        ThrowInfo(FileOpenFailed,
                  "failed to open sorted target {}: {}",
                  file.path,
                  std::strerror(errno));
    }
    storage::FileDescriptorGuard descriptor(fd);
    auto* data = static_cast<char*>(
        ::mmap(nullptr, file.file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        ThrowInfo(MmapError,
                  "failed to map sorted target {}: {}",
                  file.path,
                  std::strerror(errno));
    }
    storage::MappedRegionGuard guard(data, file.file_size);
    auto owner = std::make_shared<SortedMmapOwner>(
        data, file.file_size, file.file_size, file.path);
    guard.Release();
    return owner;
}

// Attach loaded payload owners and validate or rebuild auxiliary arrays
// before publication.
template <typename T>
IIndexReaderBasePtr
FinishPackedSortedState(PackedSortedState& state,
                        std::shared_ptr<SortedMmapOwner> data_mapping,
                        std::shared_ptr<SortedMmapOwner> offsets_mapping) {
    constexpr bool string = std::is_same_v<T, std::string_view>;
    typename SortedIndexReader<T>::OpenArgs args;
    if constexpr (string) {
        if (state.data_file) {
            args.storage.layout = SortedStringLayout::FromPackedMmap(
                std::move(data_mapping), state.meta.count);
        } else {
            args.storage.layout = SortedStringLayout::FromPackedHeap(
                std::move(*state.string_data), state.meta.count);
        }
    } else {
        args.storage.size = state.meta.index_length;
        if (state.data_file) {
            auto owner = std::move(data_mapping);
            args.storage.data =
                reinterpret_cast<const IndexStructure<T>*>(owner->Data());
            args.storage.data_owner = owner;
            args.storage.data_file_bytes = owner->MappedSize();
        } else {
            args.storage.data =
                static_cast<const IndexStructure<T>*>(state.data);
            args.storage.data_owner = state.data_owner;
            args.storage.data_heap_bytes = state.data_heap_bytes;
        }
        ValidateNumericData(
            args.storage.data, args.storage.size, state.meta.count);
    }
    if (state.has_offsets) {
        if (state.offsets_file) {
            auto owner = std::move(offsets_mapping);
            args.state.idx_to_offsets =
                reinterpret_cast<const int32_t*>(owner->Data());
            args.state.idx_to_offsets_owner = owner;
            args.state.idx_to_offsets_file_bytes = owner->MappedSize();
        } else {
            args.state.idx_to_offsets = state.offsets->data();
            args.state.idx_to_offsets_owner = state.offsets;
            args.state.idx_to_offsets_heap_bytes =
                state.offsets->capacity() * sizeof(int32_t);
        }
        args.state.idx_to_offsets_size = state.meta.count;
        args.state.valid_bitset = state.validity;
    } else if constexpr (string) {
        auto owner = std::make_shared<std::vector<int32_t>>(
            args.storage.layout->BuildOffsets(state.meta.count));
        args.state.idx_to_offsets = owner->data();
        args.state.idx_to_offsets_size = owner->size();
        args.state.idx_to_offsets_heap_bytes =
            owner->capacity() * sizeof(int32_t);
        args.state.idx_to_offsets_owner = owner;
        args.state.valid_bitset = state.validity;
    } else {
        auto rebuilt = RebuildNumericAux(args.storage.data,
                                         args.storage.size,
                                         state.meta.count,
                                         state.params);
        args.state.valid_bitset =
            std::make_shared<TargetBitmap>(std::move(rebuilt.first));
        args.state.idx_to_offsets = rebuilt.second->data();
        args.state.idx_to_offsets_size = rebuilt.second->size();
        args.state.idx_to_offsets_heap_bytes =
            rebuilt.second->capacity() * sizeof(int32_t);
        args.state.idx_to_offsets_owner = std::move(rebuilt.second);
    }
    if constexpr (string) {
        ValidateStringAux(*args.storage.layout,
                          *args.state.valid_bitset,
                          args.state.idx_to_offsets,
                          state.meta.count,
                          state.params);
    } else {
        ValidateNumericAux(args.storage.data,
                           args.storage.size,
                           *args.state.valid_bitset,
                           args.state.idx_to_offsets,
                           state.meta.count,
                           state.params);
    }
    args.state.total_num_rows = state.meta.count;
    args.state.value_type = state.params.value_type;
    args.state.nested = state.meta.nested;
    args.state.value_lookup = state.params.value_lookup;
    return std::make_unique<SortedIndexReader<T>>(std::move(args));
}

}  // namespace

/** @brief Validated metadata reused by Load; no reader or payload ownership. */
struct SortedIndexLoader::OpenedState {
    RuntimeParams params;
    NumericMeta meta;
};

SortedIndexLoader::SortedIndexLoader(OpenedIndexInput input,
                                     storage::LoadOptions options)
    : input_(std::move(input)), options_(std::move(options)) {
}

SortedIndexLoader::~SortedIndexLoader() = default;

SortedIndexLoader::OpenedState
SortedIndexLoader::ParsePackedState(const storage::IndexEntryDirectory&,
                                    const nlohmann::json& metadata,
                                    const storage::LoadOptions& options) {
    OpenedState state;
    state.params = ParseRuntimeParams(options.params);
    state.meta.count =
        ReadRequiredIndexMeta<size_t>(metadata, sort_format::kNumRows.data());
    CheckCount(state.meta.count);
    if (metadata.contains(sort_format::kNested)) {
        state.meta.has_nested = true;
        state.meta.nested =
            ReadRequiredIndexMeta<bool>(metadata, sort_format::kNested.data());
    }
    ResolveNested(state.meta, state.params.nested);
    const bool string = IsStringDataType(state.params.value_type);
    if (string) {
        const auto version = ReadRequiredIndexMeta<uint32_t>(
            metadata, sort_format::kVersion.data());
        if (version != sort_format::kStringVersion) {
            ThrowInfo(
                Unsupported, "unsupported sorted string version {}", version);
        }
    } else {
        state.meta.index_length = ReadRequiredIndexMeta<size_t>(
            metadata, sort_format::kIndexLength.data());
        CheckCount(state.meta.index_length);
    }
    return state;
}

folly::coro::Task<std::unique_ptr<IndexLoader>>
SortedIndexLoader::Open(IndexOpenRequest request) {
    return OpenIndexLoader(
        std::move(request),
        [](OpenedIndexInput input, storage::LoadOptions options)
            -> folly::coro::Task<std::unique_ptr<IndexLoader>> {
            auto loader = std::unique_ptr<SortedIndexLoader>(
                new SortedIndexLoader(std::move(input), std::move(options)));
            auto opened = std::make_unique<OpenedState>();
            if (const auto* packed =
                    std::get_if<PackedIndexSource>(&loader->input_)) {
                *opened = ParsePackedState(
                    packed->Directory(), packed->Metadata(), loader->options_);
            } else {
                const auto& legacy =
                    std::get<LegacyIndexSource>(loader->input_);
                opened->params = ParseRuntimeParams(loader->options_.params);
                if (IsStringDataType(opened->params.value_type)) {
                    static_cast<CommonMeta&>(opened->meta) =
                        co_await ReadStringMeta(legacy.use_async,
                                                *legacy.source);
                } else {
                    opened->meta = co_await ReadNumericMeta(legacy.use_async,
                                                            *legacy.source);
                }
                ResolveNested(opened->meta, opened->params.nested);
                CheckCount(opened->meta.count);
                CheckCount(opened->meta.index_length);
            }
            loader->state_ = std::move(opened);
            co_return loader;
        });
}

folly::coro::Task<IIndexReaderBasePtr>
SortedIndexLoader::Load(milvus::OpContext* context) {
    if (auto* packed = std::get_if<PackedIndexSource>(&input_)) {
        return RunPackedIndexLoad(
            *packed,
            options_,
            [this](const auto& directory,
                   const auto& metadata,
                   const auto& options) {
                return PlanPacked(directory, metadata, options, state_.get());
            },
            &FinishPacked,
            context);
    }
    return RunLegacyLoad(
        std::get<LegacyIndexSource>(input_),
        options_,
        [this](auto& source, const auto& options, bool use_async) {
            return LoadLegacy(source, options, use_async);
        },
        context);
}

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

folly::coro::Task<IIndexReaderBasePtr>
SortedIndexLoader::LoadLegacy(storage::FileSource& source,
                              const storage::LoadOptions& opts,
                              bool use_async) {
    auto projection = PrepareJsonProjectedOpen(families::kSort, source, opts);
    const auto& params = state_->params;
    if (params.value_type == DataType::NONE ||
        params.value_type == DataType::ARRAY) {
        ThrowInfo(DataTypeInvalid,
                  "sorted loader requires value_type or array_element_type");
    }
    if (IsStringDataType(params.value_type)) {
        const auto& meta = state_->meta;
        auto state =
            co_await LoadStringState(use_async, source, opts, meta, params);
        auto inner = std::make_unique<SortedIndexReader<std::string_view>>(
            std::move(state));
        co_return co_await FinishJsonProjectedOpenAsync(
            use_async, std::move(projection), source, std::move(inner));
    }

    const auto& meta = state_->meta;
    std::unique_ptr<IIndexReaderBase> inner;
    switch (params.value_type) {
        case DataType::BOOL:
            inner = std::make_unique<SortedIndexReader<bool>>(
                (co_await LoadNumericState<bool>(
                    use_async, source, opts, meta, params)));
            break;
        case DataType::INT8:
            inner = std::make_unique<SortedIndexReader<int8_t>>(
                (co_await LoadNumericState<int8_t>(
                    use_async, source, opts, meta, params)));
            break;
        case DataType::INT16:
            inner = std::make_unique<SortedIndexReader<int16_t>>(
                (co_await LoadNumericState<int16_t>(
                    use_async, source, opts, meta, params)));
            break;
        case DataType::INT32:
            inner = std::make_unique<SortedIndexReader<int32_t>>(
                (co_await LoadNumericState<int32_t>(
                    use_async, source, opts, meta, params)));
            break;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            inner = std::make_unique<SortedIndexReader<int64_t>>(
                (co_await LoadNumericState<int64_t>(
                    use_async, source, opts, meta, params)));
            break;
        case DataType::FLOAT:
            inner = std::make_unique<SortedIndexReader<float>>(
                (co_await LoadNumericState<float>(
                    use_async, source, opts, meta, params)));
            break;
        case DataType::DOUBLE:
            inner = std::make_unique<SortedIndexReader<double>>(
                (co_await LoadNumericState<double>(
                    use_async, source, opts, meta, params)));
            break;
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported sorted value type {}",
                      static_cast<int>(params.value_type));
    }
    co_return (co_await FinishJsonProjectedOpenAsync(
        use_async, std::move(projection), source, std::move(inner)));
}

IndexLoadPlan
SortedIndexLoader::PlanPacked(const storage::IndexEntryDirectory& directory,
                              const nlohmann::json& metadata,
                              const storage::LoadOptions& opts,
                              const OpenedState* cached) {
    std::optional<OpenedState> parsed;
    if (cached == nullptr) {
        parsed = ParsePackedState(directory, metadata, opts);
        cached = &*parsed;
    }
    auto state = std::make_shared<PackedSortedState>();
    state->params = cached->params;
    state->meta = cached->meta;
    const bool string = IsStringDataType(state->params.value_type);
    state->has_offsets = directory.HasEntry(sort_format::kIdxToOffsets);
    const bool has_validity = directory.HasEntry(sort_format::kValidBitset);
    if ((string && !has_validity) ||
        (!string && has_validity != state->has_offsets)) {
        ThrowInfo(DataFormatBroken,
                  "sorted V3 auxiliary entries are incomplete");
    }
    state->data_bytes = directory.At(sort_format::kIndexData).plaintext_size;
    if (!string) {
        const auto expected =
            DispatchPackedNumeric(state->params.value_type, [&]<typename T>() {
                return CheckedMultiply(state->meta.index_length,
                                       sizeof(IndexStructure<T>),
                                       "numeric data");
            });
        if (state->data_bytes != expected) {
            ThrowInfo(DataFormatBroken,
                      "sorted numeric data size disagrees with metadata");
        }
    }
    IndexLoadPlan plan;
    plan.load_context = state;
    plan.entries.reserve(4);
    state->projection = PreparePackedJsonProjectedOpen(
        families::kSort, directory, metadata, opts, plan, state->meta.count);
    if (opts.enable_mmap && state->data_bytes != 0) {
        state->data_file = PlanSortedFile(
            plan, sort_format::kIndexData, state->data_bytes, opts);
    } else {
        if (string) {
            state->string_data =
                std::make_shared<std::vector<uint8_t>>(state->data_bytes);
            state->data = state->string_data->data();
            state->data_owner = state->string_data;
            state->data_heap_bytes = state->string_data->capacity();
        } else {
            DispatchPackedNumeric(state->params.value_type, [&]<typename T>() {
                auto owner = std::make_shared<std::vector<IndexStructure<T>>>(
                    state->meta.index_length);
                state->data = owner->data();
                state->data_heap_bytes =
                    owner->capacity() * sizeof(IndexStructure<T>);
                state->data_owner = std::move(owner);
            });
        }
        plan.entries.push_back(
            {std::string(sort_format::kIndexData),
             storage::MemoryEntryTarget{state->data_owner,
                                        static_cast<uint8_t*>(state->data),
                                        state->data_bytes}});
    }
    if (has_validity) {
        state->validity =
            std::make_shared<TargetBitmap>(state->meta.count, false);
        const auto bytes = string ? (state->meta.count + 7) / 8
                                  : state->validity->size_in_bytes();
        if (directory.At(sort_format::kValidBitset).plaintext_size != bytes) {
            ThrowInfo(DataFormatBroken,
                      "sorted validity size disagrees with row count");
        }
        plan.entries.push_back(
            {std::string(sort_format::kValidBitset),
             storage::MemoryEntryTarget{
                 state->validity,
                 reinterpret_cast<uint8_t*>(state->validity->data()),
                 bytes}});
    }
    if (state->has_offsets) {
        const auto bytes = CheckedMultiply(
            state->meta.count, sizeof(int32_t), "reverse offsets");
        if (directory.At(sort_format::kIdxToOffsets).plaintext_size != bytes) {
            ThrowInfo(DataFormatBroken,
                      "sorted reverse-offset size disagrees with row count");
        }
        if (opts.enable_mmap && bytes != 0) {
            state->offsets_file =
                PlanSortedFile(plan, sort_format::kIdxToOffsets, bytes, opts);
        } else {
            state->offsets =
                std::make_shared<std::vector<int32_t>>(state->meta.count);
            plan.entries.push_back(
                {std::string(sort_format::kIdxToOffsets),
                 storage::MemoryEntryTarget{
                     state->offsets,
                     reinterpret_cast<uint8_t*>(state->offsets->data()),
                     bytes}});
        }
    }
    return plan;
}

folly::coro::Task<IIndexReaderBasePtr>
SortedIndexLoader::FinishPacked(IndexLoadPlan& plan,
                                const storage::LoadOptions& opts,
                                bool use_async) {
    const auto state =
        std::any_cast<std::shared_ptr<PackedSortedState>>(plan.load_context);
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    std::shared_ptr<SortedMmapOwner> data_mapping;
    std::shared_ptr<SortedMmapOwner> offsets_mapping;
    if (state->data_file || state->offsets_file) {
        auto local_io = [&] {
            if (state->data_file)
                data_mapping = MapSortedTarget(*state->data_file);
            if (state->offsets_file)
                offsets_mapping = MapSortedTarget(*state->offsets_file);
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(local_io, priority);
        } else {
            local_io();
        }
    }
    IIndexReaderBasePtr inner;
    if (IsStringDataType(state->params.value_type)) {
        if (state->meta.count % 8 != 0) {
            auto* bytes = reinterpret_cast<uint8_t*>(state->validity->data());
            bytes[state->meta.count / 8] &=
                static_cast<uint8_t>((1u << (state->meta.count % 8)) - 1u);
        }
        inner = FinishPackedSortedState<std::string_view>(
            *state, data_mapping, offsets_mapping);
    } else {
        inner =
            DispatchPackedNumeric(state->params.value_type, [&]<typename T>() {
                return FinishPackedSortedState<T>(
                    *state, data_mapping, offsets_mapping);
            });
    }
    co_return FinishPackedJsonProjectedOpen(std::move(state->projection),
                                            std::move(inner));
}

namespace {

const bool kSortedLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<SortedIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
