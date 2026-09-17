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

#include "index/scalar/bitmap/BitmapIndexLoader.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <map>
#include <optional>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "index/ParamUtils.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "storage/artifact/FileSourceUtils.h"
#include "storage/artifact/LocalFileUtils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/bitmap/BitmapIndexReader.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

using bitmap_params::IsStringType;

constexpr size_t kFrozenAlignment = 32;
constexpr uint64_t kMaxCoordinateCount =
    static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
constexpr std::string_view kLegacyNestedKey = "is_nested_index";
constexpr std::string_view kV3NestedKey = "is_nested";

struct RuntimeParams {
    DataType field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
    bool nullable{false};
    bool nested{false};
    bool value_lookup{true};
    bool offset_cache{false};
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    RuntimeParams result;
    result.field_type =
        ReadDataTypeParam(params, "field_type").value_or(DataType::NONE);
    const auto array_element_type =
        ReadDataTypeParam(params, "array_element_type")
            .value_or(DataType::NONE);
    const auto configured_value_type =
        ReadDataTypeParam(params, "value_type").value_or(DataType::NONE);
    if (result.field_type == DataType::NONE &&
        array_element_type != DataType::NONE) {
        result.field_type = DataType::ARRAY;
    }
    result.value_type = result.field_type == DataType::ARRAY &&
                                array_element_type != DataType::NONE
                            ? array_element_type
                            : configured_value_type;
    if ((result.value_type == DataType::NONE ||
         result.value_type == DataType::ARRAY) &&
        result.field_type != DataType::ARRAY) {
        result.value_type = result.field_type;
    }
    result.nullable =
        GetValueFromConfigOrFallback<bool>(params, "nullable", false);
    result.nested = ReadRequiredNestedParam(params, "bitmap loader");
    result.value_lookup = result.field_type != DataType::ARRAY || result.nested;
    result.offset_cache = GetValueFromConfigOrFallback<bool>(
        params, ENABLE_OFFSET_CACHE, false);
    return result;
}

struct BitmapMeta {
    size_t index_length{0};
    size_t count{0};
    bool nested{false};
    bool has_nested{false};
};

BitmapMeta
ParseLegacyMeta(const std::vector<uint8_t>& encoded) {
    const std::string text(encoded.begin(), encoded.end());
    try {
        const auto json = nlohmann::json::parse(text);
        BitmapMeta result{
            .index_length = json.at(BITMAP_INDEX_LENGTH).get<size_t>(),
            .count = json.at(BITMAP_INDEX_NUM_ROWS).get<size_t>()};
        if (json.contains(kLegacyNestedKey)) {
            result.nested = json.at(kLegacyNestedKey).get<bool>();
            result.has_nested = true;
        }
        return result;
    } catch (const nlohmann::json::parse_error&) {
        try {
            const auto yaml = YAML::Load(text);
            BitmapMeta result{
                .index_length = yaml[BITMAP_INDEX_LENGTH].as<size_t>(),
                .count = yaml[BITMAP_INDEX_NUM_ROWS].as<size_t>()};
            const auto nested = yaml[std::string(kLegacyNestedKey)];
            if (nested) {
                result.nested = nested.as<bool>();
                result.has_nested = true;
            }
            return result;
        } catch (const YAML::Exception& error) {
            ThrowInfo(DataFormatBroken,
                      "invalid bitmap V1/V2 metadata: {}",
                      error.what());
        }
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid bitmap V1/V2 metadata: {}",
                  error.what());
    }
}

BitmapMeta
ReadMeta(storage::FileSource& source) {
    if (source.Gen() == storage::Generation::V1V2) {
        auto encoded = source.ReadEntry(BITMAP_INDEX_META);
        return ParseLegacyMeta(encoded);
    }
    BitmapMeta result{.index_length = storage::ReadRequiredMeta<size_t>(
                          source, BITMAP_INDEX_LENGTH, "bitmap V3"),
                      .count = storage::ReadRequiredMeta<size_t>(
                          source, BITMAP_INDEX_NUM_ROWS, "bitmap V3")};
    if (const auto nested = source.GetMeta(kV3NestedKey); nested.has_value()) {
        try {
            result.nested = nested->get<bool>();
            result.has_nested = true;
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataFormatBroken,
                      "invalid bitmap V3 metadata {}: {}",
                      kV3NestedKey,
                      error.what());
        }
    }
    return result;
}

void
ValidateMeta(const BitmapMeta& meta, const RuntimeParams& params) {
    if (meta.count == 0 || meta.count > kMaxCoordinateCount) {
        ThrowInfo(DataFormatBroken,
                  "bitmap coordinate count {} is outside [1, {}]",
                  meta.count,
                  kMaxCoordinateCount);
    }
    if (params.value_lookup && meta.index_length > meta.count) {
        ThrowInfo(DataFormatBroken,
                  "bitmap scalar posting count {} exceeds coordinate count "
                  "{}",
                  meta.index_length,
                  meta.count);
    }
}

size_t
PackedValidityBytes(size_t count) {
    return count / 8 + static_cast<size_t>(count % 8 != 0);
}

TargetBitmap
DecodeValidity(const std::vector<uint8_t>& encoded, size_t count) {
    const auto expected = PackedValidityBytes(count);
    if (encoded.size() != expected) {
        ThrowInfo(DataFormatBroken,
                  "bitmap validity size mismatch: expected {}, got {}",
                  expected,
                  encoded.size());
    }
    TargetBitmap result(count, false);
    for (size_t i = 0; i < count; ++i) {
        if ((encoded[i / 8] & static_cast<uint8_t>(1U << (i % 8))) != 0) {
            result.set(i);
        }
    }
    return result;
}

TargetBitmap
ToBitset(const roaring::Roaring& posting, size_t count) {
    TargetBitmap result(count, false);
    for (auto coordinate : posting) {
        if (coordinate >= count) {
            ThrowInfo(DataFormatBroken,
                      "bitmap posting coordinate {} exceeds count {}",
                      coordinate,
                      count);
        }
        result.set(coordinate);
    }
    return result;
}

template <typename T>
T
ReadKey(const uint8_t*& cursor, const uint8_t* end) {
    if constexpr (std::is_same_v<T, std::string>) {
        if (static_cast<size_t>(end - cursor) < sizeof(size_t)) {
            ThrowInfo(DataFormatBroken, "truncated bitmap string key length");
        }
        size_t length = 0;
        std::memcpy(&length, cursor, sizeof(length));
        cursor += sizeof(length);
        if (length > static_cast<size_t>(end - cursor)) {
            ThrowInfo(DataFormatBroken,
                      "truncated bitmap string key: expected {} bytes, got {}",
                      length,
                      end - cursor);
        }
        std::string result(reinterpret_cast<const char*>(cursor), length);
        cursor += length;
        return result;
    } else {
        if (static_cast<size_t>(end - cursor) < sizeof(T)) {
            ThrowInfo(DataFormatBroken, "truncated bitmap numeric key");
        }
        T result;
        std::memcpy(&result, cursor, sizeof(T));
        cursor += sizeof(T);
        return result;
    }
}

template <typename T>
roaring::Roaring
ReadPosting(const uint8_t*& cursor,
            const uint8_t* end,
            size_t ordinal,
            size_t count) {
    if (cursor == end) {
        ThrowInfo(DataFormatBroken, "truncated bitmap posting {}", ordinal);
    }
    roaring::Roaring posting;
    try {
        posting = roaring::Roaring::readSafe(
            reinterpret_cast<const char*>(cursor), end - cursor);
    } catch (const std::bad_alloc&) {
        ThrowInfo(MemAllocateFailed,
                  "failed to allocate while decoding bitmap posting");
    } catch (const std::exception& error) {
        ThrowInfo(DataFormatBroken,
                  "invalid bitmap posting {}: {}",
                  ordinal,
                  error.what());
    }
    const auto consumed = posting.getSizeInBytes(true);
    if (consumed > static_cast<size_t>(end - cursor)) {
        ThrowInfo(DataFormatBroken,
                  "bitmap posting {} exceeds serialized entry",
                  ordinal);
    }
    for (auto coordinate : posting) {
        if (coordinate >= count) {
            ThrowInfo(DataFormatBroken,
                      "bitmap posting coordinate {} exceeds count {}",
                      coordinate,
                      count);
        }
    }
    cursor += consumed;
    return posting;
}

template <typename T>
BitmapRoaringPostingMap<T>
DecodePostings(const uint8_t* data,
               size_t size,
               size_t index_length,
               size_t count) {
    if (size == 0) {
        if (index_length != 0) {
            ThrowInfo(DataFormatBroken,
                      "bitmap data is empty for {} postings",
                      index_length);
        }
        return {};
    }
    const auto* cursor = data;
    const auto* end = data + size;
    BitmapRoaringPostingMap<T> postings;
    for (size_t i = 0; i < index_length; ++i) {
        auto key = ReadKey<T>(cursor, end);
        auto posting = ReadPosting<T>(cursor, end, i, count);
        const auto inserted =
            postings.emplace(std::move(key), std::move(posting)).second;
        if (!inserted) {
            ThrowInfo(DataFormatBroken,
                      "bitmap data contains duplicate key at posting {}",
                      i);
        }
    }
    if (cursor != end) {
        ThrowInfo(DataFormatBroken,
                  "bitmap data has {} trailing bytes",
                  end - cursor);
    }
    return postings;
}

template <typename T>
void
ValidateCoordinateOwnership(const BitmapRoaringPostingMap<T>& postings,
                            bool one_value_per_coordinate) {
    if (!one_value_per_coordinate) {
        return;
    }
    roaring::Roaring occupied;
    for (const auto& [_, posting] : postings) {
        if (occupied.intersect(posting)) {
            ThrowInfo(DataFormatBroken,
                      "bitmap scalar postings contain an overlapping "
                      "coordinate");
        }
        occupied |= posting;
    }
}

class TemporaryFileGuard {
 public:
    explicit TemporaryFileGuard(std::string path) : path_(std::move(path)) {
    }

    TemporaryFileGuard(const TemporaryFileGuard&) = delete;
    TemporaryFileGuard&
    operator=(const TemporaryFileGuard&) = delete;

    TemporaryFileGuard(TemporaryFileGuard&& other) noexcept
        : path_(std::move(other.path_)),
          fd_(std::exchange(other.fd_, -1)) {
    }

    TemporaryFileGuard&
    operator=(TemporaryFileGuard&& other) noexcept {
        if (this != &other) {
            Cleanup();
            path_ = std::move(other.path_);
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    ~TemporaryFileGuard() {
        Cleanup();
    }

    void
    Create() {
        AssertInfo(fd_ == -1, "bitmap temporary file was already created");
        fd_ = mkstemp(path_.MutablePath());
        if (fd_ == -1) {
            const auto error = errno;
            const auto path = path_.Release();
            ThrowInfo(FileCreateFailed,
                      "failed to create bitmap mmap file {}: {}",
                      path,
                      std::strerror(error));
        }
    }

    int
    Fd() const {
        AssertInfo(fd_ != -1, "bitmap temporary file is closed");
        return fd_;
    }

    void
    Close() {
        AssertInfo(fd_ != -1, "bitmap temporary file is already closed");
        const auto fd = std::exchange(fd_, -1);
        if (close(fd) != 0) {
            ThrowInfo(FileWriteFailed,
                      "failed to close bitmap mmap file {}: {}",
                      path_.Path(),
                      std::strerror(errno));
        }
    }

    const std::string&
    Path() const {
        return path_.Path();
    }

    std::string
    ReleasePath() {
        AssertInfo(fd_ == -1,
                   "bitmap temporary path cannot be released while open");
        return path_.Release();
    }

 private:
    void
    Cleanup() noexcept {
        if (fd_ != -1) {
            close(fd_);
            fd_ = -1;
        }
    }

    storage::LocalEntryGuard path_;
    int fd_{-1};
};

TemporaryFileGuard
CreateTemporaryFile(const std::string& directory, std::string_view prefix) {
    if (directory.empty()) {
        ThrowInfo(FileCreateFailed, "bitmap mmap directory must not be empty");
    }
    std::error_code error;
    std::filesystem::create_directories(directory, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create bitmap mmap directory {}: {}",
                  directory,
                  error.message());
    }
    TemporaryFileGuard file(
        (std::filesystem::path(directory) / (std::string(prefix) + "_XXXXXX"))
            .string());
    file.Create();
    return file;
}

void
WriteAll(int fd, const void* data, size_t size, const std::string& path) {
    const auto* cursor = static_cast<const uint8_t*>(data);
    while (size != 0) {
        const auto written = write(fd, cursor, size);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            ThrowInfo(FileWriteFailed,
                      "failed to write bitmap mmap file {}: {}",
                      path,
                      std::strerror(errno));
        }
        cursor += written;
        size -= static_cast<size_t>(written);
    }
}

size_t
AlignFrozenSize(size_t size) {
    if (size > std::numeric_limits<size_t>::max() - (kFrozenAlignment - 1)) {
        ThrowInfo(DataFormatBroken, "bitmap frozen posting size overflows");
    }
    return (size + kFrozenAlignment - 1) & ~(kFrozenAlignment - 1);
}

template <typename T>
struct FrozenPostings {
    // Declared first so the posting views are destroyed before their mapping.
    std::shared_ptr<BitmapMmapOwner> owner;
    BitmapRoaringPostingMap<T> postings;
};

template <typename T>
FrozenPostings<T>
FinishFrozenFile(TemporaryFileGuard file,
                 size_t file_size,
                 const std::map<T, std::pair<size_t, size_t>>& locations) {
    file.Close();
    AssertInfo(file_size != 0,
               "bitmap mmap requires at least one non-empty posting file");

    storage::MappedRegionGuard mapping;
    {
        const auto map_fd = open(file.Path().c_str(), O_RDONLY);
        if (map_fd == -1) {
            ThrowInfo(FileOpenFailed,
                      "failed to open bitmap mmap file {}: {}",
                      file.Path(),
                      std::strerror(errno));
        }
        storage::FileDescriptorGuard descriptor(map_fd);
        auto* mapped = static_cast<char*>(
            mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, map_fd, 0));
        const auto saved_errno = errno;
        if (mapped == MAP_FAILED) {
            ThrowInfo(MmapError,
                      "failed to map bitmap frozen file {}: {}",
                      file.Path(),
                      std::strerror(saved_errno));
        }
        mapping = storage::MappedRegionGuard(mapped, file_size);
    }

    FrozenPostings<T> result;
    result.owner = std::make_shared<BitmapMmapOwner>(
        mapping.Data(), file_size, file.Path());
    mapping.Release();
    static_cast<void>(file.ReleasePath());
    for (const auto& [key, location] : locations) {
        result.postings.emplace(
            key,
            roaring::Roaring::frozenView(result.owner->Data() + location.first,
                                         location.second));
    }
    return result;
}

template <typename T>
FrozenPostings<T>
DecodeFrozenPostings(const uint8_t* data,
                     size_t size,
                     size_t index_length,
                     size_t count,
                     TargetBitmap& validity,
                     bool rebuild_validity,
                     const std::string& directory) {
    if (size == 0) {
        ThrowInfo(DataFormatBroken,
                  "bitmap mmap data is empty for {} postings",
                  index_length);
    }
    const auto* cursor = data;
    const auto* end = data + size;
    auto file = CreateTemporaryFile(directory, "bitmap_frozen");
    std::map<T, std::pair<size_t, size_t>> locations;
    std::vector<uint8_t> frozen_buffer;
    size_t file_offset = 0;
    for (size_t i = 0; i < index_length; ++i) {
        auto key = ReadKey<T>(cursor, end);
        if (locations.find(key) != locations.end()) {
            ThrowInfo(DataFormatBroken,
                      "bitmap data contains duplicate key at posting {}",
                      i);
        }
        {
            // Keep only one decoded portable posting resident at a time.
            // The mapped reader later points at the frozen output file.
            auto posting = ReadPosting<T>(cursor, end, i, count);
            if (rebuild_validity) {
                for (auto coordinate : posting) {
                    validity.set(coordinate);
                }
            }
            const auto frozen_size = posting.getFrozenSizeInBytes();
            const auto aligned_size = AlignFrozenSize(frozen_size);
            if (aligned_size >
                std::numeric_limits<size_t>::max() - file_offset) {
                ThrowInfo(DataFormatBroken,
                          "bitmap frozen file size overflows");
            }
            frozen_buffer.assign(aligned_size, 0);
            posting.writeFrozen(reinterpret_cast<char*>(frozen_buffer.data()));
            WriteAll(file.Fd(),
                     frozen_buffer.data(),
                     frozen_buffer.size(),
                     file.Path());
            locations.emplace(std::move(key),
                              std::make_pair(file_offset, frozen_size));
            file_offset += aligned_size;
        }
    }
    if (cursor != end) {
        ThrowInfo(DataFormatBroken,
                  "bitmap data has {} trailing bytes",
                  end - cursor);
    }
    return FinishFrozenFile<T>(std::move(file), file_offset, locations);
}

template <typename T>
void
RebuildValidity(const BitmapRoaringPostingMap<T>& postings,
                TargetBitmap& validity) {
    for (const auto& [_, posting] : postings) {
        for (auto coordinate : posting) {
            validity.set(coordinate);
        }
    }
}

BitmapReaderOptions
MakeReaderOptions(TargetBitmap validity,
                  const BitmapMeta& meta,
                  const RuntimeParams& params) {
    return BitmapReaderOptions{.valid_bitset = std::move(validity),
                               .total_num_rows = meta.count,
                               .nested = meta.nested,
                               .value_lookup = params.value_lookup,
                               .value_type = params.value_type,
                               .offset_cache = params.offset_cache};
}

template <typename T>
std::unique_ptr<IIndexReaderBase>
OpenDecodedState(const uint8_t* data,
                 size_t size,
                 const BitmapMeta& meta,
                 const RuntimeParams& params,
                 TargetBitmap validity,
                 bool rebuild_validity,
                 BitmapLayout layout) {
    auto postings =
        DecodePostings<T>(data, size, meta.index_length, meta.count);
    ValidateCoordinateOwnership(postings, params.value_lookup);
    if (rebuild_validity) {
        RebuildValidity(postings, validity);
    }
    auto options = MakeReaderOptions(std::move(validity), meta, params);
    if (layout == BitmapLayout::Roaring) {
        return CreateBitmapIndexReader<T>(std::move(postings),
                                          std::move(options));
    }

    BitmapBitsetPostingMap<T> bitset_postings;
    for (const auto& [key, posting] : postings) {
        bitset_postings.emplace(key, ToBitset(posting, meta.count));
    }
    return CreateBitmapIndexReader<T>(std::move(bitset_postings),
                                      std::move(options));
}

template <typename T>
std::unique_ptr<IIndexReaderBase>
OpenDecodedMmapState(const uint8_t* data,
                     size_t size,
                     const BitmapMeta& meta,
                     const RuntimeParams& params,
                     TargetBitmap validity,
                     bool rebuild_validity,
                     const std::string& mmap_dir_path) {
    auto frozen = DecodeFrozenPostings<T>(data,
                                          size,
                                          meta.index_length,
                                          meta.count,
                                          validity,
                                          rebuild_validity,
                                          mmap_dir_path);
    ValidateCoordinateOwnership(frozen.postings, params.value_lookup);
    auto options = MakeReaderOptions(std::move(validity), meta, params);
    return CreateBitmapIndexReader<T>(std::move(frozen.postings),
                                      std::move(options),
                                      std::move(frozen.owner));
}

template <typename Result, typename F>
Result
DispatchBitmapType(DataType value_type, F&& fn) {
    switch (value_type) {
        case DataType::BOOL:
            return fn.template operator()<bool>();
        case DataType::INT8:
            return fn.template operator()<int8_t>();
        case DataType::INT16:
            return fn.template operator()<int16_t>();
        case DataType::INT32:
            return fn.template operator()<int32_t>();
        case DataType::INT64:
            return fn.template operator()<int64_t>();
        case DataType::FLOAT:
            return fn.template operator()<float>();
        case DataType::DOUBLE:
            return fn.template operator()<double>();
        case DataType::STRING:
        case DataType::VARCHAR:
            return fn.template operator()<std::string>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported bitmap value type {}",
                      static_cast<int>(value_type));
    }
}

std::unique_ptr<IIndexReaderBase>
DispatchOpen(DataType value_type,
             const uint8_t* data,
             size_t size,
             const BitmapMeta& meta,
             const RuntimeParams& params,
             TargetBitmap validity,
             bool rebuild_validity,
             BitmapLayout layout) {
    return DispatchBitmapType<std::unique_ptr<IIndexReaderBase>>(
        value_type, [&]<typename T>() {
            return OpenDecodedState<T>(data,
                                       size,
                                       meta,
                                       params,
                                       std::move(validity),
                                       rebuild_validity,
                                       layout);
        });
}

std::unique_ptr<IIndexReaderBase>
DispatchMmapOpen(DataType value_type,
                 const uint8_t* data,
                 size_t size,
                 const BitmapMeta& meta,
                 const RuntimeParams& params,
                 TargetBitmap validity,
                 bool rebuild_validity,
                 const std::string& mmap_dir_path) {
    return DispatchBitmapType<std::unique_ptr<IIndexReaderBase>>(
        value_type, [&]<typename T>() {
            return OpenDecodedMmapState<T>(data,
                                           size,
                                           meta,
                                           params,
                                           std::move(validity),
                                           rebuild_validity,
                                           mmap_dir_path);
        });
}

std::unique_ptr<IIndexReaderBase>
LoadBitmapPayload(storage::FileSource& source,
                  const storage::LoadOptions& opts,
                  const RuntimeParams& params) {
    auto meta = ReadMeta(source);
    if (meta.has_nested && meta.nested != params.nested) {
        ThrowInfo(DataFormatBroken,
                  "bitmap persisted nested value {} disagrees with runtime "
                  "value {}",
                  meta.nested,
                  params.nested);
    }
    // Old artifacts may lack this metadata. The adapter-supplied runtime
    // value is mandatory, so capability derivation and opening still use the
    // same coordinate domain without eagerly reading artifact metadata.
    meta.nested = params.nested;
    ValidateMeta(meta, params);

    TargetBitmap validity(meta.count, meta.nested || !params.nullable);
    bool rebuild_validity = params.nullable && !meta.nested;
    if (source.HasEntry(BITMAP_INDEX_VALID_BITSET)) {
        auto encoded = source.ReadEntry(BITMAP_INDEX_VALID_BITSET);
        validity = DecodeValidity(encoded, meta.count);
        rebuild_validity = false;
    }

    const auto layout =
        meta.index_length <=
                static_cast<size_t>(DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND)
            ? BitmapLayout::Bitset
            : BitmapLayout::Roaring;
    if (opts.enable_mmap && layout == BitmapLayout::Roaring) {
        auto portable_file =
            CreateTemporaryFile(opts.mmap_dir_path, "bitmap_portable");
        portable_file.Close();
        source.ReadEntryToLocalFile(BITMAP_INDEX_DATA, portable_file.Path());
        const auto size = storage::LocalFileSize(
            portable_file.Path(), "failed to size bitmap mmap input");
        if (size == 0) {
            ThrowInfo(DataFormatBroken,
                      "bitmap data is empty for non-empty mmap index");
        }
        storage::MappedRegionGuard mapping;
        {
            const auto fd = open(portable_file.Path().c_str(), O_RDONLY);
            if (fd == -1) {
                ThrowInfo(FileOpenFailed,
                          "failed to open bitmap file {}: {}",
                          portable_file.Path(),
                          std::strerror(errno));
            }
            storage::FileDescriptorGuard descriptor(fd);
            auto* mapped = static_cast<char*>(
                mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
            const auto saved_errno = errno;
            if (mapped == MAP_FAILED) {
                ThrowInfo(MmapError,
                          "failed to map bitmap file {}: {}",
                          portable_file.Path(),
                          std::strerror(saved_errno));
            }
            mapping = storage::MappedRegionGuard(mapped, size);
        }
        return DispatchMmapOpen(params.value_type,
                                reinterpret_cast<const uint8_t*>(mapping.Data()),
                                size,
                                meta,
                                params,
                                std::move(validity),
                                rebuild_validity,
                                opts.mmap_dir_path);
    }

    auto data = source.ReadEntry(BITMAP_INDEX_DATA);
    return DispatchOpen(params.value_type,
                        data.data(),
                        data.size(),
                        meta,
                        params,
                        std::move(validity),
                        rebuild_validity,
                        layout);
}

}  // namespace

ReaderCaps
BitmapIndexLoader::DeriveCaps(const Config& index_meta) {
    const auto params = ParseRuntimeParams(index_meta);
    return DeriveJsonProjectedCaps(
        families::kBitmap,
        index_meta,
        ReaderCaps{
            .predicate = true,
            .pattern_match = IsStringType(params.value_type),
            .nested = params.nested,
            .value_lookup = params.value_lookup,
            .cheap_value_lookup = params.value_lookup && params.offset_cache,
            .exact = !params.nested});
}

IIndexReaderBasePtr
BitmapIndexLoader::Open(storage::FileSource& source,
                        const storage::LoadOptions& opts) {
    auto projection = PrepareJsonProjectedOpen(families::kBitmap, source, opts);
    const auto params = ParseRuntimeParams(opts.params);
    if (params.value_type == DataType::NONE ||
        params.value_type == DataType::ARRAY) {
        ThrowInfo(DataTypeInvalid,
                  "bitmap loader requires value_type or array_element_type");
    }
    auto inner = LoadBitmapPayload(source, opts, params);
    return FinishJsonProjectedOpen(
        std::move(projection), source, std::move(inner));
}

namespace {

const bool kBitmapLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<BitmapIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
