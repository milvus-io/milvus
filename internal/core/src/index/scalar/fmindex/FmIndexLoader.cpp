// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/scalar/fmindex/FmIndexLoader.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <optional>
#include <sys/mman.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "index/ParamUtils.h"
#include "index/scalar/fmindex/FmIndexArtifact.h"
#include "index/scalar/fmindex/FmIndexBuilder.h"
#include "common/EasyAssert.h"
#include "storage/artifact/FileSourceUtils.h"
#include "index/contracts/Registry.h"
#include "index/fmindex/FMIndex.h"
#include "index/scalar/fmindex/FmIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

using fmindex_params::ReadDataType;

constexpr std::string_view kBlobEntry = "fm_index.bin";
constexpr std::string_view kNullBitmapEntry = "fm_index_null_bitmap";
constexpr std::string_view kTotalRowsMeta = "total_rows";
constexpr std::string_view kNullableMeta = "nullable";
constexpr size_t kMmapPadding = 64;

struct RuntimeParams {
    DataType value_type{DataType::VARCHAR};
    std::optional<bool> nullable;
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    RuntimeParams result;
    const auto field_type = ReadDataType(params, "field_type");
    const auto value_type = ReadDataType(params, "value_type");
    for (const auto type : {field_type, value_type}) {
        if (type.has_value() && !IsStringDataType(*type)) {
            ThrowInfo(DataTypeInvalid,
                      "FM-index requires a string value type, got {}",
                      static_cast<int>(*type));
        }
    }
    for (const auto key : {std::string_view("array_element_type"),
                           std::string_view("element_type")}) {
        const auto type = ReadDataType(params, key);
        if (type.has_value() && *type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "FM-index parameter {} must be NONE, got {}",
                      key,
                      static_cast<int>(*type));
        }
    }

    const auto nested = ReadNestedConfigParam(params, "FM-index");
    if (nested.value_or(false)) {
        ThrowInfo(DataTypeInvalid, "FM-index does not support nested input");
    }

    result.value_type =
        value_type.value_or(field_type.value_or(DataType::VARCHAR));
    if (params.contains("nullable") && !params.at("nullable").is_null()) {
        result.nullable = GetValueFromConfig<bool>(params, "nullable");
    }
    return result;
}

int64_t
ReadTotalRows(storage::FileSource& source) {
    const auto value = source.GetMeta(kTotalRowsMeta);
    if (!value.has_value()) {
        ThrowInfo(DataFormatBroken,
                  "FM-index V3 metadata total_rows is missing");
    }
    if (value->is_number_unsigned()) {
        const auto rows = value->get<uint64_t>();
        if (rows <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(rows);
        }
    } else if (value->is_number_integer()) {
        const auto rows = value->get<int64_t>();
        if (rows >= 0) {
            return rows;
        }
    }
    ThrowInfo(DataFormatBroken,
              "FM-index V3 total_rows must be a non-negative int64");
}

class StagingDirectory final {
 public:
    explicit StagingDirectory(const std::string& configured_parent) {
        std::error_code error;
        const auto parent = configured_parent.empty()
                                ? std::filesystem::temp_directory_path(error)
                                : std::filesystem::path(configured_parent);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to locate FM-index staging root: {}",
                      error.message());
        }
        std::filesystem::create_directories(parent, error);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to create FM-index staging root {}: {}",
                      parent.string(),
                      error.message());
        }
        auto pattern = (parent / "milvus-fmindex-load-XXXXXX").string();
        std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
        mutable_pattern.push_back('\0');
        if (::mkdtemp(mutable_pattern.data()) == nullptr) {
            ThrowInfo(FileCreateFailed,
                      "failed to create FM-index staging directory under {}: "
                      "{}",
                      parent.string(),
                      std::strerror(errno));
        }
        try {
            path_ = mutable_pattern.data();
        } catch (...) {
            std::error_code ignored;
            std::filesystem::remove_all(mutable_pattern.data(), ignored);
            throw;
        }
    }

    StagingDirectory(const StagingDirectory&) = delete;
    StagingDirectory&
    operator=(const StagingDirectory&) = delete;

    ~StagingDirectory() {
        if (!path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    const std::string&
    Path() const {
        return path_;
    }

    void
    Release() {
        path_.clear();
    }

 private:
    std::string path_;
};

class MappingGuard final {
 public:
    MappingGuard(void* data, size_t size) : data_(data), size_(size) {
    }

    MappingGuard(const MappingGuard&) = delete;
    MappingGuard&
    operator=(const MappingGuard&) = delete;

    ~MappingGuard() {
        if (data_ != nullptr && data_ != MAP_FAILED) {
            ::munmap(data_, size_);
        }
    }

    void
    Release() {
        data_ = nullptr;
        size_ = 0;
    }

 private:
    void* data_{nullptr};
    size_t size_{0};
};

size_t
ExpectedNullBitmapBytes(int64_t total_rows) {
    return static_cast<size_t>(total_rows / 8) +
           static_cast<size_t>(total_rows % 8 != 0);
}

void
PreflightNullBitmap(storage::FileSource& source,
                    bool nullable,
                    int64_t total_rows) {
    const auto has_entry = source.HasEntry(kNullBitmapEntry);
    if (!nullable) {
        if (has_entry) {
            ThrowInfo(DataFormatBroken,
                      "non-nullable FM-index contains a null bitmap entry");
        }
        return;
    }
    if (!has_entry) {
        ThrowInfo(DataFormatBroken,
                  "nullable FM-index is missing its null bitmap entry");
    }
    const auto observed = source.EntrySize(kNullBitmapEntry);
    const auto expected = ExpectedNullBitmapBytes(total_rows);
    if (observed < 0 || static_cast<uint64_t>(observed) != expected) {
        ThrowInfo(DataFormatBroken,
                  "FM-index null bitmap has declared size {}; expected {}",
                  observed,
                  expected);
    }
}

std::pair<size_t, size_t>
AppendMmapPadding(const std::string& path) {
    std::error_code error;
    const auto encoded_size = std::filesystem::file_size(path, error);
    if (error) {
        ThrowInfo(FileReadFailed,
                  "failed to stat staged FM-index blob {}: {}",
                  path,
                  error.message());
    }
    if (encoded_size == 0 ||
        encoded_size > std::numeric_limits<size_t>::max() - kMmapPadding) {
        ThrowInfo(DataFormatBroken,
                  "invalid staged FM-index blob size {}",
                  encoded_size);
    }
    const auto blob_size = static_cast<size_t>(encoded_size);

    const auto fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
    if (fd < 0) {
        ThrowInfo(FileOpenFailed,
                  "failed to open staged FM-index blob {} for padding: {}",
                  path,
                  std::strerror(errno));
    }
    static constexpr uint8_t kZeros[kMmapPadding] = {};
    size_t offset = 0;
    while (offset < kMmapPadding) {
        const auto written =
            ::write(fd, kZeros + offset, kMmapPadding - offset);
        if (written > 0) {
            offset += static_cast<size_t>(written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        const auto saved_errno = written == 0 ? EIO : errno;
        ::close(fd);
        ThrowInfo(FileWriteFailed,
                  "failed to pad staged FM-index blob {}: {}",
                  path,
                  std::strerror(saved_errno));
    }
    if (::fsync(fd) != 0) {
        const auto saved_errno = errno;
        ::close(fd);
        ThrowInfo(FileWriteFailed,
                  "failed to flush staged FM-index blob {}: {}",
                  path,
                  std::strerror(saved_errno));
    }
    if (::close(fd) != 0) {
        ThrowInfo(FileWriteFailed,
                  "failed to close staged FM-index blob {}: {}",
                  path,
                  std::strerror(errno));
    }
    return {blob_size, blob_size + kMmapPadding};
}

TargetBitmap
ReadNullBitmap(storage::FileSource& source, bool nullable, int64_t total_rows);

void
ValidateLoadedEngine(const fmindex::FMIndex& engine, int64_t total_rows) {
    if (!engine.valid()) {
        ThrowInfo(DataFormatBroken,
                  "FM-index blob failed structural validation");
    }
    if (engine.document_count() != static_cast<size_t>(total_rows)) {
        ThrowInfo(DataFormatBroken,
                  "FM-index metadata rows {} disagree with blob documents {}",
                  total_rows,
                  engine.document_count());
    }
}

std::shared_ptr<const FmIndexStorage>
OpenMmap(storage::FileSource& source,
         const storage::LoadOptions& opts,
         bool nullable,
         int64_t total_rows,
         DataType value_type) {
    StagingDirectory staging(opts.mmap_dir_path);
    const auto local_path =
        (std::filesystem::path(staging.Path()) / kBlobEntry).string();
    source.ReadEntryToLocalFile(kBlobEntry, local_path);
    const auto [blob_bytes, mapped_bytes] = AppendMmapPadding(local_path);

    const auto fd = ::open(local_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        ThrowInfo(FileOpenFailed,
                  "failed to open staged FM-index blob {}: {}",
                  local_path,
                  std::strerror(errno));
    }
    auto* mapped = ::mmap(nullptr, mapped_bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    const auto saved_errno = errno;
    ::close(fd);
    if (mapped == MAP_FAILED) {
        ThrowInfo(MmapError,
                  "failed to mmap staged FM-index blob {}: {}",
                  local_path,
                  std::strerror(saved_errno));
    }
    MappingGuard mapping_guard(mapped, mapped_bytes);

    auto mapped_file = std::make_shared<FmIndexMappedFile>(
        mapped, mapped_bytes, staging.Path());
    mapping_guard.Release();
    staging.Release();

    // The vendored library throws untyped std:: exceptions on a truncated or
    // oversized blob; without this boundary they reach cgo as
    // UnexpectedError(2001) instead of DataFormatBroken.
    auto engine = GuardFmIndexLibrary(
        [&] {
            return fmindex::FMIndex::LoadView(mapped_file->Data(), blob_bytes);
        },
        DataFormatBroken,
        "load");
    ValidateLoadedEngine(engine, total_rows);
    auto null_bitmap = ReadNullBitmap(source, nullable, total_rows);

    auto shared_engine =
        std::make_shared<const fmindex::FMIndex>(std::move(engine));
    // Keep the original mapping owner local until the throwing validation and
    // storage allocation complete; function-parameter destruction order is
    // not an ownership contract.
    return FmIndexStorage::Create(mapped_file,
                                  shared_engine,
                                  std::move(null_bitmap),
                                  total_rows,
                                  value_type,
                                  nullable,
                                  FmIndexStateOrigin::Persisted);
}

TargetBitmap
ReadNullBitmap(storage::FileSource& source, bool nullable, int64_t total_rows) {
    TargetBitmap result(static_cast<size_t>(total_rows), false);
    if (!nullable) {
        if (source.HasEntry(kNullBitmapEntry)) {
            ThrowInfo(DataFormatBroken,
                      "non-nullable FM-index contains a null bitmap entry");
        }
        return result;
    }
    if (!source.HasEntry(kNullBitmapEntry)) {
        ThrowInfo(DataFormatBroken,
                  "nullable FM-index is missing its null bitmap entry");
    }
    const auto packed = source.ReadEntry(kNullBitmapEntry);
    const auto expected = ExpectedNullBitmapBytes(total_rows);
    if (packed.size() != expected) {
        ThrowInfo(DataFormatBroken,
                  "FM-index null bitmap has {} bytes; expected {}",
                  packed.size(),
                  expected);
    }
    if (expected != 0 && total_rows % 8 != 0) {
        const auto tail_mask =
            static_cast<uint8_t>((1u << (total_rows % 8)) - 1u);
        if ((packed.back() & static_cast<uint8_t>(~tail_mask)) != 0) {
            ThrowInfo(DataFormatBroken,
                      "FM-index null bitmap has set padding bits");
        }
    }
    for (int64_t row = 0; row < total_rows; ++row) {
        if ((packed[static_cast<size_t>(row) >> 3] &
             static_cast<uint8_t>(1u << (row & 0x07))) != 0) {
            result.set(static_cast<size_t>(row));
        }
    }
    return result;
}

std::shared_ptr<const FmIndexStorage>
LoadStorage(storage::FileSource& source, const storage::LoadOptions& opts) {
    const auto runtime = ParseRuntimeParams(opts.params);
    if (source.Gen() != storage::Generation::V3) {
        ThrowInfo(DataFormatBroken,
                  "FM-index has no V1/V2 artifact representation");
    }
    AssertInfo(opts.mmap_dir_path.find('\0') == std::string::npos,
               "FM-index load options contain an invalid staging path");

    const auto total_rows = ReadTotalRows(source);
    if (static_cast<uint64_t>(total_rows) >
        static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "FM-index total_rows exceeds the local size_t domain");
    }
    const auto nullable =
        storage::ReadRequiredMeta<bool>(source, kNullableMeta, "FM-index V3");
    if (runtime.nullable.has_value() && *runtime.nullable != nullable) {
        ThrowInfo(DataFormatBroken,
                  "FM-index runtime nullable={} disagrees with artifact {}",
                  *runtime.nullable,
                  nullable);
    }
    if (!source.HasEntry(kBlobEntry)) {
        ThrowInfo(DataFormatBroken,
                  "FM-index blob entry fm_index.bin is missing");
    }
    PreflightNullBitmap(source, nullable, total_rows);

    if (opts.enable_mmap) {
        return OpenMmap(source, opts, nullable, total_rows, runtime.value_type);
    }

    auto blob = source.ReadEntry(kBlobEntry);
    // Same boundary as the mmap path above.
    auto engine = GuardFmIndexLibrary(
        [&] { return fmindex::FMIndex::Deserialize(std::move(blob)); },
        DataFormatBroken,
        "load");
    ValidateLoadedEngine(engine, total_rows);
    auto null_bitmap = ReadNullBitmap(source, nullable, total_rows);
    auto shared_engine =
        std::make_shared<const fmindex::FMIndex>(std::move(engine));
    return FmIndexStorage::Create({},
                                  std::move(shared_engine),
                                  std::move(null_bitmap),
                                  total_rows,
                                  runtime.value_type,
                                  nullable,
                                  FmIndexStateOrigin::Persisted);
}

}  // namespace

ReaderCaps
FmIndexLoader::DeriveCaps(const Config& index_meta) {
    static_cast<void>(ParseRuntimeParams(index_meta));
    return ReaderCaps{.pattern_match = true};
}

IIndexReaderBasePtr
FmIndexLoader::Open(storage::FileSource& source,
                    const storage::LoadOptions& opts) {
    return std::make_unique<FmIndexReader>(LoadStorage(source, opts));
}

namespace {

const bool kFmLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<FmIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
