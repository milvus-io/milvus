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

#include "storage/LocalFileIOPool.h"
#include "common/OpContext.h"
#include "index/scalar/fmindex/FmIndexLoader.h"
#include "index/IndexLoaderFactory.h"
#include "index/PackedIndexLoad.h"

#include <algorithm>
#include <bit>
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

/**
 * @brief Normalized runtime field semantics used to interpret persisted data.
 */
struct RuntimeParams {
    DataType value_type{DataType::VARCHAR};
    std::optional<bool> nullable;
};

// Require row-domain strings and preserve an optional runtime nullability
// constraint.
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

/** @brief Remove the staging directory unless handed to the mapped reader. */
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

    // The mapped reader now owns directory cleanup.
    void
    Release() {
        path_.clear();
    }

 private:
    std::string path_;
};

/** @brief Unmap a new view unless ownership reaches FmIndexMappedFile. */
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

    // FmIndexMappedFile now owns munmap; disarm rollback.
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

// Cross-check the decoded FM blob structure and document count against
// metadata.
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

/**
 * @brief Per-load FM blob and null bitmap owners retained by IndexLoadPlan.
 * @note Null bitmap storage becomes the final query allocation.
 */
struct PackedFmState {
    RuntimeParams runtime;
    int64_t total_rows{0};
    bool nullable{false};
    size_t blob_bytes{0};
    std::shared_ptr<TargetBitmap> nulls;
    std::shared_ptr<std::vector<uint8_t>> blob;
    std::shared_ptr<StagingDirectory> directory;
    std::shared_ptr<storage::IndexFileTarget> file;
};

// Validate row/nullability metadata and entry sizes without allocating
// payload targets.
void
ValidatePackedMetadata(PackedFmState& state,
                       const storage::IndexEntryDirectory& directory,
                       const nlohmann::json& metadata,
                       const storage::LoadOptions& opts) {
    state.runtime = ParseRuntimeParams(opts.params);
    if (!metadata.contains(kTotalRowsMeta) ||
        !metadata.contains(kNullableMeta)) {
        ThrowInfo(DataFormatBroken,
                  "FM-index is missing total_rows or nullable metadata");
    }
    const auto& rows = metadata.at(kTotalRowsMeta);
    if ((!rows.is_number_unsigned() && !rows.is_number_integer()) ||
        (rows.is_number_unsigned() &&
         rows.get<uint64_t>() >
             static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) ||
        (rows.is_number_integer() && !rows.is_number_unsigned() &&
         rows.get<int64_t>() < 0) ||
        !metadata.at(kNullableMeta).is_boolean()) {
        ThrowInfo(DataFormatBroken,
                  "invalid FM-index total_rows or nullable metadata");
    }
    state.total_rows = rows.get<int64_t>();
    state.nullable = metadata.at(kNullableMeta).get<bool>();
    if (static_cast<uint64_t>(state.total_rows) >
        std::numeric_limits<size_t>::max()) {
        ThrowInfo(DataFormatBroken, "FM-index row count exceeds size_t");
    }
    if (state.runtime.nullable && *state.runtime.nullable != state.nullable) {
        ThrowInfo(DataFormatBroken,
                  "FM-index runtime nullable disagrees with artifact");
    }
    if (!directory.HasEntry(kBlobEntry)) {
        ThrowInfo(DataFormatBroken, "FM-index blob is missing");
    }
    state.blob_bytes = directory.At(kBlobEntry).plaintext_size;
    if (state.blob_bytes == 0 ||
        state.blob_bytes > std::numeric_limits<size_t>::max() - kMmapPadding) {
        ThrowInfo(DataFormatBroken, "invalid FM-index blob size");
    }
    const bool has_nulls = directory.HasEntry(kNullBitmapEntry);
    if (has_nulls != state.nullable ||
        (has_nulls && directory.At(kNullBitmapEntry).plaintext_size !=
                          ExpectedNullBitmapBytes(state.total_rows))) {
        ThrowInfo(DataFormatBroken,
                  "FM-index null bitmap disagrees with metadata");
    }
    AssertInfo(opts.mmap_dir_path.find('\0') == std::string::npos,
               "FM-index load options contain an invalid staging path");
}

}  // namespace

folly::coro::Task<std::unique_ptr<IndexLoader>>
FmIndexLoader::Open(IndexOpenRequest request) {
    return OpenIndexLoader(
        std::move(request),
        [](OpenedIndexInput input, storage::LoadOptions options)
            -> folly::coro::Task<std::unique_ptr<IndexLoader>> {
            (void)DeriveCaps(options.params);
            if (!std::holds_alternative<PackedIndexSource>(input)) {
                ThrowInfo(DataFormatBroken,
                          "FM-index has no V1/V2 artifact representation");
            }
            auto loader = std::unique_ptr<FmIndexLoader>(
                new FmIndexLoader(std::get<PackedIndexSource>(std::move(input)),
                                  std::move(options)));
            PackedFmState metadata;
            ValidatePackedMetadata(metadata,
                                   loader->input_.Directory(),
                                   loader->input_.Metadata(),
                                   loader->options_);
            co_return loader;
        });
}

folly::coro::Task<IIndexReaderBasePtr>
FmIndexLoader::Load(milvus::OpContext* context) {
    return RunPackedIndexLoad(
        input_, options_, &PlanPacked, &FinishPacked, context);
}

ReaderCaps
FmIndexLoader::DeriveCaps(const Config& index_meta) {
    static_cast<void>(ParseRuntimeParams(index_meta));
    return ReaderCaps{.pattern_match = true};
}

IndexLoadPlan
FmIndexLoader::PlanPacked(const storage::IndexEntryDirectory& directory,
                          const nlohmann::json& metadata,
                          const storage::LoadOptions& opts) {
    auto state = std::make_shared<PackedFmState>();
    ValidatePackedMetadata(*state, directory, metadata, opts);
    IndexLoadPlan plan;
    plan.load_context = state;
    plan.entries.reserve(state->nullable ? 2 : 1);
    if (opts.enable_mmap) {
        state->directory =
            std::make_shared<StagingDirectory>(opts.mmap_dir_path);
        const auto path =
            (std::filesystem::path(state->directory->Path()) / kBlobEntry)
                .string();
        state->file = std::make_shared<storage::IndexFileTarget>(
            path, state->blob_bytes + kMmapPadding, true);
        plan.entries.push_back(
            {std::string(kBlobEntry),
             storage::FileEntryTarget{state->file, 0, state->file->file_size}});
    } else {
        state->blob = std::make_shared<std::vector<uint8_t>>(state->blob_bytes);
        plan.entries.push_back(
            {std::string(kBlobEntry),
             storage::MemoryEntryTarget{
                 state->blob, state->blob->data(), state->blob->size()}});
    }
    if (state->nullable) {
        state->nulls = std::make_shared<TargetBitmap>(state->total_rows, false);
        static_assert(std::endian::native == std::endian::little);
        plan.entries.push_back(
            {std::string(kNullBitmapEntry),
             storage::MemoryEntryTarget{
                 state->nulls,
                 reinterpret_cast<uint8_t*>(state->nulls->data()),
                 ExpectedNullBitmapBytes(state->total_rows)}});
    }
    return plan;
}

folly::coro::Task<IIndexReaderBasePtr>
FmIndexLoader::FinishPacked(IndexLoadPlan& plan,
                            const storage::LoadOptions& opts,
                            bool use_async) {
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    const auto state =
        std::any_cast<std::shared_ptr<PackedFmState>>(plan.load_context);
    if (state->nullable && state->total_rows % 8 != 0) {
        const auto* bytes =
            reinterpret_cast<const uint8_t*>(state->nulls->data());
        const auto mask =
            static_cast<uint8_t>((1u << (state->total_rows % 8)) - 1u);
        if ((bytes[state->total_rows / 8] & static_cast<uint8_t>(~mask)) != 0) {
            ThrowInfo(DataFormatBroken,
                      "FM-index null bitmap has set padding bits");
        }
    }
    std::shared_ptr<FmIndexMappedFile> mapping;
    fmindex::FMIndex engine;
    if (state->file) {
        AssertInfo(state->file->Prepared(),
                   "FM-index file target was not prepared");
        auto local_io = [&] {
            const auto& path = state->file->path;
            const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
            if (fd < 0) {
                ThrowInfo(FileOpenFailed,
                          "failed to open FM-index target {}: {}",
                          path,
                          std::strerror(errno));
            }
            auto* data = ::mmap(
                nullptr, state->file->file_size, PROT_READ, MAP_PRIVATE, fd, 0);
            const auto saved_errno = errno;
            ::close(fd);
            if (data == MAP_FAILED) {
                ThrowInfo(MmapError,
                          "failed to map FM-index target {}: {}",
                          path,
                          std::strerror(saved_errno));
            }
            MappingGuard guard(data, state->file->file_size);
            mapping = std::make_shared<FmIndexMappedFile>(
                data, state->file->file_size, state->directory->Path());
            guard.Release();
            state->directory->Release();
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(local_io, priority);
        } else {
            local_io();
        }
        engine = GuardFmIndexLibrary(
            [&] {
                return fmindex::FMIndex::LoadView(mapping->Data(),
                                                  state->blob_bytes);
            },
            DataFormatBroken,
            "load");
    } else {
        engine = GuardFmIndexLibrary(
            [&] {
                return fmindex::FMIndex::Deserialize(std::move(*state->blob));
            },
            DataFormatBroken,
            "load");
    }
    ValidateLoadedEngine(engine, state->total_rows);
    auto shared_engine =
        std::make_shared<const fmindex::FMIndex>(std::move(engine));
    auto nulls = state->nulls ? std::move(*state->nulls)
                              : TargetBitmap(state->total_rows, false);
    auto storage = FmIndexStorage::Create(mapping,
                                          shared_engine,
                                          std::move(nulls),
                                          state->total_rows,
                                          state->runtime.value_type,
                                          state->nullable,
                                          FmIndexStateOrigin::Persisted);
    co_return std::make_unique<FmIndexReader>(std::move(storage));
}

namespace {

const bool kFmLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<FmIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
