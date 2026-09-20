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
#include "index/scalar/ngram/NgramIndexLoader.h"
#include "index/IndexLoaderFactory.h"
#include "index/PackedIndexLoad.h"
#include "index/LegacyIndexLoad.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include "index/ParamUtils.h"
#include "index/scalar/PackedDirectoryLoad.h"
#include <array>
#include "index/scalar/ngram/NgramIndexParams.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "storage/artifact/FileSourceUtils.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"
#include "common/Slice.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "index/scalar/ngram/NgramIndexArtifact.h"
#include "index/scalar/ngram/NgramIndexReader.h"
#include "nlohmann/json.hpp"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

using ngram_params::ParseJsonPath;
using ngram_params::ParseString;
using ngram_params::ParseUnsigned;
using ngram_params::Upper;

constexpr std::string_view kAvgRowSizeEntry = "ngram_avg_row_size";
constexpr size_t kDefaultAvgRowSize = 5000;

// Reject element-domain input because NGRAM readers expose row coordinates.
void
ValidateRowDomain(const Config& params) {
    if (ReadNestedConfigParam(params, "NGRAM").value_or(false)) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM supports only the row coordinate domain");
    }
}

/**
 * @brief Normalized runtime field semantics used to interpret persisted data.
 */
struct RuntimeParams {
    DataType value_type{DataType::VARCHAR};
    uintptr_t min_gram{0};
    uintptr_t max_gram{0};
};

// Validate string/JSON-cast semantics, gram bounds and the supported Tantivy
// version.
RuntimeParams
ParseRuntimeParams(const Config& params) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid, "NGRAM load parameters must be an object");
    }
    ValidateRowDomain(params);

    const auto path = ParseJsonPath(params);
    RuntimeParams result;
    const auto field_type =
        ReadDataTypeParam(params, "field_type")
            .value_or(path.empty() ? DataType::VARCHAR : DataType::JSON);
    const auto configured_value_type =
        ReadDataTypeParam(params, "value_type")
            .value_or(field_type == DataType::JSON ? DataType::VARCHAR
                                                   : field_type);
    if (!IsStringDataType(configured_value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM value_type must be STRING, VARCHAR, or TEXT");
    }
    if (field_type == DataType::JSON) {
        if (path.empty()) {
            ThrowInfo(DataTypeInvalid, "JSON NGRAM requires json_path");
        }
        if (Upper(ParseString(params, JSON_CAST_TYPE)) != "VARCHAR") {
            ThrowInfo(DataTypeInvalid,
                      "JSON NGRAM requires VARCHAR json_cast_type");
        }
        result.value_type = DataType::VARCHAR;
    } else {
        if (!IsStringDataType(field_type)) {
            ThrowInfo(
                DataTypeInvalid,
                "NGRAM field_type must be STRING, VARCHAR, TEXT, or JSON");
        }
        if (!path.empty()) {
            ThrowInfo(DataTypeInvalid,
                      "scalar NGRAM must not carry a JSON path");
        }
        result.value_type = configured_value_type;
    }

    const auto min_gram = ParseUnsigned(params, MIN_GRAM, 0, true);
    const auto max_gram = ParseUnsigned(params, MAX_GRAM, 0, true);
    if (min_gram == 0 || max_gram == 0 || min_gram > max_gram ||
        min_gram > std::numeric_limits<uintptr_t>::max() ||
        max_gram > std::numeric_limits<uintptr_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "invalid NGRAM range min_gram={} max_gram={}",
                  min_gram,
                  max_gram);
    }
    result.min_gram = static_cast<uintptr_t>(min_gram);
    result.max_gram = static_cast<uintptr_t>(max_gram);

    const auto version = ParseUnsigned(params, TANTIVY_INDEX_VERSION, 0, false);
    if (version != 0 && version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM supports only Tantivy index version {}",
                  TANTIVY_INDEX_LATEST_VERSION);
    }
    return result;
}

/** @brief Resolved local storage preferences for this load. */
struct EffectiveLoadOptions {
    bool mmap{false};
    std::string directory_parent;
};

// Resolve explicit load options and legacy config fallbacks for local
// staging.
EffectiveLoadOptions
ResolveLoadOptions(const storage::LoadOptions& opts) {
    EffectiveLoadOptions result;
    result.mmap = opts.enable_mmap || GetValueFromConfigOrFallback<bool>(
                                          opts.params, ENABLE_MMAP, false);
    result.directory_parent = opts.mmap_dir_path;
    if (result.directory_parent.empty() && opts.params.is_object() &&
        opts.params.contains(MMAP_FILE_PATH)) {
        result.directory_parent = ParseString(opts.params, MMAP_FILE_PATH);
    }
    if (result.directory_parent.find('\0') != std::string::npos) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM staging directory must not contain NUL");
    }
    return result;
}

/**
 * @brief Validated legacy inventory separating engine files from null sidecars.
 */
struct PersistedEntries {
    std::vector<std::string> engine_files;
    bool has_null{false};
    bool has_avg{false};
};

// Validate logical entry names and separate engine files from sidecars; no
// payload I/O.
PersistedEntries
ReadPersistedEntries(storage::FileSource& source) {
    PersistedEntries result;

    result.has_null = source.HasEntry(INDEX_NULL_OFFSET);
    result.has_avg = source.HasEntry(kAvgRowSizeEntry);
    std::set<std::string> unique;
    for (const auto& name : source.EntryNames()) {
        storage::ValidateArtifactEntryName(name, "NGRAM index");
        if (name == INDEX_NULL_OFFSET || name == kAvgRowSizeEntry ||
            name == INDEX_TYPE || name == INDEX_NON_EXIST_OFFSET_FILE_NAME ||
            name == INDEX_FILE_SLICE_META) {
            continue;
        }
        if (!unique.insert(name).second) {
            ThrowInfo(
                DataFormatBroken, "duplicate NGRAM engine entry {}", name);
        }
        result.engine_files.push_back(name);
    }

    if (result.engine_files.empty()) {
        ThrowInfo(DataFormatBroken, "NGRAM artifact has no engine files");
    }
    return result;
}

using storage::LocalEntryGuard;

using storage::FileDescriptorGuard;

// Stage the null sidecar and validate its byte count before allocating the
// offset vector.
folly::coro::Task<std::shared_ptr<const std::vector<size_t>>>
ReadNullOffsets(bool use_async,
                const storage::LoadOptions& opts,
                storage::FileSource& source,
                bool has_null,
                const std::string& staging_parent,
                size_t count) {
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    auto run_io =
        [&]() -> folly::coro::Task<std::shared_ptr<const std::vector<size_t>>> {
        if (!has_null) {
            co_return std::make_shared<const std::vector<size_t>>();
        }

        // The sidecar uses a separate owned child so it can never be enumerated
        // as a Tantivy file by the publishable engine directory.
        auto staging = CreateNgramIndexDirectory(staging_parent);
        auto path = (std::filesystem::path(staging->Path()) / INDEX_NULL_OFFSET)
                        .string();
        LocalEntryGuard local(std::move(path));
        co_await source.ReadEntryToLocalFileAsync(
            INDEX_NULL_OFFSET, local.Path(), use_async);
        const auto bytes = storage::LocalFileSize(
            local.Path(), "failed to determine NGRAM entry size for");

        if (bytes == 0 || bytes % sizeof(size_t) != 0) {
            ThrowInfo(DataFormatBroken,
                      "invalid NGRAM null-offset byte size {}",
                      bytes);
        }
        const auto offset_count = bytes / sizeof(size_t);
        if (offset_count > count) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM null-offset count {} exceeds row count {}",
                      offset_count,
                      count);
        }

        std::vector<size_t> result(offset_count);
        const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
        if (fd == -1) {
            ThrowInfo(FileOpenFailed,
                      "failed to open NGRAM staging file {}: {}",
                      local.Path(),
                      std::strerror(errno));
        }
        FileDescriptorGuard descriptor(fd);
        storage::ReadAll(descriptor.Get(),
                         result.data(),
                         bytes,
                         local.Path(),
                         "NGRAM staging file");
        descriptor.CloseChecked(local.Path(), "NGRAM staging file");
        local.RemoveChecked("NGRAM staging file");
        co_return std::make_shared<const std::vector<size_t>>(
            std::move(result));
    };
    if (!use_async)
        co_return co_await run_io();
    co_return co_await folly::coro::co_withExecutor(
        storage::ResolveAsyncLoadExecutor(
            storage::LocalFileIOPool::GetInstance().GetExecutor(), priority),
        run_io());
}

// Read the native-size_t sidecar, using the historical default when it is
// absent.
folly::coro::Task<size_t>
ReadAvgRowSize(bool use_async, storage::FileSource& source, bool has_avg) {
    if (!has_avg) {
        co_return kDefaultAvgRowSize;
    }
    const auto bytes =
        co_await source.ReadEntryAsync(kAvgRowSizeEntry, use_async);
    if (bytes.size() != sizeof(size_t)) {
        ThrowInfo(DataFormatBroken,
                  "invalid NGRAM average-row-size byte size {}",
                  bytes.size());
    }
    size_t result = 0;
    std::memcpy(&result, bytes.data(), sizeof(result));
    co_return result;
}

// Account for retained payload bytes with overflow checks, using local file
// sizes.
size_t
MaterializedBytes(const std::vector<std::string>& paths,
                  bool ram_payload_only) {
    size_t total = 0;
    for (const auto& path : paths) {
        // Tantivy's in-RAM open copies every regular file except *.lock.
        if (ram_payload_only && std::string_view(path).ends_with(".lock")) {
            continue;
        }
        std::error_code error;
        const auto bytes = std::filesystem::file_size(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect NGRAM entry {}: {}",
                      path,
                      error.message());
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM artifact byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    return total;
}

/** @brief Per-load Tantivy engine, sidecars and directory retained until reader creation. */
struct NgramLoadState {
    // Declaration order makes the engine release before its backing directory.
    std::shared_ptr<storage::LocalDirectory> directory;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine;
    std::shared_ptr<const std::vector<size_t>> null_offsets;
    RuntimeParams params;
    size_t avg_row_size{0};
    bool mmap{false};
    size_t engine_bytes{0};
};

// Open staged Tantivy files and collect gram, null and resource state for
// reader creation.
folly::coro::Task<NgramLoadState>
LoadState(bool use_async,
          storage::FileSource& source,
          const storage::LoadOptions& opts,
          RuntimeParams params) {
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    const auto effective = ResolveLoadOptions(opts);
    const auto entries = ReadPersistedEntries(source);
    const auto avg_row_size =
        (co_await ReadAvgRowSize(use_async, source, entries.has_avg));

    // Keep local owners intact until the state has acquired its own shared
    // references. On every exception the engine is released before directory.
    std::shared_ptr<storage::LocalDirectory> directory;
    {
        auto local_io = [&] {
            directory = CreateNgramIndexDirectory(effective.directory_parent);
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(local_io, priority);
        } else {
            local_io();
        }
    }

    const auto paths = co_await source.ReadEntriesToLocalDirAsync(
        entries.engine_files, directory->Path(), use_async);
    if (paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "NGRAM source materialized {} of {} engine entries",
                  paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        if (std::filesystem::path(paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "NGRAM source materialized entry {} as {}",
                      entries.engine_files[i],
                      paths[i]);
        }
    }
    size_t engine_bytes = 0;
    {
        auto local_io = [&] {
            engine_bytes = MaterializedBytes(paths, !effective.mmap);
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(local_io, priority);
        } else {
            local_io();
        }
    }

    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine;
    {
        auto local_io = [&] {
            if (!tantivy_index_exist(directory->Path().c_str())) {
                ThrowInfo(DataFormatBroken,
                          "materialized NGRAM artifact is not a Tantivy index");
            }
            engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
                directory->Path().c_str(), effective.mmap, SetBitsetSealed);
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(local_io, priority);
        } else {
            local_io();
        }
    }

    auto null_offsets =
        (co_await ReadNullOffsets(use_async,
                                  opts,
                                  source,
                                  entries.has_null,
                                  effective.directory_parent,
                                  static_cast<size_t>(engine->count())));

    NgramLoadState result;
    result.directory = directory;
    result.engine = engine;
    result.null_offsets = std::move(null_offsets);
    result.params = std::move(params);
    result.avg_row_size = avg_row_size;
    result.mmap = effective.mmap;
    result.engine_bytes = engine_bytes;
    co_return result;
}

// Share initialized state with the reader; retain the directory only for
// file-backed use.
std::unique_ptr<IIndexReaderBase>
MakeReader(const NgramLoadState& state) {
    AssertInfo(state.directory != nullptr,
               "NGRAM load state requires a directory owner");
    AssertInfo(state.engine != nullptr, "NGRAM load state requires an engine");
    AssertInfo(state.null_offsets != nullptr,
               "NGRAM load state requires immutable null offsets");
    return std::make_unique<NgramIndexReader>(
        state.mmap ? state.directory : nullptr,
        state.engine,
        state.null_offsets,
        state.params.value_type,
        state.params.min_gram,
        state.params.max_gram,
        state.avg_row_size,
        state.mmap,
        state.engine_bytes);
}

/**
 * @brief Per-load payload owners and decoding state, retained by IndexLoadPlan.
 */
struct PackedNgramState {
    PackedDirectoryTargets targets;
    RuntimeParams runtime;
    EffectiveLoadOptions effective;
    JsonProjectedOpenPlan projection;
    std::shared_ptr<size_t> avg_row_size;
};

}  // namespace

folly::coro::Task<std::unique_ptr<IndexLoader>>
NgramIndexLoader::Open(IndexOpenRequest request) {
    return OpenIndexLoader(
        std::move(request),
        [](OpenedIndexInput input, storage::LoadOptions options)
            -> folly::coro::Task<std::unique_ptr<IndexLoader>> {
            auto loader = std::unique_ptr<NgramIndexLoader>(
                new NgramIndexLoader(std::move(input), std::move(options)));
            (void)DeriveCaps(loader->options_.params);
            if (const auto* legacy =
                    std::get_if<LegacyIndexSource>(&loader->input_)) {
                (void)ReadPersistedEntries(*legacy->source);
            }
            co_return loader;
        });
}

folly::coro::Task<IIndexReaderBasePtr>
NgramIndexLoader::Load(milvus::OpContext* context) {
    if (auto* packed = std::get_if<PackedIndexSource>(&input_)) {
        return RunPackedIndexLoad(
            *packed, options_, &PlanPacked, &FinishPacked, context);
    }
    return RunLegacyLoad(
        std::get<LegacyIndexSource>(input_), options_, &LoadLegacy, context);
}

ReaderCaps
NgramIndexLoader::DeriveCaps(const Config& index_meta) {
    static_cast<void>(ParseRuntimeParams(index_meta));
    return DeriveJsonProjectedCaps(
        families::kNgram,
        index_meta,
        ReaderCaps{.ngram_candidates = true, .exact = false});
}

folly::coro::Task<IIndexReaderBasePtr>
NgramIndexLoader::LoadLegacy(storage::FileSource& source,
                             const storage::LoadOptions& opts,
                             bool use_async) {
    auto projection = PrepareJsonProjectedOpen(families::kNgram, source, opts);
    auto state = (co_await LoadState(
        use_async, source, opts, ParseRuntimeParams(opts.params)));
    auto reader = MakeReader(state);
    co_return (co_await FinishJsonProjectedOpenAsync(
        use_async, std::move(projection), source, std::move(reader)));
}

IndexLoadPlan
NgramIndexLoader::PlanPacked(const storage::IndexEntryDirectory& directory,
                             const nlohmann::json& metadata,
                             const storage::LoadOptions& opts) {
    auto state = std::make_shared<PackedNgramState>();
    IndexLoadPlan plan;
    plan.load_context = state;
    state->runtime = ParseRuntimeParams(opts.params);
    state->effective = ResolveLoadOptions(opts);
    state->projection = PreparePackedJsonProjectedOpen(
        kFamily, directory, metadata, opts, plan);
    state->targets.directory =
        CreateNgramIndexDirectory(state->effective.directory_parent);
    const std::array<std::string_view, 3> reserved{
        INDEX_TYPE, INDEX_NON_EXIST_OFFSET_FILE_NAME, kAvgRowSizeEntry};
    PlanPackedDirectory(directory,
                        metadata,
                        reserved,
                        false,
                        state->effective.mmap,
                        state->targets,
                        plan);
    if (!directory.HasEntry(kAvgRowSizeEntry) ||
        directory.At(kAvgRowSizeEntry).plaintext_size != sizeof(size_t)) {
        ThrowInfo(DataFormatBroken,
                  "invalid or missing NGRAM average-row-size entry");
    }
    state->avg_row_size = std::make_shared<size_t>(0);
    plan.entries.push_back(
        {std::string(kAvgRowSizeEntry),
         storage::MemoryEntryTarget{
             state->avg_row_size,
             reinterpret_cast<uint8_t*>(state->avg_row_size.get()),
             sizeof(size_t)}});
    return plan;
}

folly::coro::Task<IIndexReaderBasePtr>
NgramIndexLoader::FinishPacked(IndexLoadPlan& plan,
                               const storage::LoadOptions& opts,
                               bool use_async) {
    const auto& state = std::any_cast<const std::shared_ptr<PackedNgramState>&>(
        plan.load_context);
    AssertInfo(state != nullptr, "Ngram packed load context is null");
    const auto& directory = state->targets.directory;
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine;
    size_t materialized_bytes = 0;
    auto local_io = [&] {
        if (!tantivy_index_exist(directory->Path().c_str())) {
            ThrowInfo(DataFormatBroken,
                      "materialized Ngram artifact is not a Tantivy index");
        }
        engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
            directory->Path().c_str(), state->effective.mmap, SetBitsetSealed);

        materialized_bytes =
            MaterializedBytes(state->targets.paths, !state->effective.mmap);
    };
    if (use_async) {
        co_await storage::RunLocalFileIOAsync(local_io, priority);
    } else {
        local_io();
    }
    auto null_offsets = FinishPackedNullOffsets(
        state->targets, static_cast<size_t>(engine->count()));
    NgramLoadState loaded;
    loaded.directory = directory;
    loaded.engine = engine;
    loaded.null_offsets = null_offsets;
    loaded.params = state->runtime;
    loaded.mmap = state->effective.mmap;
    loaded.engine_bytes = materialized_bytes;
    loaded.avg_row_size = *state->avg_row_size;
    co_return FinishPackedJsonProjectedOpen(std::move(state->projection),
                                            MakeReader(loaded));
}

namespace {

const bool kNgramLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<NgramIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
