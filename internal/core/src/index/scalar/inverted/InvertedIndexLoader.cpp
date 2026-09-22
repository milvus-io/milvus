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
#include "index/scalar/inverted/InvertedIndexLoader.h"
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
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "storage/artifact/FileSourceUtils.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"
#include "common/Slice.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/inverted/InvertedIndexArtifact.h"
#include "index/scalar/inverted/InvertedIndexReader.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

using inverted_params::IsSupportedType;

/**
 * @brief Normalized runtime field semantics used to interpret persisted data.
 */
struct RuntimeParams {
    DataType field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
    bool nested{false};
};

// Resolve scalar, array-element or JSON-cast types and reject incompatible
// domains.
RuntimeParams
ParseRuntimeParams(const Config& params) {
    RuntimeParams result;
    result.field_type =
        ReadDataTypeParam(params, "field_type").value_or(DataType::NONE);
    const auto element_type =
        ReadCompatibleScalarArrayElementType(params, "inverted");
    const auto configured =
        ReadDataTypeParam(params, "value_type").value_or(DataType::NONE);
    result.nested = ReadRequiredNestedParam(params, "inverted loader");

    if (result.field_type == DataType::ARRAY) {
        if (configured != DataType::NONE && configured != DataType::ARRAY &&
            element_type != DataType::NONE &&
            !ScalarValueTypesMatch(configured, element_type)) {
            ThrowInfo(DataTypeInvalid,
                      "inverted ARRAY value_type {} conflicts with element "
                      "type {}",
                      static_cast<int>(configured),
                      static_cast<int>(element_type));
        }
        result.value_type =
            element_type != DataType::NONE
                ? element_type
                : (configured != DataType::ARRAY ? configured : DataType::NONE);
    } else if (result.field_type == DataType::JSON) {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested inverted input requires ARRAY field_type");
        }
        result.value_type = configured;
    } else {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested inverted input requires ARRAY field_type");
        }
        if (element_type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "scalar inverted field conflicts with element type {}",
                      static_cast<int>(element_type));
        }
        result.value_type =
            configured != DataType::NONE ? configured : result.field_type;
        if (result.field_type != DataType::NONE &&
            !ScalarValueTypesMatch(result.field_type, result.value_type)) {
            ThrowInfo(DataTypeInvalid,
                      "inverted field type {} conflicts with value type {}",
                      static_cast<int>(result.field_type),
                      static_cast<int>(result.value_type));
        }
    }
    if (!IsSupportedType(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported inverted value type {}",
                  static_cast<int>(result.value_type));
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
        try {
            result.directory_parent =
                opts.params.at(MMAP_FILE_PATH).get<std::string>();
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "inverted mmap path must be a string: {}",
                      error.what());
        }
    }
    AssertInfo(result.directory_parent.find('\0') == std::string::npos,
               "inverted load options contain an invalid staging path");
    return result;
}

/**
 * @brief Validated legacy inventory separating engine files from null sidecars.
 */
struct PersistedEntries {
    std::vector<std::string> engine_files;
    bool has_null{false};
};

// Validate logical entry names and separate engine files from sidecars; no
// payload I/O.
PersistedEntries
ReadPersistedEntries(storage::FileSource& source) {
    PersistedEntries result;

    result.has_null = source.HasEntry(INDEX_NULL_OFFSET);
    for (const auto& name : source.EntryNames()) {
        storage::ValidateArtifactEntryName(name, "inverted index");
        if (name != INDEX_NULL_OFFSET && name != INDEX_TYPE &&
            name != INDEX_NON_EXIST_OFFSET_FILE_NAME &&
            name != INDEX_FILE_SLICE_META) {
            result.engine_files.push_back(name);
        }
    }

    if (result.engine_files.empty()) {
        ThrowInfo(DataFormatBroken, "inverted artifact has no engine files");
    }
    return result;
}

using storage::LocalEntryGuard;

using storage::FileDescriptorGuard;

// Stage null offsets before allocation; nested indexes retain
// element-versus-row semantics.
folly::coro::Task<std::shared_ptr<const std::vector<size_t>>>
ReadNullOffsets(bool use_async,
                const storage::LoadOptions& opts,
                storage::FileSource& source,
                bool has_null,
                const std::string& staging_parent,
                size_t count,
                bool nested) {
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
        auto staging = CreateInvertedIndexDirectory(staging_parent);
        auto path = (std::filesystem::path(staging->Path()) / INDEX_NULL_OFFSET)
                        .string();
        LocalEntryGuard local(std::move(path));
        co_await source.ReadEntryToLocalFileAsync(
            INDEX_NULL_OFFSET, local.Path(), use_async);
        const auto bytes = storage::LocalFileSize(
            local.Path(), "failed to determine inverted entry size for");

        if (bytes == 0 || bytes % sizeof(size_t) != 0) {
            ThrowInfo(DataFormatBroken,
                      "invalid inverted null-offset byte size {}",
                      bytes);
        }
        const auto offset_count = bytes / sizeof(size_t);
        if (!nested && offset_count > count) {
            ThrowInfo(DataFormatBroken,
                      "inverted null-offset count {} exceeds row count {}",
                      offset_count,
                      count);
        }

        std::vector<size_t> result(offset_count);
        const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
        if (fd == -1) {
            ThrowInfo(FileOpenFailed,
                      "failed to open inverted staging file {}: {}",
                      local.Path(),
                      std::strerror(errno));
        }
        FileDescriptorGuard descriptor(fd);
        storage::ReadAll(descriptor.Get(),
                         result.data(),
                         bytes,
                         local.Path(),
                         "inverted staging file");
        descriptor.CloseChecked(local.Path(), "inverted staging file");
        local.RemoveChecked("inverted staging file");
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

// Account for retained payload bytes with overflow checks, using local file
// sizes.
size_t
MaterializedBytes(const std::vector<std::string>& paths,
                  bool ram_payload_only) {
    size_t total = 0;
    for (const auto& path : paths) {
        // Tantivy's MmapDirectory::convert_to_ram_directory copies every
        // regular file except *.lock. Keep heap payload accounting aligned
        // with that exact ownership boundary; mmap readers retain every staged
        // file and therefore do not skip locks.
        if (ram_payload_only && std::string_view(path).ends_with(".lock")) {
            continue;
        }
        std::error_code error;
        const auto bytes = std::filesystem::file_size(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect inverted entry {}: {}",
                      path,
                      error.message());
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "inverted artifact byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    return total;
}

/**
 * @brief Own the initialized engine and its backing directory until reader
 * creation.
 */
struct InvertedLoadState {
    // Declaration order makes the engine release before its backing directory.
    std::shared_ptr<storage::LocalDirectory> directory;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine;
    std::shared_ptr<const std::vector<size_t>> null_offsets;
    RuntimeParams params;
    bool mmap{false};
    size_t engine_bytes{0};
};

// Materialize legacy Tantivy files, open the engine, then read sidecars using
// its row count.
folly::coro::Task<InvertedLoadState>
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

    // Keep local owners intact until the state has acquired its own shared
    // references. On every exception the engine is released before directory.
    std::shared_ptr<storage::LocalDirectory> directory;
    {
        auto local_io = [&] {
            directory =
                CreateInvertedIndexDirectory(effective.directory_parent);
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
                  "inverted source materialized {} of {} engine entries",
                  paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        if (std::filesystem::path(paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "inverted source materialized entry {} as {}",
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
                ThrowInfo(
                    DataFormatBroken,
                    "materialized inverted artifact is not a Tantivy index");
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
                                  static_cast<size_t>(engine->count()),
                                  params.nested));

    InvertedLoadState result;
    result.directory = directory;
    result.engine = engine;
    result.null_offsets = std::move(null_offsets);
    result.params = std::move(params);
    result.mmap = effective.mmap;
    result.engine_bytes = engine_bytes;
    co_return result;
}

// Share initialized state with the reader; retain the directory only for
// file-backed use.
std::unique_ptr<IIndexReaderBase>
MakeReader(const InvertedLoadState& state) {
    AssertInfo(state.directory != nullptr,
               "inverted load state requires a directory owner");
    AssertInfo(state.engine != nullptr,
               "inverted load state requires an engine");
    AssertInfo(state.null_offsets != nullptr,
               "inverted load state requires immutable null offsets");
    const auto engine_path_bytes = state.directory->PathHeapBytes();
    const auto make = [&]<typename T>() -> std::unique_ptr<IIndexReaderBase> {
        return std::make_unique<InvertedIndexReader<T>>(
            state.mmap ? state.directory : nullptr,
            state.engine,
            state.null_offsets,
            state.params.value_type,
            state.params.nested,
            state.mmap,
            state.engine_bytes,
            engine_path_bytes);
    };
    switch (state.params.value_type) {
        case DataType::BOOL:
            return make.template operator()<bool>();
        case DataType::INT8:
            return make.template operator()<int8_t>();
        case DataType::INT16:
            return make.template operator()<int16_t>();
        case DataType::INT32:
            return make.template operator()<int32_t>();
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return make.template operator()<int64_t>();
        case DataType::FLOAT:
            return make.template operator()<float>();
        case DataType::DOUBLE:
            return make.template operator()<double>();
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return make.template operator()<std::string_view>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported inverted value type {}",
                      static_cast<int>(state.params.value_type));
    }
}

/**
 * @brief Per-load payload owners and decoding state, retained by IndexLoadPlan.
 */
struct PackedInvertedState {
    PackedDirectoryTargets targets;
    RuntimeParams runtime;
    EffectiveLoadOptions effective;
    JsonProjectedOpenPlan projection;
};

}  // namespace

folly::coro::Task<std::unique_ptr<IndexLoader>>
InvertedIndexLoader::Open(IndexOpenRequest request) {
    return OpenIndexLoader(
        std::move(request),
        [](OpenedIndexInput input, storage::LoadOptions options)
            -> folly::coro::Task<std::unique_ptr<IndexLoader>> {
            auto loader = std::unique_ptr<InvertedIndexLoader>(
                new InvertedIndexLoader(std::move(input), std::move(options)));
            (void)DeriveCaps(loader->options_.params);
            if (const auto* legacy =
                    std::get_if<LegacyIndexSource>(&loader->input_)) {
                (void)ReadPersistedEntries(*legacy->source);
            }
            co_return loader;
        });
}

folly::coro::Task<IIndexReaderBasePtr>
InvertedIndexLoader::Load(milvus::OpContext* context) {
    if (auto* packed = std::get_if<PackedIndexSource>(&input_)) {
        return RunPackedIndexLoad(
            *packed, options_, &PlanPacked, &FinishPacked, context);
    }
    return RunLegacyLoad(
        std::get<LegacyIndexSource>(input_), options_, &LoadLegacy, context);
}

ReaderCaps
InvertedIndexLoader::DeriveCaps(const Config& index_meta) {
    const auto params = ParseRuntimeParams(index_meta);
    return DeriveJsonProjectedCaps(
        families::kInverted,
        index_meta,
        ReaderCaps{
            .predicate = true,
            .pattern_match = IsStringDataType(params.value_type),
            .nested = params.nested,
            .exact = !params.nested,
        });
}

folly::coro::Task<IIndexReaderBasePtr>
InvertedIndexLoader::LoadLegacy(storage::FileSource& source,
                                const storage::LoadOptions& opts,
                                bool use_async) {
    auto projection =
        PrepareJsonProjectedOpen(families::kInverted, source, opts);
    auto state = (co_await LoadState(
        use_async, source, opts, ParseRuntimeParams(opts.params)));
    auto reader = MakeReader(state);
    co_return (co_await FinishJsonProjectedOpenAsync(
        use_async, std::move(projection), source, std::move(reader)));
}

IndexLoadPlan
InvertedIndexLoader::PlanPacked(const storage::IndexEntryDirectory& directory,
                                const nlohmann::json& metadata,
                                const storage::LoadOptions& opts) {
    auto state = std::make_shared<PackedInvertedState>();
    IndexLoadPlan plan;
    plan.load_context = state;
    state->runtime = ParseRuntimeParams(opts.params);
    state->effective = ResolveLoadOptions(opts);
    state->projection = PreparePackedJsonProjectedOpen(
        kFamily, directory, metadata, opts, plan);
    state->targets.directory =
        CreateInvertedIndexDirectory(state->effective.directory_parent);
    const std::array<std::string_view, 2> reserved{
        INDEX_TYPE, INDEX_NON_EXIST_OFFSET_FILE_NAME};
    PlanPackedDirectory(directory,
                        metadata,
                        reserved,
                        false,
                        state->effective.mmap,
                        state->targets,
                        plan);
    return plan;
}

folly::coro::Task<IIndexReaderBasePtr>
InvertedIndexLoader::FinishPacked(IndexLoadPlan& plan,
                                  const storage::LoadOptions& opts,
                                  bool use_async) {
    const auto& state =
        std::any_cast<const std::shared_ptr<PackedInvertedState>&>(
            plan.load_context);
    AssertInfo(state != nullptr, "Inverted packed load context is null");
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
                      "materialized Inverted artifact is not a Tantivy index");
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
    auto null_offsets =
        FinishPackedNullOffsets(state->targets,
                                static_cast<size_t>(engine->count()),
                                state->runtime.nested);
    InvertedLoadState loaded;
    loaded.directory = directory;
    loaded.engine = engine;
    loaded.null_offsets = null_offsets;
    loaded.params = state->runtime;
    loaded.mmap = state->effective.mmap;
    loaded.engine_bytes = materialized_bytes;
    co_return FinishPackedJsonProjectedOpen(std::move(state->projection),
                                            MakeReader(loaded));
}

namespace {

const bool kInvertedLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<InvertedIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
