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

#include "common/OpContext.h"
#include "storage/LocalFileIOPool.h"
#include "index/vector/VectorDiskLoader.h"
#include "index/IndexLoaderFactory.h"
#include "index/LegacyIndexLoad.h"
#include "index/vector/VectorLoadUtils.h"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "index/Meta.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorIndexReader.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorDiskAnnUtils.h"
#include "index/vector/VectorLoadResource.h"
#include "index/vector/VectorParamUtils.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/DiskEngineFileHandle.h"

namespace milvus::index {
namespace {

using vector_load_params::ArtifactState;
using vector_load_params::DimensionSource;
using vector_load_params::LoadBackend;
using vector_load_params::NormalizedLoadMetadata;
using vector_load_params::ParseIdMapMmapMetadata;
using vector_load_params::ParseNormalizedLoadMetadata;
using vector_load_params::ParseVectorShapeMetadata;
using vector_load_params::ValidateLoadedDimension;
using vector_load_params::ValidateLoadedShape;

constexpr const char* kEnableDiskMmap = "enable_disk_mmap";
constexpr uint32_t kDefaultBeamwidth = 8;

// Range-check a normalized integer before narrowing to a disk-backend option.
uint32_t
ParseUint32(const nlohmann::json& value, std::string_view key) {
    const auto parsed =
        vector_params::ParseInt64(value, key, "normalized vector parameter");
    if (parsed < 0 ||
        static_cast<uint64_t>(parsed) >
            static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
        ThrowInfo(UnexpectedError,
                  "normalized vector parameter {} is outside uint32",
                  key);
    }
    return static_cast<uint32_t>(parsed);
}

using RuntimeParams = NormalizedLoadMetadata;

/**
 * @brief Validated disk-backend initialization knobs, separate from transport.
 */
struct DiskOpenOptions {
    uint32_t beamwidth{kDefaultBeamwidth};
    std::optional<int32_t> load_threads;
};

// Validate DiskANN-specific initialization settings; other backends use
// defaults.
DiskOpenOptions
ParseOpenOptions(const Config& params, const RuntimeParams& runtime) {
    DiskOpenOptions result;
    if (runtime.index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        if (params.contains(DISK_ANN_QUERY_BEAMWIDTH)) {
            result.beamwidth = ParseUint32(params.at(DISK_ANN_QUERY_BEAMWIDTH),
                                           DISK_ANN_QUERY_BEAMWIDTH);
        }
        vector_disk_ann::ValidateBeamwidth(result.beamwidth);
        if (!params.contains(DISK_ANN_LOAD_THREAD_NUM)) {
            ThrowInfo(ConfigInvalid,
                      "DiskANN load requires {}",
                      DISK_ANN_LOAD_THREAD_NUM);
        }
        result.load_threads = vector_params::ParsePositiveInt32(
            params.at(DISK_ANN_LOAD_THREAD_NUM), DISK_ANN_LOAD_THREAD_NUM);
    }
    return result;
}

// Require disk-load routing and normalize vector shape metadata.
RuntimeParams
ParseDiskLoadMetadata(const Config& params) {
    auto result = ParseNormalizedLoadMetadata(params, LoadBackend::Disk);
    if (!VectorUsesDiskLoad(result.index_type, result.version)) {
        ThrowInfo(UnexpectedError,
                  "memory-load vector index {} was routed to vector_disk",
                  result.index_type);
    }
    ParseVectorShapeMetadata(params, result);
    return result;
}

// Combine disk-load metadata with validity-map mmap preferences.
RuntimeParams
ParseRuntimeParams(const Config& params) {
    auto result = ParseDiskLoadMetadata(params);
    ParseIdMapMmapMetadata(params, result);
    return result;
}

/**
 * @brief Validated inventory and empty-index state; owns names, not payloads.
 */
struct EntryPlan {
    ArtifactState state{ArtifactState::Normal};
    std::vector<std::string> engine_names;
    bool has_validity{false};
    bool has_empty_offsets{false};
};

// Classify engine and sidecar inventory, including all-null and empty
// embedding-list artifacts.
EntryPlan
PlanEntries(storage::FileSource& source, const RuntimeParams& params) {
    EntryPlan plan;
    std::unordered_set<std::string> unique;
    for (const auto& name : source.EntryNames()) {
        if (name.empty() || !unique.insert(name).second) {
            ThrowInfo(DataFormatBroken,
                      "disk vector artifact has an empty or duplicate entry "
                      "name");
        }
        if (name == VALID_DATA_KEY) {
            plan.has_validity = true;
        } else if (name == VALID_DATA_COUNT_KEY) {
            ThrowInfo(DataFormatBroken,
                      "disk vector artifact contains memory-format validity");
        } else if (name == EMPTY_EMB_LIST_OFFSETS_KEY) {
            plan.has_empty_offsets = true;
        } else {
            plan.engine_names.push_back(name);
        }
    }

    if (plan.has_empty_offsets) {
        if (params.elem_type == DataType::NONE || !plan.engine_names.empty()) {
            ThrowInfo(DataFormatBroken,
                      "empty embedding-list artifact contains incompatible "
                      "engine entries");
        }
        plan.state = ArtifactState::EmptyEmbeddingList;
    } else if (plan.engine_names.empty()) {
        if (!plan.has_validity) {
            ThrowInfo(DataFormatBroken,
                      "disk vector artifact has no loadable state");
        }
        plan.state = ArtifactState::AllNull;
    }
    return plan;
}

// Disk artifacts store the count header and the bitmap in one entry. The
// decoded bytes stay owned by the caller: knowhere's IdMapData only views the
// bitmap until AddFromData consumes it.
ValidDataView
DecodeValidityBytes(const std::vector<uint8_t>& bytes) {
    if (bytes.size() < sizeof(uint64_t)) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector disk valid_data file is too small");
    }
    uint64_t wire_count = 0;
    std::memcpy(&wire_count, bytes.data(), sizeof(wire_count));
    const auto count = FromValidDataCount(wire_count);
    const auto bitmap_size = GetValidDataBitmapSize(count);
    if (bytes.size() < sizeof(uint64_t) + bitmap_size) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector disk valid_data bitmap is truncated");
    }
    return {true, count, bytes.data() + sizeof(uint64_t)};
}

// Publish the persisted validity into the engine's knowhere IdMap (#50524).
// Must run before Deserialize, which is what derives the dense id arrays.
folly::coro::Task<RestoredIdMap>
RestoreValidity(bool use_async,
                const storage::LoadOptions& opts,
                storage::FileSource& source,
                const EntryPlan& plan,
                const RuntimeParams& params,
                KnowhereEngine& engine,
                const std::string& local_prefix,
                std::vector<uint8_t>& owned_bytes) {
    if (!plan.has_validity) {
        co_return {};
    }
    owned_bytes = co_await source.ReadEntryAsync(VALID_DATA_KEY, use_async);
    const auto valid_data = DecodeValidityBytes(owned_bytes);
    auto mmap_flags = params.id_map_mmap;
    // A fully null column derives no dense id array at all, so mmap staging
    // would only create an empty directory.
    if (mmap_flags.Any() &&
        CountValidDataBitmap(valid_data.count, valid_data.bitmap) == 0) {
        mmap_flags = {};
    }
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    RestoredIdMap result;
    auto local_io = [&] {
        result = RestoreIdMapFromValidData(engine.native_index.GetIdMap(),
                                           valid_data,
                                           mmap_flags,
                                           local_prefix);
    };
    if (use_async && mmap_flags.Any()) {
        co_await storage::RunLocalFileIOAsync(local_io, priority);
    } else {
        local_io();
    }
    co_return result;
}

using detail::EmptyEmbeddingListState;

// Validate the persisted empty-list dimension and all-zero offset prefix sum.
EmptyEmbeddingListState
DecodeEmptyEmbeddingListBytes(const std::vector<uint8_t>& bytes) {
    constexpr auto header_size = detail::kEmptyEmbeddingListHeaderSize;
    if (bytes.size() < header_size) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset entry is too small");
    }
    auto result =
        detail::DecodeEmptyEmbeddingListPayload(bytes.data(), bytes.size());
    if (!detail::IsValidEmptyEmbeddingListDimension(result.dim) ||
        !detail::IsValidEmptyEmbeddingListOffsets(result.offsets)) {
        ThrowInfo(DataFormatBroken, "empty embedding-list state is invalid");
    }
    return result;
}

folly::coro::Task<EmptyEmbeddingListState>
DecodeEmptyEmbeddingList(bool use_async, storage::FileSource& source) {
    co_return DecodeEmptyEmbeddingListBytes(
        co_await source.ReadEntryAsync(EMPTY_EMB_LIST_OFFSETS_KEY, use_async));
}

// Replace caller path/mmap settings with the selected backend generation
// settings.
void
PrepareCommonLoadConfig(Config& config,
                        const RuntimeParams& params,
                        const DiskOpenOptions& open_options,
                        const std::string& prefix,
                        bool stream_backend,
                        bool enable_mmap) {
    config.erase(MMAP_FILE_PATH);
    config.erase(kEnableDiskMmap);
    config[ENABLE_MMAP] = enable_mmap;
    if (stream_backend &&
        params.index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        config[kEnableDiskMmap] = enable_mmap;
    }
    config[DISK_ANN_PREFIX_PATH] = prefix;
    if (params.index_type != knowhere::IndexEnum::INDEX_DISKANN) {
        return;
    }
    config[DISK_ANN_PREPARE_WARM_UP] = false;
    config[DISK_ANN_PREPARE_USE_BFS_CACHE] = false;
    AssertInfo(open_options.load_threads.has_value(),
               "validated DiskANN load thread count is missing");
    config[DISK_ANN_THREADS_NUM] = *open_options.load_threads;
}

// Create the backend mmap prefix, reporting filesystem failures before
// deserialization.
void
EnsureMmapDirectory(const std::string& path) {
    std::error_code error;
    std::filesystem::create_directories(path, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create disk vector mmap directory {}: {}",
                  path,
                  error.message());
    }
}

// Preserve the Knowhere status category while adding deserialization context.
[[noreturn]] void
ThrowDeserializeError(knowhere::Status status) {
    ThrowInfo(KnowhereStatusToErrorCode(status),
              "failed to deserialize disk vector index: status {} ({})",
              static_cast<int>(status),
              knowhere::Status2String(status));
}

// Construct Knowhere with the file handle retained as the backing-resource
// owner.
KnowhereEngine
MakeEngine(const RuntimeParams& params,
           const std::shared_ptr<storage::DiskEngineFileHandle>& handle) {
    auto manager = handle->Manager();
    auto pack = knowhere::Pack(std::move(manager));
    return KnowhereEngine(params.physical_type,
                          params.elem_type,
                          params.index_type,
                          params.metric_type,
                          params.version,
                          handle,
                          pack,
                          true);
}

// Select streaming or materialized engine input, restore sidecars and
// deserialize one reader.
folly::coro::Task<std::unique_ptr<IIndexReaderBase>>
OpenIndex(bool use_async,
          storage::FileSource& source,
          const RuntimeParams& params,
          const DiskOpenOptions& open_options,
          const EntryPlan& plan,
          const storage::LoadOptions& opts) {
    const auto priority =
        opts.op_ctx && opts.op_ctx->runtime_load_priority.value_or(0) != 0
            ? proto::common::LoadPriority::LOW
            : proto::common::LoadPriority::HIGH;
    // Handles precede the engine so failed construction/deserialization always
    // destroys the native node before its manager and local generation.
    std::shared_ptr<storage::DiskEngineFileHandle> local_handle, stream_handle,
        selected_handle;
    std::optional<KnowhereEngine> owned_engine;
    bool stream_backend = false;
    {
        auto local_io = [&] {
            local_handle = source.OpenDiskEngineFiles(
                storage::DiskEngineFileMode::LocalFiles, plan.engine_names);
            selected_handle = local_handle;
            owned_engine.emplace(MakeEngine(params, local_handle));
            auto& engine = *owned_engine;
            stream_backend = plan.state == ArtifactState::Normal &&
                             engine.native_index.LoadIndexWithStream();
            if (stream_backend) {
                stream_handle = source.OpenDiskEngineFiles(
                    storage::DiskEngineFileMode::RemoteStreams,
                    plan.engine_names);
                auto stream_engine = MakeEngine(params, stream_handle);
                AssertInfo(
                    stream_engine.native_index.LoadIndexWithStream(),
                    "disk vector stream capability changed while opening");
                engine = std::move(stream_engine);
                selected_handle = stream_handle;
            }
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(local_io, priority);
        } else {
            local_io();
        }
    }
    auto& engine = *owned_engine;
    const auto prefix = selected_handle->LocalPrefix();
    // Keep the decoded payload alive until AddFromData has copied it.
    std::vector<uint8_t> validity_bytes;
    const auto restored_id_map = co_await RestoreValidity(
        use_async, opts, source, plan, params, engine, prefix, validity_bytes);

    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        auto empty = co_await DecodeEmptyEmbeddingList(use_async, source);
        engine.SetDim(empty.dim);
        engine.SetEmptyEmbListOffsets(std::move(empty.offsets));
        if (restored_id_map.has_valid_data) {
            {
                auto local_io = [&] {
                    FinalizeRestoredIdMap(
                        engine.native_index.Node(),
                        "empty embedding-list disk vector load");
                };
                if (use_async && params.id_map_mmap.Any()) {
                    co_await storage::RunLocalFileIOAsync(local_io, priority);
                } else {
                    local_io();
                }
            }
        }
        ValidateLoadedDimension(params,
                                engine.Dim(),
                                DimensionSource::PersistedArtifact,
                                LoadBackend::Disk);
    } else if (plan.state == ArtifactState::AllNull) {
        if (!params.runtime_dim.has_value()) {
            ThrowInfo(UnexpectedError,
                      "validity-only disk vector artifact requires runtime "
                      "dim");
        }
        engine.SetDim(*params.runtime_dim);
        if (restored_id_map.has_valid_data) {
            {
                auto local_io = [&] {
                    FinalizeRestoredIdMap(engine.native_index.Node(),
                                          "all-null nullable disk vector load");
                };
                if (use_async && params.id_map_mmap.Any()) {
                    co_await storage::RunLocalFileIOAsync(local_io, priority);
                } else {
                    local_io();
                }
            }
        }
        ValidateLoadedDimension(params,
                                engine.Dim(),
                                DimensionSource::RuntimeMetadata,
                                LoadBackend::Disk);
    } else {
        const bool enable_mmap =
            opts.enable_mmap &&
            KnowhereMmapSupported(engine.KnowhereIndexType());
        if (!stream_backend) {
            const auto paths = co_await source.ReadEntriesToLocalDirAsync(
                plan.engine_names, prefix, use_async);
            if (paths.size() != plan.engine_names.size()) {
                ThrowInfo(
                    FileReadFailed,
                    "disk vector source returned an incomplete local file "
                    "set");
            }
            for (size_t i = 0; i < paths.size(); ++i) {
                if (std::filesystem::path(paths[i]).filename().string() !=
                    std::filesystem::path(plan.engine_names[i])
                        .filename()
                        .string()) {
                    ThrowInfo(DataFormatBroken,
                              "disk vector entry {} materialized as unexpected "
                              "file {}",
                              plan.engine_names[i],
                              paths[i]);
                }
            }
        } else if (enable_mmap) {
            {
                auto local_io = [&] { EnsureMmapDirectory(prefix); };
                if (use_async) {
                    co_await storage::RunLocalFileIOAsync(local_io, priority);
                } else {
                    local_io();
                }
            }
        }

        auto config = params.knowhere_config;
        PrepareCommonLoadConfig(
            config, params, open_options, prefix, stream_backend, enable_mmap);
        {
            auto local_io = [&] {
                const auto status = engine.native_index.Deserialize(
                    knowhere::BinarySet{}, config);
                selected_handle->RethrowFirstFailure();
                // The knowhere deserialize API has no OpContext entrance. FileSource
                // observes its captured cancellation/priority while materializing
                // sidecars/non-stream files. Stream backends retain their storage input
                // through the engine backing owner; neither source nor opts.op_ctx is
                // retained by the reader.
                if (status != knowhere::Status::success) {
                    ThrowDeserializeError(status);
                }
            };
            if (use_async) {
                co_await storage::RunLocalFileIOAsync(local_io, priority);
            } else {
                local_io();
            }
        }
        engine.SetDim(engine.native_index.Dim());
        ValidateLoadedDimension(params,
                                engine.Dim(),
                                DimensionSource::PersistedArtifact,
                                LoadBackend::Disk);
    }

    ValidateLoadedShape(params, plan.state, engine, LoadBackend::Disk);
    co_return std::make_unique<VectorIndexReader>(open_options.beamwidth,
                                                  std::move(engine));
}

}  // namespace

folly::coro::Task<std::unique_ptr<IndexLoader>>
VectorDiskLoader::Open(IndexOpenRequest request) {
    return OpenIndexLoader(
        std::move(request),
        [](OpenedIndexInput input, storage::LoadOptions options)
            -> folly::coro::Task<std::unique_ptr<IndexLoader>> {
            (void)DeriveCaps(options.params);
            if (!std::holds_alternative<LegacyIndexSource>(input)) {
                ThrowInfo(Unsupported,
                          "vector indexes have no V3 persisted format");
            }
            auto loader =
                std::unique_ptr<VectorDiskLoader>(new VectorDiskLoader(
                    std::get<LegacyIndexSource>(std::move(input)),
                    std::move(options)));
            const auto runtime = ParseRuntimeParams(loader->options_.params);
            (void)ParseOpenOptions(loader->options_.params, runtime);
            (void)PlanEntries(*loader->input_.source, runtime);
            co_return loader;
        });
}

folly::coro::Task<IIndexReaderBasePtr>
VectorDiskLoader::Load(milvus::OpContext* context) {
    return RunLegacyLoad(input_, options_, &LoadLegacy, context);
}

ReaderCaps
VectorDiskLoader::DeriveCaps(const Config& index_meta) {
    (void)ParseRuntimeParams(index_meta);
    // ReaderCaps is scalar-shaped. Vector family/type metadata is sufficient
    // for current pre-pin selection; no speculative vector cap fields live here.
    return {};
}

folly::coro::Task<IIndexReaderBasePtr>
VectorDiskLoader::LoadLegacy(storage::FileSource& source,
                             const storage::LoadOptions& opts,
                             bool use_async) {
    auto params = ParseDiskLoadMetadata(opts.params);
    const auto open_options = ParseOpenOptions(opts.params, params);
    ParseIdMapMmapMetadata(opts.params, params);
    if (source.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "disk vector indexes have no V3 persisted format");
    }
    const auto plan = PlanEntries(source, params);
    co_return co_await OpenIndex(
        use_async, source, params, open_options, plan, opts);
}

std::vector<std::string>
VectorDiskLoader::AsyncEntryNames(storage::FileSource& source,
                                  const storage::LoadOptions& opts) {
    const auto params = ParseRuntimeParams(opts.params);
    const auto plan = PlanEntries(source, params);
    if (plan.state == ArtifactState::Normal) {
        auto handle = source.OpenDiskEngineFiles(
            storage::DiskEngineFileMode::LocalFiles, plan.engine_names);
        auto engine = MakeEngine(params, handle);
        if (!engine.native_index.LoadIndexWithStream()) {
            return source.EntryNames();
        }
    }
    std::vector<std::string> names;
    if (plan.has_validity) {
        names.emplace_back(VALID_DATA_KEY);
    }
    if (plan.has_empty_offsets) {
        names.emplace_back(EMPTY_EMB_LIST_OFFSETS_KEY);
    }
    return names;
}

}  // namespace milvus::index
