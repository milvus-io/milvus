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

#include "index/vector/VectorDiskLoader.h"
#include "index/vector/VectorLoadUtils.h"

#include <algorithm>
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
#include "index/vector/VectorLoadResource.h"
#include "index/vector/VectorParamUtils.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/DiskEngineFileHandle.h"

namespace milvus::index {
namespace {

using vector_load_params::IsPhysicalVectorType;
using vector_load_params::ReadAliasedBool;
using vector_load_params::ReadAliasedDataType;
using vector_load_params::ReadAliasedInt64;
using vector_load_params::ReadBool;
using vector_load_params::ReadDataType;
using vector_load_params::ReadInt64;
using vector_load_params::ReadRequiredString;

constexpr std::string_view kEmptyEmbListOffsets = "empty_emb_list_offsets";
constexpr const char* kEnableDiskMmap = "enable_disk_mmap";
constexpr uint32_t kDefaultBeamwidth = 8;
constexpr uint32_t kMinDiskAnnBeamwidth = 1;
constexpr uint32_t kMaxDiskAnnBeamwidth = 128;

enum class ArtifactState {
    Normal,
    AllNull,
    EmptyEmbeddingList,
};

uint32_t
ParseUint32(const nlohmann::json& value, std::string_view key) {
    const auto parsed = vector_params::ParseInt64(
        value, key, "normalized vector parameter");
    if (parsed < 0 ||
        static_cast<uint64_t>(parsed) >
            static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
        ThrowInfo(UnexpectedError,
                  "normalized vector parameter {} is outside uint32",
                  key);
    }
    return static_cast<uint32_t>(parsed);
}

struct RuntimeParams {
    DataType field_type{DataType::NONE};
    DataType physical_type{DataType::NONE};
    DataType elem_type{DataType::NONE};
    IndexType index_type;
    MetricType metric_type;
    IndexVersion version{0};
    // Exact expectation for dense vectors; validity-only fallback for sparse.
    std::optional<int64_t> runtime_dim;
    std::optional<int64_t> num_rows;
    std::optional<bool> nullable;
    uint32_t beamwidth{kDefaultBeamwidth};
    std::optional<int32_t> load_threads;
    bool mmap_i2o{false};
    bool mmap_o2i{false};
    Config knowhere_config;
};

RuntimeParams
ParseRuntimeParams(const Config& params, bool parse_open_options) {
    if (!params.is_object()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector load parameters must be an object");
    }

    RuntimeParams result;
    const auto field_type = ReadDataType(params, "field_type");
    if (!field_type.has_value()) {
        ThrowInfo(UnexpectedError,
                  "vector loader requires normalized field_type");
    }
    result.field_type = *field_type;
    const auto configured_value = ReadDataType(params, "value_type");
    const auto configured_element = ReadAliasedDataType(
        params, {"element_type", "array_element_type"}, "element type");

    if (result.field_type == DataType::VECTOR_ARRAY) {
        if (!configured_element.has_value() ||
            *configured_element == DataType::NONE) {
            ThrowInfo(UnexpectedError,
                      "VECTOR_ARRAY loader requires an element type");
        }
        result.elem_type = *configured_element;
        result.physical_type = configured_value.value_or(result.elem_type);
        if (result.physical_type != result.elem_type) {
            ThrowInfo(UnexpectedError,
                      "VECTOR_ARRAY value type {} conflicts with element type "
                      "{}",
                      result.physical_type,
                      result.elem_type);
        }
        if (result.elem_type == DataType::VECTOR_SPARSE_U32_F32) {
            ThrowInfo(Unsupported,
                      "sparse vectors are not supported as embedding-list "
                      "elements");
        }
    } else {
        if (!IsPhysicalVectorType(result.field_type)) {
            ThrowInfo(UnexpectedError,
                      "vector disk loader received non-vector field type {}",
                      result.field_type);
        }
        if (configured_element.has_value() &&
            *configured_element != DataType::NONE) {
            ThrowInfo(UnexpectedError,
                      "ordinary vector field has unexpected element type {}",
                      *configured_element);
        }
        result.physical_type = configured_value.value_or(result.field_type);
        if (result.physical_type != result.field_type) {
            ThrowInfo(UnexpectedError,
                      "vector field type {} conflicts with value type {}",
                      result.field_type,
                      result.physical_type);
        }
    }
    if (!IsPhysicalVectorType(result.physical_type)) {
        ThrowInfo(UnexpectedError,
                  "unsupported vector physical type {}",
                  result.physical_type);
    }

    const auto nested = ReadAliasedBool(
        params, {"nested", "is_nested", "is_nested_index"}, "nested");
    if (nested.value_or(false)) {
        ThrowInfo(Unsupported,
                  "vector indexes do not use scalar nested coordinates");
    }
    result.nullable = ReadBool(params, "nullable");
    result.index_type = ReadRequiredString(params, INDEX_TYPE);
    result.metric_type = ReadRequiredString(params, METRIC_TYPE);

    const auto version = ReadInt64(params, INDEX_ENGINE_VERSION);
    if (!version.has_value() ||
        *version < std::numeric_limits<IndexVersion>::min() ||
        *version > std::numeric_limits<IndexVersion>::max()) {
        ThrowInfo(UnexpectedError,
                  "vector loader requires a valid index_engine_version");
    }
    result.version = static_cast<IndexVersion>(*version);
    if (!VectorUsesDiskLoad(result.index_type, result.version)) {
        ThrowInfo(UnexpectedError,
                  "memory-load vector index {} was routed to vector_disk",
                  result.index_type);
    }

    result.runtime_dim = ReadInt64(params, DIM_KEY);
    if (result.runtime_dim.has_value() &&
        (*result.runtime_dim < 0 ||
         (*result.runtime_dim == 0 &&
          result.physical_type != DataType::VECTOR_SPARSE_U32_F32))) {
        ThrowInfo(UnexpectedError,
                  "normalized vector dimension {} is invalid for type {}",
                  *result.runtime_dim,
                  result.physical_type);
    }
    result.num_rows =
        ReadAliasedInt64(params, {"num_rows", "index_num_rows"}, "row count");
    if (result.num_rows.has_value() && *result.num_rows < 0) {
        ThrowInfo(UnexpectedError,
                  "normalized vector row count must be non-negative");
    }

    if (parse_open_options &&
        result.index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        if (params.contains(DISK_ANN_QUERY_BEAMWIDTH)) {
            result.beamwidth = ParseUint32(params.at(DISK_ANN_QUERY_BEAMWIDTH),
                                           DISK_ANN_QUERY_BEAMWIDTH);
        }
        if (result.beamwidth < kMinDiskAnnBeamwidth ||
            result.beamwidth > kMaxDiskAnnBeamwidth) {
            ThrowInfo(ConfigInvalid,
                      "DiskANN beamwidth {} is outside [{}, {}]",
                      result.beamwidth,
                      kMinDiskAnnBeamwidth,
                      kMaxDiskAnnBeamwidth);
        }
        if (!params.contains(DISK_ANN_LOAD_THREAD_NUM)) {
            ThrowInfo(ConfigInvalid,
                      "DiskANN load requires {}",
                      DISK_ANN_LOAD_THREAD_NUM);
        }
        result.load_threads = vector_params::ParsePositiveInt32(
            params.at(DISK_ANN_LOAD_THREAD_NUM), DISK_ANN_LOAD_THREAD_NUM);
    }
    result.mmap_i2o = ReadBool(params, ENABLE_MMAP_I2O_MAP).value_or(false);
    result.mmap_o2i = ReadBool(params, ENABLE_MMAP_O2I_MAP).value_or(false);
    result.knowhere_config = params;
    return result;
}

struct EntryPlan {
    ArtifactState state{ArtifactState::Normal};
    std::vector<std::string> engine_names;
    bool has_validity{false};
    bool has_empty_offsets{false};
};

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
        } else if (name == kEmptyEmbListOffsets) {
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
RestoredIdMap
RestoreValidity(storage::FileSource& source,
                const EntryPlan& plan,
                const RuntimeParams& params,
                KnowhereEngine& engine,
                const std::string& local_prefix,
                std::vector<uint8_t>& owned_bytes) {
    if (!plan.has_validity) {
        return {};
    }
    owned_bytes = source.ReadEntry(VALID_DATA_KEY);
    const auto valid_data = DecodeValidityBytes(owned_bytes);
    IdMapMmapFlags mmap_flags;
    mmap_flags.enable_i2o = params.mmap_i2o;
    mmap_flags.enable_o2i = params.mmap_o2i;
    // A fully null column derives no dense id array at all, so mmap staging
    // would only create an empty directory.
    if (mmap_flags.Any() &&
        CountValidDataBitmap(valid_data.count, valid_data.bitmap) == 0) {
        mmap_flags = {};
    }
    return RestoreIdMapFromValidData(engine.native_index.GetIdMap(),
                                     valid_data,
                                     mmap_flags,
                                     local_prefix);
}

using detail::EmptyEmbeddingListState;

EmptyEmbeddingListState
DecodeEmptyEmbeddingListBytes(const std::vector<uint8_t>& bytes) {
    constexpr auto header_size = detail::kEmptyEmbeddingListHeaderSize;
    if (bytes.size() < header_size) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset entry is too small");
    }
    auto result =
        detail::ReadEmptyEmbeddingListPayload(bytes.data(), bytes.size());
    if (result.dim <= 0 || result.offsets.front() != 0 ||
        result.offsets.back() != 0 ||
        !std::is_sorted(result.offsets.begin(), result.offsets.end())) {
        ThrowInfo(DataFormatBroken, "empty embedding-list state is invalid");
    }
    return result;
}

EmptyEmbeddingListState
DecodeEmptyEmbeddingList(storage::FileSource& source) {
    return DecodeEmptyEmbeddingListBytes(
        source.ReadEntry(kEmptyEmbListOffsets));
}

void
PrepareCommonLoadConfig(Config& config,
                        const RuntimeParams& params,
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
    AssertInfo(params.load_threads.has_value(),
               "validated DiskANN load thread count is missing");
    config[DISK_ANN_THREADS_NUM] = *params.load_threads;
}

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

void
ValidateDimension(const RuntimeParams& params,
                  int64_t loaded_dim,
                  bool persisted) {
    const bool valid = loaded_dim > 0 ||
                       (loaded_dim == 0 && params.physical_type ==
                                               DataType::VECTOR_SPARSE_U32_F32);
    if (!valid) {
        ThrowInfo(persisted ? DataFormatBroken : UnexpectedError,
                  "loaded disk vector dimension {} is invalid for type {}",
                  loaded_dim,
                  params.physical_type);
    }
    if (params.physical_type != DataType::VECTOR_SPARSE_U32_F32 &&
        params.runtime_dim.has_value() && *params.runtime_dim != loaded_dim) {
        ThrowInfo(UnexpectedError,
                  "runtime vector dimension {} disagrees with loaded "
                  "dimension {}",
                  *params.runtime_dim,
                  loaded_dim);
    }
}

// The nullable row mapping lives in the engine's knowhere IdMap (#50524), so
// every shape check reads it instead of a Milvus-side offset mapping.
struct LoadedValidity {
    bool enabled{false};
    int64_t total_count{0};
    int64_t valid_count{0};
};

LoadedValidity
InspectValidity(const KnowhereEngine& engine) {
    const auto& id_map = engine.native_index.GetIdMap();
    const auto valid_bitmap = id_map.ValidBitmap();
    if (valid_bitmap.empty()) {
        return {};
    }
    return {true,
            static_cast<int64_t>(valid_bitmap.size()),
            static_cast<int64_t>(id_map.InCount())};
}

void
ValidateShape(const RuntimeParams& params,
              const EntryPlan& plan,
              const KnowhereEngine& engine) {
    const auto valid = InspectValidity(engine);
    if (params.nullable.has_value()) {
        const bool zero_rows = params.num_rows.value_or(-1) == 0;
        if ((!*params.nullable && valid.enabled) ||
            (*params.nullable && !valid.enabled && !zero_rows)) {
            ThrowInfo(UnexpectedError,
                      "runtime nullable metadata disagrees with disk vector "
                      "validity");
        }
    }
    if (plan.state == ArtifactState::AllNull &&
        (!valid.enabled || valid.valid_count != 0)) {
        ThrowInfo(DataFormatBroken,
                  "validity-only disk vector artifact contains valid rows");
    }
    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        const auto& offsets = engine.EmptyEmbListOffsets();
        if (valid.enabled) {
            if (static_cast<uint64_t>(valid.valid_count) + 1 !=
                offsets.size()) {
                ThrowInfo(DataFormatBroken,
                          "empty embedding-list offsets disagree with valid "
                          "parent rows");
            }
        } else if (params.num_rows.has_value() &&
                   static_cast<uint64_t>(*params.num_rows) + 1 !=
                       offsets.size()) {
            ThrowInfo(UnexpectedError,
                      "runtime row count disagrees with empty embedding-list "
                      "offsets");
        }
    }
    if (plan.state == ArtifactState::Normal &&
        params.elem_type == DataType::NONE && valid.enabled &&
        engine.native_index.Count() != valid.valid_count) {
        ThrowInfo(DataFormatBroken,
                  "loaded disk vector count {} disagrees with valid row count "
                  "{}",
                  engine.native_index.Count(),
                  valid.valid_count);
    }
    if (params.num_rows.has_value()) {
        if (valid.enabled && valid.total_count != *params.num_rows) {
            ThrowInfo(UnexpectedError,
                      "runtime row count {} disagrees with nullable vector "
                      "row count {}",
                      *params.num_rows,
                      valid.total_count);
        }
        if (!valid.enabled && params.elem_type == DataType::NONE &&
            plan.state == ArtifactState::Normal &&
            engine.native_index.Count() != *params.num_rows) {
            ThrowInfo(UnexpectedError,
                      "runtime row count {} disagrees with loaded disk vector "
                      "count {}",
                      *params.num_rows,
                      engine.native_index.Count());
        }
    }
}

[[noreturn]] void
ThrowDeserializeError(knowhere::Status status) {
    ThrowInfo(KnowhereStatusToErrorCode(status),
              "failed to deserialize disk vector index: status {} ({})",
              static_cast<int>(status),
              knowhere::Status2String(status));
}

std::unique_ptr<IIndexReaderBase>
OpenIndex(storage::FileSource& source,
          const RuntimeParams& params,
          const EntryPlan& plan,
          const storage::LoadOptions& opts) {
    auto make_engine =
        [&](const std::shared_ptr<storage::DiskEngineFileHandle>& handle) {
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
    };

    // Handles precede the engine so failed construction/deserialization always
    // destroys the native node before its manager and local generation.
    auto local_handle = source.OpenDiskEngineFiles(
        storage::DiskEngineFileMode::LocalFiles, plan.engine_names);
    std::shared_ptr<storage::DiskEngineFileHandle> stream_handle;
    std::shared_ptr<storage::DiskEngineFileHandle> selected_handle =
        local_handle;
    auto engine = make_engine(local_handle);
    const bool stream_backend = plan.state == ArtifactState::Normal &&
                                engine.native_index.LoadIndexWithStream();
    if (stream_backend) {
        stream_handle = source.OpenDiskEngineFiles(
            storage::DiskEngineFileMode::RemoteStreams, plan.engine_names);
        auto stream_engine = make_engine(stream_handle);
        AssertInfo(stream_engine.native_index.LoadIndexWithStream(),
                   "disk vector stream capability changed while opening");
        engine = std::move(stream_engine);
        selected_handle = stream_handle;
    }
    const auto prefix = selected_handle->LocalPrefix();
    // Keep the decoded payload alive until AddFromData has copied it.
    std::vector<uint8_t> validity_bytes;
    const auto restored_id_map = RestoreValidity(
        source, plan, params, engine, prefix, validity_bytes);

    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        auto empty = DecodeEmptyEmbeddingList(source);
        engine.SetDim(empty.dim);
        engine.SetEmptyEmbListOffsets(std::move(empty.offsets));
        if (restored_id_map.has_valid_data) {
            FinalizeRestoredIdMap(engine.native_index.Node(),
                                  "empty embedding-list disk vector load");
        }
        ValidateDimension(params, engine.Dim(), true);
    } else if (plan.state == ArtifactState::AllNull) {
        if (!params.runtime_dim.has_value()) {
            ThrowInfo(UnexpectedError,
                      "validity-only disk vector artifact requires runtime "
                      "dim");
        }
        engine.SetDim(*params.runtime_dim);
        if (restored_id_map.has_valid_data) {
            FinalizeRestoredIdMap(engine.native_index.Node(),
                                  "all-null nullable disk vector load");
        }
        ValidateDimension(params, engine.Dim(), false);
    } else {
        const bool enable_mmap = opts.enable_mmap &&
                                 KnowhereMmapSupported(
                                     engine.KnowhereIndexType());
        if (!stream_backend) {
            const auto paths =
                source.ReadEntriesToLocalDir(plan.engine_names, prefix);
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
            EnsureMmapDirectory(prefix);
        }

        auto config = params.knowhere_config;
        PrepareCommonLoadConfig(
            config, params, prefix, stream_backend, enable_mmap);
        const auto status =
            engine.native_index.Deserialize(knowhere::BinarySet{}, config);
        selected_handle->RethrowFirstFailure();
        // The knowhere deserialize API has no OpContext entrance. FileSource
        // observes its captured cancellation/priority while materializing
        // sidecars/non-stream files. Stream backends retain their storage input
        // through the engine backing owner; neither source nor opts.op_ctx is
        // retained by the reader.
        if (status != knowhere::Status::success) {
            ThrowDeserializeError(status);
        }
        engine.SetDim(engine.native_index.Dim());
        ValidateDimension(params, engine.Dim(), true);
    }

    ValidateShape(params, plan, engine);
    return std::make_unique<VectorIndexReader>(params.beamwidth,
                                               std::move(engine));
}

}  // namespace

ReaderCaps
VectorDiskLoader::DeriveCaps(const Config& index_meta) {
    (void)ParseRuntimeParams(index_meta, false);
    // ReaderCaps is scalar-shaped. Vector family/type metadata is sufficient
    // for current pre-pin selection; no speculative vector cap fields live here.
    return {};
}

IIndexReaderBasePtr
VectorDiskLoader::Open(storage::FileSource& source,
                       const storage::LoadOptions& opts) {
    const auto params = ParseRuntimeParams(opts.params, true);
    if (source.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "disk vector indexes have no V3 persisted format");
    }
    const auto plan = PlanEntries(source, params);
    return OpenIndex(source, params, plan, opts);
}

}  // namespace milvus::index
