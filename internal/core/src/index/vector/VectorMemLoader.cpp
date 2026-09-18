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

#include "index/vector/VectorMemLoader.h"
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
#include "index/vector/VectorIndexReader.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorLoadResource.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/LocalDirectory.h"

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

enum class ArtifactState {
    Normal,
    AllNull,
    EmptyEmbeddingList,
};

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
    bool mmap_i2o{false};
    bool mmap_o2i{false};
    Config knowhere_config;
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
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
                      "vector memory loader received non-vector field type {}",
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
                  "vector indexes do not use the scalar nested coordinate "
                  "mode");
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
    if (VectorUsesDiskLoad(result.index_type, result.version)) {
        ThrowInfo(UnexpectedError,
                  "disk-load vector index {} was routed to vector_mem",
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
    result.mmap_i2o = ReadBool(params, ENABLE_MMAP_I2O_MAP).value_or(false);
    result.mmap_o2i = ReadBool(params, ENABLE_MMAP_O2I_MAP).value_or(false);
    result.knowhere_config = params;
    return result;
}

struct EntryPlan {
    ArtifactState state{ArtifactState::Normal};
    std::vector<std::string> all_names;
    std::vector<std::string> engine_names;
    bool has_validity{false};
    bool has_emb_meta{false};
    bool has_emb_raw{false};
};

EntryPlan
PlanEntries(storage::FileSource& source, const RuntimeParams& params) {
    EntryPlan plan;
    plan.all_names = source.EntryNames();
    std::unordered_set<std::string> unique;
    unique.reserve(plan.all_names.size());
    bool has_valid_count = false;
    bool has_valid_data = false;
    bool has_empty_offsets = false;
    for (const auto& name : plan.all_names) {
        if (name.empty() || !unique.insert(name).second) {
            ThrowInfo(DataFormatBroken,
                      "vector artifact has an empty or duplicate entry name");
        }
        if (name == VALID_DATA_COUNT_KEY) {
            has_valid_count = true;
        } else if (name == VALID_DATA_KEY) {
            has_valid_data = true;
        } else if (name == kEmptyEmbListOffsets) {
            has_empty_offsets = true;
        } else if (name == knowhere::meta::EMB_LIST_META) {
            plan.has_emb_meta = true;
        } else if (name == knowhere::meta::EMB_LIST_RAW_INDEX) {
            plan.has_emb_raw = true;
        } else {
            plan.engine_names.push_back(name);
        }
    }
    if (has_valid_count != has_valid_data) {
        ThrowInfo(DataFormatBroken,
                  "nullable vector valid_data entries are incomplete");
    }
    plan.has_validity = has_valid_count;

    if (has_empty_offsets) {
        if (params.elem_type == DataType::NONE || !plan.engine_names.empty() ||
            plan.has_emb_meta || plan.has_emb_raw) {
            ThrowInfo(DataFormatBroken,
                      "empty embedding-list artifact contains incompatible "
                      "engine entries");
        }
        plan.state = ArtifactState::EmptyEmbeddingList;
        return plan;
    }

    if (plan.engine_names.empty()) {
        if (!plan.has_validity || plan.has_emb_meta || plan.has_emb_raw) {
            ThrowInfo(DataFormatBroken,
                      "vector artifact has no loadable engine state");
        }
        plan.state = ArtifactState::AllNull;
        return plan;
    }

    if (params.elem_type == DataType::NONE) {
        if (plan.has_emb_meta || plan.has_emb_raw) {
            ThrowInfo(DataFormatBroken,
                      "ordinary vector artifact contains embedding-list "
                      "sidecars");
        }
    } else if (!plan.has_emb_meta) {
        ThrowInfo(DataFormatBroken,
                  "embedding-list vector artifact has no EMB_LIST_META entry");
    }
    return plan;
}

void
AppendReadEntry(storage::FileSource& source,
                const std::string& name,
                knowhere::BinarySet& entries) {
    auto bytes = source.ReadEntry(name);
    if (bytes.size() >
        static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "vector artifact entry {} exceeds BinarySet size",
                  name);
    }
    const auto size = static_cast<int64_t>(bytes.size());
    auto owner = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
    std::shared_ptr<uint8_t[]> data(owner, owner->data());
    entries.Append(name, std::move(data), size);
}

knowhere::BinarySet
ReadEntries(storage::FileSource& source,
            const std::vector<std::string>& names) {
    knowhere::BinarySet entries;
    for (const auto& name : names) {
        AppendReadEntry(source, name, entries);
    }
    return entries;
}

using detail::EmptyEmbeddingListState;

EmptyEmbeddingListState
DecodeEmptyEmbeddingList(const knowhere::BinarySet& entries) {
    const auto entry = entries.GetByName(std::string(kEmptyEmbListOffsets));
    constexpr auto header_size = detail::kEmptyEmbeddingListHeaderSize;
    if (entry == nullptr || entry->size < 0 ||
        static_cast<uint64_t>(entry->size) < header_size ||
        entry->data == nullptr) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offset entry is invalid");
    }
    auto result = detail::ReadEmptyEmbeddingListPayload(
        entry->data.get(), static_cast<uint64_t>(entry->size));
    if (result.offsets.front() != 0 || result.offsets.back() != 0 ||
        !std::is_sorted(result.offsets.begin(), result.offsets.end())) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list offsets are not an all-zero prefix "
                  "sum");
    }
    if (result.dim <= 0) {
        ThrowInfo(DataFormatBroken,
                  "empty embedding-list dimension {} is invalid",
                  result.dim);
    }
    return result;
}

void
SetWarmup(Config& config, storage::WarmupPolicy warmup) {
    switch (warmup) {
        case storage::WarmupPolicy::Disable:
            config[WARMUP] = "disable";
            return;
        case storage::WarmupPolicy::Sync:
            config[WARMUP] = "sync";
            return;
        case storage::WarmupPolicy::Async:
            config[WARMUP] = "async";
            return;
    }
    ThrowInfo(UnexpectedError, "unknown vector warmup policy");
}

[[noreturn]] void
ThrowDeserializeError(knowhere::Status status) {
    ThrowInfo(KnowhereStatusToErrorCode(status),
              "failed to deserialize vector index: status {} ({})",
              static_cast<int>(status),
              knowhere::Status2String(status));
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
                  "loaded vector dimension {} is invalid for type {}",
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
                      "runtime nullable metadata disagrees with vector "
                      "validity entries");
        }
    }
    if (plan.state == ArtifactState::AllNull) {
        if (!valid.enabled || valid.valid_count != 0) {
            ThrowInfo(DataFormatBroken,
                      "validity-only vector artifact contains valid rows");
        }
    }
    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        const auto& offsets = engine.EmptyEmbListOffsets();
        if (valid.enabled) {
            if (static_cast<uint64_t>(valid.valid_count) + 1 !=
                offsets.size()) {
                ThrowInfo(DataFormatBroken,
                          "empty embedding-list offset count disagrees with "
                          "nullable valid row count");
            }
        } else if (params.num_rows.has_value() &&
                   static_cast<uint64_t>(*params.num_rows) + 1 !=
                       offsets.size()) {
            ThrowInfo(UnexpectedError,
                      "runtime vector row count disagrees with empty "
                      "embedding-list offsets");
        }
    }
    if (plan.state == ArtifactState::Normal &&
        params.elem_type == DataType::NONE && valid.enabled &&
        engine.native_index.Count() != valid.valid_count) {
        ThrowInfo(DataFormatBroken,
                  "loaded vector count {} disagrees with nullable valid row "
                  "count {}",
                  engine.native_index.Count(),
                  valid.valid_count);
    }
    if (params.num_rows.has_value()) {
        if (valid.enabled && valid.total_count != *params.num_rows) {
            ThrowInfo(UnexpectedError,
                      "runtime vector row count {} disagrees with nullable "
                      "row count {}",
                      *params.num_rows,
                      valid.total_count);
        }
        if (!valid.enabled && params.elem_type == DataType::NONE &&
            plan.state == ArtifactState::Normal &&
            engine.native_index.Count() != *params.num_rows) {
            ThrowInfo(UnexpectedError,
                      "runtime vector row count {} disagrees with loaded "
                      "count {}",
                      *params.num_rows,
                      engine.native_index.Count());
        }
    }
}

// Publish the persisted validity into the engine's id map. This must happen
// BEFORE the engine is deserialized: knowhere derives both mapping directions
// from the bitmap inside Deserialize, and a metadata-only artifact never
// reaches Deserialize at all, so its map is finalized explicitly.
RestoredIdMap
RestoreValidity(storage::FileSource& source,
                const EntryPlan& plan,
                const RuntimeParams& params,
                KnowhereEngine& engine,
                const std::string& mmap_path_prefix,
                knowhere::BinarySet* existing = nullptr) {
    if (!plan.has_validity) {
        return {};
    }
    knowhere::BinarySet local;
    auto& entries = existing == nullptr ? local : *existing;
    if (!entries.Contains(VALID_DATA_COUNT_KEY)) {
        AppendReadEntry(source, VALID_DATA_COUNT_KEY, entries);
        AppendReadEntry(source, VALID_DATA_KEY, entries);
    }
    IdMapMmapFlags mmap_flags;
    if (!mmap_path_prefix.empty()) {
        mmap_flags.enable_i2o = params.mmap_i2o;
        mmap_flags.enable_o2i = params.mmap_o2i;
    }
    return RestoreIdMapFromBinarySet(entries,
                                     engine.native_index.GetIdMap(),
                                     mmap_flags,
                                     mmap_path_prefix);
}

struct OpenedMemState {
    explicit OpenedMemState(const RuntimeParams& params)
        : engine(params.physical_type,
                 params.elem_type,
                 params.index_type,
                 params.metric_type,
                 params.version) {
    }

    KnowhereEngine engine;
};

void
PopulateState(OpenedMemState& state,
              storage::FileSource& source,
              const storage::LoadOptions& opts,
              const RuntimeParams& params,
              const EntryPlan& plan) {
    auto config = params.knowhere_config;
    config.erase(MMAP_FILE_PATH);
    config.erase(EMB_LIST_META_PATH);
    config.erase(EMB_LIST_RAW_INDEX_PATH);
    SetWarmup(config, opts.warmup);

    // The id map's derived arrays may be file-backed, and knowhere removes
    // each backing file with its mapping, so the staging directory only has to
    // outlive the engine. A metadata-only artifact owns no engine file, so it
    // creates that directory on its own when mmap was requested.
    const auto id_map_mmap_requested = params.mmap_i2o || params.mmap_o2i;
    auto stage_id_map_mmap_dir = [&]() -> std::string {
        if (!plan.has_validity || !id_map_mmap_requested) {
            return {};
        }
        auto local_files = storage::LocalDirectory::CreateOwned(
            opts.mmap_dir_path, "vector_id_map_XXXXXX", "vector id map mmap");
        auto path = local_files->Path();
        state.engine.backing_owner = std::move(local_files);
        return path;
    };

    if (plan.state == ArtifactState::EmptyEmbeddingList) {
        auto entries = ReadEntries(source, plan.all_names);
        auto empty = DecodeEmptyEmbeddingList(entries);
        state.engine.SetDim(empty.dim);
        state.engine.SetEmptyEmbListOffsets(std::move(empty.offsets));
        const auto restored = RestoreValidity(source,
                                              plan,
                                              params,
                                              state.engine,
                                              stage_id_map_mmap_dir(),
                                              &entries);
        if (restored.has_valid_data) {
            FinalizeRestoredIdMap(state.engine.native_index.Node(),
                                  "empty embedding-list vector load");
        }
        ValidateDimension(params, state.engine.Dim(), true);
    } else if (plan.state == ArtifactState::AllNull) {
        if (!params.runtime_dim.has_value()) {
            ThrowInfo(UnexpectedError,
                      "validity-only vector artifact requires runtime dim");
        }
        state.engine.SetDim(*params.runtime_dim);
        const auto restored = RestoreValidity(
            source, plan, params, state.engine, stage_id_map_mmap_dir());
        if (restored.has_valid_data) {
            FinalizeRestoredIdMap(state.engine.native_index.Node(),
                                  "all-null nullable vector load");
        }
        ValidateDimension(params, state.engine.Dim(), false);
    } else {
        const bool mmap = opts.enable_mmap &&
                          KnowhereMmapSupported(
                              state.engine.KnowhereIndexType());
        if (mmap) {
            auto local_files = storage::LocalDirectory::CreateOwned(
                opts.mmap_dir_path, "vector_mem_XXXXXX", "vector mmap");
            const auto& directory = local_files->Path();
            state.engine.backing_owner = std::move(local_files);
            const auto main_path =
                (std::filesystem::path(directory) / "index").string();
            source.ReadEntriesToLocalFile(plan.engine_names, main_path);
            config[ENABLE_MMAP] = true;
            if (params.elem_type != DataType::NONE) {
                const auto meta_path =
                    (std::filesystem::path(directory) / EMB_LIST_META_FILE_NAME)
                        .string();
                source.ReadEntryToLocalFile(knowhere::meta::EMB_LIST_META,
                                            meta_path);
                config[EMB_LIST_META_PATH] = meta_path;
                if (plan.has_emb_raw) {
                    const auto raw_path = (std::filesystem::path(directory) /
                                           EMB_LIST_RAW_INDEX_FILE_NAME)
                                              .string();
                    source.ReadEntryToLocalFile(
                        knowhere::meta::EMB_LIST_RAW_INDEX, raw_path);
                    config[EMB_LIST_RAW_INDEX_PATH] = raw_path;
                }
            }
            // Restore before deserializing: Deserialize is what derives the
            // dense id arrays from the validity bitmap.
            RestoreValidity(
                source, plan, params, state.engine, directory);
            const auto status = state.engine.native_index.DeserializeFromFile(
                main_path, config);
            // The knowhere deserialize API has no OpContext entrance. Remote
            // reads and local materialization observe the context captured by
            // FileSource; the borrowed opts.op_ctx is never retained here.
            if (status != knowhere::Status::success) {
                ThrowDeserializeError(status);
            }
        } else {
            config[ENABLE_MMAP] = false;
            auto entries = ReadEntries(source, plan.all_names);
            RestoreValidity(
                source, plan, params, state.engine, std::string{}, &entries);
            const auto status =
                state.engine.native_index.Deserialize(entries, config);
            if (status != knowhere::Status::success) {
                ThrowDeserializeError(status);
            }
            entries.clear();
        }
        state.engine.SetDim(state.engine.native_index.Dim());
        ValidateDimension(params, state.engine.Dim(), true);
    }

    ValidateShape(params, plan, state.engine);
}

}  // namespace

ReaderCaps
VectorMemLoader::DeriveCaps(const Config& index_meta) {
    (void)ParseRuntimeParams(index_meta);
    // ReaderCaps is scalar-shaped. Family/type metadata identifies this as a
    // vector reader; no vector capability expansion is implied here.
    return {};
}

IIndexReaderBasePtr
VectorMemLoader::Open(storage::FileSource& source,
                      const storage::LoadOptions& opts) {
    const auto params = ParseRuntimeParams(opts.params);
    if (source.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "in-memory vector indexes have no V3 persisted format");
    }
    const auto plan = PlanEntries(source, params);
    if (plan.has_validity && (params.mmap_i2o || params.mmap_o2i) &&
        opts.mmap_dir_path.empty()) {
        ThrowInfo(UnexpectedError,
                  "nullable vector mmap mapping requires a staging parent");
    }
    OpenedMemState state(params);
    PopulateState(state, source, opts, params, plan);
    return std::make_unique<VectorIndexReader>(std::move(state.engine));
}

}  // namespace milvus::index
