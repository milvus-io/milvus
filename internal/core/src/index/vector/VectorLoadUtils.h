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

#pragma once

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorLoadParamUtils.h"
#include "index/vector/VectorParamUtils.h"
#include "nlohmann/json.hpp"

namespace milvus::index::vector_load_params {

inline std::optional<int64_t>
ReadInt64(const Config& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return std::nullopt;
    }
    return vector_params::ParseInt64(
        params.at(key), key, "normalized vector parameter");
}

template <typename T, typename Read>
std::optional<T>
ReadVectorAliases(const Config& params,
                  std::initializer_list<std::string_view> keys,
                  std::string_view label,
                  Read&& read) {
    return ReadAliasedParam<T>(
        keys,
        [&](std::string_view key) { return read(params, key); },
        [label](auto first, auto second) {
            ThrowInfo(UnexpectedError,
                      "normalized vector {} parameters {} and {} disagree",
                      label,
                      first,
                      second);
        });
}

inline std::optional<int64_t>
ReadAliasedInt64(const Config& params,
                 std::initializer_list<std::string_view> keys,
                 std::string_view label) {
    return ReadVectorAliases<int64_t>(params, keys, label, ReadInt64);
}

inline std::optional<DataType>
ReadDataType(const Config& params, std::string_view key) {
    if (params.contains(key) && params.at(key).is_null()) {
        return std::nullopt;
    }
    return ReadDataTypeParam(params, key);
}

inline std::optional<DataType>
ReadAliasedDataType(const Config& params,
                    std::initializer_list<std::string_view> keys,
                    std::string_view label) {
    return ReadVectorAliases<DataType>(params, keys, label, ReadDataType);
}

inline std::optional<bool>
ReadBool(const Config& params, std::string_view key) {
    return GetValueFromConfig<bool>(params, std::string(key));
}

inline std::optional<bool>
ReadAliasedBool(const Config& params,
                std::initializer_list<std::string_view> keys,
                std::string_view label) {
    return ReadVectorAliases<bool>(params, keys, label, ReadBool);
}

inline std::string
ReadRequiredString(const Config& params, std::string_view key) {
    if (!params.contains(key) || !params.at(key).is_string()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector parameter {} must be a string",
                  key);
    }
    auto value = params.at(key).get<std::string>();
    if (value.empty()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector parameter {} must not be empty",
                  key);
    }
    return value;
}

inline bool
IsPhysicalVectorType(DataType type) {
    switch (type) {
        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY:
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
        case DataType::VECTOR_SPARSE_U32_F32:
        case DataType::VECTOR_INT8:
            return true;
        default:
            return false;
    }
}

enum class LoadBackend {
    Memory,
    Disk,
};

enum class ArtifactState {
    Normal,
    AllNull,
    EmptyEmbeddingList,
};

enum class DimensionSource {
    PersistedArtifact,
    RuntimeMetadata,
};

struct NormalizedLoadMetadata {
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
    IdMapMmapFlags id_map_mmap;
    Config knowhere_config;
};

inline NormalizedLoadMetadata
ParseNormalizedLoadMetadata(const Config& params, LoadBackend backend) {
    if (!params.is_object()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector load parameters must be an object");
    }

    NormalizedLoadMetadata result;
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
            if (backend == LoadBackend::Memory) {
                ThrowInfo(
                    UnexpectedError,
                    "vector memory loader received non-vector field type {}",
                    result.field_type);
            }
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
        if (backend == LoadBackend::Memory) {
            ThrowInfo(Unsupported,
                      "vector indexes do not use the scalar nested coordinate "
                      "mode");
        }
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
    return result;
}

inline void
ParseVectorShapeMetadata(const Config& params, NormalizedLoadMetadata& result) {
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
}

// Kept as a separate final phase so the disk loader can preserve the existing
// order in which DiskANN-only open parameters are validated before mmap flags.
inline void
ParseIdMapMmapMetadata(const Config& params, NormalizedLoadMetadata& result) {
    result.id_map_mmap.enable_i2o =
        ReadLenientIdMapMmapFlag(params, ENABLE_MMAP_I2O_MAP);
    result.id_map_mmap.enable_o2i =
        ReadLenientIdMapMmapFlag(params, ENABLE_MMAP_O2I_MAP);
    result.knowhere_config = params;
}

inline void
ValidateLoadedDimension(const NormalizedLoadMetadata& params,
                        int64_t loaded_dim,
                        DimensionSource source,
                        LoadBackend backend) {
    const bool valid = loaded_dim > 0 ||
                       (loaded_dim == 0 && params.physical_type ==
                                               DataType::VECTOR_SPARSE_U32_F32);
    if (!valid) {
        const auto error = source == DimensionSource::PersistedArtifact
                               ? DataFormatBroken
                               : UnexpectedError;
        if (backend == LoadBackend::Memory) {
            ThrowInfo(error,
                      "loaded vector dimension {} is invalid for type {}",
                      loaded_dim,
                      params.physical_type);
        }
        ThrowInfo(error,
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

template <typename Engine>
LoadedValidity
InspectLoadedValidity(const Engine& engine) {
    const auto& id_map = engine.native_index.GetIdMap();
    const auto valid_bitmap = id_map.ValidBitmap();
    if (valid_bitmap.empty()) {
        return {};
    }
    return {true,
            static_cast<int64_t>(valid_bitmap.size()),
            static_cast<int64_t>(id_map.InCount())};
}

template <typename Engine>
void
ValidateLoadedShape(const NormalizedLoadMetadata& params,
                    ArtifactState state,
                    const Engine& engine,
                    LoadBackend backend) {
    const auto valid = InspectLoadedValidity(engine);
    if (params.nullable.has_value()) {
        const bool zero_rows = params.num_rows.value_or(-1) == 0;
        if ((!*params.nullable && valid.enabled) ||
            (*params.nullable && !valid.enabled && !zero_rows)) {
            if (backend == LoadBackend::Memory) {
                ThrowInfo(UnexpectedError,
                          "runtime nullable metadata disagrees with vector "
                          "validity entries");
            }
            ThrowInfo(UnexpectedError,
                      "runtime nullable metadata disagrees with disk vector "
                      "validity");
        }
    }
    if (state == ArtifactState::AllNull &&
        (!valid.enabled || valid.valid_count != 0)) {
        if (backend == LoadBackend::Memory) {
            ThrowInfo(DataFormatBroken,
                      "validity-only vector artifact contains valid rows");
        }
        ThrowInfo(DataFormatBroken,
                  "validity-only disk vector artifact contains valid rows");
    }
    if (state == ArtifactState::EmptyEmbeddingList) {
        const auto& offsets = engine.EmptyEmbListOffsets();
        if (valid.enabled) {
            if (static_cast<uint64_t>(valid.valid_count) + 1 !=
                offsets.size()) {
                if (backend == LoadBackend::Memory) {
                    ThrowInfo(
                        DataFormatBroken,
                        "empty embedding-list offset count disagrees with "
                        "nullable valid row count");
                }
                ThrowInfo(DataFormatBroken,
                          "empty embedding-list offsets disagree with valid "
                          "parent rows");
            }
        } else if (params.num_rows.has_value() &&
                   static_cast<uint64_t>(*params.num_rows) + 1 !=
                       offsets.size()) {
            if (backend == LoadBackend::Memory) {
                ThrowInfo(UnexpectedError,
                          "runtime vector row count disagrees with empty "
                          "embedding-list offsets");
            }
            ThrowInfo(UnexpectedError,
                      "runtime row count disagrees with empty embedding-list "
                      "offsets");
        }
    }
    if (state == ArtifactState::Normal && params.elem_type == DataType::NONE &&
        valid.enabled && engine.native_index.Count() != valid.valid_count) {
        if (backend == LoadBackend::Memory) {
            ThrowInfo(DataFormatBroken,
                      "loaded vector count {} disagrees with nullable valid "
                      "row count {}",
                      engine.native_index.Count(),
                      valid.valid_count);
        }
        ThrowInfo(DataFormatBroken,
                  "loaded disk vector count {} disagrees with valid row count "
                  "{}",
                  engine.native_index.Count(),
                  valid.valid_count);
    }
    if (params.num_rows.has_value()) {
        if (valid.enabled && valid.total_count != *params.num_rows) {
            if (backend == LoadBackend::Memory) {
                ThrowInfo(UnexpectedError,
                          "runtime vector row count {} disagrees with nullable "
                          "row count {}",
                          *params.num_rows,
                          valid.total_count);
            }
            ThrowInfo(UnexpectedError,
                      "runtime row count {} disagrees with nullable vector "
                      "row count {}",
                      *params.num_rows,
                      valid.total_count);
        }
        if (!valid.enabled && params.elem_type == DataType::NONE &&
            state == ArtifactState::Normal &&
            engine.native_index.Count() != *params.num_rows) {
            if (backend == LoadBackend::Memory) {
                ThrowInfo(UnexpectedError,
                          "runtime vector row count {} disagrees with loaded "
                          "count {}",
                          *params.num_rows,
                          engine.native_index.Count());
            }
            ThrowInfo(
                UnexpectedError,
                "runtime row count {} disagrees with loaded disk vector count "
                "{}",
                *params.num_rows,
                engine.native_index.Count());
        }
    }
}

}  // namespace milvus::index::vector_load_params
