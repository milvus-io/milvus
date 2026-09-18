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

#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/contracts/Registry.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorDiskBuilder.h"
#include "index/vector/VectorDiskLoader.h"
#include "index/vector/VectorLoadResource.h"
#include "index/vector/VectorMemBuilder.h"
#include "index/vector/VectorMemLoader.h"
#include "index/vector/VectorParamUtils.h"

// Register the canonical kVectorMem and kVectorDisk families from Families.h.
// Knowhere index-type names remain constructor parameters, not registry keys.
// Loader entries are stateless function pairs; disk builders register per physical type.
// Memory builders consume one resident VectorBuildInput; disk builders consume
// one caller-owned prepared-file generation.

namespace milvus::index {
namespace {

int64_t
ReadRequiredInt64(const BuildParams& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector build parameter {} is missing",
                  key);
    }
    return vector_params::ParseInt64(
        params.at(key), key, "normalized vector build parameter");
}

DataType
ReadRequiredDataType(const BuildParams& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector build parameter {} is missing",
                  key);
    }
    return *ReadDataTypeParam(params, key);
}

std::optional<DataType>
ReadDataType(const BuildParams& params, std::string_view key) {
    if (params.contains(key) && params.at(key).is_null()) {
        return std::nullopt;
    }
    return ReadDataTypeParam(params, key);
}

std::optional<DataType>
ReadAliasedDataType(const BuildParams& params,
                    std::initializer_list<std::string_view> keys,
                    std::string_view label) {
    std::optional<DataType> result;
    std::string_view first_key;
    for (const auto key : keys) {
        const auto value = ReadDataType(params, key);
        if (!value.has_value()) {
            continue;
        }
        if (result.has_value() && *result != *value) {
            ThrowInfo(UnexpectedError,
                      "normalized vector {} parameters {} and {} disagree",
                      label,
                      first_key,
                      key);
        }
        if (!result.has_value()) {
            result = value;
            first_key = key;
        }
    }
    return result;
}

bool
ReadNested(const BuildParams& params) {
    return ReadNestedConfigParam(params, "normalized vector").value_or(false);
}

std::string
ReadRequiredString(const BuildParams& params, std::string_view key) {
    if (!params.contains(key) || !params.at(key).is_string()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector build parameter {} must be a string",
                  key);
    }
    auto result = params.at(key).get<std::string>();
    if (result.empty()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector build parameter {} must not be empty",
                  key);
    }
    return result;
}

template <typename T>
struct ParsedVectorBuilderParams {
    DataType elem_type{DataType::NONE};
    IndexType index_type;
    MetricType metric_type;
    IndexVersion version{0};
    int64_t dim{0};
};

template <typename T>
ParsedVectorBuilderParams<T>
ParseVectorBuilderParams(const BuildParams& params, bool disk) {
    if (!params.is_object()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector build parameters must be an object");
    }

    constexpr auto physical_type = PhysicalVectorDataType<T>();
    const auto field_type = ReadRequiredDataType(params, "field_type");
    const auto value_type = ReadRequiredDataType(params, "value_type");
    if (value_type != physical_type) {
        ThrowInfo(UnexpectedError,
                  "vector value type {} does not match builder type {}",
                  value_type,
                  physical_type);
    }

    const auto configured_element = ReadAliasedDataType(
        params, {ELEMENT_TYPE_KEY, "array_element_type"}, "element type");
    DataType elem_type = DataType::NONE;
    if (field_type == DataType::VECTOR_ARRAY) {
        if (physical_type == DataType::VECTOR_SPARSE_U32_F32) {
            ThrowInfo(Unsupported,
                      "sparse vectors are not supported as embedding-list "
                      "elements");
        }
        if (!configured_element.has_value() ||
            *configured_element != physical_type) {
            ThrowInfo(UnexpectedError,
                      "VECTOR_ARRAY element type does not match builder type "
                      "{}",
                      physical_type);
        }
        elem_type = physical_type;
    } else {
        if (field_type != physical_type) {
            ThrowInfo(UnexpectedError,
                      "vector field type {} does not match builder type {}",
                      field_type,
                      physical_type);
        }
        if (configured_element.has_value() &&
            *configured_element != DataType::NONE) {
            ThrowInfo(UnexpectedError,
                      "ordinary vector field has unexpected element type {}",
                      *configured_element);
        }
    }
    if (ReadNested(params)) {
        ThrowInfo(Unsupported,
                  "vector indexes do not use scalar nested coordinates");
    }

    auto index_type = ReadRequiredString(params, INDEX_TYPE);
    auto metric_type = ReadRequiredString(params, METRIC_TYPE);
    const auto encoded_version =
        ReadRequiredInt64(params, INDEX_ENGINE_VERSION);
    if (encoded_version < std::numeric_limits<IndexVersion>::min() ||
        encoded_version > std::numeric_limits<IndexVersion>::max()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector index engine version is out of range");
    }
    const auto version = static_cast<IndexVersion>(encoded_version);
    if (VectorUsesDiskLoad(index_type, version) != disk) {
        ThrowInfo(UnexpectedError,
                  "vector index {} was routed to the wrong build family",
                  index_type);
    }

    const auto dim = ReadRequiredInt64(params, DIM_KEY);
    if (dim < 0 ||
        (dim == 0 && physical_type != DataType::VECTOR_SPARSE_U32_F32)) {
        ThrowInfo(ConfigInvalid,
                  "vector dimension {} is invalid for type {}",
                  dim,
                  physical_type);
    }
    return {.elem_type = elem_type,
            .index_type = std::move(index_type),
            .metric_type = std::move(metric_type),
            .version = version,
            .dim = dim};
}

template <typename T>
std::unique_ptr<IArtifactBuilder<VectorBuildInput<T>>>
CreateMemBuilder(const BuildParams& params) {
    auto parsed = ParseVectorBuilderParams<T>(params, false);
    return std::make_unique<VectorMemBuilder<T>>(parsed.elem_type,
                                                 std::move(parsed.index_type),
                                                 std::move(parsed.metric_type),
                                                 parsed.version,
                                                 parsed.dim,
                                                 params);
}

template <typename T>
std::unique_ptr<IArtifactBuilder<PreparedVectorBuildFiles<T>>>
CreateDiskBuilder(const BuildParams& params) {
    auto parsed = ParseVectorBuilderParams<T>(params, true);
    auto local_dir = ReadRequiredString(params, "local_dir");
    return std::make_unique<VectorDiskBuilder<T>>(parsed.elem_type,
                                                  std::move(parsed.index_type),
                                                  std::move(parsed.metric_type),
                                                  parsed.version,
                                                  parsed.dim,
                                                  params,
                                                  std::move(local_dir));
}

template <typename T>
void
RegisterMemBuilder() {
    BuilderRegistry<VectorBuildInput<T>>::Instance().Register(
        families::kVectorMem,
        [](const BuildParams& params) { return CreateMemBuilder<T>(params); });
}

template <typename T>
void
RegisterDiskBuilder() {
    BuilderRegistry<PreparedVectorBuildFiles<T>>::Instance().Register(
        families::kVectorDisk,
        [](const BuildParams& params) { return CreateDiskBuilder<T>(params); });
}

struct VectorFamilyRegistrar {
    VectorFamilyRegistrar() {
        LoaderRegistry::Instance().Register<VectorMemLoader>();
        LoaderRegistry::Instance().Register<VectorDiskLoader>();

        RegisterMemBuilder<float>();
        RegisterMemBuilder<bin1>();
        RegisterMemBuilder<float16>();
        RegisterMemBuilder<bfloat16>();
        RegisterMemBuilder<int8>();
        RegisterMemBuilder<sparse_u32_f32>();

        RegisterDiskBuilder<float>();
        RegisterDiskBuilder<bin1>();
        RegisterDiskBuilder<float16>();
        RegisterDiskBuilder<bfloat16>();
        RegisterDiskBuilder<int8>();
        RegisterDiskBuilder<sparse_u32_f32>();
    }
};

[[maybe_unused]] const VectorFamilyRegistrar registrar;

}  // namespace
}  // namespace milvus::index
