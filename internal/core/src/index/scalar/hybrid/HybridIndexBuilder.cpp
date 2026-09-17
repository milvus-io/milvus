// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/scalar/hybrid/HybridIndexBuilder.h"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include "index/ParamUtils.h"
#include "common/Array.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "index/scalar/hybrid/HybridIndexArtifact.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

int64_t
ParseInteger(const nlohmann::json& value, std::string_view key) {
    if (value.is_number_unsigned()) {
        const auto parsed = value.get<uint64_t>();
        if (parsed <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(parsed);
        }
    } else if (value.is_number_integer()) {
        return value.get<int64_t>();
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        int64_t parsed = 0;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), parsed);
        if (!text.empty() && error == std::errc{} &&
            end == text.data() + text.size()) {
            return parsed;
        }
    }
    ThrowInfo(DataTypeInvalid, "HYBRID parameter {} must be an integer", key);
}

int64_t
RequiredInteger(const BuildParams& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        ThrowInfo(DataTypeInvalid, "HYBRID parameter {} is required", key);
    }
    return ParseInteger(params.at(key), key);
}

int64_t
OptionalInteger(const BuildParams& params,
                std::string_view key,
                int64_t default_value) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return default_value;
    }
    return ParseInteger(params.at(key), key);
}

bool
ParseNested(const BuildParams& params) {
    return ReadNestedConfigParam(params, "HYBRID").value_or(false);
}

DataType
RequireDataType(const BuildParams& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        ThrowInfo(DataTypeInvalid, "HYBRID parameter {} is required", key);
    }
    return ParseDataTypeValue(params.at(key), key);
}

DataType
ParseElementType(const BuildParams& params) {
    auto current = !params.contains("array_element_type") ||
                           params.at("array_element_type").is_null()
                       ? DataType::NONE
                       : ParseDataTypeValue(params.at("array_element_type"),
                                            "array_element_type");
    auto legacy = !params.contains("element_type") ||
                          params.at("element_type").is_null()
                      ? DataType::NONE
                      : ParseDataTypeValue(params.at("element_type"),
                                           "element_type");
    if (current == DataType::NONE) {
        current = legacy;
    } else if (legacy != DataType::NONE && current != legacy &&
               !(IsStringDataType(current) && IsStringDataType(legacy))) {
        ThrowInfo(DataTypeInvalid,
                  "HYBRID array_element_type conflicts with element_type");
    }
    return current;
}

template <typename T>
bool
CompatibleValueType(DataType type) {
    if constexpr (std::is_same_v<T, bool>) {
        return type == DataType::BOOL;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return type == DataType::INT8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return type == DataType::INT16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return type == DataType::INT32;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return type == DataType::INT64 || type == DataType::TIMESTAMPTZ;
    } else if constexpr (std::is_same_v<T, float>) {
        return type == DataType::FLOAT;
    } else if constexpr (std::is_same_v<T, double>) {
        return type == DataType::DOUBLE;
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        return IsStringDataType(type);
    }
    return false;
}

bool
CompatibleArrayElementType(DataType actual, DataType expected) {
    return actual == expected ||
           (IsStringDataType(actual) && IsStringDataType(expected)) ||
           (actual == DataType::INT32 &&
            (expected == DataType::INT8 || expected == DataType::INT16));
}

bool
IsSupportedArrayElement(DataType type) {
    return type == DataType::BOOL || type == DataType::INT8 ||
           type == DataType::INT16 || type == DataType::INT32 ||
           type == DataType::INT64 || type == DataType::FLOAT ||
           type == DataType::DOUBLE || IsStringDataType(type);
}

ScalarIndexType
SelectorForFamily(const IndexFamily& family) {
    if (family == families::kBitmap) {
        return ScalarIndexType::BITMAP;
    }
    if (family == families::kSort) {
        return ScalarIndexType::STLSORT;
    }
    if (family == families::kMarisa) {
        return ScalarIndexType::MARISA;
    }
    if (family == families::kInverted) {
        return ScalarIndexType::INVERTED;
    }
    ThrowInfo(UnexpectedError,
              "HYBRID selected family {} has no persisted selector",
              family);
}

template <typename T>
struct OwnedProbeValue {
    using Type = T;

    static Type
    Copy(T value) {
        return value;
    }
};

template <>
struct OwnedProbeValue<std::string_view> {
    using Type = std::string;

    static Type
    Copy(std::string_view value) {
        return std::string(value);
    }
};

template <typename T>
class CardinalityProbe {
 public:
    explicit CardinalityProbe(DataType) {
    }

    void
    Observe(const ScalarBuildBatch<T>& batch, size_t limit) {
        for (size_t i = 0; i < batch.values.size() && values_.size() < limit;
             ++i) {
            if (!batch.validity || batch.validity[i]) {
                values_.insert(OwnedProbeValue<T>::Copy(batch.values[i]));
            }
        }
    }

    size_t
    Size() const {
        return values_.size();
    }

 private:
    std::set<typename OwnedProbeValue<T>::Type> values_;
};

template <>
class CardinalityProbe<ArrayView> {
 public:
    using Sets = std::variant<std::set<bool>,
                              std::set<int8_t>,
                              std::set<int16_t>,
                              std::set<int32_t>,
                              std::set<int64_t>,
                              std::set<float>,
                              std::set<double>,
                              std::set<std::string>>;

    explicit CardinalityProbe(DataType element_type)
        : element_type_(element_type) {
        switch (element_type_) {
            case DataType::BOOL:
                values_.emplace<std::set<bool>>();
                break;
            case DataType::INT8:
                values_.emplace<std::set<int8_t>>();
                break;
            case DataType::INT16:
                values_.emplace<std::set<int16_t>>();
                break;
            case DataType::INT32:
                values_.emplace<std::set<int32_t>>();
                break;
            case DataType::INT64:
                values_.emplace<std::set<int64_t>>();
                break;
            case DataType::FLOAT:
                values_.emplace<std::set<float>>();
                break;
            case DataType::DOUBLE:
                values_.emplace<std::set<double>>();
                break;
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
                values_.emplace<std::set<std::string>>();
                break;
            default:
                ThrowInfo(DataTypeInvalid,
                          "HYBRID does not support ARRAY element type {}",
                          static_cast<int>(element_type_));
        }
    }

    void
    Observe(const ScalarBuildBatch<ArrayView>& batch, size_t limit) {
        for (size_t row = 0; row < batch.values.size() && Size() < limit;
             ++row) {
            if (batch.validity && !batch.validity[row]) {
                continue;
            }
            const auto& array = batch.values[row];
            if (array.length() < 0 ||
                !CompatibleArrayElementType(array.get_element_type(),
                                            element_type_)) {
                ThrowInfo(DataTypeInvalid,
                          "HYBRID ARRAY row {} has incompatible element type",
                          row);
            }
            if (array.length() != 0 && array.data() == nullptr) {
                ThrowInfo(DataFormatBroken,
                          "HYBRID ARRAY row {} has null element data",
                          row);
            }
            for (int i = 0; i < array.length() && Size() < limit; ++i) {
                Insert(array, i);
            }
        }
    }

    size_t
    Size() const {
        return std::visit([](const auto& values) { return values.size(); },
                          values_);
    }

 private:
    void
    Insert(const ArrayView& array, int offset) {
        switch (element_type_) {
            case DataType::BOOL:
                std::get<std::set<bool>>(values_).insert(
                    array.get_data<bool>(offset));
                return;
            case DataType::INT8: {
                const auto value = array.get_data<int32_t>(offset);
                if (value < std::numeric_limits<int8_t>::min() ||
                    value > std::numeric_limits<int8_t>::max()) {
                    ThrowInfo(DataFormatBroken,
                              "HYBRID ARRAY INT8 value {} is out of range",
                              value);
                }
                std::get<std::set<int8_t>>(values_).insert(
                    static_cast<int8_t>(value));
                return;
            }
            case DataType::INT16: {
                const auto value = array.get_data<int32_t>(offset);
                if (value < std::numeric_limits<int16_t>::min() ||
                    value > std::numeric_limits<int16_t>::max()) {
                    ThrowInfo(DataFormatBroken,
                              "HYBRID ARRAY INT16 value {} is out of range",
                              value);
                }
                std::get<std::set<int16_t>>(values_).insert(
                    static_cast<int16_t>(value));
                return;
            }
            case DataType::INT32:
                std::get<std::set<int32_t>>(values_).insert(
                    array.get_data<int32_t>(offset));
                return;
            case DataType::INT64:
                std::get<std::set<int64_t>>(values_).insert(
                    array.get_data<int64_t>(offset));
                return;
            case DataType::FLOAT:
                std::get<std::set<float>>(values_).insert(
                    array.get_data<float>(offset));
                return;
            case DataType::DOUBLE:
                std::get<std::set<double>>(values_).insert(
                    array.get_data<double>(offset));
                return;
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
                std::get<std::set<std::string>>(values_).insert(
                    std::string(array.get_data<std::string_view>(offset)));
                return;
            default:
                ThrowInfo(DataTypeInvalid,
                          "HYBRID does not support ARRAY element type {}",
                          static_cast<int>(element_type_));
        }
    }

    DataType element_type_;
    Sets values_;
};

template <typename T>
HybridBuildParams
ParseHybridBuildParams(const BuildParams& params) {
    AssertInfo(params.is_object(), "HYBRID build parameters must be an object");
    HybridBuildParams result;
    result.delegate_params = params;
    const auto limit = RequiredInteger(params, BITMAP_INDEX_CARDINALITY_LIMIT);
    if (limit <= 0 || limit > std::numeric_limits<int32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "HYBRID bitmap cardinality limit {} is outside (0, {}]",
                  limit,
                  std::numeric_limits<int32_t>::max());
    }
    result.cardinality_limit = static_cast<int32_t>(limit);
    const auto field_type = RequireDataType(params, "field_type");
    result.value_type = RequireDataType(params, "value_type");
    result.element_type = ParseElementType(params);
    const auto nested = ParseNested(params);

    if constexpr (!std::is_same_v<T, ArrayView>) {
        if (!CompatibleValueType<T>(result.value_type)) {
            ThrowInfo(DataTypeInvalid,
                      "HYBRID typed builder conflicts with value_type {}",
                      static_cast<int>(result.value_type));
        }
    }
    if (field_type == DataType::ARRAY) {
        if (!IsSupportedArrayElement(result.element_type)) {
            ThrowInfo(DataTypeInvalid,
                      "HYBRID does not support ARRAY element type {}",
                      static_cast<int>(result.element_type));
        }
        if (nested) {
            if constexpr (std::is_same_v<T, ArrayView>) {
                ThrowInfo(DataTypeInvalid,
                          "nested HYBRID ARRAY requires an element builder");
            } else if (!CompatibleValueType<T>(result.element_type)) {
                ThrowInfo(DataTypeInvalid,
                          "nested HYBRID value_type conflicts with ARRAY "
                          "element type");
            }
        } else {
            if constexpr (std::is_same_v<T, ArrayView>) {
                if (!ScalarValueTypesMatch(result.value_type,
                                           result.element_type)) {
                    ThrowInfo(DataTypeInvalid,
                              "HYBRID ARRAY value_type conflicts with element "
                              "type");
                }
            } else {
                ThrowInfo(DataTypeInvalid,
                          "ordinary HYBRID ARRAY requires an ArrayView builder");
            }
        }
    } else {
        if (nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested HYBRID build requires ARRAY field_type");
        }
        if constexpr (std::is_same_v<T, ArrayView>) {
            ThrowInfo(DataTypeInvalid,
                      "ArrayView HYBRID build requires ARRAY field_type");
        }
        if (!CompatibleValueType<T>(field_type)) {
            ThrowInfo(DataTypeInvalid,
                      "HYBRID field_type conflicts with its typed builder");
        }
    }

    if (field_type == DataType::ARRAY) {
        result.low_cardinality_family = families::kBitmap;
        result.high_cardinality_family = families::kInverted;
        return result;
    }

    const auto version = OptionalInteger(params,
                                         SCALAR_INDEX_ENGINE_VERSION,
                                         kLastVersionWithoutHybridIndexConfig);
    if (version >= kHybridIndexConfigVersion) {
        result.low_cardinality_family =
            GetLowCardinalityFamilyFromConfig(params);
        result.high_cardinality_family =
            GetHighCardinalityFamilyFromConfig(params);
    } else {
        result.low_cardinality_family = families::kBitmap;
        if constexpr (std::is_same_v<T, std::string_view>) {
            result.high_cardinality_family = families::kInverted;
        } else if constexpr (std::is_integral_v<T>) {
            result.high_cardinality_family = families::kSort;
        } else {
            result.high_cardinality_family = families::kInverted;
        }
    }
    return result;
}

}  // namespace

template <typename T>
HybridIndexBuilder<T>::HybridIndexBuilder(HybridBuildParams params)
    : params_(std::move(params)) {
    AssertInfo(params_.cardinality_limit > 0,
               "HYBRID cardinality limit must be positive");
    AssertInfo(!params_.low_cardinality_family.empty() &&
                   !params_.high_cardinality_family.empty(),
               "HYBRID family selection is not configured");
}

template <typename T>
HybridIndexBuilder<T>::~HybridIndexBuilder() = default;

template <typename T>
storage::ArtifactPtr
HybridIndexBuilder<T>::Build(const ScalarBuildInput<T>& input) && {
    std::string family;
    {
        CardinalityProbe<T> probe(params_.element_type);
        const auto limit = static_cast<size_t>(params_.cardinality_limit);
        for (const auto& batch : input.batches) {
            if (probe.Size() >= limit) {
                break;
            }
            probe.Observe(batch, limit);
        }
        family = SelectFamily(probe.Size());
    }
    auto delegate = BuilderRegistry<ScalarBuildInput<T>>::Instance().Create(
        family, params_.delegate_params);
    if (delegate == nullptr) {
        ThrowInfo(Unsupported,
                  "HYBRID selected family {} has no builder for value type {}",
                  family,
                  static_cast<int>(params_.value_type));
    }
    const auto selector = SelectorForFamily(family);
    auto artifact = std::move(*delegate).Build(input);
    return std::make_unique<HybridIndexArtifact>(std::move(artifact), selector);
}

template <typename T>
std::string
HybridIndexBuilder<T>::SelectFamily(size_t distinct_count) const {
    return distinct_count >= static_cast<size_t>(params_.cardinality_limit)
               ? params_.high_cardinality_family
               : params_.low_cardinality_family;
}

namespace {

template <typename T>
bool
RegisterHybridBuilder() {
    BuilderRegistry<ScalarBuildInput<T>>::Instance().Register(
        families::kHybrid, [](const BuildParams& params) {
            return std::make_unique<HybridIndexBuilder<T>>(
                ParseHybridBuildParams<T>(params));
        });
    return true;
}

const bool kRegistered =
    RegisterHybridBuilder<bool>() && RegisterHybridBuilder<int8_t>() &&
    RegisterHybridBuilder<int16_t>() && RegisterHybridBuilder<int32_t>() &&
    RegisterHybridBuilder<int64_t>() && RegisterHybridBuilder<float>() &&
    RegisterHybridBuilder<double>() &&
    RegisterHybridBuilder<std::string_view>() &&
    RegisterHybridBuilder<ArrayView>();

}  // namespace

#define INSTANTIATE_HYBRID_BUILDER(T) template class HybridIndexBuilder<T>;
INSTANTIATE_HYBRID_BUILDER(bool)
INSTANTIATE_HYBRID_BUILDER(int8_t)
INSTANTIATE_HYBRID_BUILDER(int16_t)
INSTANTIATE_HYBRID_BUILDER(int32_t)
INSTANTIATE_HYBRID_BUILDER(int64_t)
INSTANTIATE_HYBRID_BUILDER(float)
INSTANTIATE_HYBRID_BUILDER(double)
INSTANTIATE_HYBRID_BUILDER(std::string_view)
INSTANTIATE_HYBRID_BUILDER(ArrayView)
#undef INSTANTIATE_HYBRID_BUILDER

}  // namespace milvus::index
