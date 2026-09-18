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

#include "index/scalar/inverted/InvertedIndexBuilder.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

#include "index/ParamUtils.h"
#include "index/scalar/inverted/InvertedIndexReader.h"
#include "common/EasyAssert.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/inverted/InvertedIndexArtifact.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index {
namespace {

using inverted_params::IsSupportedType;

bool
ParseNested(const Config& params, bool fallback = false) {
    return ReadNestedConfigParam(params, "inverted").value_or(fallback);
}

int64_t
ParseIntegral(const Config& params,
              std::string_view key,
              int64_t fallback,
              bool required) {
    if (!params.is_object() || !params.contains(key)) {
        if (required) {
            ThrowInfo(
                DataTypeInvalid, "inverted builder requires parameter {}", key);
        }
        return fallback;
    }
    const auto& encoded = params.at(key);
    if (encoded.is_number_unsigned()) {
        uint64_t value;
        try {
            value = encoded.get<uint64_t>();
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid inverted parameter {}: {}",
                      key,
                      error.what());
        }
        if (value >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            ThrowInfo(
                DataTypeInvalid, "inverted parameter {} is out of range", key);
        }
        return static_cast<int64_t>(value);
    }
    if (encoded.is_number_integer()) {
        try {
            return encoded.get<int64_t>();
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid inverted parameter {}: {}",
                      key,
                      error.what());
        }
    }
    if (encoded.is_string()) {
        try {
            const auto text = encoded.get<std::string>();
            size_t parsed = 0;
            const auto value = std::stoll(text, &parsed);
            if (parsed == text.size()) {
                return value;
            }
        } catch (const std::invalid_argument&) {
        } catch (const std::out_of_range&) {
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid inverted parameter {}: {}",
                      key,
                      error.what());
        }
    }
    ThrowInfo(DataTypeInvalid, "inverted parameter {} must be an integer", key);
}

std::string
ParseString(const Config& params,
            std::string_view key,
            std::string fallback = {}) {
    return ReadStringParam(params, key, std::move(fallback), "inverted");
}

InvertedBuildParams
ParseBuildParams(const Config& params,
                 DataType default_type,
                 bool array_builder) {
    InvertedBuildParams result;
    result.field_name =
        std::to_string(ParseIntegral(params, FIELD_ID, 0, true));
    result.field_type =
        ReadDataTypeParam(params, "field_type")
            .value_or(array_builder ? DataType::ARRAY : default_type);
    result.nested = ParseNested(params);
    result.local_dir = ParseString(params, "local_dir");

    const auto array_element_type =
        ReadDataTypeParam(params, "array_element_type")
            .value_or(DataType::NONE);
    const auto legacy_element_type =
        ReadDataTypeParam(params, "element_type").value_or(DataType::NONE);
    if (array_element_type != DataType::NONE &&
        legacy_element_type != DataType::NONE &&
        !ScalarValueTypesMatch(array_element_type, legacy_element_type)) {
        ThrowInfo(DataTypeInvalid,
                  "inverted array_element_type {} conflicts with element_type "
                  "{}",
                  static_cast<int>(array_element_type),
                  static_cast<int>(legacy_element_type));
    }
    const auto element_type = array_element_type != DataType::NONE
                                  ? array_element_type
                                  : legacy_element_type;
    const auto configured =
        ReadDataTypeParam(params, "value_type").value_or(DataType::NONE);

    if (array_builder) {
        if (result.field_type != DataType::ARRAY || result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "inverted ArrayView input requires non-nested ARRAY "
                      "field_type");
        }
        result.value_type =
            element_type != DataType::NONE
                ? element_type
                : (configured != DataType::NONE && configured != DataType::ARRAY
                       ? configured
                       : DataType::NONE);
    } else if (result.field_type == DataType::ARRAY) {
        if (!result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "non-nested ARRAY inverted input must use ArrayView");
        }
        result.value_type =
            element_type != DataType::NONE
                ? element_type
                : (configured != DataType::NONE && configured != DataType::ARRAY
                       ? configured
                       : default_type);
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
    }

    if (result.field_type == DataType::ARRAY && configured != DataType::NONE &&
        configured != DataType::ARRAY && element_type != DataType::NONE &&
        !ScalarValueTypesMatch(configured, element_type)) {
        ThrowInfo(DataTypeInvalid,
                  "inverted ARRAY value_type {} conflicts with element type "
                  "{}",
                  static_cast<int>(configured),
                  static_cast<int>(element_type));
    }

    if (!IsSupportedType(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported inverted value type {}",
                  static_cast<int>(result.value_type));
    }
    if (!array_builder &&
        !ScalarValueTypesMatch(result.value_type, default_type)) {
        ThrowInfo(DataTypeInvalid,
                  "inverted value type {} does not match builder type {}",
                  static_cast<int>(result.value_type),
                  static_cast<int>(default_type));
    }
    if (result.field_type != DataType::ARRAY &&
        result.field_type != DataType::JSON &&
        !ScalarValueTypesMatch(result.field_type, result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "inverted field type {} conflicts with value type {}",
                  static_cast<int>(result.field_type),
                  static_cast<int>(result.value_type));
    }

    const auto engine_version =
        ParseIntegral(params, SCALAR_INDEX_ENGINE_VERSION, 1, false);
    const auto configured_tantivy =
        ParseIntegral(params, TANTIVY_INDEX_VERSION, 0, false);
    if (engine_version < 0 || configured_tantivy < 0 ||
        configured_tantivy > std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "invalid inverted engine versions scalar={} tantivy={}",
                  engine_version,
                  configured_tantivy);
    }
    result.tantivy_index_version =
        configured_tantivy != 0
            ? static_cast<uint32_t>(configured_tantivy)
            : engine_version <= 1 ? TANTIVY_INDEX_MINIMUM_VERSION
                                  : TANTIVY_INDEX_LATEST_VERSION;
    result.single_segment = engine_version == 0;
    if (result.tantivy_index_version != TANTIVY_INDEX_MINIMUM_VERSION &&
        result.tantivy_index_version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported Tantivy index version {}",
                  result.tantivy_index_version);
    }
    if (result.single_segment &&
        result.tantivy_index_version != TANTIVY_INDEX_MINIMUM_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "single-segment inverted index requires Tantivy version {}",
                  TANTIVY_INDEX_MINIMUM_VERSION);
    }
    return result;
}

TantivyDataType
TantivyType(DataType type) {
    if (type == DataType::BOOL) {
        return TantivyDataType::Bool;
    }
    if (type == DataType::FLOAT || type == DataType::DOUBLE) {
        return TantivyDataType::F64;
    }
    if (IsStringDataType(type)) {
        return TantivyDataType::Keyword;
    }
    if (type == DataType::INT8 || type == DataType::INT16 ||
        type == DataType::INT32 || type == DataType::INT64 ||
        type == DataType::TIMESTAMPTZ) {
        return TantivyDataType::I64;
    }
    ThrowInfo(DataTypeInvalid,
              "unsupported inverted value type {}",
              static_cast<int>(type));
}

void
CheckAppend(size_t current, size_t count) {
    constexpr size_t kMaxDocs = std::numeric_limits<uint32_t>::max();
    if (count > std::numeric_limits<size_t>::max() - current ||
        current > kMaxDocs || count > kMaxDocs - current) {
        ThrowInfo(DataTypeInvalid,
                  "inverted document count {} + {} exceeds uint32 count "
                  "domain",
                  current,
                  count);
    }
}

std::shared_ptr<milvus::tantivy::TantivyIndexWrapper>
CreateEngine(const InvertedBuildParams& params,
             const std::shared_ptr<storage::LocalDirectory>& directory) {
    return std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        params.field_name.c_str(),
        TantivyType(params.value_type),
        directory->Path().c_str(),
        params.tantivy_index_version,
        params.single_segment,
        true);
}

template <typename T>
void
AddContiguous(milvus::tantivy::TantivyIndexWrapper& engine,
              const T* values,
              size_t n,
              size_t offset,
              bool single_segment) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        std::string value;
        for (size_t i = 0; i < n; ++i) {
            AssignString(value, values[i]);
            if (single_segment) {
                engine.add_data_by_single_segment_writer(&value, 1);
            } else {
                engine.add_data(&value, 1, static_cast<int64_t>(offset + i));
            }
        }
    } else if (single_segment) {
        engine.add_data_by_single_segment_writer(values, n);
    } else {
        engine.add_data(values, n, static_cast<int64_t>(offset));
    }
}

template <typename T>
void
AddOneDocument(milvus::tantivy::TantivyIndexWrapper& engine,
               const T* value,
               bool valid,
               size_t offset,
               bool single_segment) {
    if (single_segment) {
        engine.add_array_data_by_single_segment_writer(value, valid ? 1 : 0);
    } else {
        engine.add_array_data(
            value, valid ? 1 : 0, static_cast<int64_t>(offset));
    }
}

template <typename T>
void
AddArrayDocument(milvus::tantivy::TantivyIndexWrapper& engine,
                 const ArrayView& array,
                 size_t offset,
                 bool single_segment) {
    const auto* values = static_cast<const T*>(array.data());
    const auto length = static_cast<size_t>(array.length());
    if (single_segment) {
        engine.add_array_data_by_single_segment_writer(values, length);
    } else {
        engine.add_array_data(values, length, static_cast<int64_t>(offset));
    }
}

void
AddEmptyDocument(milvus::tantivy::TantivyIndexWrapper& engine,
                 size_t offset,
                 bool single_segment) {
    const int64_t* empty = nullptr;
    if (single_segment) {
        engine.add_array_data_by_single_segment_writer(empty, 0);
    } else {
        engine.add_array_data(empty, 0, static_cast<int64_t>(offset));
    }
}

void
AddArrayByType(milvus::tantivy::TantivyIndexWrapper& engine,
               const ArrayView& array,
               DataType type,
               size_t offset,
               bool single_segment,
               std::vector<std::string>& string_scratch) {
    switch (type) {
        case DataType::BOOL:
            return AddArrayDocument<bool>(
                engine, array, offset, single_segment);
        case DataType::INT8:
        case DataType::INT16:
            // INT8/INT16 ARRAY elements are physically stored as int32_t.
            return AddArrayDocument<int32_t>(
                engine, array, offset, single_segment);
        case DataType::INT32:
            return AddArrayDocument<int32_t>(
                engine, array, offset, single_segment);
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return AddArrayDocument<int64_t>(
                engine, array, offset, single_segment);
        case DataType::FLOAT:
            return AddArrayDocument<float>(
                engine, array, offset, single_segment);
        case DataType::DOUBLE:
            return AddArrayDocument<double>(
                engine, array, offset, single_segment);
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT: {
            string_scratch.clear();
            string_scratch.reserve(static_cast<size_t>(array.length()));
            for (int i = 0; i < array.length(); ++i) {
                string_scratch.emplace_back(
                    array.get_data<std::string_view>(i));
            }
            if (single_segment) {
                engine.add_array_data_by_single_segment_writer(
                    string_scratch.data(), string_scratch.size());
            } else {
                engine.add_array_data(string_scratch.data(),
                                      string_scratch.size(),
                                      static_cast<int64_t>(offset));
            }
            return;
        }
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported inverted ARRAY element type {}",
                      static_cast<int>(type));
    }
}

storage::ArtifactPtr
FinishBuilder(std::shared_ptr<storage::LocalDirectory> directory,
              std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
              std::vector<size_t> null_offsets) {
    AssertInfo(engine != nullptr, "inverted builder has no writer to seal");
    engine->finish();
    engine.reset();
    return std::make_unique<InvertedIndexArtifact>(std::move(directory),
                                                   std::move(null_offsets));
}

}  // namespace

template <typename T>
InvertedIndexBuilder<T>::InvertedIndexBuilder(InvertedBuildParams params)
    : params_(std::move(params)),
      directory_(CreateInvertedIndexDirectory(params_.local_dir)),
      engine_(CreateEngine(params_, directory_)) {
}

template <typename T>
InvertedIndexBuilder<T>::~InvertedIndexBuilder() = default;

template <typename T>
void
InvertedIndexBuilder<T>::AddBatch(const ScalarBuildBatch<T>& batch) {
    const auto n = batch.values.size();
    if (params_.nested && batch.validity) {
        for (size_t i = 0; i < n; ++i) {
            AssertInfo(batch.validity[i],
                       "nested inverted input uses flattened element "
                       "coordinates and requires absent or all-true "
                       "validity");
        }
    }
    CheckAppend(count_, n);
    if (!params_.nested && batch.validity) {
        size_t invalid = 0;
        for (size_t i = 0; i < n; ++i) {
            invalid += batch.validity[i] ? 0 : 1;
        }
        if (invalid > null_offsets_.max_size() - null_offsets_.size()) {
            ThrowInfo(DataTypeInvalid,
                      "inverted null-offset count exceeds vector capacity");
        }
        null_offsets_.reserve(null_offsets_.size() + invalid);
    }

    if (params_.nested || !batch.validity) {
        AddContiguous(
            *engine_, batch.values.data(), n, count_, params_.single_segment);
        count_ += n;
        return;
    }

    std::string string_scratch;
    for (size_t i = 0; i < n; ++i) {
        const auto offset = count_ + i;
        const bool is_valid = batch.validity[i];
        if (!is_valid) {
            null_offsets_.push_back(offset);
        }
        if constexpr (std::is_same_v<T, std::string_view>) {
            if (is_valid) {
                AssignString(string_scratch, batch.values[i]);
            } else {
                string_scratch.clear();
            }
            AddOneDocument(*engine_,
                           &string_scratch,
                           is_valid,
                           offset,
                           params_.single_segment);
        } else {
            AddOneDocument(*engine_,
                           batch.values.data() + i,
                           is_valid,
                           offset,
                           params_.single_segment);
        }
    }
    count_ += n;
}

template <typename T>
storage::ArtifactPtr
InvertedIndexBuilder<T>::Build(const ScalarBuildInput<T>& input) && {
    for (const auto& batch : input.batches) {
        AddBatch(batch);
    }
    auto engine = std::exchange(engine_, nullptr);
    auto directory = std::move(directory_);
    auto null_offsets = std::move(null_offsets_);
    return FinishBuilder(std::move(directory),
                         std::move(engine),
                         std::move(null_offsets));
}

InvertedArrayIndexBuilder::InvertedArrayIndexBuilder(InvertedBuildParams params)
    : params_(std::move(params)),
      directory_(CreateInvertedIndexDirectory(params_.local_dir)),
      engine_(CreateEngine(params_, directory_)) {
}

InvertedArrayIndexBuilder::~InvertedArrayIndexBuilder() = default;

void
InvertedArrayIndexBuilder::AddBatch(const ScalarBuildBatch<ArrayView>& batch) {
    const auto n = batch.values.size();
    CheckAppend(count_, n);
    for (size_t i = 0; i < n; ++i) {
        const bool is_valid = !batch.validity || batch.validity[i];
        if (!is_valid) {
            continue;
        }
        if (batch.values[i].length() < 0) {
            ThrowInfo(DataTypeInvalid,
                      "inverted ARRAY row {} has negative length {}",
                      i,
                      batch.values[i].length());
        }
        if (!ScalarValueTypesMatch(batch.values[i].get_element_type(),
                                   params_.value_type)) {
            ThrowInfo(DataTypeInvalid,
                      "ARRAY element type {} does not match inverted type {}",
                      static_cast<int>(batch.values[i].get_element_type()),
                      static_cast<int>(params_.value_type));
        }
    }
    size_t invalid = 0;
    if (batch.validity) {
        for (size_t i = 0; i < n; ++i) {
            invalid += batch.validity[i] ? 0 : 1;
        }
    }
    if (invalid > null_offsets_.max_size() - null_offsets_.size()) {
        ThrowInfo(DataTypeInvalid,
                  "inverted null-offset count exceeds vector capacity");
    }
    null_offsets_.reserve(null_offsets_.size() + invalid);

    std::vector<std::string> string_scratch;
    for (size_t i = 0; i < n; ++i) {
        const auto offset = count_ + i;
        const bool is_valid = !batch.validity || batch.validity[i];
        if (!is_valid) {
            null_offsets_.push_back(offset);
            AddEmptyDocument(*engine_, offset, params_.single_segment);
            continue;
        }
        AddArrayByType(*engine_,
                       batch.values[i],
                       params_.value_type,
                       offset,
                       params_.single_segment,
                       string_scratch);
    }
    count_ += n;
}

storage::ArtifactPtr
InvertedArrayIndexBuilder::Build(const ScalarBuildInput<ArrayView>& input) && {
    for (const auto& batch : input.batches) {
        AddBatch(batch);
    }
    auto engine = std::exchange(engine_, nullptr);
    auto directory = std::move(directory_);
    auto null_offsets = std::move(null_offsets_);
    return FinishBuilder(std::move(directory),
                         std::move(engine),
                         std::move(null_offsets));
}

namespace {

template <typename T>
bool
RegisterInvertedBuilder() {
    BuilderRegistry<ScalarBuildInput<T>>::Instance().Register(
        families::kInverted, [](const BuildParams& params) {
            return std::make_unique<InvertedIndexBuilder<T>>(
                ParseBuildParams(params, CppDataType<T>(), false));
        });
    return true;
}

bool
RegisterInvertedArrayBuilder() {
    BuilderRegistry<ScalarBuildInput<ArrayView>>::Instance().Register(
        families::kInverted, [](const BuildParams& params) {
            return std::make_unique<InvertedArrayIndexBuilder>(
                ParseBuildParams(params, DataType::NONE, true));
        });
    return true;
}

const bool kRegistered =
    RegisterInvertedBuilder<bool>() && RegisterInvertedBuilder<int8_t>() &&
    RegisterInvertedBuilder<int16_t>() && RegisterInvertedBuilder<int32_t>() &&
    RegisterInvertedBuilder<int64_t>() && RegisterInvertedBuilder<float>() &&
    RegisterInvertedBuilder<double>() &&
    RegisterInvertedBuilder<std::string_view>() &&
    RegisterInvertedArrayBuilder();

}  // namespace

#define INSTANTIATE_INVERTED_BUILDER(T) template class InvertedIndexBuilder<T>;
INSTANTIATE_INVERTED_BUILDER(bool)
INSTANTIATE_INVERTED_BUILDER(int8_t)
INSTANTIATE_INVERTED_BUILDER(int16_t)
INSTANTIATE_INVERTED_BUILDER(int32_t)
INSTANTIATE_INVERTED_BUILDER(int64_t)
INSTANTIATE_INVERTED_BUILDER(float)
INSTANTIATE_INVERTED_BUILDER(double)
INSTANTIATE_INVERTED_BUILDER(std::string_view)
#undef INSTANTIATE_INVERTED_BUILDER

}  // namespace milvus::index
