// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "indexbuilder/JsonBuildMaterializer.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "common/Array.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/JsonCastFunction.h"
#include "common/JsonCastType.h"
#include "common/JsonUtils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/scalar/json/JsonProjectedIndexArtifact.h"
#include "index/scalar/ngram/JsonProjectedString.h"
#include "simdjson/dom/array.h"
#include "simdjson/dom/element.h"

namespace milvus::indexbuilder {
namespace {

constexpr size_t kChunkRows = 64 * 1024;

struct ProjectionParams {
    std::string json_path;
    std::vector<std::string> path_tokens;
    JsonCastType cast_type;
    JsonCastFunction cast_function;
    bool emit_legacy_non_exist_sidecar{true};
};

enum class RowPresence {
    FieldNull,
    NoValue,
    Present,
};

std::string
ReadString(const index::BuildParams& params,
           std::string_view key,
           bool required) {
    if (!params.contains(key)) {
        AssertInfo(!required, "typed JSON build requires parameter {}", key);
        return {};
    }
    AssertInfo(params.at(key).is_string(),
               "typed JSON parameter {} must be a string",
               key);
    return params.at(key).get<std::string>();
}

DataType
RequireDataType(const index::BuildParams& params, std::string_view key) {
    AssertInfo(
        params.contains(key), "typed JSON build requires parameter {}", key);
    return index::ParseDataTypeValue(params.at(key), key);
}

ProjectionParams
ParseProjectionParams(DataType value_type,
                      const index::IndexFamily& family,
                      const index::BuildParams& params) {
    AssertInfo(params.is_object(),
               "typed JSON build parameters must be an object");
    AssertInfo(RequireDataType(params, "field_type") == DataType::JSON,
               "typed JSON build requires JSON field_type");
    const auto nested =
        index::ReadNestedConfigParam(params, "typed JSON projection");
    AssertInfo(nested.has_value() && !*nested,
               "typed JSON projection requires normalized row domain");
    for (const auto key : {std::string_view("element_type"),
                           std::string_view("array_element_type")}) {
        if (params.contains(key)) {
            AssertInfo(RequireDataType(params, key) == DataType::NONE,
                       "typed JSON outer parameter {} must be NONE",
                       key);
        }
    }

    const auto cast_name = ReadString(params, JSON_CAST_TYPE, true);
    AssertInfo(cast_name == "BOOL" || cast_name == "DOUBLE" ||
                   cast_name == "VARCHAR" || cast_name == "ARRAY_BOOL" ||
                   cast_name == "ARRAY_DOUBLE" || cast_name == "ARRAY_VARCHAR",
               "unsupported typed JSON cast type {}",
               cast_name);
    const auto cast_type = JsonCastType::FromString(cast_name);
    AssertInfo(value_type == index::JsonProjectedValueTypeForCast(cast_type) &&
                   RequireDataType(params, "value_type") == value_type,
               "typed JSON value_type disagrees with cast {}",
               cast_type);

    const auto index_type = ReadString(params, index::INDEX_TYPE, true);
    const bool scalar = cast_type.data_type() != JsonCastType::DataType::ARRAY;
    bool family_supported = false;
    if (family == index::families::kHybrid) {
        family_supported = index_type == index::HYBRID_INDEX_TYPE && scalar;
    } else if (family == index::families::kInverted) {
        family_supported = index_type == index::INVERTED_INDEX_TYPE;
    } else if (family == index::families::kSort) {
        family_supported =
            index_type == index::ASCENDING_SORT && scalar &&
            (value_type == DataType::DOUBLE || value_type == DataType::VARCHAR);
    } else if (family == index::families::kBitmap) {
        family_supported =
            index_type == index::BITMAP_INDEX_TYPE && scalar &&
            (value_type == DataType::BOOL || value_type == DataType::VARCHAR);
    } else if (family == index::families::kNgram) {
        family_supported = index_type == index::NGRAM_INDEX_TYPE && scalar &&
                           value_type == DataType::VARCHAR;
    }
    AssertInfo(
        family_supported && (scalar || family == index::families::kInverted),
        "typed JSON cast {} is not supported by family {}",
        cast_type,
        family);

    const bool has_json_path = params.contains(JSON_PATH);
    const bool has_nested_path = params.contains("nested_path");
    AssertInfo(has_json_path || has_nested_path,
               "typed JSON build requires json_path");
    const auto json_path =
        has_json_path ? ReadString(params, JSON_PATH, true) : std::string{};
    const auto nested_path = has_nested_path
                                 ? ReadString(params, "nested_path", true)
                                 : std::string{};
    AssertInfo(!has_json_path || !has_nested_path || json_path == nested_path,
               "typed JSON json_path and nested_path disagree");
    auto path = has_json_path ? json_path : nested_path;
    static_cast<void>(index::JsonProjectedIndexSpec(path, cast_type, 0));
    auto path_tokens = parse_json_pointer(path);

    const auto cast_function_name =
        ReadString(params, JSON_CAST_FUNCTION, false);
    AssertInfo(cast_function_name.empty() ||
                   (cast_function_name == "STRING_TO_DOUBLE" &&
                    cast_type.data_type() == JsonCastType::DataType::DOUBLE),
               "unsupported typed JSON cast function {} for {}",
               cast_function_name,
               cast_type);
    return {std::move(path),
            std::move(path_tokens),
            cast_type,
            JsonCastFunction::FromString(cast_function_name),
            family != index::families::kNgram};
}

index::BuildParams
MakeInnerParams(const index::IndexFamily& family,
                JsonCastType cast_type,
                DataType value_type,
                const index::BuildParams& outer) {
    auto inner = outer;
    if (family == index::families::kHybrid) {
        inner["field_type"] = static_cast<int32_t>(value_type);
        inner["element_type"] = static_cast<int32_t>(DataType::NONE);
        inner["array_element_type"] = static_cast<int32_t>(DataType::NONE);
    } else if (cast_type.data_type() == JsonCastType::DataType::ARRAY) {
        inner["field_type"] = static_cast<int32_t>(DataType::ARRAY);
        inner["element_type"] = static_cast<int32_t>(value_type);
        inner["array_element_type"] = static_cast<int32_t>(value_type);
    }
    return inner;
}

class JsonBatch {
 public:
    explicit JsonBatch(const FieldDataPtr& batch) : owner_(batch) {
        AssertInfo(
            owner_ != nullptr && owner_->get_data_type() == DataType::JSON,
            "typed JSON source has incompatible field data");
        documents_ = static_cast<const Json*>(owner_->Data());
        AssertInfo(documents_ != nullptr || owner_->Length() == 0,
                   "JSON batch has null row data");
        validity_ = owner_->IsNullable()
                        ? ValidityView::FromPacked(owner_->ValidData())
                        : ValidityView{};
        AssertInfo(!owner_->IsNullable() || owner_->Length() == 0 || validity_,
                   "nullable JSON batch has no validity bitmap");
    }

    size_t
    Count() const {
        return owner_->Length();
    }

    bool
    IsValid(size_t row) const {
        return !validity_ || validity_[row];
    }

    const Json&
    Document(size_t row) const {
        return documents_[row];
    }

 private:
    FieldDataPtr owner_;
    const Json* documents_{nullptr};
    ValidityView validity_;
};

RowPresence
Locate(const JsonBatch& batch, size_t row, const ProjectionParams& params) {
    if (!batch.IsValid(row))
        return RowPresence::FieldNull;
    const auto& document = batch.Document(row);
    AssertInfo(!document.data().empty(),
               "valid JSON row {} has an empty document",
               row);
    auto root = document.dom_doc();
    if (!path_exists(root.value(), params.path_tokens) ||
        !document.exist(params.json_path)) {
        return RowPresence::NoValue;
    }
    return RowPresence::Present;
}

template <typename T>
std::optional<T>
ExtractScalar(const Json& document, const ProjectionParams& params) {
    if constexpr (std::is_same_v<T, double>) {
        if (params.cast_function.match<double>()) {
            return JsonCastFunction::CastJsonValue<double>(
                params.cast_function, document, params.json_path);
        }
    }
    auto value = document.at<T>(params.json_path);
    if (value.error() != simdjson::SUCCESS)
        return std::nullopt;
    return value.value();
}

template <typename T>
class ProjectedBase : public BuildInputMaterializer {
 public:
    ProjectedBase(
        std::unique_ptr<
            index::IArtifactBuilder<index::ScalarBuildInput<T>>> builder,
        ProjectionParams params,
        int64_t expected_rows)
        : builder_(std::move(builder)),
          params_(std::move(params)),
          expected_rows_(expected_rows) {
        AssertInfo(builder_ != nullptr,
                   "typed JSON materializer has no builder");
        spec_ = builder_->InputSpec();
        AssertInfo(spec_.side_inputs.empty(),
                   "typed JSON builder declared unsupported side input");
    }

    const index::BuilderInputSpec&
    InputSpec() const override {
        return spec_;
    }

 protected:
    const ProjectionParams&
    Params() const {
        return params_;
    }

    void
    AddRows(size_t rows, const std::vector<size_t>& local_non_exist) {
        AssertInfo(
            rows <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) &&
                row_count_ <= expected_rows_ &&
                static_cast<int64_t>(rows) <= expected_rows_ - row_count_,
            "typed JSON source exceeds expected row count {}",
            expected_rows_);
        for (const auto offset : local_non_exist) {
            AssertInfo(offset < rows,
                       "typed JSON local non-exist offset is out of range");
            non_exist_offsets_.push_back(static_cast<size_t>(row_count_) +
                                         offset);
        }
        row_count_ += static_cast<int64_t>(rows);
    }

    storage::ArtifactPtr
    BuildProjected(std::span<const index::ScalarBuildBatch<T>> batches) {
        AssertInfo(row_count_ == expected_rows_,
                   "typed JSON source produced {} rows, expected {}",
                   row_count_,
                   expected_rows_);
        index::ScalarBuildInput<T> input{batches};
        auto builder = std::move(builder_);
        try {
            auto inner = std::move(*builder).Build(input);
            return std::make_unique<index::JsonProjectedIndexArtifact>(
                std::move(inner),
                params_.json_path,
                params_.cast_type,
                row_count_,
                std::move(non_exist_offsets_),
                params_.emit_legacy_non_exist_sidecar);
        } catch (...) {
            builder.reset();
            throw;
        }
    }

 private:
    std::unique_ptr<index::IArtifactBuilder<index::ScalarBuildInput<T>>>
        builder_;
    ProjectionParams params_;
    index::BuilderInputSpec spec_;
    int64_t expected_rows_{0};
    int64_t row_count_{0};
    std::vector<size_t> non_exist_offsets_;
};

template <typename T>
class ScalarProjectedMaterializer final : public ProjectedBase<T> {
 public:
    struct Batch {
        std::vector<T> values;
        std::unique_ptr<bool[]> validity;
        size_t size{0};
    };
    using ProjectedBase<T>::ProjectedBase;

    void
    Add(const FieldDataPtr& input) override {
        const JsonBatch source(input);
        for (size_t begin = 0; begin < source.Count(); begin += kChunkRows) {
            const auto count = std::min(kChunkRows, source.Count() - begin);
            auto batch = std::make_unique<Batch>();
            batch->values.resize(count);
            batch->validity =
                count == 0 ? nullptr : std::make_unique<bool[]>(count);
            batch->size = count;
            std::vector<size_t> non_exist;
            for (size_t i = 0; i < count; ++i) {
                const auto row = begin + i;
                const auto presence = Locate(source, row, this->Params());
                if (presence != RowPresence::Present) {
                    batch->values[i] = T{};
                    batch->validity[i] = false;
                    non_exist.push_back(i);
                    continue;
                }
                const auto value =
                    ExtractScalar<T>(source.Document(row), this->Params());
                batch->values[i] = value.value_or(T{});
                batch->validity[i] = value.has_value();
            }
            this->AddRows(count, non_exist);
            batches_.push_back(std::move(batch));
        }
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<T>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) {
            views.push_back(
                {batch->values,
                 ValidityView::FromExpanded(batch->validity.get())});
        }
        return this->BuildProjected(views);
    }

 private:
    std::vector<std::unique_ptr<Batch>> batches_;
};

template <>
class ScalarProjectedMaterializer<bool> final : public ProjectedBase<bool> {
 public:
    struct Batch {
        std::unique_ptr<bool[]> values;
        std::unique_ptr<bool[]> validity;
        size_t size{0};
    };
    using ProjectedBase<bool>::ProjectedBase;

    void
    Add(const FieldDataPtr& input) override {
        const JsonBatch source(input);
        for (size_t begin = 0; begin < source.Count(); begin += kChunkRows) {
            const auto count = std::min(kChunkRows, source.Count() - begin);
            auto batch = std::make_unique<Batch>();
            batch->values =
                count == 0 ? nullptr : std::make_unique<bool[]>(count);
            batch->validity =
                count == 0 ? nullptr : std::make_unique<bool[]>(count);
            batch->size = count;
            std::vector<size_t> non_exist;
            for (size_t i = 0; i < count; ++i) {
                const auto row = begin + i;
                const auto presence = Locate(source, row, this->Params());
                if (presence != RowPresence::Present) {
                    batch->values[i] = false;
                    batch->validity[i] = false;
                    non_exist.push_back(i);
                    continue;
                }
                const auto value =
                    ExtractScalar<bool>(source.Document(row), this->Params());
                batch->values[i] = value.value_or(false);
                batch->validity[i] = value.has_value();
            }
            this->AddRows(count, non_exist);
            batches_.push_back(std::move(batch));
        }
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<bool>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) {
            views.push_back(
                {{batch->values.get(), batch->size},
                 ValidityView::FromExpanded(batch->validity.get())});
        }
        return BuildProjected(views);
    }

 private:
    std::vector<std::unique_ptr<Batch>> batches_;
};

class StringProjectedMaterializer final
    : public ProjectedBase<std::string_view> {
 public:
    struct Batch {
        std::vector<std::string> owners;
        std::vector<std::string_view> values;
        std::unique_ptr<bool[]> validity;
    };
    using ProjectedBase<std::string_view>::ProjectedBase;

    void
    Add(const FieldDataPtr& input) override {
        const JsonBatch source(input);
        for (size_t begin = 0; begin < source.Count(); begin += kChunkRows) {
            const auto count = std::min(kChunkRows, source.Count() - begin);
            auto batch = std::make_unique<Batch>();
            batch->owners.resize(count);
            batch->values.resize(count);
            batch->validity =
                count == 0 ? nullptr : std::make_unique<bool[]>(count);
            std::vector<size_t> non_exist;
            for (size_t i = 0; i < count; ++i) {
                const auto row = begin + i;
                const auto presence = Locate(source, row, Params());
                if (presence != RowPresence::Present) {
                    batch->validity[i] = false;
                    non_exist.push_back(i);
                    continue;
                }
                const auto value = ExtractScalar<std::string_view>(
                    source.Document(row), Params());
                if (value.has_value())
                    batch->owners[i].assign(*value);
                batch->validity[i] = value.has_value();
            }
            for (size_t i = 0; i < count; ++i)
                batch->values[i] = batch->owners[i];
            AddRows(count, non_exist);
            batches_.push_back(std::move(batch));
        }
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<std::string_view>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) {
            views.push_back(
                {batch->values,
                 ValidityView::FromExpanded(batch->validity.get())});
        }
        return BuildProjected(views);
    }

 private:
    std::vector<std::unique_ptr<Batch>> batches_;
};

class NgramProjectedMaterializer final
    : public ProjectedBase<index::JsonProjectedString> {
 public:
    struct Batch {
        std::vector<std::string> owners;
        std::vector<index::JsonProjectedString> values;
    };
    using ProjectedBase<index::JsonProjectedString>::ProjectedBase;

    void
    Add(const FieldDataPtr& input) override {
        const JsonBatch source(input);
        for (size_t begin = 0; begin < source.Count(); begin += kChunkRows) {
            const auto count = std::min(kChunkRows, source.Count() - begin);
            auto batch = std::make_unique<Batch>();
            batch->owners.resize(count);
            batch->values.resize(count);
            std::vector<size_t> non_exist;
            std::vector<index::JsonProjectedStringState> states(count);
            for (size_t i = 0; i < count; ++i) {
                const auto row = begin + i;
                const auto presence = Locate(source, row, Params());
                if (presence == RowPresence::FieldNull) {
                    states[i] = index::JsonProjectedStringState::FieldNull;
                    non_exist.push_back(i);
                } else if (presence == RowPresence::NoValue) {
                    states[i] = index::JsonProjectedStringState::NoValue;
                    non_exist.push_back(i);
                } else {
                    const auto value = ExtractScalar<std::string_view>(
                        source.Document(row), Params());
                    if (value.has_value()) {
                        batch->owners[i].assign(*value);
                        states[i] = index::JsonProjectedStringState::Value;
                    } else {
                        // Existing-but-bad-cast is not a non-exist offset.
                        states[i] = index::JsonProjectedStringState::NoValue;
                    }
                }
            }
            for (size_t i = 0; i < count; ++i) {
                batch->values[i] = {batch->owners[i], states[i]};
            }
            AddRows(count, non_exist);
            batches_.push_back(std::move(batch));
        }
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<index::JsonProjectedString>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) views.push_back({batch->values, {}});
        return BuildProjected(views);
    }

 private:
    std::vector<std::unique_ptr<Batch>> batches_;
};

class ArrayProjectedMaterializer final : public ProjectedBase<ArrayView> {
 public:
    struct Row {
        std::vector<uint8_t> bool_values;
        std::vector<double> double_values;
        std::string string_bytes;
        std::vector<uint32_t> string_offsets;
        char empty_data{0};
        uint32_t empty_offset{0};
    };
    struct Batch {
        std::vector<Row> rows;
        std::vector<ArrayView> views;
        std::unique_ptr<bool[]> validity;
    };

    ArrayProjectedMaterializer(
        std::unique_ptr<
            index::IArtifactBuilder<index::ScalarBuildInput<ArrayView>>>
            builder,
        ProjectionParams params,
        int64_t expected_rows,
        DataType element_type)
        : ProjectedBase(std::move(builder), std::move(params), expected_rows),
          element_type_(element_type) {
    }

    void
    Add(const FieldDataPtr& input) override {
        const JsonBatch source(input);
        for (size_t begin = 0; begin < source.Count(); begin += kChunkRows) {
            const auto count = std::min(kChunkRows, source.Count() - begin);
            auto batch = std::make_unique<Batch>();
            batch->rows.resize(count);
            batch->views.reserve(count);
            batch->validity =
                count == 0 ? nullptr : std::make_unique<bool[]>(count);
            std::vector<size_t> non_exist;
            for (size_t i = 0; i < count; ++i) {
                const auto row = begin + i;
                const auto presence = Locate(source, row, Params());
                batch->validity[i] = presence != RowPresence::FieldNull;
                if (presence != RowPresence::Present)
                    non_exist.push_back(i);
                if (presence == RowPresence::Present) {
                    ExtractArray(source.Document(row), batch->rows[i]);
                }
            }
            for (auto& row : batch->rows) batch->views.push_back(MakeView(row));
            AddRows(count, non_exist);
            batches_.push_back(std::move(batch));
        }
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<ArrayView>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) {
            views.push_back(
                {batch->views,
                 ValidityView::FromExpanded(batch->validity.get())});
        }
        return BuildProjected(views);
    }

 private:
    void
    ExtractArray(const Json& document, Row& row) const {
        auto root = document.dom_doc();
        auto array = root.value().at_pointer(Params().json_path).get_array();
        if (array.error() != simdjson::SUCCESS)
            return;
        for (const auto element : array.value()) {
            if (element_type_ == DataType::BOOL) {
                auto value = element.get<bool>();
                if (value.error() == simdjson::SUCCESS)
                    row.bool_values.push_back(value.value() ? 1 : 0);
            } else if (element_type_ == DataType::DOUBLE) {
                auto value = element.get<double>();
                if (value.error() == simdjson::SUCCESS)
                    row.double_values.push_back(value.value());
            } else {
                auto value = element.get<std::string_view>();
                if (value.error() != simdjson::SUCCESS)
                    continue;
                AssertInfo(row.string_bytes.size() <=
                                   std::numeric_limits<uint32_t>::max() &&
                               value.value().size() <=
                                   std::numeric_limits<uint32_t>::max() -
                                       row.string_bytes.size(),
                           "typed JSON string ARRAY exceeds uint32 domain");
                row.string_offsets.push_back(
                    static_cast<uint32_t>(row.string_bytes.size()));
                row.string_bytes.append(value.value());
            }
        }
    }

    ArrayView
    MakeView(Row& row) const {
        if (element_type_ == DataType::BOOL) {
            AssertInfo(row.bool_values.size() <=
                           static_cast<size_t>(std::numeric_limits<int>::max()),
                       "typed JSON BOOL ARRAY exceeds int domain");
            return {row.bool_values.empty()
                        ? &row.empty_data
                        : reinterpret_cast<char*>(row.bool_values.data()),
                    static_cast<int>(row.bool_values.size()),
                    row.bool_values.size(),
                    DataType::BOOL,
                    nullptr};
        }
        if (element_type_ == DataType::DOUBLE) {
            AssertInfo(
                row.double_values.size() <=
                        static_cast<size_t>(std::numeric_limits<int>::max()) &&
                    row.double_values.size() <=
                        std::numeric_limits<size_t>::max() / sizeof(double),
                "typed JSON DOUBLE ARRAY exceeds size domain");
            return {row.double_values.empty()
                        ? &row.empty_data
                        : reinterpret_cast<char*>(row.double_values.data()),
                    static_cast<int>(row.double_values.size()),
                    row.double_values.size() * sizeof(double),
                    DataType::DOUBLE,
                    nullptr};
        }
        AssertInfo(row.string_offsets.size() <=
                       static_cast<size_t>(std::numeric_limits<int>::max()),
                   "typed JSON VARCHAR ARRAY exceeds int domain");
        return {row.string_bytes.empty() ? &row.empty_data
                                         : row.string_bytes.data(),
                static_cast<int>(row.string_offsets.size()),
                row.string_bytes.size(),
                DataType::VARCHAR,
                row.string_offsets.empty() ? &row.empty_offset
                                           : row.string_offsets.data()};
    }

    DataType element_type_{DataType::NONE};
    std::vector<std::unique_ptr<Batch>> batches_;
};

template <typename T>
std::unique_ptr<index::IArtifactBuilder<index::ScalarBuildInput<T>>>
CreateBuilder(const index::IndexFamily& family,
              const index::BuildParams& params,
              DataType value_type) {
    auto builder =
        index::BuilderRegistry<index::ScalarBuildInput<T>>::Instance().Create(
            family, params);
    AssertInfo(builder != nullptr,
               "index family {} has no typed JSON builder for value type {}",
               family,
               value_type);
    return builder;
}

}  // namespace

BuildInputMaterializerPtr
MakeJsonBuildMaterializer(DataType value_type,
                          int64_t expected_rows,
                          const index::IndexFamily& family,
                          const index::BuildParams& params) {
    auto projection = ParseProjectionParams(value_type, family, params);
    auto inner_params =
        MakeInnerParams(family, projection.cast_type, value_type, params);
    if (projection.cast_type.data_type() == JsonCastType::DataType::ARRAY) {
        return std::make_unique<ArrayProjectedMaterializer>(
            CreateBuilder<ArrayView>(family, inner_params, value_type),
            std::move(projection),
            expected_rows,
            value_type);
    }
    if (family == index::families::kNgram) {
        return std::make_unique<NgramProjectedMaterializer>(
            CreateBuilder<index::JsonProjectedString>(
                family, inner_params, value_type),
            std::move(projection),
            expected_rows);
    }
    switch (value_type) {
        case DataType::BOOL:
            return std::make_unique<ScalarProjectedMaterializer<bool>>(
                CreateBuilder<bool>(family, inner_params, value_type),
                std::move(projection),
                expected_rows);
        case DataType::DOUBLE:
            return std::make_unique<ScalarProjectedMaterializer<double>>(
                CreateBuilder<double>(family, inner_params, value_type),
                std::move(projection),
                expected_rows);
        case DataType::VARCHAR:
            return std::make_unique<StringProjectedMaterializer>(
                CreateBuilder<std::string_view>(
                    family, inner_params, value_type),
                std::move(projection),
                expected_rows);
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported typed JSON value type {}",
                      value_type);
    }
}

}  // namespace milvus::indexbuilder
