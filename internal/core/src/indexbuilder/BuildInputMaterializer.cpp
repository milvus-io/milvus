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

#include "indexbuilder/BuildInputMaterializer.h"

#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "index/Families.h"
#include "index/ParamUtils.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "indexbuilder/JsonBuildMaterializer.h"

namespace milvus::indexbuilder {
namespace {

bool
CompatibleArrayElementType(DataType actual, DataType expected) {
    return index::ScalarValueTypesMatch(actual, expected) ||
           (actual == DataType::INT32 &&
            (expected == DataType::INT8 || expected == DataType::INT16));
}

std::optional<DataType>
ReadDataType(const index::BuildParams& params, std::string_view key) {
    if (params.contains(key) && params.at(key).is_null()) {
        return std::nullopt;
    }
    return index::ReadDataTypeParam(params, key);
}

bool
ParseNested(const index::BuildParams& params) {
    return index::ReadNestedConfigParam(params, "build materializer")
        .value_or(false);
}

DataType
ResolveArrayElementType(const index::BuildParams& params) {
    auto current = ReadDataType(params, "array_element_type");
    auto legacy = ReadDataType(params, "element_type");
    if (current == DataType::NONE)
        current.reset();
    if (legacy == DataType::NONE)
        legacy.reset();
    AssertInfo(!current.has_value() || !legacy.has_value() ||
                   index::ScalarValueTypesMatch(*current, *legacy),
               "array_element_type conflicts with element_type");
    return current.value_or(legacy.value_or(DataType::NONE));
}

bool
IsSupportedScalarType(DataType type) {
    return type == DataType::BOOL || type == DataType::INT8 ||
           type == DataType::INT16 || type == DataType::INT32 ||
           type == DataType::INT64 || type == DataType::TIMESTAMPTZ ||
           type == DataType::FLOAT || type == DataType::DOUBLE ||
           IsStringDataType(type) || type == DataType::GEOMETRY;
}

size_t
FixedArrayElementSize(DataType type) {
    switch (type) {
        case DataType::BOOL:
            return sizeof(bool);
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
            return sizeof(int32_t);
        case DataType::INT64:
            return sizeof(int64_t);
        case DataType::FLOAT:
            return sizeof(float);
        case DataType::DOUBLE:
            return sizeof(double);
        default:
            return 0;
    }
}

void
ValidateArrayRow(const Array& array,
                 DataType expected_type,
                 size_t row,
                 bool validate_narrow_values) {
    AssertInfo(array.length() >= 0,
               "ARRAY row {} has negative element count {}",
               row,
               array.length());
    AssertInfo(
        CompatibleArrayElementType(array.get_element_type(), expected_type),
        "ARRAY row {} element type {} does not match expected {}",
        row,
        array.get_element_type(),
        expected_type);
    const auto count = static_cast<size_t>(array.length());
    if (count == 0)
        return;
    AssertInfo(array.data() != nullptr,
               "ARRAY row {} has null data for {} elements",
               row,
               count);
    if (IsVariableDataType(expected_type)) {
        const auto* offsets = array.get_offsets_data();
        AssertInfo(offsets != nullptr,
                   "ARRAY row {} has no variable-width offsets",
                   row);
        size_t previous = 0;
        for (size_t i = 0; i < count; ++i) {
            const auto offset = static_cast<size_t>(offsets[i]);
            AssertInfo(offset >= previous && offset <= array.byte_size(),
                       "ARRAY row {} has invalid offset {} at {}",
                       row,
                       offset,
                       i);
            previous = offset;
        }
    } else {
        const auto width = FixedArrayElementSize(expected_type);
        AssertInfo(width != 0 &&
                       count <= std::numeric_limits<size_t>::max() / width &&
                       array.byte_size() >= count * width,
                   "ARRAY row {} has invalid byte size {}",
                   row,
                   array.byte_size());
    }
    if (validate_narrow_values &&
        (expected_type == DataType::INT8 || expected_type == DataType::INT16)) {
        const auto min = expected_type == DataType::INT8
                             ? std::numeric_limits<int8_t>::min()
                             : std::numeric_limits<int16_t>::min();
        const auto max = expected_type == DataType::INT8
                             ? std::numeric_limits<int8_t>::max()
                             : std::numeric_limits<int16_t>::max();
        for (size_t i = 0; i < count; ++i) {
            if (!array.is_element_valid(static_cast<int>(i))) {
                continue;
            }
            const auto value =
                array.get_data_unchecked<int32_t>(static_cast<int>(i));
            AssertInfo(value >= min && value <= max,
                       "ARRAY row {} value {} at {} exceeds target range",
                       row,
                       value,
                       i);
        }
    }
}

ValidityView
BatchValidity(const FieldDataPtr& batch) {
    AssertInfo(!batch->IsNullable() || batch->Length() == 0 ||
                   batch->ValidData() != nullptr,
               "nullable scalar batch has no validity bitmap");
    return batch->IsNullable() ? ValidityView::FromPacked(batch->ValidData())
                               : ValidityView{};
}

template <typename T>
class TypedMaterializerBase : public BuildInputMaterializer {
 public:
    TypedMaterializerBase(int64_t expected_rows,
                          const index::IndexFamily& family,
                          const index::BuildParams& params)
        : expected_rows_(expected_rows),
          builder_(
              index::BuilderRegistry<index::ScalarBuildInput<T>>::Instance()
                  .Create(family, params)) {
        AssertInfo(builder_ != nullptr,
                   "index family {} has no scalar builder for requested input",
                   family);
        spec_ = builder_->InputSpec();
        AssertInfo(spec_.side_inputs.empty(),
                   "scalar builder declared unsupported side input");
    }

    const index::BuilderInputSpec&
    InputSpec() const override {
        return spec_;
    }

 protected:
    void
    AddRows(size_t rows) {
        AssertInfo(!consumed_, "cannot add to consumed scalar materializer");
        AssertInfo(
            rows <= static_cast<size_t>(std::numeric_limits<int64_t>::max()) &&
                rows_seen_ <= expected_rows_ &&
                static_cast<int64_t>(rows) <= expected_rows_ - rows_seen_,
            "scalar source exceeds expected row count {}",
            expected_rows_);
        rows_seen_ += static_cast<int64_t>(rows);
    }

    storage::ArtifactPtr
    BuildBatches(std::span<const index::ScalarBuildBatch<T>> batches) {
        AssertInfo(!consumed_, "scalar materializer was already consumed");
        consumed_ = true;
        AssertInfo(rows_seen_ == expected_rows_,
                   "scalar source produced {} rows, expected {}",
                   rows_seen_,
                   expected_rows_);
        index::ScalarBuildInput<T> input{batches};
        auto builder = std::move(builder_);
        try {
            return std::move(*builder).Build(input);
        } catch (...) {
            builder.reset();
            throw;
        }
    }

 private:
    int64_t expected_rows_{0};
    int64_t rows_seen_{0};
    std::unique_ptr<index::IArtifactBuilder<index::ScalarBuildInput<T>>>
        builder_;
    index::BuilderInputSpec spec_;
    bool consumed_{false};
};

template <typename T>
class FixedMaterializer final : public TypedMaterializerBase<T> {
 public:
    FixedMaterializer(DataType source_type,
                      int64_t expected_rows,
                      const index::IndexFamily& family,
                      const index::BuildParams& params)
        : TypedMaterializerBase<T>(expected_rows, family, params),
          source_type_(source_type) {
    }

    void
    Add(const FieldDataPtr& batch) override {
        AssertInfo(batch != nullptr, "scalar source produced a null batch");
        AssertInfo(
            index::ScalarValueTypesMatch(batch->get_data_type(), source_type_),
            "scalar source type {} disagrees with {}",
            batch->get_data_type(),
            source_type_);
        const auto rows = batch->Length();
        const auto* values = static_cast<const T*>(batch->Data());
        AssertInfo(values != nullptr || rows == 0,
                   "scalar batch has null data for {} rows",
                   rows);
        static_cast<void>(BatchValidity(batch));
        this->AddRows(rows);
        owners_.push_back(batch);
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<T>> batches;
        batches.reserve(owners_.size());
        for (const auto& owner : owners_) {
            batches.push_back(
                {std::span<const T>(static_cast<const T*>(owner->Data()),
                                    owner->Length()),
                 BatchValidity(owner)});
        }
        return this->BuildBatches(batches);
    }

 private:
    DataType source_type_{DataType::NONE};
    std::vector<FieldDataPtr> owners_;
};

class StringMaterializer final
    : public TypedMaterializerBase<std::string_view> {
 public:
    struct Batch {
        FieldDataPtr owner;
        std::vector<std::string_view> views;
        ValidityView validity;
    };

    StringMaterializer(DataType source_type,
                       int64_t expected_rows,
                       const index::IndexFamily& family,
                       const index::BuildParams& params)
        : TypedMaterializerBase(expected_rows, family, params),
          source_type_(source_type) {
    }

    void
    Add(const FieldDataPtr& input) override {
        AssertInfo(input != nullptr, "string source produced a null batch");
        AssertInfo(
            index::ScalarValueTypesMatch(input->get_data_type(), source_type_),
            "string source type {} disagrees with {}",
            input->get_data_type(),
            source_type_);
        auto batch = std::make_unique<Batch>();
        batch->owner = input;
        batch->validity = BatchValidity(input);
        const auto* values = static_cast<const std::string*>(input->Data());
        AssertInfo(values != nullptr || input->Length() == 0,
                   "string batch has null row data");
        batch->views.reserve(input->Length());
        for (size_t row = 0; row < input->Length(); ++row) {
            batch->views.emplace_back(values[row]);
        }
        this->AddRows(input->Length());
        batches_.push_back(std::move(batch));
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<std::string_view>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) {
            views.push_back({batch->views, batch->validity});
        }
        return BuildBatches(views);
    }

 private:
    DataType source_type_{DataType::NONE};
    std::vector<std::unique_ptr<Batch>> batches_;
};

class JsonFlatMaterializer final
    : public TypedMaterializerBase<std::string_view> {
 public:
    struct Batch {
        FieldDataPtr owner;
        std::vector<std::string_view> views;
        ValidityView validity;
    };

    using TypedMaterializerBase<std::string_view>::TypedMaterializerBase;

    void
    Add(const FieldDataPtr& input) override {
        AssertInfo(input != nullptr && input->get_data_type() == DataType::JSON,
                   "flat JSON source has incompatible field data");
        auto batch = std::make_unique<Batch>();
        batch->owner = input;
        batch->validity = BatchValidity(input);
        const auto* documents = static_cast<const Json*>(input->Data());
        AssertInfo(documents != nullptr || input->Length() == 0,
                   "JSON batch has null row data");
        batch->views.reserve(input->Length());
        for (size_t row = 0; row < input->Length(); ++row) {
            batch->views.emplace_back(!batch->validity || batch->validity[row]
                                          ? documents[row].data()
                                          : std::string_view{});
        }
        this->AddRows(input->Length());
        batches_.push_back(std::move(batch));
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<std::string_view>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) {
            views.push_back({batch->views, batch->validity});
        }
        return BuildBatches(views);
    }

 private:
    std::vector<std::unique_ptr<Batch>> batches_;
};

class ArrayViewMaterializer final : public TypedMaterializerBase<ArrayView> {
 public:
    struct Batch {
        FieldDataPtr owner;
        std::vector<ArrayView> views;
        ValidityView validity;
        char empty_data{0};
        uint32_t empty_offset{0};
    };

    ArrayViewMaterializer(DataType element_type,
                          int64_t expected_rows,
                          const index::IndexFamily& family,
                          const index::BuildParams& params)
        : TypedMaterializerBase(expected_rows, family, params),
          element_type_(element_type) {
    }

    void
    Add(const FieldDataPtr& input) override {
        AssertInfo(
            input != nullptr && input->get_data_type() == DataType::ARRAY,
            "ARRAY source has incompatible field data");
        auto batch = std::make_unique<Batch>();
        batch->owner = input;
        batch->validity = BatchValidity(input);
        batch->views.reserve(input->Length());
        for (size_t row = 0; row < input->Length(); ++row) {
            const bool valid = !batch->validity || batch->validity[row];
            const auto* array =
                valid ? static_cast<const Array*>(input->RawValue(row))
                      : nullptr;
            if (valid)
                ValidateArrayRow(*array, element_type_, row, true);
            const bool empty = !valid || array->length() == 0;
            batch->views.emplace_back(
                empty ? &batch->empty_data : const_cast<char*>(array->data()),
                empty ? 0 : array->length(),
                empty ? 0 : array->byte_size(),
                element_type_,
                empty && IsVariableDataType(element_type_)
                    ? &batch->empty_offset
                    : (empty ? nullptr : array->get_offsets_data()),
                empty ? nullptr : array->get_element_valid_data(),
                !empty && array->is_element_nullable(),
                !empty && array->has_invalid_element());
        }
        this->AddRows(input->Length());
        batches_.push_back(std::move(batch));
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<ArrayView>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) {
            views.push_back({batch->views, batch->validity});
        }
        return BuildBatches(views);
    }

 private:
    DataType element_type_{DataType::NONE};
    std::vector<std::unique_ptr<Batch>> batches_;
};

template <typename T>
class NestedArrayMaterializer final : public TypedMaterializerBase<T> {
 public:
    NestedArrayMaterializer(DataType element_type,
                            int64_t expected_rows,
                            const index::IndexFamily& family,
                            const index::BuildParams& params)
        : TypedMaterializerBase<T>(expected_rows, family, params),
          element_type_(element_type) {
    }

    void
    Add(const FieldDataPtr& input) override {
        AssertInfo(
            input != nullptr && input->get_data_type() == DataType::ARRAY,
            "nested ARRAY source has incompatible field data");
        const auto validity = BatchValidity(input);
        auto values = std::make_unique<std::vector<T>>();
        for (size_t row = 0; row < input->Length(); ++row) {
            if (validity && !validity[row])
                continue;
            const auto& array =
                *static_cast<const Array*>(input->RawValue(row));
            ValidateArrayRow(array, element_type_, row, true);
            for (int i = 0; i < array.length(); ++i) {
                if constexpr (std::is_same_v<T, int8_t> ||
                              std::is_same_v<T, int16_t>) {
                    values->push_back(
                        static_cast<T>(array.get_data_unchecked<int32_t>(i)));
                } else {
                    values->push_back(array.get_data_unchecked<T>(i));
                }
            }
        }
        this->AddRows(input->Length());
        batches_.push_back(std::move(values));
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<T>> views;
        views.reserve(batches_.size());
        for (const auto& values : batches_) {
            views.push_back({*values, {}});
        }
        return this->BuildBatches(views);
    }

 private:
    DataType element_type_{DataType::NONE};
    std::vector<std::unique_ptr<std::vector<T>>> batches_;
};

class NestedBoolMaterializer final : public TypedMaterializerBase<bool> {
 public:
    struct Batch {
        std::unique_ptr<bool[]> values;
        size_t size{0};
    };

    NestedBoolMaterializer(int64_t expected_rows,
                           const index::IndexFamily& family,
                           const index::BuildParams& params)
        : TypedMaterializerBase(expected_rows, family, params) {
    }

    void
    Add(const FieldDataPtr& input) override {
        AssertInfo(
            input != nullptr && input->get_data_type() == DataType::ARRAY,
            "nested BOOL ARRAY source has incompatible field data");
        const auto validity = BatchValidity(input);
        size_t size = 0;
        for (size_t row = 0; row < input->Length(); ++row) {
            if (validity && !validity[row])
                continue;
            const auto& array =
                *static_cast<const Array*>(input->RawValue(row));
            ValidateArrayRow(array, DataType::BOOL, row, false);
            AssertInfo(static_cast<size_t>(array.length()) <=
                           std::numeric_limits<size_t>::max() - size,
                       "nested BOOL ARRAY element count overflows size_t");
            size += array.length();
        }
        auto batch = std::make_unique<Batch>();
        batch->values = size == 0 ? nullptr : std::make_unique<bool[]>(size);
        batch->size = size;
        size_t next = 0;
        for (size_t row = 0; row < input->Length(); ++row) {
            if (validity && !validity[row])
                continue;
            const auto& array =
                *static_cast<const Array*>(input->RawValue(row));
            for (int i = 0; i < array.length(); ++i) {
                batch->values[next++] = array.get_data_unchecked<bool>(i);
            }
        }
        this->AddRows(input->Length());
        batches_.push_back(std::move(batch));
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<bool>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) {
            views.push_back({{batch->values.get(), batch->size}, {}});
        }
        return BuildBatches(views);
    }

 private:
    std::vector<std::unique_ptr<Batch>> batches_;
};

class NestedStringMaterializer final
    : public TypedMaterializerBase<std::string_view> {
 public:
    struct Batch {
        FieldDataPtr owner;
        std::vector<std::string_view> values;
    };

    NestedStringMaterializer(DataType element_type,
                             int64_t expected_rows,
                             const index::IndexFamily& family,
                             const index::BuildParams& params)
        : TypedMaterializerBase(expected_rows, family, params),
          element_type_(element_type) {
    }

    void
    Add(const FieldDataPtr& input) override {
        AssertInfo(
            input != nullptr && input->get_data_type() == DataType::ARRAY,
            "nested string ARRAY source has incompatible field data");
        const auto validity = BatchValidity(input);
        auto batch = std::make_unique<Batch>();
        batch->owner = input;
        for (size_t row = 0; row < input->Length(); ++row) {
            if (validity && !validity[row])
                continue;
            const auto& array =
                *static_cast<const Array*>(input->RawValue(row));
            ValidateArrayRow(array, element_type_, row, false);
            for (int i = 0; i < array.length(); ++i) {
                batch->values.push_back(
                    array.get_data_unchecked<std::string_view>(i));
            }
        }
        this->AddRows(input->Length());
        batches_.push_back(std::move(batch));
    }

    storage::ArtifactPtr
        Build() &&
        override {
        std::vector<index::ScalarBuildBatch<std::string_view>> views;
        views.reserve(batches_.size());
        for (const auto& batch : batches_) {
            views.push_back({batch->values, {}});
        }
        return BuildBatches(views);
    }

 private:
    DataType element_type_{DataType::NONE};
    std::vector<std::unique_ptr<Batch>> batches_;
};

template <typename T>
BuildInputMaterializerPtr
MakeFixed(DataType source_type,
          int64_t expected_rows,
          const index::IndexFamily& family,
          const index::BuildParams& params) {
    return std::make_unique<FixedMaterializer<T>>(
        source_type, expected_rows, family, params);
}

template <typename T>
BuildInputMaterializerPtr
MakeNested(DataType element_type,
           int64_t expected_rows,
           const index::IndexFamily& family,
           const index::BuildParams& params) {
    return std::make_unique<NestedArrayMaterializer<T>>(
        element_type, expected_rows, family, params);
}

}  // namespace

BuildInputMaterializerPtr
MakeScalarBuildInputMaterializer(DataType source_type,
                                 DataType value_type,
                                 int64_t expected_rows,
                                 const index::IndexFamily& family,
                                 const index::BuildParams& params) {
    AssertInfo(params.is_object(), "build parameters must be an object");
    if (source_type == DataType::JSON) {
        AssertInfo(!ParseNested(params),
                   "JSON build input supports only the row coordinate domain");
        if (family == index::families::kJsonFlat &&
            value_type == DataType::JSON) {
            const auto configured_value = ReadDataType(params, "value_type");
            AssertInfo(
                configured_value.has_value() &&
                    *configured_value == DataType::JSON &&
                    params.contains(JSON_CAST_TYPE) &&
                    params.at(JSON_CAST_TYPE).is_string() &&
                    params.at(JSON_CAST_TYPE).get<std::string>() == "JSON",
                "flat JSON build requires JSON value_type and cast");
            return std::make_unique<JsonFlatMaterializer>(
                expected_rows, family, params);
        }
        return MakeJsonBuildMaterializer(
            value_type, expected_rows, family, params);
    }

    const auto nested = ParseNested(params);
    const auto element_type = ResolveArrayElementType(params);
    if (source_type == DataType::ARRAY) {
        AssertInfo(element_type != DataType::NONE &&
                       IsSupportedScalarType(element_type) &&
                       element_type != DataType::GEOMETRY &&
                       element_type != DataType::TIMESTAMPTZ,
                   "unsupported ARRAY element type {}",
                   element_type);
        if (!nested) {
            AssertInfo(
                index::ScalarValueTypesMatch(value_type, element_type),
                "ordinary ARRAY value type {} conflicts with element type {}",
                value_type,
                element_type);
            return std::make_unique<ArrayViewMaterializer>(
                element_type, expected_rows, family, params);
        }
        AssertInfo(index::ScalarValueTypesMatch(value_type, element_type),
                   "nested ARRAY value type {} conflicts with element type {}",
                   value_type,
                   element_type);
        switch (value_type) {
            case DataType::BOOL:
                return std::make_unique<NestedBoolMaterializer>(
                    expected_rows, family, params);
            case DataType::INT8:
                return MakeNested<int8_t>(
                    element_type, expected_rows, family, params);
            case DataType::INT16:
                return MakeNested<int16_t>(
                    element_type, expected_rows, family, params);
            case DataType::INT32:
                return MakeNested<int32_t>(
                    element_type, expected_rows, family, params);
            case DataType::INT64:
                return MakeNested<int64_t>(
                    element_type, expected_rows, family, params);
            case DataType::FLOAT:
                return MakeNested<float>(
                    element_type, expected_rows, family, params);
            case DataType::DOUBLE:
                return MakeNested<double>(
                    element_type, expected_rows, family, params);
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
                return std::make_unique<NestedStringMaterializer>(
                    element_type, expected_rows, family, params);
            default:
                ThrowInfo(Unsupported,
                          "unsupported nested ARRAY value type {}",
                          value_type);
        }
    }

    AssertInfo(!nested, "nested build input requires ARRAY field type");
    AssertInfo(IsSupportedScalarType(source_type) &&
                   index::ScalarValueTypesMatch(source_type, value_type),
               "field type {} does not match scalar value type {}",
               source_type,
               value_type);
    switch (value_type) {
        case DataType::BOOL:
            return MakeFixed<bool>(source_type, expected_rows, family, params);
        case DataType::INT8:
            return MakeFixed<int8_t>(
                source_type, expected_rows, family, params);
        case DataType::INT16:
            return MakeFixed<int16_t>(
                source_type, expected_rows, family, params);
        case DataType::INT32:
            return MakeFixed<int32_t>(
                source_type, expected_rows, family, params);
        case DataType::INT64:
            return MakeFixed<int64_t>(
                source_type, expected_rows, family, params);
        case DataType::FLOAT:
            return MakeFixed<float>(source_type, expected_rows, family, params);
        case DataType::DOUBLE:
            return MakeFixed<double>(
                source_type, expected_rows, family, params);
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::GEOMETRY:
            return std::make_unique<StringMaterializer>(
                source_type, expected_rows, family, params);
        default:
            ThrowInfo(Unsupported,
                      "unsupported scalar build value type {}",
                      value_type);
    }
}

}  // namespace milvus::indexbuilder
