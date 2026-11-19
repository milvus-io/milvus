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

#pragma once

#include <stdint.h>
#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "pb/schema.pb.h"

namespace milvus {
using TypeParams = std::map<std::string, std::string>;
using TokenizerParams = std::string;

TokenizerParams
ParseTokenizerParams(const TypeParams& params);

// A lightweight handle to an immutable field description. Copies share the
// description without retaining its originating Schema or Collection.
class FieldMeta {
 public:
    FieldMeta(const FieldMeta&) = default;
    FieldMeta(FieldMeta&&) = default;
    FieldMeta&
    operator=(const FieldMeta&) = delete;
    FieldMeta&
    operator=(FieldMeta&&) = default;

    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              bool nullable,
              std::optional<DefaultValueType> default_value,
              std::string external_field_mapping = "",
              std::string local_format = LOCAL_FORMAT_RAW)
        : FieldMeta(Data{
              .name_ = std::move(name),
              .id_ = id,
              .type_ = type,
              .nullable_ = nullable,
              .default_value_ = std::move(default_value),
              .external_field_mapping_ = std::move(external_field_mapping),
              .local_format_ = std::move(local_format),
          }) {
        Assert(!IsVectorDataType(data_->type_));
    }

    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              int64_t max_length,
              bool nullable,
              std::optional<DefaultValueType> default_value,
              std::string external_field_mapping = "",
              std::string local_format = LOCAL_FORMAT_RAW)
        : FieldMeta(Data{
              .name_ = std::move(name),
              .id_ = id,
              .type_ = type,
              .nullable_ = nullable,
              .default_value_ = std::move(default_value),
              .string_info_ = StringInfo{max_length},
              .external_field_mapping_ = std::move(external_field_mapping),
              .local_format_ = std::move(local_format),
          }) {
        Assert(IsStringDataType(data_->type_));
    }

    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              int64_t max_length,
              bool nullable,
              bool enable_match,
              bool enable_analyzer,
              std::map<std::string, std::string>& params,
              std::optional<DefaultValueType> default_value,
              std::string external_field_mapping = "",
              std::string local_format = LOCAL_FORMAT_RAW)
        : FieldMeta(Data{
              .name_ = std::move(name),
              .id_ = id,
              .type_ = type,
              .nullable_ = nullable,
              .default_value_ = std::move(default_value),
              .string_info_ =
                  StringInfo{
                      max_length,
                      enable_match,
                      enable_analyzer,
                      std::move(params),
                  },
              .external_field_mapping_ = std::move(external_field_mapping),
              .local_format_ = std::move(local_format),
          }) {
        Assert(IsStringDataType(data_->type_));
    }

    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              DataType element_type,
              bool nullable,
              bool element_nullable,
              std::optional<DefaultValueType> default_value,
              std::string external_field_mapping = "",
              std::string local_format = LOCAL_FORMAT_RAW,
              std::optional<milvus::proto::schema::TypeSchema> type_schema =
                  std::nullopt,
              TypeParams type_params = {}) {
        Assert(type == DataType::ARRAY);
        AssertInfo(element_type != DataType::ARRAY || type_schema.has_value(),
                   "nested ARRAY field {} requires type_schema",
                   name.get());
        if (type_schema.has_value()) {
            AssertInfo(
                type == DataType::ARRAY && element_type == DataType::ARRAY,
                "type_schema is only supported for nested ARRAY "
                "FieldMeta");
            ValidateNestedTypeSchema(*type_schema, name.get());

            AssertInfo(type_schema->nullable() == nullable,
                       "nested ARRAY field {} root nullable does not match "
                       "FieldSchema.nullable",
                       name.get());
            AssertInfo(
                type_schema->array_element().nullable() == element_nullable,
                "nested ARRAY field {} element nullable does not "
                "match FieldSchema.element_nullable",
                name.get());
        } else if (element_nullable) {
            ValidateArrayLeafType(element_type, name.get());
            proto::schema::TypeSchema type;
            type.set_nullable(nullable);
            auto* leaf = type.mutable_array_element();
            leaf->set_leaf_type(ToProtoDataType(element_type));
            leaf->set_nullable(true);
            for (const auto& [key, value] : type_params) {
                if (key != MAX_LENGTH && key != "max_capacity") {
                    continue;
                }
                auto* param =
                    (key == MAX_LENGTH ? leaf : &type)->add_type_params();
                param->set_key(key);
                param->set_value(value);
            }
            type_schema = std::move(type);
        }
        data_ = std::make_shared<const Data>(Data{
            .name_ = std::move(name),
            .id_ = id,
            .type_ = type,
            .element_type_ = element_type,
            .nullable_ = nullable,
            .element_nullable_ = element_nullable,
            .default_value_ = std::move(default_value),
            .external_field_mapping_ = std::move(external_field_mapping),
            .local_format_ = std::move(local_format),
            .type_schema_ = std::move(type_schema),
            .array_type_params_ = std::move(type_params),
        });
    }

    // pass in any value for dim for sparse vector is ok as it'll never be used:
    // get_dim() not allowed to be invoked on a sparse vector field.
    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              int64_t dim,
              std::optional<knowhere::MetricType> metric_type,
              bool nullable,
              std::optional<DefaultValueType> default_value,
              std::string external_field_mapping = "",
              std::string local_format = LOCAL_FORMAT_RAW)
        : FieldMeta(Data{
              .name_ = std::move(name),
              .id_ = id,
              .type_ = type,
              .nullable_ = nullable,
              .default_value_ = std::move(default_value),
              .vector_info_ = VectorInfo{dim, std::move(metric_type)},
              .external_field_mapping_ = std::move(external_field_mapping),
              .local_format_ = std::move(local_format),
          }) {
        Assert(IsVectorDataType(data_->type_) &&
               data_->type_ != DataType::VECTOR_ARRAY);
        Assert(!data_->default_value_.has_value() &&
               "vector fields do not support default values");
    }

    // array of vector type
    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              DataType element_type,
              int64_t dim,
              std::optional<knowhere::MetricType> metric_type,
              bool nullable,
              bool element_nullable,
              std::string external_field_mapping = "",
              std::string local_format = LOCAL_FORMAT_RAW)
        : FieldMeta(Data{
              .name_ = std::move(name),
              .id_ = id,
              .type_ = type,
              .element_type_ = element_type,
              .nullable_ = nullable,
              .element_nullable_ = element_nullable,
              .vector_info_ = VectorInfo{dim, std::move(metric_type)},
              .external_field_mapping_ = std::move(external_field_mapping),
              .local_format_ = std::move(local_format),
          }) {
        Assert(data_->type_ == DataType::VECTOR_ARRAY);
        Assert(IsBinaryVectorDataType(data_->element_type_) ||
               IsDenseFloatVectorDataType(data_->element_type_) ||
               IsIntVectorDataType(data_->element_type_));
    }

    // for json stats shredding column field meta,
    // we need to pass in the main field id
    FieldMeta(FieldName name,
              FieldId id,
              int64_t main_field_id,
              DataType type,
              bool nullable,
              std::optional<DefaultValueType> default_value,
              std::string external_field_mapping = "",
              std::string local_format = LOCAL_FORMAT_RAW)
        : FieldMeta(Data{
              .name_ = std::move(name),
              .id_ = id,
              .type_ = type,
              .nullable_ = nullable,
              .default_value_ = std::move(default_value),
              .main_field_id_ = main_field_id,
              .external_field_mapping_ = std::move(external_field_mapping),
              .local_format_ = std::move(local_format),
          }) {
        Assert(!IsVectorDataType(data_->type_));
    }

    int64_t
    get_dim() const {
        Assert(IsVectorDataType(data_->type_));
        // should not attempt to get dim() of a sparse vector from schema.
        Assert(!IsSparseFloatVectorDataType(data_->type_));
        Assert(data_->vector_info_.has_value());
        return data_->vector_info_->dim_;
    }

    int64_t
    get_max_len() const {
        Assert(IsStringDataType(data_->type_));
        Assert(data_->string_info_.has_value());
        return data_->string_info_->max_length;
    }

    int64_t
    get_main_field_id() const {
        return data_->main_field_id_;
    }

    bool
    enable_match() const;

    bool
    enable_analyzer() const;

    TokenizerParams
    get_analyzer_params() const;

    std::optional<knowhere::MetricType>
    get_metric_type() const {
        Assert(IsVectorDataType(data_->type_));
        Assert(data_->vector_info_.has_value());
        return data_->vector_info_->metric_type_;
    }

    const FieldName&
    get_name() const {
        return data_->name_;
    }

    const FieldId&
    get_id() const {
        return data_->id_;
    }

    DataType
    get_data_type() const {
        return data_->type_;
    }

    DataType
    get_element_type() const {
        return data_->element_type_;
    }

    const milvus::proto::schema::TypeSchema&
    get_array_type_schema() const {
        Assert(data_->type_ == DataType::ARRAY && is_native_list_array());
        return *data_->type_schema_;
    }

    bool
    is_nested_array() const {
        return data_->type_ == DataType::ARRAY &&
               data_->element_type_ == DataType::ARRAY;
    }

    // Whether this Array field uses the Arrow list storage format introduced
    // for element-level nulls and nested Arrays, as opposed to the legacy
    // formats. It selects the new storage, load and chunk paths, and it is
    // the C++ twin of Go typeutil.IsNativeListArrayField.
    //
    // True for:
    //   - element_nullable scalar Array<T>: Arrow list<T>, loaded into
    //     ColumnarArrayChunk;
    //   - nested scalar Array<Array<T>>, whichever level is nullable: Arrow
    //     list<list<T>>, loaded into ColumnarArrayChunk;
    //   - element_nullable ArrayOfVector: Arrow list<Binary>, a null child is
    //     a null vector, loaded into VectorArrayChunk.
    // False for:
    //   - single-level scalar Array<T> with element_nullable=false: one
    //     protobuf-serialized ScalarField per row in an Arrow Binary column,
    //     loaded into ArrayChunk;
    //   - ArrayOfVector with element_nullable=false: Arrow
    //     list<FixedSizeBinary>. It is an Arrow list too, but it is the
    //     released format. VectorArrayChunk loads both vector formats; for
    //     ArrayOfVector this predicate is equivalent to element_nullable;
    //   - every non-Array type.
    //
    // This is a transitional concept. It only exists because the legacy and
    // the new Array formats coexist. Once arbitrary Struct/Array nesting is
    // modeled as a recursive column tree, every Array level will be an Arrow
    // list with its own offsets and validity, and this predicate should go
    // away together with the legacy formats.
    bool
    is_native_list_array() const {
        return (data_->type_ == DataType::ARRAY &&
                (data_->element_nullable_ || is_nested_array())) ||
               (data_->type_ == DataType::VECTOR_ARRAY &&
                data_->element_nullable_);
    }

    bool
    has_nullable_array_element() const {
        return data_->type_ == DataType::ARRAY && is_native_list_array() &&
               (data_->element_nullable_ ||
                (is_nested_array() && data_->type_schema_->array_element()
                                          .array_element()
                                          .nullable()));
    }

    bool
    is_vector() const {
        return IsVectorDataType(data_->type_);
    }

    bool
    is_json() const {
        return data_->type_ == DataType::JSON;
    }

    bool
    is_string() const {
        return IsStringDataType(data_->type_);
    }

    bool
    is_nullable() const {
        return data_->nullable_;
    }

    bool
    is_element_nullable() const {
        return data_->element_nullable_;
    }

    bool
    NeedLoad() const {
        return data_->external_field_mapping_.empty();
    }

    const std::string&
    get_external_field_mapping() const {
        return data_->external_field_mapping_;
    }

    bool
    has_default_value() const {
        return data_->default_value_.has_value();
    }

    const std::optional<DefaultValueType>&
    default_value() const {
        return data_->default_value_;
    }

    bool
    is_external_field() const {
        return !data_->external_field_mapping_.empty();
    }

    const std::string&
    get_external_field() const {
        return data_->external_field_mapping_;
    }

    void
    set_external_field(const std::string& external_field) {
        if (data_->external_field_mapping_ == external_field) {
            return;
        }
        auto data = std::make_shared<Data>(*data_);
        data->external_field_mapping_ = external_field;
        data_ = std::move(data);
    }

    const std::string&
    get_local_format() const {
        return data_->local_format_;
    }

    milvus::proto::schema::FieldSchema
    ToProto() const;

    size_t
    get_sizeof() const {
        AssertInfo(!IsSparseFloatVectorDataType(data_->type_),
                   "should not attempt to get_sizeof() of a sparse vector from "
                   "schema");
        static const size_t ARRAY_SIZE = 128;
        static const size_t JSON_SIZE = 512;
        // assume float vector with dim 512, array length 10
        static const size_t VECTOR_ARRAY_SIZE = 512 * 10 * 4;
        if (data_->type_ == DataType::VECTOR_ARRAY) {
            return VECTOR_ARRAY_SIZE;
        } else if (is_vector()) {
            return GetDataTypeSize(data_->type_, get_dim());
        } else if (is_string()) {
            Assert(data_->string_info_.has_value());
            return data_->string_info_->max_length;
        } else if (IsVariableDataType(data_->type_)) {
            return data_->type_ == DataType::ARRAY ? ARRAY_SIZE : JSON_SIZE;
        } else {
            return GetDataTypeSize(data_->type_);
        }
    }

 public:
    static FieldMeta
    ParseFrom(const milvus::proto::schema::FieldSchema& schema_proto);

 private:
    static void
    ValidateNestedTypeSchema(const proto::schema::TypeSchema& type_schema,
                             const std::string& field_name);

    static void
    ValidateArrayLeafType(DataType leaf_type, const std::string& field_name);

    struct VectorInfo {
        int64_t dim_;
        std::optional<knowhere::MetricType> metric_type_;
    };
    struct StringInfo {
        int64_t max_length;
        bool enable_match;
        bool enable_analyzer;
        std::map<std::string, std::string> params;
    };
    struct Data {
        FieldName name_;
        FieldId id_;
        DataType type_ = DataType::NONE;
        DataType element_type_ = DataType::NONE;
        bool nullable_;
        bool element_nullable_ = false;
        std::optional<DefaultValueType> default_value_;
        std::optional<VectorInfo> vector_info_;
        std::optional<StringInfo> string_info_;
        // for json stats, the main field id is the real field id
        // of collection schema, the field id is the json shredding field id
        int64_t main_field_id_ = INVALID_FIELD_ID;
        std::string external_field_mapping_;
        std::string local_format_ = LOCAL_FORMAT_RAW;
        std::optional<milvus::proto::schema::TypeSchema> type_schema_;
        TypeParams array_type_params_;
    };

    explicit FieldMeta(Data data)
        : data_(std::make_shared<const Data>(std::move(data))) {
    }

    std::shared_ptr<const Data> data_;
};

std::shared_ptr<arrow::DataType>
GetArrowDataType(const FieldMeta& field_meta);

}  // namespace milvus
