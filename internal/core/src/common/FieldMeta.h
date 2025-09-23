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

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/EasyAssert.h"
#include "common/Consts.h"
#include "common/Types.h"

namespace milvus {
using TypeParams = std::map<std::string, std::string>;
using TokenizerParams = std::string;

TokenizerParams
ParseTokenizerParams(const TypeParams& params);

class FieldMeta {
 public:
    static const FieldMeta RowIdMeta;
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
              std::optional<DefaultValueType> default_value)
        : name_(std::move(name)),
          id_(id),
          type_(type),
          nullable_(nullable),
          default_value_(std::move(default_value)) {
        Assert(!IsVectorDataType(type_));
    }

    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              int64_t max_length,
              bool nullable,
              std::optional<DefaultValueType> default_value)
        : name_(std::move(name)),
          id_(id),
          type_(type),
          nullable_(nullable),
          string_info_(StringInfo{max_length}),
          default_value_(std::move(default_value)) {
        Assert(IsStringDataType(type_));
    }

    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              int64_t max_length,
              bool nullable,
              bool enable_match,
              bool enable_analyzer,
              std::map<std::string, std::string>& params,
              std::optional<DefaultValueType> default_value)
        : name_(std::move(name)),
          id_(id),
          type_(type),
          nullable_(nullable),
          string_info_(StringInfo{
              max_length,
              enable_match,
              enable_analyzer,
              std::move(params),
          }),
          default_value_(std::move(default_value)) {
        Assert(IsStringDataType(type_));
    }

    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              DataType element_type,
              bool nullable,
              std::optional<DefaultValueType> default_value)
        : name_(std::move(name)),
          id_(id),
          type_(type),
          element_type_(element_type),
          nullable_(nullable),
          default_value_(std::move(default_value)) {
        Assert(IsArrayDataType(type_));
    }

    // pass in any value for dim for sparse vector is ok as it'll never be used:
    // get_dim() not allowed to be invoked on a sparse vector field.
    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              int64_t dim,
              std::optional<knowhere::MetricType> metric_type,
              bool nullable,
              std::optional<DefaultValueType> default_value)
        : name_(std::move(name)),
          id_(id),
          type_(type),
          nullable_(nullable),
          vector_info_(VectorInfo{dim, std::move(metric_type)}),
          default_value_(std::move(default_value)) {
        Assert(IsVectorDataType(type_));
        Assert(!nullable);
    }

    // array of vector type
    FieldMeta(FieldName name,
              FieldId id,
              DataType type,
              DataType element_type,
              int64_t dim,
              std::optional<knowhere::MetricType> metric_type)
        : name_(std::move(name)),
          id_(id),
          type_(type),
          nullable_(false),
          element_type_(element_type),
          vector_info_(VectorInfo{dim, std::move(metric_type)}) {
        Assert(type_ == DataType::VECTOR_ARRAY);
        Assert(IsVectorDataType(element_type_));
    }

    // for json stats shredding column field meta,
    // we need to pass in the main field id
    FieldMeta(FieldName name,
              FieldId id,
              int64_t main_field_id,
              DataType type,
              bool nullable,
              std::optional<DefaultValueType> default_value)
        : name_(std::move(name)),
          id_(id),
          main_field_id_(main_field_id),
          type_(type),
          nullable_(nullable),
          default_value_(std::move(default_value)) {
        Assert(!IsVectorDataType(type_));
    }

    int64_t
    get_dim() const {
        Assert(IsVectorDataType(type_));
        // should not attempt to get dim() of a sparse vector from schema.
        Assert(!IsSparseFloatVectorDataType(type_));
        Assert(vector_info_.has_value());
        return vector_info_->dim_;
    }

    int64_t
    get_max_len() const {
        Assert(IsStringDataType(type_));
        Assert(string_info_.has_value());
        return string_info_->max_length;
    }

    int64_t
    get_main_field_id() const {
        return main_field_id_;
    }

    bool
    enable_match() const;

    bool
    enable_analyzer() const;

    bool
    enable_growing_jsonStats() const;

    TokenizerParams
    get_analyzer_params() const;

    std::optional<knowhere::MetricType>
    get_metric_type() const {
        Assert(IsVectorDataType(type_));
        Assert(vector_info_.has_value());
        return vector_info_->metric_type_;
    }

    const FieldName&
    get_name() const {
        return name_;
    }

    const FieldId&
    get_id() const {
        return id_;
    }

    DataType
    get_data_type() const {
        return type_;
    }

    DataType
    get_element_type() const {
        return element_type_;
    }

    bool
    is_vector() const {
        return IsVectorDataType(type_);
    }

    bool
    is_json() const {
        return type_ == DataType::JSON;
    }

    bool
    is_string() const {
        return IsStringDataType(type_);
    }

    bool
    is_nullable() const {
        return nullable_;
    }

    bool
    has_default_value() const {
        return default_value_.has_value();
    }

    std::optional<DefaultValueType>
    default_value() const {
        return default_value_;
    }

    size_t
    get_sizeof() const {
        AssertInfo(!IsSparseFloatVectorDataType(type_),
                   "should not attempt to get_sizeof() of a sparse vector from "
                   "schema");
        static const size_t ARRAY_SIZE = 128;
        static const size_t JSON_SIZE = 512;
        // assume float vector with dim 512, array length 10
        static const size_t VECTOR_ARRAY_SIZE = 512 * 10 * 4;
        if (type_ == DataType::VECTOR_ARRAY) {
            return VECTOR_ARRAY_SIZE;
        } else if (is_vector()) {
            return GetDataTypeSize(type_, get_dim());
        } else if (is_string()) {
            Assert(string_info_.has_value());
            return string_info_->max_length;
        } else if (IsVariableDataType(type_)) {
            return type_ == DataType::ARRAY ? ARRAY_SIZE : JSON_SIZE;
        } else {
            return GetDataTypeSize(type_);
        }
    }

 public:
    static FieldMeta
    ParseFrom(const milvus::proto::schema::FieldSchema& schema_proto);

 private:
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
    FieldName name_;
    FieldId id_;
    DataType type_ = DataType::NONE;
    DataType element_type_ = DataType::NONE;
    bool nullable_;
    std::optional<DefaultValueType> default_value_;
    std::optional<VectorInfo> vector_info_;
    std::optional<StringInfo> string_info_;
    // for json stats, the main field id is the real field id
    // of collection schema, the field id is the json shredding field id
    int64_t main_field_id_ = INVALID_FIELD_ID;
};

}  // namespace milvus
