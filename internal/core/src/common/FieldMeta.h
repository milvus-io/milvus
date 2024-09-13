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

#include "common/EasyAssert.h"
#include "common/Types.h"

namespace milvus {
using TypeParams = std::map<std::string, std::string>;
using TokenizerParams = std::map<std::string, std::string>;

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

    FieldMeta(const FieldName& name, FieldId id, DataType type, bool nullable)
        : name_(name), id_(id), type_(type), nullable_(nullable) {
        Assert(!IsVectorDataType(type_));
    }

    FieldMeta(const FieldName& name,
              FieldId id,
              DataType type,
              int64_t max_length,
              bool nullable)
        : name_(name),
          id_(id),
          type_(type),
          string_info_(StringInfo{max_length}),
          nullable_(nullable) {
        Assert(IsStringDataType(type_));
    }

    FieldMeta(const FieldName& name,
              FieldId id,
              DataType type,
              int64_t max_length,
              bool nullable,
              bool enable_match,
              std::map<std::string, std::string>& params)
        : name_(name),
          id_(id),
          type_(type),
          string_info_(StringInfo{max_length, enable_match, std::move(params)}),
          nullable_(nullable) {
        Assert(IsStringDataType(type_));
    }

    FieldMeta(const FieldName& name,
              FieldId id,
              DataType type,
              DataType element_type,
              bool nullable)
        : name_(name),
          id_(id),
          type_(type),
          element_type_(element_type),
          nullable_(nullable) {
        Assert(IsArrayDataType(type_));
    }

    // pass in any value for dim for sparse vector is ok as it'll never be used:
    // get_dim() not allowed to be invoked on a sparse vector field.
    FieldMeta(const FieldName& name,
              FieldId id,
              DataType type,
              int64_t dim,
              std::optional<knowhere::MetricType> metric_type,
              bool nullable)
        : name_(name),
          id_(id),
          type_(type),
          vector_info_(VectorInfo{dim, std::move(metric_type)}),
          nullable_(nullable) {
        Assert(IsVectorDataType(type_));
        Assert(!nullable);
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

    bool
    enable_match() const;

    TokenizerParams
    get_tokenizer_params() const;

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
    is_string() const {
        return IsStringDataType(type_);
    }

    bool
    is_nullable() const {
        return nullable_;
    }

    size_t
    get_sizeof() const {
        AssertInfo(!IsSparseFloatVectorDataType(type_),
                   "should not attempt to get_sizeof() of a sparse vector from "
                   "schema");
        static const size_t ARRAY_SIZE = 128;
        static const size_t JSON_SIZE = 512;
        if (is_vector()) {
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
        std::map<std::string, std::string> params;
    };
    FieldName name_;
    FieldId id_;
    DataType type_ = DataType::NONE;
    DataType element_type_ = DataType::NONE;
    bool nullable_;
    std::optional<VectorInfo> vector_info_;
    std::optional<StringInfo> string_info_;
};

}  // namespace milvus
