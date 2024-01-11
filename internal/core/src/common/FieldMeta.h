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

#include "common/Types.h"
#include "common/EasyAssert.h"

namespace milvus {

inline size_t
datatype_sizeof(DataType data_type, int dim = 1) {
    switch (data_type) {
        case DataType::BOOL:
            return sizeof(bool);
        case DataType::INT8:
            return sizeof(int8_t);
        case DataType::INT16:
            return sizeof(int16_t);
        case DataType::INT32:
            return sizeof(int32_t);
        case DataType::INT64:
            return sizeof(int64_t);
        case DataType::FLOAT:
            return sizeof(float);
        case DataType::DOUBLE:
            return sizeof(double);
        case DataType::VECTOR_FLOAT:
            return sizeof(float) * dim;
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0, "dim={}", dim);
            return dim / 8;
        }
        case DataType::VECTOR_FLOAT16: {
            return sizeof(float16) * dim;
        }
        case DataType::VECTOR_BFLOAT16: {
            return sizeof(bfloat16) * dim;
        }
        default: {
            throw SegcoreError(DataTypeInvalid,
                               fmt::format("invalid type is {}", data_type));
        }
    }
}

// TODO: use magic_enum when available
inline std::string
datatype_name(DataType data_type) {
    switch (data_type) {
        case DataType::NONE:
            return "none";
        case DataType::BOOL:
            return "bool";
        case DataType::INT8:
            return "int8_t";
        case DataType::INT16:
            return "int16_t";
        case DataType::INT32:
            return "int32_t";
        case DataType::INT64:
            return "int64_t";
        case DataType::FLOAT:
            return "float";
        case DataType::DOUBLE:
            return "double";
        case DataType::STRING:
            return "string";
        case DataType::VARCHAR:
            return "varChar";
        case DataType::ARRAY:
            return "array";
        case DataType::JSON:
            return "json";
        case DataType::VECTOR_FLOAT:
            return "vector_float";
        case DataType::VECTOR_BINARY: {
            return "vector_binary";
        }
        case DataType::VECTOR_FLOAT16: {
            return "vector_float16";
        }
        case DataType::VECTOR_BFLOAT16: {
            return "vector_bfloat16";
        }
        default: {
            PanicInfo(DataTypeInvalid, "Unsupported DataType({})", data_type);
        }
    }
}

inline bool
datatype_is_vector(DataType datatype) {
    return datatype == DataType::VECTOR_BINARY ||
           datatype == DataType::VECTOR_FLOAT ||
           datatype == DataType::VECTOR_FLOAT16 ||
           datatype == DataType::VECTOR_BFLOAT16;
}

inline bool
datatype_is_string(DataType datatype) {
    switch (datatype) {
        case DataType::VARCHAR:
        case DataType::STRING:
            return true;
        default:
            return false;
    }
}

inline bool
datatype_is_binary(DataType datatype) {
    switch (datatype) {
        case DataType::ARRAY:
        case DataType::JSON:
            return true;
        default:
            return false;
    }
}

inline bool
datatype_is_json(DataType datatype) {
    return datatype == DataType::JSON;
}

inline bool
datatype_is_array(DataType datatype) {
    return datatype == DataType::ARRAY;
}

inline bool
datatype_is_variable(DataType datatype) {
    switch (datatype) {
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::ARRAY:
        case DataType::JSON:
            return true;
        default:
            return false;
    }
}

inline bool
datatype_is_integer(DataType datatype) {
    switch (datatype) {
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
            return true;
        default:
            return false;
    }
}

inline bool
datatype_is_floating(DataType datatype) {
    switch (datatype) {
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return true;
        default:
            return false;
    }
}

class FieldMeta {
 public:
    static const FieldMeta RowIdMeta;
    FieldMeta(const FieldMeta&) = default;
    FieldMeta(FieldMeta&&) = default;
    FieldMeta&
    operator=(const FieldMeta&) = delete;
    FieldMeta&
    operator=(FieldMeta&&) = default;

    FieldMeta(const FieldName& name, FieldId id, DataType type)
        : name_(name), id_(id), type_(type) {
        Assert(!datatype_is_vector(type_));
    }

    FieldMeta(const FieldName& name,
              FieldId id,
              DataType type,
              int64_t max_length)
        : name_(name),
          id_(id),
          type_(type),
          string_info_(StringInfo{max_length}) {
        Assert(datatype_is_string(type_));
    }

    FieldMeta(const FieldName& name,
              FieldId id,
              DataType type,
              DataType element_type)
        : name_(name), id_(id), type_(type), element_type_(element_type) {
        Assert(datatype_is_array(type_));
    }

    FieldMeta(const FieldName& name,
              FieldId id,
              DataType type,
              int64_t dim,
              std::optional<knowhere::MetricType> metric_type)
        : name_(name),
          id_(id),
          type_(type),
          vector_info_(VectorInfo{dim, std::move(metric_type)}) {
        Assert(datatype_is_vector(type_));
    }

    int64_t
    get_dim() const {
        Assert(datatype_is_vector(type_));
        Assert(vector_info_.has_value());
        return vector_info_->dim_;
    }

    int64_t
    get_max_len() const {
        Assert(datatype_is_string(type_));
        Assert(string_info_.has_value());
        return string_info_->max_length;
    }

    std::optional<knowhere::MetricType>
    get_metric_type() const {
        Assert(datatype_is_vector(type_));
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
        return datatype_is_vector(type_);
    }

    bool
    is_string() const {
        return datatype_is_string(type_);
    }

    size_t
    get_sizeof() const {
        static const size_t ARRAY_SIZE = 128;
        static const size_t JSON_SIZE = 512;
        if (is_vector()) {
            return datatype_sizeof(type_, get_dim());
        } else if (is_string()) {
            return string_info_->max_length;
        } else if (datatype_is_variable(type_)) {
            return type_ == DataType::ARRAY ? ARRAY_SIZE : JSON_SIZE;
        } else {
            return datatype_sizeof(type_);
        }
    }

 private:
    struct VectorInfo {
        int64_t dim_;
        std::optional<knowhere::MetricType> metric_type_;
    };
    struct StringInfo {
        int64_t max_length;
    };
    FieldName name_;
    FieldId id_;
    DataType type_ = DataType::NONE;
    DataType element_type_ = DataType::NONE;
    std::optional<VectorInfo> vector_info_;
    std::optional<StringInfo> string_info_;
};

}  // namespace milvus
