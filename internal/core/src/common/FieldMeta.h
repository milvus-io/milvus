// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#include "common/Types.h"
#include "utils/Status.h"
#include "utils/EasyAssert.h"
#include <string>
#include <stdexcept>

namespace milvus {
inline int
datatype_sizeof(DataType data_type, int dim = 1) {
    switch (data_type) {
        case DataType::BOOL:
            return sizeof(bool);
        case DataType::DOUBLE:
            return sizeof(double);
        case DataType::FLOAT:
            return sizeof(float);
        case DataType::INT8:
            return sizeof(uint8_t);
        case DataType::INT16:
            return sizeof(uint16_t);
        case DataType::INT32:
            return sizeof(uint32_t);
        case DataType::INT64:
            return sizeof(uint64_t);
        case DataType::VECTOR_FLOAT:
            return sizeof(float) * dim;
        case DataType::VECTOR_BINARY: {
            Assert(dim % 8 == 0);
            return dim / 8;
        }
        default: {
            throw std::invalid_argument("unsupported data type");
            return 0;
        }
    }
}

// TODO: use magic_enum when available
inline std::string
datatype_name(DataType data_type) {
    switch (data_type) {
        case DataType::BOOL:
            return "bool";
        case DataType::DOUBLE:
            return "double";
        case DataType::FLOAT:
            return "float";
        case DataType::INT8:
            return "int8_t";
        case DataType::INT16:
            return "int16_t";
        case DataType::INT32:
            return "int32_t";
        case DataType::INT64:
            return "int64_t";
        case DataType::VECTOR_FLOAT:
            return "vector_float";
        case DataType::VECTOR_BINARY: {
            return "vector_binary";
        }
        default: {
            auto err_msg = "Unsupported DataType(" + std::to_string((int)data_type) + ")";
            PanicInfo(err_msg);
        }
    }
}

inline bool
datatype_is_vector(DataType datatype) {
    return datatype == DataType::VECTOR_BINARY || datatype == DataType::VECTOR_FLOAT;
}

struct FieldMeta {
 public:
    FieldMeta(std::string_view name, DataType type) : name_(name), type_(type) {
        Assert(!is_vector());
    }

    FieldMeta(std::string_view name, DataType type, int64_t dim, MetricType metric_type)
        : name_(name), type_(type), vector_info_(VectorInfo{dim, metric_type}) {
        Assert(is_vector());
    }

    bool
    is_vector() const {
        Assert(type_ != DataType::NONE);
        return type_ == DataType::VECTOR_BINARY || type_ == DataType::VECTOR_FLOAT;
    }

    int64_t
    get_dim() const {
        Assert(is_vector());
        Assert(vector_info_.has_value());
        return vector_info_->dim_;
    }

    const std::string&
    get_name() const {
        return name_;
    }

    DataType
    get_data_type() const {
        return type_;
    }

    int
    get_sizeof() const {
        if (is_vector()) {
            return datatype_sizeof(type_, get_dim());
        } else {
            return datatype_sizeof(type_, 1);
        }
    }

 private:
    struct VectorInfo {
        int64_t dim_;
        MetricType metric_type_;
    };
    std::string name_;
    DataType type_ = DataType::NONE;
    std::optional<VectorInfo> vector_info_;
};
}  // namespace milvus
