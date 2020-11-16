#pragma once
#include "utils/Types.h"
#include "utils/Status.h"
#include "utils/EasyAssert.h"

#include <stdexcept>
namespace milvus {

using Timestamp = uint64_t;  // TODO: use TiKV-like timestamp
using engine::DataType;
using engine::FieldElementType;

inline int
field_sizeof(DataType data_type, int dim = 1) {
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
field_is_vector(DataType datatype) {
    return datatype == DataType::VECTOR_BINARY || datatype == DataType::VECTOR_FLOAT;
}

struct FieldMeta {
 public:
    FieldMeta(std::string_view name, DataType type, int dim = 1) : name_(name), type_(type), dim_(dim) {
    }

    bool
    is_vector() const {
        Assert(type_ != DataType::NONE);
        return type_ == DataType::VECTOR_BINARY || type_ == DataType::VECTOR_FLOAT;
    }

    void
    set_dim(int dim) {
        dim_ = dim;
    }

    int
    get_dim() const {
        return dim_;
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
        return field_sizeof(type_, dim_);
    }

 private:
    std::string name_;
    DataType type_ = DataType::NONE;
    int dim_ = 1;
};
}  // namespace milvus