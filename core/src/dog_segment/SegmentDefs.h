#pragma once

#include <vector>
#include <assert.h>
#include <stdexcept>

#include "utils/Types.h"
// #include "knowhere/index/Index.h"
#include "utils/Status.h"

namespace milvus::dog_segment {
using Timestamp = uint64_t;  // TODO: use TiKV-like timestamp
using engine::DataType;
using engine::FieldElementType;

struct DogDataChunk {
    void* raw_data;      // schema
    int sizeof_per_row;  // alignment
    int64_t count;
};

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
            assert(dim % 8 == 0);
            return dim / 8;
        }
        default: {
            throw std::invalid_argument("unsupported data type");
            return 0;
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
        assert(type_ != DataType::NONE);
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

class Schema {
 public:
    void
    AddField(std::string_view field_name, DataType data_type, int dim = 1) {
        auto field_meta = FieldMeta(field_name, data_type, dim);
        this->AddField(std::move(field_meta));
    }

    void
    AddField(FieldMeta field_meta) {
        auto offset = fields_.size();
        fields_.emplace_back(field_meta);
        offsets_.emplace(field_meta.get_name(), offset);
        auto field_sizeof = field_meta.get_sizeof();
        sizeof_infos_.push_back(field_sizeof);
        total_sizeof_ += field_sizeof;
    }

    auto
    begin() {
        return fields_.begin();
    }

    auto
    end() {
        return fields_.end();
    }
    auto
    begin() const {
        return fields_.begin();
    }

    auto
    end() const {
        return fields_.end();
    }

    int
    size() const {
        return fields_.size();
    }

    const FieldMeta&
    operator[](int field_index) const {
        return fields_[field_index];
    }

    auto
    get_total_sizeof() const {
        return total_sizeof_;
    }

    const std::vector<int>& get_sizeof_infos() {
        return sizeof_infos_;
    }


    const FieldMeta&
    operator[](const std::string& field_name) const {
        auto offset_iter = offsets_.find(field_name);
        assert(offset_iter != offsets_.end());
        auto offset = offset_iter->second;
        return (*this)[offset];
    }

 private:
    // this is where data holds
    std::vector<FieldMeta> fields_;

 private:
    // a mapping for random access
    std::unordered_map<std::string, int> offsets_;
    std::vector<int> sizeof_infos_;
    int total_sizeof_ = 0;
};

using SchemaPtr = std::shared_ptr<Schema>;

}  // namespace milvus::dog_segment
