#pragma once

#include <vector>
#include <unordered_map>

// #include "db/Types.h"
// #include "knowhere/index/Index.h"
#include "utils/Status.h"
#include "utils/Types.h"
#include <cassert>

using Timestamp = uint64_t;  // TODO: use TiKV-like timestamp
namespace milvus::dog_segment {
using engine::DataType;
using engine::FieldElementType;

struct DogDataChunk {
    void* raw_data;      // schema
    int sizeof_per_row;  // alignment
    int64_t count;
};

struct IndexConfig {
    // TODO
    // std::unordered_map<std::string, knowhere::Config> configs;
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
            assert(false);
            return 0;
        }
    }
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
        auto index = fields_.size();
        fields_.emplace_back(field_meta);
        indexes_.emplace(field_meta.get_name(), index);
        total_sizeof_ = field_meta.get_sizeof();
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

    int size() const {
        return fields_.size();
    }

    const FieldMeta&
    operator[](int field_index) const {
        return fields_[field_index];
    }

    const FieldMeta&
    operator[](const std::string& field_name) const {
        auto index_iter = indexes_.find(field_name);
        assert(index_iter != indexes_.end());
        auto index = index_iter->second;
        return (*this)[index];
    }

 private:
    // this is where data holds
    std::vector<FieldMeta> fields_;

 private:
    // a mapping for random access
    std::unordered_map<std::string, int> indexes_;
    int total_sizeof_;
};

using SchemaPtr = std::shared_ptr<Schema>;

class IndexData {
 public:
    virtual std::vector<char>
    serilize() = 0;

    static std::shared_ptr<IndexData>
    deserialize(int64_t size, const char* blob);
};

}  // namespace milvus::dog_segment
