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

#include <string>
#include <memory>
#include <utility>
#include <unordered_map>
#include <vector>

#include <oneapi/tbb/concurrent_queue.h>

#include "common/FieldDataInterface.h"
#include "common/Channel.h"
#include "common/ArrowDataWrapper.h"

namespace milvus {

template <typename Type>
class FieldData : public FieldDataImpl<Type, true> {
 public:
    static_assert(IsScalar<Type> || std::is_same_v<Type, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataImpl<Type, true>::FieldDataImpl(
              1, data_type, nullable, buffered_num_rows) {
    }
    static_assert(IsScalar<Type> || std::is_same_v<Type, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       FixedVector<Type>&& inner_data)
        : FieldDataImpl<Type, true>::FieldDataImpl(
              1, data_type, nullable, std::move(inner_data)) {
    }
};

template <>
class FieldData<std::string> : public FieldDataStringImpl {
 public:
    static_assert(IsScalar<std::string> || std::is_same_v<std::string, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataStringImpl(data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<Json> : public FieldDataJsonImpl {
 public:
    static_assert(IsScalar<std::string> || std::is_same_v<std::string, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataJsonImpl(data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<Geometry> : public FieldDataGeometryImpl {
 public:
    static_assert(IsScalar<Geometry>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataGeometryImpl(data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<Array> : public FieldDataArrayImpl {
 public:
    static_assert(IsScalar<Array> || std::is_same_v<std::string, PkType>);
    explicit FieldData(DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataArrayImpl(data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<VectorArray> : public FieldDataVectorArrayImpl {
 public:
    explicit FieldData(int64_t dim,
                       DataType element_type,
                       int64_t buffered_num_rows = 0)
        : FieldDataVectorArrayImpl(DataType::VECTOR_ARRAY, buffered_num_rows),
          dim_(dim),
          element_type_(element_type) {
        AssertInfo(element_type != DataType::NONE,
                   "element_type must be specified for VECTOR_ARRAY");
    }

    int64_t
    get_dim() const override {
        return dim_;
    }

    DataType
    get_element_type() const {
        return element_type_;
    }

    void
    set_element_type(DataType element_type) {
        element_type_ = element_type;
    }

    const VectorArray*
    value_at(ssize_t offset) const {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return &data_[offset];
    }

 private:
    int64_t dim_;
    DataType element_type_;
};

template <typename Type, bool is_type_entire_row = false>
class FieldDataVectorImpl : public FieldDataImpl<Type, is_type_entire_row> {
 private:
    struct LogicalToPhysicalMapping {
        bool mapping{false};
        std::unordered_map<int64_t, int64_t> l2p_map;
        std::vector<int64_t> l2p_vec;

        int64_t
        get_physical_offset(int64_t logical_offset) const {
            if (!mapping) {
                return logical_offset;
            }
            if (!l2p_map.empty()) {
                auto it = l2p_map.find(logical_offset);
                if (it != l2p_map.end()) {
                    return it->second;
                }
                return -1;
            }
            if (logical_offset < static_cast<int64_t>(l2p_vec.size())) {
                return l2p_vec[logical_offset];
            }
            return -1;
        }

        void
        build(const uint8_t* valid_data,
              int64_t start_physical,
              int64_t start_logical,
              int64_t total_count,
              int64_t valid_count) {
            if (total_count == 0) {
                return;
            }

            mapping = true;

            // use map when valid ratio < 10%
            bool use_map = (valid_count * 10 < total_count);

            if (use_map) {
                int64_t physical_idx = start_physical;
                for (int64_t i = 0; i < total_count; ++i) {
                    int64_t bit_pos = start_logical + i;
                    if (valid_data == nullptr ||
                        ((valid_data[bit_pos >> 3] >> (bit_pos & 0x07)) & 1)) {
                        l2p_map[start_logical + i] = physical_idx++;
                    }
                }
            } else {
                // resize l2p_vec if needed
                int64_t required_size = start_logical + total_count;
                if (static_cast<int64_t>(l2p_vec.size()) < required_size) {
                    l2p_vec.resize(required_size, -1);
                }
                int64_t physical_idx = start_physical;
                for (int64_t i = 0; i < total_count; ++i) {
                    int64_t bit_pos = start_logical + i;
                    if (valid_data == nullptr ||
                        ((valid_data[bit_pos >> 3] >> (bit_pos & 0x07)) & 1)) {
                        l2p_vec[start_logical + i] = physical_idx++;
                    } else {
                        l2p_vec[start_logical + i] = -1;
                    }
                }
            }
        }
    };

    void
    resize_field_data(int64_t num_rows, int64_t valid_count) {
        Assert(this->nullable_);
        std::lock_guard lck(this->num_rows_mutex_);
        if (num_rows > this->num_rows_) {
            this->num_rows_ = num_rows;
            this->valid_data_.resize((num_rows + 7) / 8, 0x00);
        }
        if (valid_count > this->valid_count_) {
            this->data_.resize(valid_count * this->dim_);
        }
    }

    LogicalToPhysicalMapping l2p_mapping_;

 public:
    using FieldDataImpl<Type, is_type_entire_row>::FieldDataImpl;
    using FieldDataImpl<Type, is_type_entire_row>::resize_field_data;

    void
    FillFieldData(const void* field_data,
                  const uint8_t* valid_data,
                  ssize_t element_count,
                  ssize_t offset) override;

    const void*
    RawValue(ssize_t offset) const override {
        auto physical_offset = l2p_mapping_.get_physical_offset(offset);
        if (physical_offset == -1) {
            return nullptr;
        }
        return &this->data_[physical_offset * this->dim_];
    }

    int64_t
    DataSize() const override {
        auto dim = this->dim_;
        if (this->nullable_) {
            return sizeof(Type) * this->valid_count_ * dim;
        }
        return sizeof(Type) * this->length_ * dim;
    }

    int64_t
    DataSize(ssize_t offset) const override {
        auto dim = this->dim_;
        AssertInfo(offset < this->get_num_rows(),
                   "field data subscript out of range");
        return sizeof(Type) * dim;
    }

    int64_t
    get_valid_rows() const override {
        if (this->nullable_) {
            return this->valid_count_;
        }
        return this->get_num_rows();
    }
};

class FieldDataSparseVectorImpl
    : public FieldDataVectorImpl<knowhere::sparse::SparseRow<SparseValueType>,
                                 true> {
    using Base =
        FieldDataVectorImpl<knowhere::sparse::SparseRow<SparseValueType>, true>;

 public:
    // Bring base class FillFieldData overloads into scope (for nullable support)
    using Base::FillFieldData;

    explicit FieldDataSparseVectorImpl(DataType data_type,
                                       bool nullable = false,
                                       int64_t total_num_rows = 0)
        : FieldDataVectorImpl<knowhere::sparse::SparseRow<SparseValueType>,
                              true>(
              /*dim=*/1, data_type, nullable, total_num_rows),
          vec_dim_(0) {
        AssertInfo(data_type == DataType::VECTOR_SPARSE_U32_F32,
                   "invalid data type for sparse vector");
    }

    int64_t
    DataSize() const override {
        int64_t data_size = 0;
        size_t count = nullable_ ? valid_count_ : length_;
        for (size_t i = 0; i < count; ++i) {
            data_size += data_[i].data_byte_size();
        }
        return data_size;
    }

    int64_t
    DataSize(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        size_t count = nullable_ ? valid_count_ : length_;
        AssertInfo(
            offset < count,
            "subscript position don't has valid value offset={}, count={}",
            offset,
            count);
        return data_[offset].data_byte_size();
    }

    void
    FillFieldData(const void* source, ssize_t element_count) override {
        if (element_count == 0) {
            return;
        }

        std::lock_guard lck(tell_mutex_);
        if (length_ + element_count > get_num_rows()) {
            FieldDataImpl::resize_field_data(length_ + element_count);
        }
        auto ptr =
            static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
                source);
        for (int64_t i = 0; i < element_count; ++i) {
            auto& row = ptr[i];
            vec_dim_ = std::max(vec_dim_, row.dim());
        }
        std::copy_n(ptr, element_count, data_.data() + length_);
        length_ += element_count;
    }

    void
    FillFieldData(const std::shared_ptr<arrow::BinaryArray>& array) override {
        auto n = array->length();
        if (n == 0) {
            return;
        }

        std::lock_guard lck(tell_mutex_);
        if (length_ + n > get_num_rows()) {
            FieldDataImpl::resize_field_data(length_ + n);
        }

        for (int64_t i = 0; i < array->length(); ++i) {
            auto view = array->GetView(i);
            auto& row = data_[length_ + i];
            row = CopyAndWrapSparseRow(view.data(), view.size());
            vec_dim_ = std::max(vec_dim_, row.dim());
        }
        length_ += n;
    }

    int64_t
    Dim() const {
        return vec_dim_;
    }

 private:
    int64_t vec_dim_ = 0;
};

template <>
class FieldData<FloatVector> : public FieldDataVectorImpl<float, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataVectorImpl<float, false>::FieldDataVectorImpl(
              dim, data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<BinaryVector> : public FieldDataVectorImpl<uint8_t, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataVectorImpl(dim / 8, data_type, nullable, buffered_num_rows),
          binary_dim_(dim) {
        Assert(dim % 8 == 0);
    }

    int64_t
    get_dim() const override {
        return binary_dim_;
    }

 private:
    int64_t binary_dim_;
};

template <>
class FieldData<Float16Vector> : public FieldDataVectorImpl<float16, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataVectorImpl<float16, false>::FieldDataVectorImpl(
              dim, data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<BFloat16Vector> : public FieldDataVectorImpl<bfloat16, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataVectorImpl<bfloat16, false>::FieldDataVectorImpl(
              dim, data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<SparseFloatVector> : public FieldDataSparseVectorImpl {
 public:
    explicit FieldData(DataType data_type,
                       bool nullable = false,
                       int64_t buffered_num_rows = 0)
        : FieldDataSparseVectorImpl(data_type, nullable, buffered_num_rows) {
    }
};

template <>
class FieldData<Int8Vector> : public FieldDataVectorImpl<int8, false> {
 public:
    explicit FieldData(int64_t dim,
                       DataType data_type,
                       bool nullable,
                       int64_t buffered_num_rows = 0)
        : FieldDataVectorImpl<int8, false>::FieldDataVectorImpl(
              dim, data_type, nullable, buffered_num_rows) {
    }
};

using FieldDataPtr = std::shared_ptr<FieldDataBase>;
using FieldDataChannel = Channel<FieldDataPtr>;
using FieldDataChannelPtr = std::shared_ptr<FieldDataChannel>;

FieldDataPtr
InitScalarFieldData(const DataType& type, bool nullable, int64_t cap_rows);

}  // namespace milvus
