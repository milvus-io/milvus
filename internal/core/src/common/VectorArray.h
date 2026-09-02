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

#include <algorithm>
#include <memory>

#include "common/FastMem.h"

#include "FieldMeta.h"
#include "Types.h"
#include "common/VectorTrait.h"

namespace milvus {
// Internal representation of one VECTOR_ARRAY row. length_ is the logical
// element count; data_ stores valid vectors only in compact order.
class VectorArray : public milvus::VectorTrait {
 public:
    VectorArray() = default;

    ~VectorArray() = default;

    VectorArray(const void* data,
                int num_vectors,
                int64_t dim,
                DataType element_type)
        : VectorArray(
              data, num_vectors, dim, element_type, TargetBitmapView(), false) {
    }

    // num_vectors is the logical count. For an element-nullable row, data
    // contains only the vectors selected by element_valid_data.
    VectorArray(const void* data,
                int num_vectors,
                int64_t dim,
                DataType element_type,
                const TargetBitmapView& element_valid_data,
                bool element_nullable)
        : dim_(dim),
          length_(num_vectors),
          element_type_(element_type),
          element_nullable_(element_nullable) {
        assert(num_vectors >= 0);
        assert(dim > 0);

        init_element_valid_data(element_valid_data);
        physical_length_ = element_nullable_
                               ? static_cast<int>(element_valid_data_.count())
                               : length_;
        size_ = physical_length_ *
                milvus::vector_bytes_per_element(element_type, dim);

        if (size_ > 0) {
            assert(data != nullptr);
            data_ = std::make_unique<char[]>(size_);
            milvus::fastmem::FastMemcpy(data_.get(), data, size_);
        }
    }

    // One row of VectorFieldProto
    explicit VectorArray(const VectorFieldProto& vector_field)
        : VectorArray(vector_field, false) {
    }

    VectorArray(const VectorFieldProto& vector_field, bool element_nullable)
        : dim_(vector_field.dim()), element_nullable_(element_nullable) {
        if (!element_nullable_) {
            AssertInfo(vector_field.valid_data_size() == 0,
                       "non-element-nullable vector array cannot carry "
                       "element valid_data");
        } else {
            length_ = vector_field.valid_data_size();
            AssertInfo(dim_ > 0 || length_ == 0,
                       "VectorArray dim must be positive, dim={}",
                       dim_);
        }

        const char* payload_data = nullptr;
        size_t payload_size = 0;
        switch (vector_field.data_case()) {
            case VectorFieldProto::kFloatVector: {
                element_type_ = DataType::VECTOR_FLOAT;
                payload_size =
                    vector_field.float_vector().data().size() * sizeof(float);
                payload_data = reinterpret_cast<const char*>(
                    vector_field.float_vector().data().data());
                break;
            }
            case VectorFieldProto::kBinaryVector: {
                element_type_ = DataType::VECTOR_BINARY;
                payload_size = vector_field.binary_vector().size();
                payload_data = vector_field.binary_vector().data();
                break;
            }
            case VectorFieldProto::kFloat16Vector: {
                element_type_ = DataType::VECTOR_FLOAT16;
                payload_size = vector_field.float16_vector().size();
                payload_data = vector_field.float16_vector().data();
                break;
            }
            case VectorFieldProto::kBfloat16Vector: {
                element_type_ = DataType::VECTOR_BFLOAT16;
                payload_size = vector_field.bfloat16_vector().size();
                payload_data = vector_field.bfloat16_vector().data();
                break;
            }
            case VectorFieldProto::kInt8Vector: {
                element_type_ = DataType::VECTOR_INT8;
                payload_size = vector_field.int8_vector().size();
                payload_data = vector_field.int8_vector().data();
                break;
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(vector_field.data_case()));
            }
        }

        if (payload_size == 0 && dim_ == 0) {
            return;
        }
        AssertInfo(dim_ > 0, "VectorArray dim must be positive, dim={}", dim_);
        auto bytes_per_vector =
            milvus::vector_bytes_per_element(element_type_, dim_);
        AssertInfo(payload_size % bytes_per_vector == 0,
                   "vector array payload size {} must be aligned to vector "
                   "byte width {}",
                   payload_size,
                   bytes_per_vector);
        if (!element_nullable_) {
            physical_length_ = payload_size / bytes_per_vector;
            length_ = physical_length_;
            size_ = payload_size;
            if (size_ > 0) {
                data_ = std::make_unique<char[]>(size_);
                milvus::fastmem::FastMemcpy(data_.get(), payload_data, size_);
            }
            return;
        }

        const auto valid_count = std::count(vector_field.valid_data().begin(),
                                            vector_field.valid_data().end(),
                                            true);
        AssertInfo(
            payload_size == static_cast<size_t>(valid_count) * bytes_per_vector,
            "nullable vector array compact payload size {} must match "
            "valid element count {} and vector byte width {}",
            payload_size,
            valid_count,
            bytes_per_vector);
        physical_length_ = valid_count;
        size_ = payload_size;
        if (size_ > 0) {
            data_ = std::make_unique<char[]>(size_);
            milvus::fastmem::FastMemcpy(
                data_.get(), payload_data, static_cast<size_t>(size_));
        }
        init_element_valid_data(vector_field.valid_data());
    }

    explicit VectorArray(const VectorArray& other)
        : dim_(other.dim_),
          length_(other.length_),
          physical_length_(other.physical_length_),
          size_(other.size_),
          element_type_(other.element_type_),
          element_nullable_(other.element_nullable_),
          element_valid_data_(other.element_valid_data_.clone()) {
        if (size_ > 0) {
            data_ = std::make_unique<char[]>(size_);
            milvus::fastmem::FastMemcpy(data_.get(), other.data_.get(), size_);
        }
    }

    friend void
    swap(VectorArray& array1, VectorArray& array2) noexcept {
        using std::swap;
        swap(array1.data_, array2.data_);
        swap(array1.size_, array2.size_);
        swap(array1.length_, array2.length_);
        swap(array1.physical_length_, array2.physical_length_);
        swap(array1.dim_, array2.dim_);
        swap(array1.element_type_, array2.element_type_);
        swap(array1.element_nullable_, array2.element_nullable_);
        swap(array1.element_valid_data_, array2.element_valid_data_);
    }

    VectorArray(VectorArray&& other) noexcept : VectorArray() {
        swap(*this, other);
    }

    VectorArray&
    operator=(const VectorArray& other) {
        VectorArray temp(other);
        swap(*this, temp);
        return *this;
    }

    VectorArray&
    operator=(VectorArray&& other) noexcept {
        swap(*this, other);
        return *this;
    }

    VectorFieldProto
    output_data() const {
        VectorFieldProto vector_field;
        vector_field.set_dim(dim_);
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                auto* obj = vector_field.mutable_float_vector();
                if (size_ > 0) {
                    auto data = reinterpret_cast<const float*>(data_.get());
                    obj->mutable_data()->Add(data,
                                             data + physical_length_ * dim_);
                }
                break;
            }
            case DataType::VECTOR_BINARY: {
                if (size_ > 0) {
                    vector_field.set_binary_vector(data_.get(), size_);
                } else {
                    vector_field.mutable_binary_vector();
                }
                break;
            }
            case DataType::VECTOR_FLOAT16: {
                if (size_ > 0) {
                    vector_field.set_float16_vector(data_.get(), size_);
                } else {
                    vector_field.mutable_float16_vector();
                }
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                if (size_ > 0) {
                    vector_field.set_bfloat16_vector(data_.get(), size_);
                } else {
                    vector_field.mutable_bfloat16_vector();
                }
                break;
            }
            case DataType::VECTOR_INT8: {
                if (size_ > 0) {
                    vector_field.set_int8_vector(data_.get(), size_);
                } else {
                    vector_field.mutable_int8_vector();
                }
                break;
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
        if (element_nullable_) {
            vector_field.mutable_valid_data()->Reserve(length_);
            for (int i = 0; i < length_; ++i) {
                vector_field.add_valid_data(element_valid_data_[i]);
            }
        }
        return vector_field;
    }

    int
    physical_length() const {
        return physical_length_;
    }

    size_t
    byte_size() const {
        return size_;
    }

    size_t
    get_element_valid_data_byte_size() const {
        return element_valid_data_.size_in_bytes();
    }

    int64_t
    dim() const {
        return dim_;
    }

    DataType
    get_element_type() const {
        return element_type_;
    }

    const char*
    data() const {
        return data_.get();
    }

 private:
    void
    init_element_valid_data(const TargetBitmapView& element_valid_data) {
        if (!element_nullable_) {
            AssertInfo(element_valid_data.size() == 0,
                       "non-element-nullable vector array cannot carry "
                       "element valid data, bitmap_length={}",
                       element_valid_data.size());
            return;
        }
        AssertInfo(element_valid_data.size() == length_,
                   "element valid data bitmap length must equal vector array "
                   "logical length, bitmap_length={}, array_length={}",
                   element_valid_data.size(),
                   length_);
        element_valid_data_ = TargetBitmap(element_valid_data);
    }

    void
    init_element_valid_data(
        const google::protobuf::RepeatedField<bool>& element_valid_data) {
        if (!element_nullable_) {
            return;
        }
        AssertInfo(element_valid_data.size() == length_,
                   "element valid data length must equal vector array logical "
                   "length, valid_data_size={}, array_length={}",
                   element_valid_data.size(),
                   length_);
        element_valid_data_ = TargetBitmap(length_, false);
        for (int i = 0; i < length_; ++i) {
            if (element_valid_data.Get(i)) {
                element_valid_data_.set(i);
            }
        }
    }

    int64_t dim_ = 0;
    std::unique_ptr<char[]> data_;
    // number of vectors in this array
    int length_ = 0;
    // number of valid vectors stored in the compact payload
    int physical_length_ = 0;
    // size of the array in bytes
    int size_ = 0;
    DataType element_type_ = DataType::NONE;
    bool element_nullable_ = false;
    // TODO: TargetBitmap currently adds 32 bytes per row on 64-bit builds,
    // including for non-element-nullable arrays. If this becomes an issue, use
    // std::unique_ptr<uint64_t[]> for the validity bitmap or move nullable
    // storage into a dedicated NullableVectorArray class.
    TargetBitmap element_valid_data_{};
};

class VectorArrayView {
 public:
    VectorArrayView() = default;

    VectorArrayView(const VectorArrayView& other) = default;

    VectorArrayView(
        char* data, int64_t dim, int len, size_t size, DataType element_type)
        : VectorArrayView(
              data, dim, len, size, element_type, TargetBitmapView(), false) {
    }

    VectorArrayView(char* data,
                    int64_t dim,
                    int len,
                    size_t size,
                    DataType element_type,
                    const TargetBitmapView& element_valid_data,
                    bool element_nullable)
        : data_(data),
          dim_(dim),
          length_(len),
          size_(size),
          element_type_(element_type),
          element_nullable_(element_nullable),
          element_valid_data_(element_valid_data) {
        if (element_nullable_) {
            AssertInfo(
                element_valid_data_.size() == length_,
                "element valid data bitmap length must equal vector array "
                "logical length, bitmap_length={}, array_length={}",
                element_valid_data_.size(),
                length_);
            physical_length_ = static_cast<int>(element_valid_data_.count());
        } else {
            AssertInfo(element_valid_data_.size() == 0,
                       "non-element-nullable vector array cannot carry "
                       "element valid data, bitmap_length={}",
                       element_valid_data_.size());
            physical_length_ = length_;
        }
        auto bytes_per_vector =
            milvus::vector_bytes_per_element(element_type_, dim_);
        AssertInfo(size_ == physical_length_ * bytes_per_vector,
                   "vector array compact payload size {} must match physical "
                   "element count {} and vector byte width {}",
                   size_,
                   physical_length_,
                   bytes_per_vector);
        AssertInfo(data_ != nullptr || size_ == 0,
                   "non-empty vector array payload cannot be null");
    }

    VectorFieldProto
    output_data() const {
        VectorFieldProto vector_field;
        vector_field.set_dim(dim_);
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                auto* obj = vector_field.mutable_float_vector();
                if (size_ > 0) {
                    auto data = reinterpret_cast<const float*>(data_);
                    obj->mutable_data()->Add(data,
                                             data + physical_length_ * dim_);
                }
                break;
            }
            case DataType::VECTOR_BINARY: {
                if (size_ > 0) {
                    vector_field.set_binary_vector(data_, size_);
                } else {
                    vector_field.mutable_binary_vector();
                }
                break;
            }
            case DataType::VECTOR_FLOAT16: {
                if (size_ > 0) {
                    vector_field.set_float16_vector(data_, size_);
                } else {
                    vector_field.mutable_float16_vector();
                }
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                if (size_ > 0) {
                    vector_field.set_bfloat16_vector(data_, size_);
                } else {
                    vector_field.mutable_bfloat16_vector();
                }
                break;
            }
            case DataType::VECTOR_INT8: {
                if (size_ > 0) {
                    vector_field.set_int8_vector(data_, size_);
                } else {
                    vector_field.mutable_int8_vector();
                }
                break;
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
        if (element_nullable_) {
            vector_field.mutable_valid_data()->Reserve(length_);
            for (int i = 0; i < length_; ++i) {
                vector_field.add_valid_data(element_valid_data_[i]);
            }
        }
        return vector_field;
    }

    int
    physical_length() const {
        return physical_length_;
    }

 private:
    char* data_{nullptr};
    int64_t dim_ = 0;
    // number of vectors in this array
    int length_ = 0;
    // number of valid vectors stored in the compact payload
    int physical_length_ = 0;
    // size of the array in bytes
    int size_ = 0;
    DataType element_type_ = DataType::NONE;
    bool element_nullable_ = false;
    TargetBitmapView element_valid_data_{};
};

}  // namespace milvus
