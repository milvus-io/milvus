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

#include <memory>
#include <cstring>

#include "FieldMeta.h"
#include "Types.h"
#include "common/VectorTrait.h"

namespace milvus {
// Internal representation of proto::schema::VectorField which is recognized as one row
// of data type VECTOR_ARRAY
class VectorArray : public milvus::VectorTrait {
 public:
    VectorArray() = default;

    ~VectorArray() = default;

    VectorArray(const void* data,
                int num_vectors,
                int64_t dim,
                DataType element_type)
        : dim_(dim), length_(num_vectors), element_type_(element_type) {
        assert(data != nullptr);
        assert(num_vectors > 0);
        assert(dim > 0);

        size_ =
            num_vectors * milvus::vector_bytes_per_element(element_type, dim);

        data_ = std::make_unique<char[]>(size_);
        std::memcpy(data_.get(), data, size_);
    }

    // One row of VectorFieldProto
    explicit VectorArray(const VectorFieldProto& vector_field) {
        dim_ = vector_field.dim();
        switch (vector_field.data_case()) {
            case VectorFieldProto::kFloatVector: {
                element_type_ = DataType::VECTOR_FLOAT;
                // data size should be array length * dim
                length_ = vector_field.float_vector().data().size() / dim_;
                auto data = new float[length_ * dim_];
                size_ =
                    vector_field.float_vector().data().size() * sizeof(float);
                std::copy(vector_field.float_vector().data().begin(),
                          vector_field.float_vector().data().end(),
                          data);
                data_ = std::unique_ptr<char[]>(reinterpret_cast<char*>(data));
                break;
            }
            case VectorFieldProto::kBinaryVector: {
                element_type_ = DataType::VECTOR_BINARY;
                int bytes_per_vector = (dim_ + 7) / 8;
                length_ =
                    vector_field.binary_vector().size() / bytes_per_vector;
                size_ = vector_field.binary_vector().size();
                data_ = std::make_unique<char[]>(size_);
                std::memcpy(
                    data_.get(), vector_field.binary_vector().data(), size_);
                break;
            }
            case VectorFieldProto::kFloat16Vector: {
                element_type_ = DataType::VECTOR_FLOAT16;
                int bytes_per_element = 2;  // 2 bytes per float16
                length_ = vector_field.float16_vector().size() /
                          (dim_ * bytes_per_element);
                size_ = vector_field.float16_vector().size();
                data_ = std::make_unique<char[]>(size_);
                std::memcpy(
                    data_.get(), vector_field.float16_vector().data(), size_);
                break;
            }
            case VectorFieldProto::kBfloat16Vector: {
                element_type_ = DataType::VECTOR_BFLOAT16;
                int bytes_per_element = 2;  // 2 bytes per bfloat16
                length_ = vector_field.bfloat16_vector().size() /
                          (dim_ * bytes_per_element);
                size_ = vector_field.bfloat16_vector().size();
                data_ = std::make_unique<char[]>(size_);
                std::memcpy(
                    data_.get(), vector_field.bfloat16_vector().data(), size_);
                break;
            }
            case VectorFieldProto::kInt8Vector: {
                element_type_ = DataType::VECTOR_INT8;
                length_ = vector_field.int8_vector().size() / dim_;
                size_ = vector_field.int8_vector().size();
                data_ = std::make_unique<char[]>(size_);
                std::memcpy(
                    data_.get(), vector_field.int8_vector().data(), size_);
                break;
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(vector_field.data_case()));
            }
        }
    }

    explicit VectorArray(const VectorArray& other)
        : VectorArray(other.data_.get(),
                      other.length_,
                      other.dim_,
                      other.size_,
                      other.element_type_) {
    }

    friend void
    swap(VectorArray& array1, VectorArray& array2) noexcept {
        using std::swap;
        swap(array1.data_, array2.data_);
        swap(array1.size_, array2.size_);
        swap(array1.length_, array2.length_);
        swap(array1.dim_, array2.dim_);
        swap(array1.element_type_, array2.element_type_);
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

    bool
    operator==(const VectorArray& other) const {
        if (element_type_ != other.element_type_ || length_ != other.length_ ||
            size_ != other.size_) {
            return false;
        }

        if (length_ == 0) {
            return true;
        }

        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                auto* a = reinterpret_cast<const float*>(data_.get());
                auto* b = reinterpret_cast<const float*>(other.data_.get());
                return std::equal(
                    a, a + length_ * dim_, b, [](float x, float y) {
                        return std::abs(x - y) < 1e-6f;
                    });
            }
            default: {
                // TODO(SpadeA): add other vector types
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
    }

    template <typename VectorElement>
    VectorElement*
    get_data(const int index) const {
        AssertInfo(index >= 0 && index < length_,
                   "index out of range, index={}, length={}",
                   index,
                   length_);
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                static_assert(std::is_same_v<VectorElement, float>,
                              "VectorElement must be float for VECTOR_FLOAT");
                return reinterpret_cast<VectorElement*>(data_.get()) +
                       index * dim_;
            }
            case DataType::VECTOR_BINARY: {
                // Binary vectors are packed bits
                int bytes_per_vector = (dim_ + 7) / 8;
                return reinterpret_cast<VectorElement*>(
                    data_.get() + index * bytes_per_vector);
            }
            case DataType::VECTOR_FLOAT16:
            case DataType::VECTOR_BFLOAT16: {
                // Float16/BFloat16 are 2 bytes per element
                return reinterpret_cast<VectorElement*>(data_.get() +
                                                        index * dim_ * 2);
            }
            case DataType::VECTOR_INT8: {
                // Int8 is 1 byte per element
                return reinterpret_cast<VectorElement*>(data_.get() +
                                                        index * dim_);
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
    }

    VectorFieldProto
    output_data() const {
        VectorFieldProto vector_field;
        vector_field.set_dim(dim_);
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                auto data = reinterpret_cast<const float*>(data_.get());
                vector_field.mutable_float_vector()->mutable_data()->Add(
                    data, data + length_ * dim_);
                break;
            }
            case DataType::VECTOR_BINARY: {
                vector_field.set_binary_vector(data_.get(), size_);
                break;
            }
            case DataType::VECTOR_FLOAT16: {
                vector_field.set_float16_vector(data_.get(), size_);
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                vector_field.set_bfloat16_vector(data_.get(), size_);
                break;
            }
            case DataType::VECTOR_INT8: {
                vector_field.set_int8_vector(data_.get(), size_);
                break;
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
        return vector_field;
    }

    int
    length() const {
        return length_;
    }

    size_t
    byte_size() const {
        return size_;
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

    bool
    is_same_array(const VectorFieldProto& vector_field) {
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                if (vector_field.data_case() !=
                    VectorFieldProto::kFloatVector) {
                    return false;
                }

                if (length_ !=
                    vector_field.float_vector().data().size() / dim_) {
                    return false;
                }

                if (length_ == 0) {
                    return true;
                }

                const float* a = reinterpret_cast<const float*>(data_.get());
                const float* b = vector_field.float_vector().data().data();
                return std::equal(
                    a, a + length_ * dim_, b, [](float x, float y) {
                        return std::abs(x - y) < 1e-6f;
                    });
            }
            default: {
                // TODO(SpadeA): add other vector types
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
    }

 private:
    VectorArray(
        char* data, int len, int dim, size_t size, DataType element_type)
        : size_(size), length_(len), dim_(dim), element_type_(element_type) {
        data_ = std::make_unique<char[]>(size);
        std::copy(data, data + size, data_.get());
    }

    int64_t dim_ = 0;
    std::unique_ptr<char[]> data_;
    // number of vectors in this array
    int length_ = 0;
    // size of the array in bytes
    int size_ = 0;
    DataType element_type_ = DataType::NONE;
};

class VectorArrayView {
 public:
    VectorArrayView() = default;

    VectorArrayView(const VectorArrayView& other)
        : VectorArrayView(other.data_,
                          other.dim_,
                          other.length_,
                          other.size_,
                          other.element_type_) {
    }

    VectorArrayView(
        char* data, int64_t dim, int len, size_t size, DataType element_type)
        : data_(data),
          dim_(dim),
          length_(len),
          size_(size),
          element_type_(element_type) {
    }

    template <typename VectorElement>
    VectorElement*
    get_data(const int index) const {
        AssertInfo(index >= 0 && index < length_,
                   "index out of range, index={}, length={}",
                   index,
                   length_);
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                static_assert(std::is_same_v<VectorElement, float>,
                              "VectorElement must be float for VECTOR_FLOAT");
                return reinterpret_cast<VectorElement*>(data_) + index * dim_;
            }
            case DataType::VECTOR_BINARY: {
                // Binary vectors are packed bits
                int bytes_per_vector = (dim_ + 7) / 8;
                return reinterpret_cast<VectorElement*>(
                    data_ + index * bytes_per_vector);
            }
            case DataType::VECTOR_FLOAT16:
            case DataType::VECTOR_BFLOAT16: {
                // Float16/BFloat16 are 2 bytes per element
                return reinterpret_cast<VectorElement*>(data_ +
                                                        index * dim_ * 2);
            }
            case DataType::VECTOR_INT8: {
                // Int8 is 1 byte per element
                return reinterpret_cast<VectorElement*>(data_ + index * dim_);
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
    }

    VectorFieldProto
    output_data() const {
        VectorFieldProto vector_array;
        vector_array.set_dim(dim_);
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                auto data = reinterpret_cast<const float*>(data_);
                vector_array.mutable_float_vector()->mutable_data()->Add(
                    data, data + length_ * dim_);
                break;
            }
            case DataType::VECTOR_BINARY: {
                vector_array.set_binary_vector(data_, size_);
                break;
            }
            case DataType::VECTOR_FLOAT16: {
                vector_array.set_float16_vector(data_, size_);
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                vector_array.set_bfloat16_vector(data_, size_);
                break;
            }
            case DataType::VECTOR_INT8: {
                vector_array.set_int8_vector(data_, size_);
                break;
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
        return vector_array;
    }

    bool
    is_same_array(const VectorFieldProto& vector_field) {
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                if (vector_field.data_case() !=
                    VectorFieldProto::kFloatVector) {
                    return false;
                }

                if (length_ !=
                    vector_field.float_vector().data().size() / dim_) {
                    return false;
                }

                if (length_ == 0) {
                    return true;
                }

                const float* a = reinterpret_cast<const float*>(data_);
                const float* b = vector_field.float_vector().data().data();
                return std::equal(
                    a, a + length_ * dim_, b, [](float x, float y) {
                        return std::abs(x - y) < 1e-6f;
                    });
            }
            default: {
                // TODO(SpadeA): add other vector types
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
    }

 private:
    char* data_{nullptr};
    int64_t dim_ = 0;
    // number of vectors in this array
    int length_ = 0;
    // size of the array in bytes
    int size_ = 0;
    DataType element_type_ = DataType::NONE;
};

}  // namespace milvus