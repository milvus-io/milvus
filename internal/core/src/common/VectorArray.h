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
#include <cmath>
#include <cstring>
#include <memory>

#include "common/FastMem.h"

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
        : VectorArray(data,
                      num_vectors,
                      dim,
                      element_type,
                      TargetBitmapView(),
                      false) {
    }

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
        assert(data != nullptr || num_vectors == 0);
        assert(num_vectors >= 0);
        assert(dim > 0);

        size_ =
            num_vectors * milvus::vector_bytes_per_element(element_type, dim);

        if (size_ > 0) {
            data_ = std::make_unique<char[]>(size_);
            milvus::fastmem::FastMemcpy(data_.get(), data, size_);
        }
        init_element_valid_data(element_valid_data);
    }

    // One row of VectorFieldProto
    explicit VectorArray(const VectorFieldProto& vector_field)
        : VectorArray(vector_field, false) {
    }

    VectorArray(const VectorFieldProto& vector_field, bool element_nullable) {
        element_nullable_ = element_nullable;
        if (element_nullable_) {
            init_from_compact_proto(vector_field);
            return;
        }
        AssertInfo(vector_field.valid_data_size() == 0,
                   "non-element-nullable vector array cannot carry element "
                   "valid_data");
        dim_ = vector_field.dim();
        switch (vector_field.data_case()) {
            case VectorFieldProto::kFloatVector: {
                element_type_ = DataType::VECTOR_FLOAT;
                // data size should be array length * dim
                length_ = vector_field.float_vector().data().size() / dim_;
                auto data = new float[length_ * dim_];
                size_ =
                    vector_field.float_vector().data().size() * sizeof(float);
                milvus::fastmem::FastMemcpy(
                    data,
                    vector_field.float_vector().data().data(),
                    vector_field.float_vector().data().size() * sizeof(float));
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
                milvus::fastmem::FastMemcpy(
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
                milvus::fastmem::FastMemcpy(
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
                milvus::fastmem::FastMemcpy(
                    data_.get(), vector_field.bfloat16_vector().data(), size_);
                break;
            }
            case VectorFieldProto::kInt8Vector: {
                element_type_ = DataType::VECTOR_INT8;
                length_ = vector_field.int8_vector().size() / dim_;
                size_ = vector_field.int8_vector().size();
                data_ = std::make_unique<char[]>(size_);
                milvus::fastmem::FastMemcpy(
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
                      other.element_type_,
                      other.element_nullable_
                          ? other.element_valid_data_.view()
                          : TargetBitmapView(),
                      other.element_nullable_) {
        has_invalid_element_ = other.has_invalid_element_;
    }

    friend void
    swap(VectorArray& array1, VectorArray& array2) noexcept {
        using std::swap;
        swap(array1.data_, array2.data_);
        swap(array1.size_, array2.size_);
        swap(array1.length_, array2.length_);
        swap(array1.dim_, array2.dim_);
        swap(array1.element_type_, array2.element_type_);
        swap(array1.element_nullable_, array2.element_nullable_);
        swap(array1.element_valid_data_, array2.element_valid_data_);
        swap(array1.has_invalid_element_, array2.has_invalid_element_);
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
            size_ != other.size_ || element_nullable_ != other.element_nullable_) {
            return false;
        }

        if (length_ == 0) {
            return true;
        }
        if (element_nullable_) {
            for (int i = 0; i < length_; ++i) {
                if (is_element_valid(i) != other.is_element_valid(i)) {
                    return false;
                }
            }
        }

        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                auto* a = reinterpret_cast<const float*>(data_.get());
                auto* b = reinterpret_cast<const float*>(other.data_.get());
                for (int i = 0; i < length_; ++i) {
                    if (!is_element_valid(i)) {
                        continue;
                    }
                    auto* lhs = a + i * dim_;
                    auto* rhs = b + i * dim_;
                    if (!std::equal(lhs, lhs + dim_, rhs, [](float x, float y) {
                            return std::abs(x - y) < 1e-6f;
                        })) {
                        return false;
                    }
                }
                return true;
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
        output_vector_payload(vector_field, element_nullable_);
        if (element_nullable_) {
            vector_field.mutable_valid_data()->Reserve(length_);
            for (int i = 0; i < length_; ++i) {
                vector_field.add_valid_data(is_element_valid(i));
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
    is_element_nullable() const {
        return element_nullable_;
    }

    bool
    is_element_valid(int index) const {
        AssertInfo(index >= 0 && index < length_,
                   "index out of range, index={}, length={}",
                   index,
                   length_);
        if (!element_nullable_) {
            return true;
        }
        AssertInfo(element_valid_data_.size() == length_,
                   "element valid data bitmap length must equal vector array "
                   "logical length, bitmap_length={}, array_length={}",
                   element_valid_data_.size(),
                   length_);
        return element_valid_data_[index];
    }

    const TargetBitmap&
    get_element_valid_data() const {
        return element_valid_data_;
    }

    size_t
    get_element_valid_data_byte_size() const {
        return element_valid_data_.size_in_bytes();
    }

    bool
    has_invalid_element() const {
        return has_invalid_element_;
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
    void
    init_element_valid_data(const TargetBitmapView& element_valid_data) {
        if (!element_nullable_) {
            return;
        }
        AssertInfo(element_valid_data.size() == length_,
                   "element valid data bitmap length must equal vector array "
                   "logical length, bitmap_length={}, array_length={}",
                   element_valid_data.size(),
                   length_);
        element_valid_data_ = TargetBitmap(element_valid_data);
        has_invalid_element_ = !element_valid_data_.all();
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
        has_invalid_element_ = !element_valid_data_.all();
    }

    static int
    count_valid(const google::protobuf::RepeatedField<bool>& valid_data) {
        int count = 0;
        for (bool valid : valid_data) {
            if (valid) {
                ++count;
            }
        }
        return count;
    }

    void
    init_from_compact_proto(const VectorFieldProto& vector_field) {
        dim_ = vector_field.dim();
        length_ = vector_field.valid_data_size();
        AssertInfo(dim_ > 0 || length_ == 0,
                   "VectorArray dim must be positive, dim={}",
                   dim_);

        const auto valid_count = count_valid(vector_field.valid_data());
        const char* compact_data = nullptr;
        size_t compact_size = 0;
        switch (vector_field.data_case()) {
            case VectorFieldProto::kFloatVector: {
                element_type_ = DataType::VECTOR_FLOAT;
                compact_size =
                    vector_field.float_vector().data().size() * sizeof(float);
                compact_data = reinterpret_cast<const char*>(
                    vector_field.float_vector().data().data());
                break;
            }
            case VectorFieldProto::kBinaryVector: {
                element_type_ = DataType::VECTOR_BINARY;
                compact_size = vector_field.binary_vector().size();
                compact_data = vector_field.binary_vector().data();
                break;
            }
            case VectorFieldProto::kFloat16Vector: {
                element_type_ = DataType::VECTOR_FLOAT16;
                compact_size = vector_field.float16_vector().size();
                compact_data = vector_field.float16_vector().data();
                break;
            }
            case VectorFieldProto::kBfloat16Vector: {
                element_type_ = DataType::VECTOR_BFLOAT16;
                compact_size = vector_field.bfloat16_vector().size();
                compact_data = vector_field.bfloat16_vector().data();
                break;
            }
            case VectorFieldProto::kInt8Vector: {
                element_type_ = DataType::VECTOR_INT8;
                compact_size = vector_field.int8_vector().size();
                compact_data = vector_field.int8_vector().data();
                break;
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(vector_field.data_case()));
            }
        }

        auto bytes_per_vector =
            milvus::vector_bytes_per_element(element_type_, dim_);
        AssertInfo(compact_size == static_cast<size_t>(valid_count) *
                                       bytes_per_vector,
                   "nullable vector array compact payload size {} must match "
                   "valid element count {} and vector byte width {}",
                   compact_size,
                   valid_count,
                   bytes_per_vector);
        size_ = length_ * bytes_per_vector;
        if (size_ > 0) {
            data_ = std::make_unique<char[]>(size_);
            std::memset(data_.get(), 0, size_);
        }
        int physical_index = 0;
        for (int i = 0; i < length_; ++i) {
            if (!vector_field.valid_data(i)) {
                continue;
            }
            milvus::fastmem::FastMemcpy(data_.get() + i * bytes_per_vector,
                                        compact_data +
                                            physical_index * bytes_per_vector,
                                        bytes_per_vector);
            ++physical_index;
        }
        init_element_valid_data(vector_field.valid_data());
    }

    void
    output_vector_payload(VectorFieldProto& vector_field,
                          bool compact_valid_only) const {
        vector_field.set_dim(dim_);
        auto bytes_per_vector =
            milvus::vector_bytes_per_element(element_type_, dim_);
        auto should_emit = [this, compact_valid_only](int i) {
            return !compact_valid_only || is_element_valid(i);
        };
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                auto* obj = vector_field.mutable_float_vector();
                auto data = reinterpret_cast<const float*>(data_.get());
                for (int i = 0; i < length_; ++i) {
                    if (!should_emit(i)) {
                        continue;
                    }
                    auto* start = data + i * dim_;
                    obj->mutable_data()->Add(start, start + dim_);
                }
                break;
            }
            case DataType::VECTOR_BINARY: {
                auto* bytes = vector_field.mutable_binary_vector();
                for (int i = 0; i < length_; ++i) {
                    if (should_emit(i)) {
                        bytes->append(data_.get() + i * bytes_per_vector,
                                      bytes_per_vector);
                    }
                }
                break;
            }
            case DataType::VECTOR_FLOAT16: {
                auto* bytes = vector_field.mutable_float16_vector();
                for (int i = 0; i < length_; ++i) {
                    if (should_emit(i)) {
                        bytes->append(data_.get() + i * bytes_per_vector,
                                      bytes_per_vector);
                    }
                }
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                auto* bytes = vector_field.mutable_bfloat16_vector();
                for (int i = 0; i < length_; ++i) {
                    if (should_emit(i)) {
                        bytes->append(data_.get() + i * bytes_per_vector,
                                      bytes_per_vector);
                    }
                }
                break;
            }
            case DataType::VECTOR_INT8: {
                auto* bytes = vector_field.mutable_int8_vector();
                for (int i = 0; i < length_; ++i) {
                    if (should_emit(i)) {
                        bytes->append(data_.get() + i * bytes_per_vector,
                                      bytes_per_vector);
                    }
                }
                break;
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
    }

    int64_t dim_ = 0;
    std::unique_ptr<char[]> data_;
    // number of vectors in this array
    int length_ = 0;
    // size of the array in bytes
    int size_ = 0;
    DataType element_type_ = DataType::NONE;
    bool element_nullable_ = false;
    TargetBitmap element_valid_data_{};
    bool has_invalid_element_ = false;
};

class VectorArrayView {
 public:
    VectorArrayView() = default;

    VectorArrayView(const VectorArrayView& other)
        : VectorArrayView(other.data_,
                          other.dim_,
                          other.length_,
                          other.size_,
                          other.element_type_,
                          other.element_valid_data_,
                          other.element_nullable_) {
    }

    VectorArrayView(
        char* data, int64_t dim, int len, size_t size, DataType element_type)
        : VectorArrayView(data,
                          dim,
                          len,
                          size,
                          element_type,
                          TargetBitmapView(),
                          false) {
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
        AssertInfo(!element_nullable_ || element_valid_data_.size() == length_,
                   "element valid data bitmap length must equal vector array "
                   "logical length, bitmap_length={}, array_length={}",
                   element_valid_data_.size(),
                   length_);
    }

    explicit VectorArrayView(const VectorArray& array)
        : VectorArrayView(const_cast<char*>(array.data()),
                          array.dim(),
                          array.length(),
                          array.byte_size(),
                          array.get_element_type(),
                          array.is_element_nullable()
                              ? array.get_element_valid_data().view()
                              : TargetBitmapView(),
                          array.is_element_nullable()) {
        has_invalid_element_computed_ = array.is_element_nullable();
        has_invalid_element_ = array.has_invalid_element();
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
        VectorFieldProto vector_field;
        output_vector_payload(vector_field, element_nullable_);
        if (element_nullable_) {
            vector_field.mutable_valid_data()->Reserve(length_);
            for (int i = 0; i < length_; ++i) {
                vector_field.add_valid_data(is_element_valid(i));
            }
        }
        return vector_field;
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

    int
    length() const {
        return length_;
    }

    size_t
    byte_size() const {
        return size_;
    }

    const char*
    data() const {
        return data_;
    }

    int64_t
    dim() const {
        return dim_;
    }

    DataType
    get_element_type() const {
        return element_type_;
    }

    bool
    is_element_nullable() const {
        return element_nullable_;
    }

    bool
    is_element_valid(int index) const {
        AssertInfo(index >= 0 && index < length_,
                   "index out of range, index={}, length={}",
                   index,
                   length_);
        if (!element_nullable_) {
            return true;
        }
        AssertInfo(element_valid_data_.size() == length_,
                   "element valid data bitmap length must equal vector array "
                   "logical length, bitmap_length={}, array_length={}",
                   element_valid_data_.size(),
                   length_);
        return element_valid_data_[index];
    }

    const TargetBitmapView&
    get_element_valid_data() const {
        return element_valid_data_;
    }

    size_t
    get_element_valid_data_byte_size() const {
        return element_valid_data_.size_in_bytes();
    }

    bool
    has_invalid_element() const {
        if (!element_nullable_) {
            return false;
        }
        if (has_invalid_element_computed_) {
            return has_invalid_element_;
        }
        for (int i = 0; i < length_; ++i) {
            if (!is_element_valid(i)) {
                has_invalid_element_ = true;
                has_invalid_element_computed_ = true;
                return true;
            }
        }
        has_invalid_element_ = false;
        has_invalid_element_computed_ = true;
        return false;
    }

 private:
    void
    output_vector_payload(VectorFieldProto& vector_field,
                          bool compact_valid_only) const {
        vector_field.set_dim(dim_);
        auto bytes_per_vector =
            milvus::vector_bytes_per_element(element_type_, dim_);
        auto should_emit = [this, compact_valid_only](int i) {
            return !compact_valid_only || is_element_valid(i);
        };
        switch (element_type_) {
            case DataType::VECTOR_FLOAT: {
                auto* obj = vector_field.mutable_float_vector();
                auto data = reinterpret_cast<const float*>(data_);
                for (int i = 0; i < length_; ++i) {
                    if (!should_emit(i)) {
                        continue;
                    }
                    auto* start = data + i * dim_;
                    obj->mutable_data()->Add(start, start + dim_);
                }
                break;
            }
            case DataType::VECTOR_BINARY: {
                auto* bytes = vector_field.mutable_binary_vector();
                for (int i = 0; i < length_; ++i) {
                    if (should_emit(i)) {
                        bytes->append(data_ + i * bytes_per_vector,
                                      bytes_per_vector);
                    }
                }
                break;
            }
            case DataType::VECTOR_FLOAT16: {
                auto* bytes = vector_field.mutable_float16_vector();
                for (int i = 0; i < length_; ++i) {
                    if (should_emit(i)) {
                        bytes->append(data_ + i * bytes_per_vector,
                                      bytes_per_vector);
                    }
                }
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                auto* bytes = vector_field.mutable_bfloat16_vector();
                for (int i = 0; i < length_; ++i) {
                    if (should_emit(i)) {
                        bytes->append(data_ + i * bytes_per_vector,
                                      bytes_per_vector);
                    }
                }
                break;
            }
            case DataType::VECTOR_INT8: {
                auto* bytes = vector_field.mutable_int8_vector();
                for (int i = 0; i < length_; ++i) {
                    if (should_emit(i)) {
                        bytes->append(data_ + i * bytes_per_vector,
                                      bytes_per_vector);
                    }
                }
                break;
            }
            default: {
                ThrowInfo(NotImplemented,
                          "Not implemented vector type: {}",
                          static_cast<int>(element_type_));
            }
        }
    }

    char* data_{nullptr};
    int64_t dim_ = 0;
    // number of vectors in this array
    int length_ = 0;
    // size of the array in bytes
    int size_ = 0;
    DataType element_type_ = DataType::NONE;
    bool element_nullable_ = false;
    TargetBitmapView element_valid_data_{};
    mutable bool has_invalid_element_computed_ = false;
    mutable bool has_invalid_element_ = false;
};

}  // namespace milvus
