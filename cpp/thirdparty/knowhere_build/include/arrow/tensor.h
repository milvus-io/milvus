// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_TENSOR_H
#define ARROW_TENSOR_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

static inline bool is_tensor_supported(Type::type type_id) {
  switch (type_id) {
    case Type::UINT8:
    case Type::INT8:
    case Type::UINT16:
    case Type::INT16:
    case Type::UINT32:
    case Type::INT32:
    case Type::UINT64:
    case Type::INT64:
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
      return true;
    default:
      break;
  }
  return false;
}

template <typename SparseIndexType>
class SparseTensorImpl;

class ARROW_EXPORT Tensor {
 public:
  virtual ~Tensor() = default;

  /// Constructor with no dimension names or strides, data assumed to be row-major
  Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
         const std::vector<int64_t>& shape);

  /// Constructor with non-negative strides
  Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
         const std::vector<int64_t>& shape, const std::vector<int64_t>& strides);

  /// Constructor with non-negative strides and dimension names
  Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
         const std::vector<int64_t>& shape, const std::vector<int64_t>& strides,
         const std::vector<std::string>& dim_names);

  std::shared_ptr<DataType> type() const { return type_; }
  std::shared_ptr<Buffer> data() const { return data_; }

  const uint8_t* raw_data() const { return data_->data(); }
  uint8_t* raw_mutable_data() { return data_->mutable_data(); }

  const std::vector<int64_t>& shape() const { return shape_; }
  const std::vector<int64_t>& strides() const { return strides_; }

  int ndim() const { return static_cast<int>(shape_.size()); }

  const std::vector<std::string>& dim_names() const { return dim_names_; }
  const std::string& dim_name(int i) const;

  /// Total number of value cells in the tensor
  int64_t size() const;

  /// Return true if the underlying data buffer is mutable
  bool is_mutable() const { return data_->is_mutable(); }

  /// Either row major or column major
  bool is_contiguous() const;

  /// AKA "C order"
  bool is_row_major() const;

  /// AKA "Fortran order"
  bool is_column_major() const;

  Type::type type_id() const;

  bool Equals(const Tensor& other) const;

  /// Compute the number of non-zero values in the tensor
  Status CountNonZero(int64_t* result) const;

 protected:
  Tensor() {}

  std::shared_ptr<DataType> type_;
  std::shared_ptr<Buffer> data_;
  std::vector<int64_t> shape_;
  std::vector<int64_t> strides_;

  /// These names are optional
  std::vector<std::string> dim_names_;

  template <typename SparseIndexType>
  friend class SparseTensorImpl;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Tensor);
};

template <typename TYPE>
class NumericTensor : public Tensor {
 public:
  using TypeClass = TYPE;
  using value_type = typename TypeClass::c_type;

  /// Constructor with non-negative strides and dimension names
  NumericTensor(const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape,
                const std::vector<int64_t>& strides,
                const std::vector<std::string>& dim_names)
      : Tensor(TypeTraits<TYPE>::type_singleton(), data, shape, strides, dim_names) {}

  /// Constructor with no dimension names or strides, data assumed to be row-major
  NumericTensor(const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape)
      : NumericTensor(data, shape, {}, {}) {}

  /// Constructor with non-negative strides
  NumericTensor(const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape,
                const std::vector<int64_t>& strides)
      : NumericTensor(data, shape, strides, {}) {}

  const value_type& Value(const std::vector<int64_t>& index) const {
    int64_t offset = CalculateValueOffset(index);
    const value_type* ptr = reinterpret_cast<const value_type*>(raw_data() + offset);
    return *ptr;
  }

 protected:
  int64_t CalculateValueOffset(const std::vector<int64_t>& index) const {
    int64_t offset = 0;
    for (size_t i = 0; i < index.size(); ++i) {
      offset += index[i] * strides_[i];
    }
    return offset;
  }
};

}  // namespace arrow

#endif  // ARROW_TENSOR_H
