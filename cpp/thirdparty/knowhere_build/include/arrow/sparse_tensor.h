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

#ifndef ARROW_SPARSE_TENSOR_H
#define ARROW_SPARSE_TENSOR_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/tensor.h"

namespace arrow {

// ----------------------------------------------------------------------
// SparseIndex class

struct SparseTensorFormat {
  /// EXPERIMENTAL: The index format type of SparseTensor
  enum type { COO, CSR };
};

/// \brief EXPERIMENTAL: The base class for the index of a sparse tensor
///
/// SparseIndex describes where the non-zero elements are within a SparseTensor.
///
/// There are several ways to represent this.  The format_id is used to
/// distinguish what kind of representation is used.  Each possible value of
/// format_id must have only one corresponding concrete subclass of SparseIndex.
class ARROW_EXPORT SparseIndex {
 public:
  explicit SparseIndex(SparseTensorFormat::type format_id, int64_t non_zero_length)
      : format_id_(format_id), non_zero_length_(non_zero_length) {}

  virtual ~SparseIndex() = default;

  /// \brief Return the identifier of the format type
  SparseTensorFormat::type format_id() const { return format_id_; }

  /// \brief Return the number of non zero values in the sparse tensor related
  /// to this sparse index
  int64_t non_zero_length() const { return non_zero_length_; }

  /// \brief Return the string representation of the sparse index
  virtual std::string ToString() const = 0;

 protected:
  SparseTensorFormat::type format_id_;
  int64_t non_zero_length_;
};

namespace internal {
template <typename SparseIndexType>
class SparseIndexBase : public SparseIndex {
 public:
  explicit SparseIndexBase(int64_t non_zero_length)
      : SparseIndex(SparseIndexType::format_id, non_zero_length) {}
};
}  // namespace internal

// ----------------------------------------------------------------------
// SparseCOOIndex class

/// \brief EXPERIMENTAL: The index data for a COO sparse tensor
///
/// A COO sparse index manages the location of its non-zero values by their
/// coordinates.
class ARROW_EXPORT SparseCOOIndex : public internal::SparseIndexBase<SparseCOOIndex> {
 public:
  using CoordsTensor = NumericTensor<Int64Type>;

  static constexpr SparseTensorFormat::type format_id = SparseTensorFormat::COO;

  // Constructor with a column-major NumericTensor
  explicit SparseCOOIndex(const std::shared_ptr<CoordsTensor>& coords);

  /// \brief Return a tensor that has the coordinates of the non-zero values
  const std::shared_ptr<CoordsTensor>& indices() const { return coords_; }

  /// \brief Return a string representation of the sparse index
  std::string ToString() const override;

  /// \brief Return whether the COO indices are equal
  bool Equals(const SparseCOOIndex& other) const {
    return indices()->Equals(*other.indices());
  }

 protected:
  std::shared_ptr<CoordsTensor> coords_;
};

// ----------------------------------------------------------------------
// SparseCSRIndex class

/// \brief EXPERIMENTAL: The index data for a CSR sparse matrix
///
/// A CSR sparse index manages the location of its non-zero values by two
/// vectors.
///
/// The first vector, called indptr, represents the range of the rows; the i-th
/// row spans from indptr[i] to indptr[i+1] in the corresponding value vector.
/// So the length of an indptr vector is the number of rows + 1.
///
/// The other vector, called indices, represents the column indices of the
/// corresponding non-zero values.  So the length of an indices vector is same
/// as the number of non-zero-values.
class ARROW_EXPORT SparseCSRIndex : public internal::SparseIndexBase<SparseCSRIndex> {
 public:
  using IndexTensor = NumericTensor<Int64Type>;

  static constexpr SparseTensorFormat::type format_id = SparseTensorFormat::CSR;

  // Constructor with two index vectors
  explicit SparseCSRIndex(const std::shared_ptr<IndexTensor>& indptr,
                          const std::shared_ptr<IndexTensor>& indices);

  /// \brief Return a 1D tensor of indptr vector
  const std::shared_ptr<IndexTensor>& indptr() const { return indptr_; }

  /// \brief Return a 1D tensor of indices vector
  const std::shared_ptr<IndexTensor>& indices() const { return indices_; }

  /// \brief Return a string representation of the sparse index
  std::string ToString() const override;

  /// \brief Return whether the CSR indices are equal
  bool Equals(const SparseCSRIndex& other) const {
    return indptr()->Equals(*other.indptr()) && indices()->Equals(*other.indices());
  }

 protected:
  std::shared_ptr<IndexTensor> indptr_;
  std::shared_ptr<IndexTensor> indices_;
};

// ----------------------------------------------------------------------
// SparseTensor class

/// \brief EXPERIMENTAL: The base class of sparse tensor container
class ARROW_EXPORT SparseTensor {
 public:
  virtual ~SparseTensor() = default;

  SparseTensorFormat::type format_id() const { return sparse_index_->format_id(); }

  /// \brief Return a value type of the sparse tensor
  std::shared_ptr<DataType> type() const { return type_; }

  /// \brief Return a buffer that contains the value vector of the sparse tensor
  std::shared_ptr<Buffer> data() const { return data_; }

  /// \brief Return an immutable raw data pointer
  const uint8_t* raw_data() const { return data_->data(); }

  /// \brief Return a mutable raw data pointer
  uint8_t* raw_mutable_data() const { return data_->mutable_data(); }

  /// \brief Return a shape vector of the sparse tensor
  const std::vector<int64_t>& shape() const { return shape_; }

  /// \brief Return a sparse index of the sparse tensor
  const std::shared_ptr<SparseIndex>& sparse_index() const { return sparse_index_; }

  /// \brief Return a number of dimensions of the sparse tensor
  int ndim() const { return static_cast<int>(shape_.size()); }

  /// \brief Return a vector of dimension names
  const std::vector<std::string>& dim_names() const { return dim_names_; }

  /// \brief Return the name of the i-th dimension
  const std::string& dim_name(int i) const;

  /// \brief Total number of value cells in the sparse tensor
  int64_t size() const;

  /// \brief Return true if the underlying data buffer is mutable
  bool is_mutable() const { return data_->is_mutable(); }

  /// \brief Total number of non-zero cells in the sparse tensor
  int64_t non_zero_length() const {
    return sparse_index_ ? sparse_index_->non_zero_length() : 0;
  }

  /// \brief Return whether sparse tensors are equal
  bool Equals(const SparseTensor& other) const;

 protected:
  // Constructor with all attributes
  SparseTensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
               const std::vector<int64_t>& shape,
               const std::shared_ptr<SparseIndex>& sparse_index,
               const std::vector<std::string>& dim_names);

  std::shared_ptr<DataType> type_;
  std::shared_ptr<Buffer> data_;
  std::vector<int64_t> shape_;
  std::shared_ptr<SparseIndex> sparse_index_;

  // These names are optional
  std::vector<std::string> dim_names_;
};

// ----------------------------------------------------------------------
// SparseTensorImpl class

namespace internal {

ARROW_EXPORT
void MakeSparseTensorFromTensor(const Tensor& tensor,
                                SparseTensorFormat::type sparse_format_id,
                                std::shared_ptr<SparseIndex>* sparse_index,
                                std::shared_ptr<Buffer>* data);

}  // namespace internal

/// \brief EXPERIMENTAL: Concrete sparse tensor implementation classes with sparse index
/// type
template <typename SparseIndexType>
class SparseTensorImpl : public SparseTensor {
 public:
  virtual ~SparseTensorImpl() = default;

  // Constructor with all attributes
  SparseTensorImpl(const std::shared_ptr<SparseIndexType>& sparse_index,
                   const std::shared_ptr<DataType>& type,
                   const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape,
                   const std::vector<std::string>& dim_names)
      : SparseTensor(type, data, shape, sparse_index, dim_names) {}

  // Constructor for empty sparse tensor
  SparseTensorImpl(const std::shared_ptr<DataType>& type,
                   const std::vector<int64_t>& shape,
                   const std::vector<std::string>& dim_names = {})
      : SparseTensorImpl(NULLPTR, type, NULLPTR, shape, dim_names) {}

  // Constructor with a dense tensor
  explicit SparseTensorImpl(const Tensor& tensor)
      : SparseTensorImpl(NULLPTR, tensor.type(), NULLPTR, tensor.shape(),
                         tensor.dim_names_) {
    internal::MakeSparseTensorFromTensor(tensor, SparseIndexType::format_id,
                                         &sparse_index_, &data_);
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(SparseTensorImpl);
};

/// \brief EXPERIMENTAL: Type alias for COO sparse tensor
using SparseTensorCOO = SparseTensorImpl<SparseCOOIndex>;

/// \brief EXPERIMENTAL: Type alias for CSR sparse matrix
using SparseTensorCSR = SparseTensorImpl<SparseCSRIndex>;
using SparseMatrixCSR = SparseTensorImpl<SparseCSRIndex>;

}  // namespace arrow

#endif  // ARROW_SPARSE_TENSOR_H
