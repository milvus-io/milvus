/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "Tensor.cuh"

namespace faiss { namespace gpu {

template <typename T,
          int Dim,
          bool InnerContig = false,
          typename IndexT = int,
          template <typename U> class PtrTraits = traits::DefaultPtrTraits>
class HostTensor : public Tensor<T, Dim, InnerContig, IndexT, PtrTraits> {
 public:
  typedef IndexT IndexType;
  typedef typename PtrTraits<T>::PtrType DataPtrType;

  /// Default constructor
  __host__ HostTensor();

  /// Destructor
  __host__ ~HostTensor();

  /// Constructs a tensor of the given size, allocating memory for it
  /// locally
  __host__ HostTensor(const IndexT sizes[Dim]);
  __host__ HostTensor(std::initializer_list<IndexT> sizes);

  /// Constructs a tensor of the given size and stride, referencing a
  /// memory region we do not own
  __host__ HostTensor(DataPtrType data,
                      const IndexT sizes[Dim]);
  __host__ HostTensor(DataPtrType data,
                      std::initializer_list<IndexT> sizes);

  /// Constructs a tensor of the given size and stride, referencing a
  /// memory region we do not own
  __host__ HostTensor(DataPtrType data,
                      const IndexT sizes[Dim],
                      const IndexT strides[Dim]);

  /// Copies a tensor into ourselves, allocating memory for it
  /// locally. If the tensor is on the GPU, then we will copy it to
  /// ourselves wrt the given stream.
  __host__ HostTensor(Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t,
                      cudaStream_t stream);

  /// Call to zero out memory
  __host__ HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>& zero();

  /// Returns the maximum difference seen between two tensors
  __host__ T
  maxDiff(const HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>& t) const;

  /// Are the two tensors exactly equal?
  __host__ bool
  equal(const HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>& t) const {
    return (maxDiff(t) == (T) 0);
  }

 private:
  enum AllocState {
    /// This tensor itself owns the memory, which must be freed via
    /// cudaFree
    Owner,

    /// This tensor itself is not an owner of the memory; there is
    /// nothing to free
    NotOwner,
  };

  AllocState state_;
};

} } // namespace

#include "HostTensor-inl.cuh"
