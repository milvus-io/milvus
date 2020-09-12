/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/impl/FaissAssert.h>
#include <faiss/gpu/utils/Tensor.cuh>
#include <initializer_list>

namespace faiss { namespace gpu {

template <int Dim, bool InnerContig = false, typename IndexT = int>
class NoTypeTensor {
 public:
  NoTypeTensor()
      : mem_(nullptr),
        typeSize_(0) {
  }

  template <typename T>
  NoTypeTensor(Tensor<T, Dim, InnerContig, IndexT>& t)
      : mem_(t.data()),
        typeSize_(sizeof(T)) {
    for (int i = 0; i < Dim; ++i) {
      size_[i] = t.getSize(i);
      stride_[i] = t.getStride(i);
    }
  }

  NoTypeTensor(void* mem, int typeSize, std::initializer_list<IndexT> sizes)
      : mem_(mem),
        typeSize_(typeSize) {

    int i = 0;
    for (auto s : sizes) {
      size_[i++] = s;
    }

    stride_[Dim - 1] = (IndexT) 1;
    for (int j = Dim - 2; j >= 0; --j) {
      stride_[j] = stride_[j + 1] * size_[j + 1];
    }
  }

  NoTypeTensor(void* mem, int typeSize, int sizes[Dim])
      : mem_(mem),
        typeSize_(typeSize) {
    for (int i = 0; i < Dim; ++i) {
      size_[i] = sizes[i];
    }

    stride_[Dim - 1] = (IndexT) 1;
    for (int i = Dim - 2; i >= 0; --i) {
      stride_[i] = stride_[i + 1] * sizes[i + 1];
    }
  }

  NoTypeTensor(void* mem, int typeSize,
               IndexT sizes[Dim], IndexT strides[Dim])
    : mem_(mem),
      typeSize_(typeSize) {
    for (int i = 0; i < Dim; ++i) {
      size_[i] = sizes[i];
      stride_[i] = strides[i];
    }
  }

  int getTypeSize() const {
    return typeSize_;
  }

  IndexT getSize(int dim) const {
    FAISS_ASSERT(dim < Dim);
    return size_[dim];
  }

  IndexT getStride(int dim) const {
    FAISS_ASSERT(dim < Dim);
    return stride_[dim];
  }

  template <typename T>
  Tensor<T, Dim, InnerContig, IndexT> toTensor() {
    FAISS_ASSERT(sizeof(T) == typeSize_);

    return Tensor<T, Dim, InnerContig, IndexT>((T*) mem_, size_, stride_);
  }

  NoTypeTensor<Dim, InnerContig, IndexT> narrowOutermost(IndexT start,
                                                         IndexT size) {
    char* newPtr = (char*) mem_;

    if (start > 0) {
      newPtr += typeSize_ * start * stride_[0];
    }

    IndexT newSize[Dim];
    for (int i = 0; i < Dim; ++i) {
      if (i == 0) {
        assert(start + size <= size_[0]);
        newSize[i] = size;
      } else {
        newSize[i] = size_[i];
      }
    }

    return NoTypeTensor<Dim, InnerContig, IndexT>(
      newPtr, typeSize_, newSize, stride_);
  }

 private:
  void* mem_;
  int typeSize_;
  IndexT size_[Dim];
  IndexT stride_[Dim];
};

} } // namespace
