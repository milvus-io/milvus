/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "../GpuFaissAssert.h"
#include "DeviceUtils.h"
#include <limits>

namespace faiss { namespace gpu {

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::Tensor()
    : data_(nullptr) {
  static_assert(Dim > 0, "must have > 0 dimensions");

  for (int i = 0; i < Dim; ++i) {
    size_[i] = 0;
    stride_[i] = (IndexT) 1;
  }
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::Tensor(
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t) {
  this->operator=(t);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::Tensor(
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>&& t) {
  this->operator=(std::move(t));
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>&
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::operator=(
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t) {
  data_ = t.data_;
  for (int i = 0; i < Dim; ++i) {
    size_[i] = t.size_[i];
    stride_[i] = t.stride_[i];
  }

  return *this;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>&
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::operator=(
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>&& t) {
  data_ = t.data_; t.data_ = nullptr;
  for (int i = 0; i < Dim; ++i) {
    stride_[i] = t.stride_[i]; t.stride_[i] = 0;
    size_[i] = t.size_[i]; t.size_[i] = 0;
  }

  return *this;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::
Tensor(DataPtrType data, const IndexT sizes[Dim])
    : data_(data) {
  static_assert(Dim > 0, "must have > 0 dimensions");

  for (int i = 0; i < Dim; ++i) {
    size_[i] = sizes[i];
  }

  stride_[Dim - 1] = (IndexT) 1;
  for (int i = Dim - 2; i >= 0; --i) {
    stride_[i] = stride_[i + 1] * sizes[i + 1];
  }
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::
Tensor(DataPtrType data, std::initializer_list<IndexT> sizes)
    : data_(data) {
  GPU_FAISS_ASSERT(sizes.size() == Dim);
  static_assert(Dim > 0, "must have > 0 dimensions");

  int i = 0;
  for (auto s : sizes) {
    size_[i++] = s;
  }

  stride_[Dim - 1] = (IndexT) 1;
  for (int j = Dim - 2; j >= 0; --j) {
    stride_[j] = stride_[j + 1] * size_[j + 1];
  }
}


template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::Tensor(
  DataPtrType data, const IndexT sizes[Dim], const IndexT strides[Dim])
    : data_(data) {
  static_assert(Dim > 0, "must have > 0 dimensions");

  for (int i = 0; i < Dim; ++i) {
    size_[i] = sizes[i];
    stride_[i] = strides[i];
  }
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ void
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::copyFrom(
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t,
  cudaStream_t stream) {
  // The tensor must be fully contiguous
  GPU_FAISS_ASSERT(this->isContiguous());

  // Size must be the same (since dimensions are checked and
  // continuity is assumed, we need only check total number of
  // elements
  GPU_FAISS_ASSERT(this->numElements() == t.numElements());

  if (t.numElements() > 0) {
    GPU_FAISS_ASSERT(this->data_);
    GPU_FAISS_ASSERT(t.data());

    int ourDev = getDeviceForAddress(this->data_);
    int tDev = getDeviceForAddress(t.data());

    if (tDev == -1) {
      CUDA_VERIFY(cudaMemcpyAsync(this->data_,
                                  t.data(),
                                  this->getSizeInBytes(),
                                  ourDev == -1 ? cudaMemcpyHostToHost :
                                  cudaMemcpyHostToDevice,
                                  stream));
    } else {
      CUDA_VERIFY(cudaMemcpyAsync(this->data_,
                                  t.data(),
                                  this->getSizeInBytes(),
                                  ourDev == -1 ? cudaMemcpyDeviceToHost :
                                  cudaMemcpyDeviceToDevice,
                                  stream));
    }
  }
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ void
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::copyTo(
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t,
  cudaStream_t stream) {
  // The tensor must be fully contiguous
  GPU_FAISS_ASSERT(this->isContiguous());

  // Size must be the same (since dimensions are checked and
  // continuity is assumed, we need only check total number of
  // elements
  GPU_FAISS_ASSERT(this->numElements() == t.numElements());

  if (t.numElements() > 0) {
    GPU_FAISS_ASSERT(this->data_);
    GPU_FAISS_ASSERT(t.data());

    int ourDev = getDeviceForAddress(this->data_);
    int tDev = getDeviceForAddress(t.data());

    if (tDev == -1) {
      CUDA_VERIFY(cudaMemcpyAsync(t.data(),
                                  this->data_,
                                  this->getSizeInBytes(),
                                  ourDev == -1 ? cudaMemcpyHostToHost :
                                  cudaMemcpyDeviceToHost,
                                  stream));
    } else {
      CUDA_VERIFY(cudaMemcpyAsync(t.data(),
                                  this->data_,
                                  this->getSizeInBytes(),
                                  ourDev == -1 ? cudaMemcpyHostToDevice :
                                  cudaMemcpyDeviceToDevice,
                                  stream));
    }
  }
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <typename OtherT, int OtherDim>
__host__ __device__ bool
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::isSame(
  const Tensor<OtherT, OtherDim, InnerContig, IndexT, PtrTraits>& rhs) const {
  if (Dim != OtherDim) {
    return false;
  }

  for (int i = 0; i < Dim; ++i) {
    if (this->getSize(i) != rhs.getSize(i)) {
      return false;
    }

    if (this->getStride(i) != rhs.getStride(i)) {
      return false;
    }
  }

  return true;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <typename OtherT, int OtherDim>
__host__ __device__ bool
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::isSameSize(
  const Tensor<OtherT, OtherDim, InnerContig, IndexT, PtrTraits>& rhs) const {
  if (Dim != OtherDim) {
    return false;
  }

  for (int i = 0; i < Dim; ++i) {
    if (this->getSize(i) != rhs.getSize(i)) {
      return false;
    }
  }

  return true;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <typename U>
__host__ __device__ Tensor<U, Dim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::cast() {
  static_assert(sizeof(U) == sizeof(T), "cast must be to same size object");

  return Tensor<U, Dim, InnerContig, IndexT, PtrTraits>(
    reinterpret_cast<U*>(data_), size_, stride_);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <typename U>
__host__ __device__ const Tensor<U, Dim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::cast() const {
  static_assert(sizeof(U) == sizeof(T), "cast must be to same size object");

  return Tensor<U, Dim, InnerContig, IndexT, PtrTraits>(
    reinterpret_cast<U*>(data_), size_, stride_);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <typename U>
__host__ __device__ Tensor<U, Dim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::castResize() {
  static_assert(sizeof(U) >= sizeof(T), "only handles greater sizes");
  constexpr int kMultiple = sizeof(U) / sizeof(T);

  GPU_FAISS_ASSERT(canCastResize<U>());

  IndexT newSize[Dim];
  IndexT newStride[Dim];

  for (int i = 0; i < Dim - 1; ++i) {
    newSize[i] = size_[i];
    newStride[i] = stride_[i] / kMultiple;
  }

  newStride[Dim - 1] = 1; // this is the same as the old stride
  newSize[Dim - 1] = size_[Dim - 1] / kMultiple;

  return Tensor<U, Dim, InnerContig, IndexT, PtrTraits>(
    reinterpret_cast<U*>(data_), newSize, newStride);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <typename U>
__host__ __device__ const Tensor<U, Dim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::castResize() const {
  return const_cast<Tensor<T, Dim, InnerContig, IndexT, PtrTraits>*>(this)->
    castResize<U>();
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <typename U>
__host__ __device__ bool
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::canCastResize() const {
  static_assert(sizeof(U) >= sizeof(T), "only handles greater sizes");
  constexpr int kMultiple = sizeof(U) / sizeof(T);

  // Ensure that the base pointer is sizeof(U) aligned
  if (((uintptr_t) data_) % sizeof(U) != 0) {
    return false;
  }

  // Check all outer strides
  for (int i = 0; i < Dim - 1; ++i) {
    if (stride_[i] % kMultiple != 0) {
      return false;
    }
  }

  // Check inner size
  if (size_[Dim - 1] % kMultiple != 0) {
    return false;
  }

  if (stride_[Dim - 1] != 1) {
    return false;
  }

  return true;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <typename NewIndexT>
__host__ Tensor<T, Dim, InnerContig, NewIndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::castIndexType() const {
  if (sizeof(NewIndexT) < sizeof(IndexT)) {
    GPU_FAISS_ASSERT(this->canUseIndexType<NewIndexT>());
  }

  NewIndexT newSize[Dim];
  NewIndexT newStride[Dim];
  for (int i = 0; i < Dim; ++i) {
    newSize[i] = (NewIndexT) size_[i];
    newStride[i] = (NewIndexT) stride_[i];
  }

  return Tensor<T, Dim, InnerContig, NewIndexT, PtrTraits>(
    data_, newSize, newStride);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <typename NewIndexT>
__host__ bool
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::canUseIndexType() const {
  static_assert(sizeof(size_t) >= sizeof(IndexT),
                "index size too large");
  static_assert(sizeof(size_t) >= sizeof(NewIndexT),
                "new index size too large");

  // Find maximum offset that can be calculated
  // FIXME: maybe also consider offset in bytes? multiply by sizeof(T)?
  size_t maxOffset = 0;

  for (int i = 0; i < Dim; ++i) {
    size_t curMaxOffset = (size_t) size_[i] * (size_t) stride_[i];
    if (curMaxOffset > maxOffset) {
      maxOffset = curMaxOffset;
    }
  }

  if (maxOffset > (size_t) std::numeric_limits<NewIndexT>::max()) {
    return false;
  }

  return true;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ size_t
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::numElements() const {
  size_t size = (size_t) getSize(0);

  for (int i = 1; i < Dim; ++i) {
    size *= (size_t) getSize(i);
  }

  return size;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ bool
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::isContiguous() const {
  long prevSize = 1;

  for (int i = Dim - 1; i >= 0; --i) {
    if (getSize(i) != (IndexT) 1) {
      if (getStride(i) == prevSize) {
        prevSize *= getSize(i);
      } else {
        return false;
      }
    }
  }

  return true;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ bool
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::isConsistentlySized(int i) const {
  if (i == 0 && getStride(i) > 0 && getSize(i) > 0) {
    return true;
  } else if ((i > 0) && (i < Dim) && (getStride(i) > 0) &&
             ((getStride(i - 1) / getStride(i)) >= getSize(i))) {
    return true;
  }

  return false;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ bool
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::isConsistentlySized() const {
  for (int i = 0; i < Dim; ++i) {
    if (!isConsistentlySized(i)) {
      return false;
    }
  }

  return true;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ bool
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::isContiguousDim(int i) const {
  return (i == Dim - 1) || // just in case
    ((i < Dim - 1) &&
     ((getStride(i) / getStride(i + 1)) == getSize(i + 1)));
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ Tensor<T, Dim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::transpose(int dim1,
                                                     int dim2) const {
  GPU_FAISS_ASSERT(dim1 >= 0 && dim1 < Dim);
  GPU_FAISS_ASSERT(dim1 >= 0 && dim2 < Dim);

  // If a tensor is innermost contiguous, one cannot transpose the innermost
  // dimension
  if (InnerContig) {
    GPU_FAISS_ASSERT(dim1 != Dim - 1 && dim2 != Dim - 1);
  }

  IndexT newSize[Dim];
  IndexT newStride[Dim];

  for (int i = 0; i < Dim; ++i) {
    newSize[i] = size_[i];
    newStride[i] = stride_[i];
  }

  IndexT tmp = newSize[dim1];
  newSize[dim1] = newSize[dim2];
  newSize[dim2] = tmp;

  tmp = newStride[dim1];
  newStride[dim1] = newStride[dim2];
  newStride[dim2] = tmp;

  return Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(data_, newSize, newStride);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <int NewDim>
__host__ __device__ Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::upcastOuter() {
  // Can only create tensors of greater dimension
  static_assert(NewDim > Dim, "Can only upcast to greater dim");

  IndexT newSize[NewDim];
  IndexT newStride[NewDim];

  int shift = NewDim - Dim;

  for (int i = 0; i < NewDim; ++i) {
    if (i < shift) {
      // These are the extended dimensions
      newSize[i] = (IndexT) 1;
      newStride[i] = size_[0] * stride_[0];
    } else {
      // Shift the remaining dimensions
      newSize[i] = size_[i - shift];
      newStride[i] = stride_[i - shift];
    }
  }

  return Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>(
    data_, newSize, newStride);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <int NewDim>
__host__ __device__ Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::upcastInner() {
  // Can only create tensors of greater dimension
  static_assert(NewDim > Dim, "Can only upcast to greater dim");

  IndexT newSize[NewDim];
  IndexT newStride[NewDim];

  for (int i = 0; i < NewDim; ++i) {
    if (i < Dim) {
      // Existing dimensions get copied over
      newSize[i] = size_[i];
      newStride[i] = stride_[i];
    } else {
      // Extended dimensions
      newSize[i] = (IndexT) 1;
      newStride[i] = (IndexT) 1;
    }
  }

  return Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>(
    data_, newSize, newStride);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <int NewDim>
__host__ __device__ Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::downcastOuter() {
  // Can only create tensors of lesser dimension
  static_assert(NewDim < Dim, "Can only downcast to lesser dim");

  // We can't downcast non-contiguous tensors, since it leaves
  // garbage data in the tensor. The tensor needs to be contiguous
  // in all of the dimensions we are collapsing (no padding in
  // them).
  for (int i = 0; i < Dim - NewDim; ++i) {
    bool cont = isContiguousDim(i);
    GPU_FAISS_ASSERT(cont);
  }

  IndexT newSize[NewDim];
  IndexT newStride[NewDim];

  int ignoredDims = Dim - NewDim;
  IndexT collapsedSize = 1;

  for (int i = 0; i < Dim; ++i) {
    if (i < ignoredDims) {
      // Collapse these dimensions
      collapsedSize *= getSize(i);
    } else {
      // Non-collapsed dimensions
      if (i == ignoredDims) {
        // This is the first non-collapsed dimension
        newSize[i - ignoredDims] = collapsedSize * getSize(i);
      } else {
        // Subsequent non-collapsed dimensions
        newSize[i - ignoredDims] = getSize(i);
      }

      newStride[i - ignoredDims] = getStride(i);
    }
  }

  return Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>(
    data_, newSize, newStride);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <int NewDim>
__host__ __device__ Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::downcastInner() {
  // Can only create tensors of lesser dimension
  static_assert(NewDim < Dim, "Can only downcast to lesser dim");

  // We can't downcast non-contiguous tensors, since it leaves
  // garbage data in the tensor. The tensor needs to be contiguous
  // in all of the dimensions we are collapsing (no padding in
  // them).
  for (int i = NewDim; i < Dim; ++i) {
    GPU_FAISS_ASSERT(isContiguousDim(i));
  }

  IndexT newSize[NewDim];
  IndexT newStride[NewDim];

  IndexT collapsedSize = 1;

  for (int i = Dim - 1; i >= 0; --i) {
    if (i >= NewDim) {
      // Collapse these dimensions
      collapsedSize *= getSize(i);
    } else {
      // Non-collapsed dimensions
      if (i == NewDim - 1) {
        // This is the first non-collapsed dimension
        newSize[i] = collapsedSize * getSize(i);
        newStride[i] = getStride(Dim - 1);
      } else {
        // Subsequent non-collapsed dimensions
        newSize[i] = getSize(i);
        newStride[i] = getStride(i);
      }
    }
  }

  return Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>(
    data_, newSize, newStride);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <int SubDim>
__host__ __device__ Tensor<T, SubDim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::view(DataPtrType at) {
  static_assert(SubDim >= 1 && SubDim < Dim,
                "can only create view of lesser dim");

  IndexT viewSizes[SubDim];
  IndexT viewStrides[SubDim];

  for (int i = 0; i < SubDim; ++i) {
    viewSizes[i] = size_[Dim - SubDim + i];
    viewStrides[i] = stride_[Dim - SubDim + i];
  }

  return Tensor<T, SubDim, InnerContig, IndexT, PtrTraits>(
    at, viewSizes, viewStrides);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <int SubDim>
__host__ __device__ Tensor<T, SubDim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::view() {
  return view<SubDim>(data_);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ Tensor<T, Dim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::narrowOutermost(IndexT start,
                                                                IndexT size) {
  return this->narrow(0, start, size);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ Tensor<T, Dim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::narrow(int dim,
                                                       IndexT start,
                                                       IndexT size) {
  DataPtrType newData = data_;

  GPU_FAISS_ASSERT(start >= 0 &&
                   start < size_[dim] &&
                   (start + size) <= size_[dim]);

  if (start > 0) {
    newData += (size_t) start * stride_[dim];
  }

  IndexT newSize[Dim];
  for (int i = 0; i < Dim; ++i) {
    if (i == dim) {
      GPU_FAISS_ASSERT(start + size <= size_[dim]);
      newSize[i] = size;
    } else {
      newSize[i] = size_[i];
    }
  }

  // If we were innermost contiguous before, we are still innermost contiguous
  return Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(newData, newSize, stride_);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
template <int NewDim>
__host__ __device__ Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>
Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::view(
  std::initializer_list<IndexT> sizes) {
  GPU_FAISS_ASSERT(this->isContiguous());

  GPU_FAISS_ASSERT(sizes.size() == NewDim);

  // The total size of the new view must be the same as the total size
  // of the old view
  size_t curSize = numElements();
  size_t newSize = 1;

  for (auto s : sizes) {
    newSize *= s;
  }

  GPU_FAISS_ASSERT(curSize == newSize);
  return Tensor<T, NewDim, true, IndexT, PtrTraits>(data(), sizes);
}

} } // namespace
