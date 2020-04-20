/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


namespace faiss { namespace gpu {

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::HostTensor() :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(),
    state_(AllocState::NotOwner) {
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::~HostTensor() {
  if (state_ == AllocState::Owner) {
    FAISS_ASSERT(this->data_ != nullptr);
    delete[] this->data_;
    this->data_ = nullptr;
  }
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::HostTensor(
  HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>&& t) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(),
    state_(AllocState::NotOwner) {
  this->operator=(std::move(t));
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>&
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::operator=(
  HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>&& t) {
  if (this->state_ == AllocState::Owner) {
    FAISS_ASSERT(this->data_ != nullptr);
    delete[] this->data_;
    this->data_ = nullptr;
  }

  this->Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::operator=(
    std::move(t));

  this->state_ = t.state_; t.state_ = AllocState::NotOwner;

  return *this;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::HostTensor(
  const IndexT sizes[Dim]) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(nullptr, sizes),
    state_(AllocState::Owner) {

  this->data_ = new T[this->numElements()];
  FAISS_ASSERT(this->data_ != nullptr);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::HostTensor(
  std::initializer_list<IndexT> sizes) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(nullptr, sizes),
    state_(AllocState::Owner) {
  this->data_ = new T[this->numElements()];
  FAISS_ASSERT(this->data_ != nullptr);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::HostTensor(
  DataPtrType data,
  const IndexT sizes[Dim]) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(data, sizes),
    state_(AllocState::NotOwner) {
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::HostTensor(
  DataPtrType data,
  std::initializer_list<IndexT> sizes) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(data, sizes),
    state_(AllocState::NotOwner) {
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::HostTensor(
  DataPtrType data,
  const IndexT sizes[Dim],
  const IndexT strides[Dim]) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(data, sizes, strides),
    state_(AllocState::NotOwner) {
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::HostTensor(
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t,
  cudaStream_t stream) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(nullptr, t.sizes(), t.strides()),
    state_(AllocState::Owner) {
  // Only contiguous arrays handled for now
  FAISS_ASSERT(t.isContiguous());

  this->data_ = new T[t.numElements()];
  this->copyFrom(t, stream);
}

/// Call to zero out memory
template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>&
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::zero() {
  // Region must be contiguous
  FAISS_ASSERT(this->isContiguous());

  if (this->data_ != nullptr) {
    memset(this->data_, 0, this->getSizeInBytes());
  }

  return *this;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ T
HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>::maxDiff(
  const HostTensor<T, Dim, InnerContig, IndexT, PtrTraits>& t) const {
  auto size = this->numElements();

  FAISS_ASSERT(size == t.numElements());
  FAISS_ASSERT(size > 0);

  if (InnerContig) {
    auto a = this->data();
    auto b = t.data();

    T maxDiff = a[0] - b[0];
    // FIXME: type-specific abs()
    maxDiff = maxDiff < 0 ? maxDiff * (T) -1 : maxDiff;

    for (IndexT i = 1; i < size; ++i) {
      auto diff = a[i] - b[i];
      // FIXME: type-specific abs
      diff = diff < 0 ? diff * (T) -1 : diff;
      if (diff > maxDiff) {
        maxDiff = diff;
      }
    }

    return maxDiff;
  } else {
    // non-contiguous
    // FIXME
    FAISS_ASSERT(false);
    return (T) 0;
  }
}

} } // namespace
