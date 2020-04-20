/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <utility> // std::move

namespace faiss { namespace gpu {

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor() :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(),
    state_(AllocState::NotOwner),
    space_(MemorySpace::Device) {
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>&& t) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(),
    state_(AllocState::NotOwner),
    space_(MemorySpace::Device) {
  this->operator=(std::move(t));
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>&
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::operator=(
  DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>&& t) {
  if (this->state_ == AllocState::Owner) {
    CUDA_VERIFY(cudaFree(this->data_));
  }

  this->Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::operator=(
    std::move(t));

  this->state_ = t.state_; t.state_ = AllocState::NotOwner;
  this->space_ = t.space_;
  this->reservation_ = std::move(t.reservation_);

  return *this;
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::~DeviceTensor() {
  if (state_ == AllocState::Owner) {
    FAISS_ASSERT(this->data_ || (this->getSizeInBytes() == 0));
    CUDA_VERIFY(cudaFree(this->data_));
    this->data_ = nullptr;
  }

  // Otherwise, if we have a temporary memory reservation, then its
  // destructor will return the reservation
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  const IndexT sizes[Dim],
  MemorySpace space) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(nullptr, sizes),
    state_(AllocState::Owner),
    space_(space) {

  allocMemorySpace(space, &this->data_, this->getSizeInBytes());
  FAISS_ASSERT(this->data_ || (this->getSizeInBytes() == 0));
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  std::initializer_list<IndexT> sizes,
  MemorySpace space) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(nullptr, sizes),
    state_(AllocState::Owner),
    space_(space) {

  allocMemorySpace(space, &this->data_, this->getSizeInBytes());
  FAISS_ASSERT(this->data_ || (this->getSizeInBytes() == 0));
}

// memory reservation constructor
template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  DeviceMemory& m,
  const IndexT sizes[Dim],
  cudaStream_t stream,
  MemorySpace space) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(nullptr, sizes),
    state_(AllocState::Reservation),
    space_(space) {

  // FIXME: add MemorySpace to DeviceMemory
  auto memory = m.getMemory(stream, this->getSizeInBytes());

  this->data_ = (T*) memory.get();
  FAISS_ASSERT(this->data_ || (this->getSizeInBytes() == 0));
  reservation_ = std::move(memory);
}

// memory reservation constructor
template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  DeviceMemory& m,
  std::initializer_list<IndexT> sizes,
  cudaStream_t stream,
  MemorySpace space) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(nullptr, sizes),
    state_(AllocState::Reservation),
    space_(space) {

  // FIXME: add MemorySpace to DeviceMemory
  auto memory = m.getMemory(stream, this->getSizeInBytes());

  this->data_ = (T*) memory.get();
  FAISS_ASSERT(this->data_ || (this->getSizeInBytes() == 0));
  reservation_ = std::move(memory);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  DataPtrType data,
  const IndexT sizes[Dim],
  MemorySpace space) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(data, sizes),
    state_(AllocState::NotOwner),
    space_(space) {
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  DataPtrType data,
  std::initializer_list<IndexT> sizes,
  MemorySpace space) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(data, sizes),
    state_(AllocState::NotOwner),
    space_(space) {
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  DataPtrType data,
  const IndexT sizes[Dim],
  const IndexT strides[Dim],
  MemorySpace space) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(data, sizes, strides),
    state_(AllocState::NotOwner),
    space_(space) {
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t,
  cudaStream_t stream,
  MemorySpace space) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(nullptr, t.sizes(), t.strides()),
    state_(AllocState::Owner),
    space_(space) {

  allocMemorySpace(space_, &this->data_, this->getSizeInBytes());
  FAISS_ASSERT(this->data_ || (this->getSizeInBytes() == 0));
  this->copyFrom(t, stream);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::DeviceTensor(
  DeviceMemory& m,
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t,
  cudaStream_t stream,
  MemorySpace space) :
    Tensor<T, Dim, InnerContig, IndexT, PtrTraits>(nullptr, t.sizes(), t.strides()),
    state_(AllocState::Reservation),
    space_(space) {

  // FIXME: add MemorySpace to DeviceMemory
  auto memory = m.getMemory(stream, this->getSizeInBytes());

  this->data_ = (T*) memory.get();
  FAISS_ASSERT(this->data_ || (this->getSizeInBytes() == 0));
  reservation_ = std::move(memory);

  this->copyFrom(t, stream);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>&
DeviceTensor<T, Dim, InnerContig, IndexT, PtrTraits>::zero(
  cudaStream_t stream) {
  if (this->data_) {
    // Region must be contiguous
    FAISS_ASSERT(this->isContiguous());

    CUDA_VERIFY(cudaMemsetAsync(
                  this->data_, 0, this->getSizeInBytes(), stream));
  }

  return *this;
}

} } // namespace
