/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <assert.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <initializer_list>

/// Multi-dimensional array class for CUDA device and host usage.
/// Originally from Facebook's fbcunn, since added to the Torch GPU
/// library cutorch as well.

namespace faiss { namespace gpu {

/// Our tensor type
template <typename T,
          int Dim,
          bool InnerContig,
          typename IndexT,
          template <typename U> class PtrTraits>
class Tensor;

/// Type of a subspace of a tensor
namespace detail {
template <typename TensorType,
          int SubDim,
          template <typename U> class PtrTraits>
class SubTensor;
}

namespace traits {

template <typename T>
struct RestrictPtrTraits {
  typedef T* __restrict__ PtrType;
};

template <typename T>
struct DefaultPtrTraits {
  typedef T* PtrType;
};

}

/**
   Templated multi-dimensional array that supports strided access of
   elements. Main access is through `operator[]`; e.g.,
   `tensor[x][y][z]`.

   - `T` is the contained type (e.g., `float`)
   - `Dim` is the tensor rank
   - If `InnerContig` is true, then the tensor is assumed to be innermost
   - contiguous, and only operations that make sense on contiguous
   - arrays are allowed (e.g., no transpose). Strides are still
   - calculated, but innermost stride is assumed to be 1.
   - `IndexT` is the integer type used for size/stride arrays, and for
   - all indexing math. Default is `int`, but for large tensors, `long`
   - can be used instead.
   - `PtrTraits` are traits applied to our data pointer (T*). By default,
   - this is just T*, but RestrictPtrTraits can be used to apply T*
   - __restrict__ for alias-free analysis.
*/
template <typename T,
          int Dim,
          bool InnerContig = false,
          typename IndexT = int,
          template <typename U> class PtrTraits = traits::DefaultPtrTraits>
class Tensor {
 public:
  enum { NumDim = Dim };
  typedef T DataType;
  typedef IndexT IndexType;
  enum { IsInnerContig = InnerContig };
  typedef typename PtrTraits<T>::PtrType DataPtrType;
  typedef Tensor<T, Dim, InnerContig, IndexT, PtrTraits> TensorType;

  /// Default constructor
  __host__ __device__ Tensor();

  /// Copy constructor
  __host__ __device__ Tensor(Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t);

  /// Move constructor
  __host__ __device__ Tensor(Tensor<T, Dim, InnerContig, IndexT, PtrTraits>&& t);

  /// Assignment
  __host__ __device__ Tensor<T, Dim, InnerContig, IndexT, PtrTraits>&
  operator=(Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t);

  /// Move assignment
  __host__ __device__ Tensor<T, Dim, InnerContig, IndexT, PtrTraits>&
  operator=(Tensor<T, Dim, InnerContig, IndexT, PtrTraits>&& t);

  /// Constructor that calculates strides with no padding
  __host__ __device__ Tensor(DataPtrType data,
                             const IndexT sizes[Dim]);
  __host__ __device__ Tensor(DataPtrType data,
                             std::initializer_list<IndexT> sizes);

  /// Constructor that takes arbitrary size/stride arrays.
  /// Errors if you attempt to pass non-contiguous strides to a
  /// contiguous tensor.
  __host__ __device__ Tensor(DataPtrType data,
                             const IndexT sizes[Dim],
                             const IndexT strides[Dim]);

  /// Copies a tensor into ourselves; sizes must match
  __host__ void copyFrom(Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t,
                         cudaStream_t stream);

  /// Copies ourselves into a tensor; sizes must match
  __host__ void copyTo(Tensor<T, Dim, InnerContig, IndexT, PtrTraits>& t,
                       cudaStream_t stream);

  /// Returns true if the two tensors are of the same dimensionality,
  /// size and stride.
  template <typename OtherT, int OtherDim>
  __host__ __device__ bool
  isSame(const Tensor<OtherT, OtherDim, InnerContig, IndexT, PtrTraits>& rhs) const;

  /// Returns true if the two tensors are of the same dimensionality and size
  template <typename OtherT, int OtherDim>
  __host__ __device__ bool
  isSameSize(const Tensor<OtherT, OtherDim, InnerContig, IndexT, PtrTraits>& rhs) const;

  /// Cast to a tensor of a different type of the same size and
  /// stride. U and our type T must be of the same size
  template <typename U>
  __host__ __device__ Tensor<U, Dim, InnerContig, IndexT, PtrTraits> cast();

  /// Const version of `cast`
  template <typename U>
  __host__ __device__
  const Tensor<U, Dim, InnerContig, IndexT, PtrTraits> cast() const;

  /// Cast to a tensor of a different type which is potentially a
  /// different size than our type T. Tensor must be aligned and the
  /// innermost dimension must be a size that is a multiple of
  /// sizeof(U) / sizeof(T), and the stride of the innermost dimension
  /// must be contiguous. The stride of all outer dimensions must be a
  /// multiple of sizeof(U) / sizeof(T) as well.
  template <typename U>
  __host__ __device__ Tensor<U, Dim, InnerContig, IndexT, PtrTraits> castResize();

  /// Const version of `castResize`
  template <typename U>
  __host__ __device__ const Tensor<U, Dim, InnerContig, IndexT, PtrTraits>
  castResize() const;

  /// Returns true if we can castResize() this tensor to the new type
  template <typename U>
  __host__ __device__ bool canCastResize() const;

  /// Attempts to cast this tensor to a tensor of a different IndexT.
  /// Fails if size or stride entries are not representable in the new
  /// IndexT.
  template <typename NewIndexT>
  __host__ Tensor<T, Dim, InnerContig, NewIndexT, PtrTraits>
  castIndexType() const;

  /// Returns true if we can use this indexing type to access all elements
  /// index type
  template <typename NewIndexT>
  __host__ bool canUseIndexType() const;

  /// Returns a raw pointer to the start of our data.
  __host__ __device__ inline DataPtrType data() {
    return data_;
  }

  /// Returns a raw pointer to the end of our data, assuming
  /// continuity
  __host__ __device__ inline DataPtrType end() {
    return data() + numElements();
  }

  /// Returns a raw pointer to the start of our data (const).
  __host__ __device__ inline
  const DataPtrType data() const {
    return data_;
  }

  /// Returns a raw pointer to the end of our data, assuming
  /// continuity (const)
  __host__ __device__ inline DataPtrType end() const {
    return data() + numElements();
  }

  /// Cast to a different datatype
  template <typename U>
  __host__ __device__ inline
  typename PtrTraits<U>::PtrType dataAs() {
    return reinterpret_cast<typename PtrTraits<U>::PtrType>(data_);
  }

  /// Cast to a different datatype
  template <typename U>
  __host__ __device__ inline
  const typename PtrTraits<const U>::PtrType dataAs() const {
    return reinterpret_cast<typename PtrTraits<const U>::PtrType>(data_);
  }

  /// Returns a read/write view of a portion of our tensor.
  __host__ __device__ inline
  detail::SubTensor<TensorType, Dim - 1, PtrTraits>
    operator[](IndexT);

  /// Returns a read/write view of a portion of our tensor (const).
  __host__ __device__ inline
  const detail::SubTensor<TensorType, Dim - 1, PtrTraits>
    operator[](IndexT) const;

  /// Returns the size of a given dimension, `[0, Dim - 1]`. No bounds
  /// checking.
  __host__ __device__ inline IndexT getSize(int i) const {
    return size_[i];
  }

  /// Returns the stride of a given dimension, `[0, Dim - 1]`. No bounds
  /// checking.
  __host__ __device__ inline IndexT getStride(int i) const {
    return stride_[i];
  }

  /// Returns the total number of elements contained within our data
  /// (product of `getSize(i)`)
  __host__ __device__ size_t numElements() const;

  /// If we are contiguous, returns the total size in bytes of our
  /// data
  __host__ __device__ size_t getSizeInBytes() const {
    return numElements() * sizeof(T);
  }

  /// Returns the size array.
  __host__ __device__ inline const IndexT* sizes() const {
    return size_;
  }

  /// Returns the stride array.
  __host__ __device__ inline const IndexT* strides() const {
    return stride_;
  }

  /// Returns true if there is no padding within the tensor and no
  /// re-ordering of the dimensions.
  /// ~~~
  /// (stride(i) == size(i + 1) * stride(i + 1)) && stride(dim - 1) == 0
  /// ~~~
  __host__ __device__ bool isContiguous() const;

  /// Returns whether a given dimension has only increasing stride
  /// from the previous dimension. A tensor that was permuted by
  /// exchanging size and stride only will fail this check.
  /// If `i == 0` just check `size > 0`. Returns `false` if `stride` is `<= 0`.
  __host__ __device__ bool isConsistentlySized(int i) const;

  // Returns whether at each dimension `stride <= size`.
  // If this is not the case then iterating once over the size space will
  // touch the same memory locations multiple times.
  __host__ __device__ bool isConsistentlySized() const;

  /// Returns true if the given dimension index has no padding
  __host__ __device__ bool isContiguousDim(int i) const;

  /// Returns a tensor of the same dimension after transposing the two
  /// dimensions given. Does not actually move elements; transposition
  /// is made by permuting the size/stride arrays.
  /// If the dimensions are not valid, asserts.
  __host__ __device__ Tensor<T, Dim, InnerContig, IndexT, PtrTraits>
  transpose(int dim1, int dim2) const;

  /// Transpose a tensor, exchanging a non-innermost dimension with the
  /// innermost dimension, returning a no longer innermost contiguous tensor
  __host__ __device__ Tensor<T, Dim, false, IndexT, PtrTraits>
  transposeInnermost(int dim1) const;

  /// Upcast a tensor of dimension `D` to some tensor of dimension
  /// D' > D by padding the leading dimensions by 1
  /// e.g., upcasting a 2-d tensor `[2][3]` to a 4-d tensor `[1][1][2][3]`
  template <int NewDim>
  __host__ __device__ Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>
  upcastOuter();

  /// Upcast a tensor of dimension `D` to some tensor of dimension
  /// D' > D by padding the lowest/most varying dimensions by 1
  /// e.g., upcasting a 2-d tensor `[2][3]` to a 4-d tensor `[2][3][1][1]`
  template <int NewDim>
  __host__ __device__ Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>
  upcastInner();

  /// Downcast a tensor of dimension `D` to some tensor of dimension
  /// D' < D by collapsing the leading dimensions. asserts if there is
  /// padding on the leading dimensions.
  template <int NewDim>
  __host__ __device__
  Tensor<T, NewDim, InnerContig, IndexT, PtrTraits> downcastOuter();

  /// Downcast a tensor of dimension `D` to some tensor of dimension
  /// D' < D by collapsing the leading dimensions. asserts if there is
  /// padding on the leading dimensions.
  template <int NewDim>
  __host__ __device__
  Tensor<T, NewDim, InnerContig, IndexT, PtrTraits> downcastInner();

  /// Returns a tensor that is a view of the `SubDim`-dimensional slice
  /// of this tensor, starting at `at`.
  template <int SubDim>
  __host__ __device__ Tensor<T, SubDim, InnerContig, IndexT, PtrTraits>
  view(DataPtrType at);

  /// Returns a tensor that is a view of the `SubDim`-dimensional slice
  /// of this tensor, starting where our data begins
  template <int SubDim>
  __host__ __device__ Tensor<T, SubDim, InnerContig, IndexT, PtrTraits>
  view();

  /// Returns a tensor of the same dimension that is a view of the
  /// original tensor with the specified dimension restricted to the
  /// elements in the range [start, start + size)
  __host__ __device__ Tensor<T, Dim, InnerContig, IndexT, PtrTraits>
  narrowOutermost(IndexT start, IndexT size);

  /// Returns a tensor of the same dimension that is a view of the
  /// original tensor with the specified dimension restricted to the
  /// elements in the range [start, start + size).
  /// Can occur in an arbitrary dimension
  __host__ __device__ Tensor<T, Dim, InnerContig, IndexT, PtrTraits>
  narrow(int dim, IndexT start, IndexT size);

  /// Returns a view of the given tensor expressed as a tensor of a
  /// different number of dimensions.
  /// Only works if we are contiguous.
  template <int NewDim>
  __host__ __device__ Tensor<T, NewDim, InnerContig, IndexT, PtrTraits>
  view(std::initializer_list<IndexT> sizes);

  protected:
  /// Raw pointer to where the tensor data begins
  DataPtrType data_;

  /// Array of strides (in sizeof(T) terms) per each dimension
  IndexT stride_[Dim];

  /// Size per each dimension
  IndexT size_[Dim];
};

// Utilities for checking a collection of tensors
namespace detail {

template <typename IndexType>
bool canUseIndexType() {
  return true;
}

template <typename IndexType, typename T, typename... U>
bool canUseIndexType(const T& arg, const U&... args) {
  return arg.template canUseIndexType() &&
    canUseIndexType(args...);
}

} // namespace detail

template <typename IndexType, typename... T>
bool canUseIndexType(const T&... args) {
  return detail::canUseIndexType(args...);
}

namespace detail {

/// Specialization for a view of a single value (0-dimensional)
template <typename TensorType, template <typename U> class PtrTraits>
class SubTensor<TensorType, 0, PtrTraits> {
 public:
  __host__ __device__ SubTensor<TensorType, 0, PtrTraits>
  operator=(typename TensorType::DataType val) {
    *data_ = val;
    return *this;
  }

  // operator T&
  __host__ __device__ operator typename TensorType::DataType&() {
    return *data_;
  }

  // const operator T& returning const T&
  __host__ __device__ operator const typename TensorType::DataType&() const {
    return *data_;
  }

  // operator& returning T*
  __host__ __device__ typename TensorType::DataType* operator&() {
    return data_;
  }

  // const operator& returning const T*
  __host__ __device__ const typename TensorType::DataType* operator&() const {
    return data_;
  }

  /// Returns a raw accessor to our slice.
  __host__ __device__ inline typename TensorType::DataPtrType data() {
    return data_;
  }

  /// Returns a raw accessor to our slice (const).
  __host__ __device__ inline
  const typename TensorType::DataPtrType data() const {
    return data_;
  }

  /// Cast to a different datatype.
  template <typename T>
  __host__ __device__ T& as() {
    return *dataAs<T>();
  }

  /// Cast to a different datatype (const).
  template <typename T>
  __host__ __device__ const T& as() const {
    return *dataAs<T>();
  }

  /// Cast to a different datatype
  template <typename T>
  __host__ __device__ inline
  typename PtrTraits<T>::PtrType dataAs() {
    return reinterpret_cast<typename PtrTraits<T>::PtrType>(data_);
  }

  /// Cast to a different datatype (const)
  template <typename T>
  __host__ __device__ inline
  typename PtrTraits<const T>::PtrType dataAs() const {
    return reinterpret_cast<typename PtrTraits<const T>::PtrType>(data_);
  }

  /// Use the texture cache for reads
  __device__ inline typename TensorType::DataType ldg() const {
#if __CUDA_ARCH__ >= 350
    return __ldg(data_);
#else
    return *data_;
#endif
  }

  /// Use the texture cache for reads; cast as a particular type
  template <typename T>
  __device__ inline T ldgAs() const {
#if __CUDA_ARCH__ >= 350
    return __ldg(dataAs<T>());
#else
    return as<T>();
#endif
  }

 protected:
  /// One dimension greater can create us
  friend class SubTensor<TensorType, 1, PtrTraits>;

  /// Our parent tensor can create us
  friend class Tensor<typename TensorType::DataType,
                      1,
                      TensorType::IsInnerContig,
                      typename TensorType::IndexType,
                      PtrTraits>;

  __host__ __device__ inline SubTensor(
    TensorType& t,
    typename TensorType::DataPtrType data)
      : tensor_(t),
        data_(data) {
  }

  /// The tensor we're referencing
  TensorType& tensor_;

  /// Where our value is located
  typename TensorType::DataPtrType const data_;
};

/// A `SubDim`-rank slice of a parent Tensor
template <typename TensorType,
          int SubDim,
          template <typename U> class PtrTraits>
class SubTensor {
 public:
  /// Returns a view of the data located at our offset (the dimension
  /// `SubDim` - 1 tensor).
  __host__ __device__ inline
  SubTensor<TensorType, SubDim - 1, PtrTraits>
    operator[](typename TensorType::IndexType index) {
    if (TensorType::IsInnerContig && SubDim == 1) {
      // Innermost dimension is stride 1 for contiguous arrays
      return SubTensor<TensorType, SubDim - 1, PtrTraits>(
        tensor_, data_ + index);
    } else {
      return SubTensor<TensorType, SubDim - 1, PtrTraits>(
        tensor_,
        data_ + index * tensor_.getStride(TensorType::NumDim - SubDim));
    }
  }

  /// Returns a view of the data located at our offset (the dimension
  /// `SubDim` - 1 tensor) (const).
  __host__ __device__ inline
  const SubTensor<TensorType, SubDim - 1, PtrTraits>
    operator[](typename TensorType::IndexType index) const {
    if (TensorType::IsInnerContig && SubDim == 1) {
      // Innermost dimension is stride 1 for contiguous arrays
      return SubTensor<TensorType, SubDim - 1, PtrTraits>(
        tensor_, data_ + index);
    } else {
      return SubTensor<TensorType, SubDim - 1, PtrTraits>(
        tensor_,
        data_ + index * tensor_.getStride(TensorType::NumDim - SubDim));
    }
  }

  // operator& returning T*
  __host__ __device__ typename TensorType::DataType* operator&() {
    return data_;
  }

  // const operator& returning const T*
  __host__ __device__ const typename TensorType::DataType* operator&() const {
    return data_;
  }

  /// Returns a raw accessor to our slice.
  __host__ __device__ inline typename TensorType::DataPtrType data() {
    return data_;
  }

  /// Returns a raw accessor to our slice (const).
  __host__ __device__ inline
  const typename TensorType::DataPtrType data() const {
    return data_;
  }

  /// Cast to a different datatype.
  template <typename T>
  __host__ __device__ T& as() {
    return *dataAs<T>();
  }

  /// Cast to a different datatype (const).
  template <typename T>
  __host__ __device__ const T& as() const {
    return *dataAs<T>();
  }

  /// Cast to a different datatype
  template <typename T>
  __host__ __device__ inline
  typename PtrTraits<T>::PtrType dataAs() {
    return reinterpret_cast<typename PtrTraits<T>::PtrType>(data_);
  }

  /// Cast to a different datatype (const)
  template <typename T>
  __host__ __device__ inline
  typename PtrTraits<const T>::PtrType dataAs() const {
    return reinterpret_cast<typename PtrTraits<const T>::PtrType>(data_);
  }

  /// Use the texture cache for reads
  __device__ inline typename TensorType::DataType ldg() const {
#if __CUDA_ARCH__ >= 350
    return __ldg(data_);
#else
    return *data_;
#endif
  }

  /// Use the texture cache for reads; cast as a particular type
  template <typename T>
  __device__ inline T ldgAs() const {
#if __CUDA_ARCH__ >= 350
    return __ldg(dataAs<T>());
#else
    return as<T>();
#endif
  }

  /// Returns a tensor that is a view of the SubDim-dimensional slice
  /// of this tensor, starting where our data begins
  Tensor<typename TensorType::DataType,
         SubDim,
         TensorType::IsInnerContig,
         typename TensorType::IndexType,
         PtrTraits> view() {
    return tensor_.template view<SubDim>(data_);
  }

 protected:
  /// One dimension greater can create us
  friend class SubTensor<TensorType, SubDim + 1, PtrTraits>;

  /// Our parent tensor can create us
  friend class
  Tensor<typename TensorType::DataType,
         TensorType::NumDim,
         TensorType::IsInnerContig,
         typename TensorType::IndexType,
         PtrTraits>;

  __host__ __device__ inline SubTensor(
    TensorType& t,
    typename TensorType::DataPtrType data)
      : tensor_(t),
        data_(data) {
  }

  /// The tensor we're referencing
  TensorType& tensor_;

  /// The start of our sub-region
  typename TensorType::DataPtrType const data_;
};

} // namespace detail

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ inline
detail::SubTensor<Tensor<T, Dim, InnerContig, IndexT, PtrTraits>,
                  Dim - 1, PtrTraits>
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::operator[](IndexT index) {
  return detail::SubTensor<TensorType, Dim - 1, PtrTraits>(
    detail::SubTensor<TensorType, Dim, PtrTraits>(
      *this, data_)[index]);
}

template <typename T, int Dim, bool InnerContig,
          typename IndexT, template <typename U> class PtrTraits>
__host__ __device__ inline
const detail::SubTensor<Tensor<T, Dim, InnerContig, IndexT, PtrTraits>,
                        Dim - 1, PtrTraits>
  Tensor<T, Dim, InnerContig, IndexT, PtrTraits>::operator[](IndexT index) const {
  return detail::SubTensor<TensorType, Dim - 1, PtrTraits>(
    detail::SubTensor<TensorType, Dim, PtrTraits>(
      const_cast<TensorType&>(*this), data_)[index]);
}

} } // namespace

#include <faiss/gpu/utils/Tensor-inl.cuh>
