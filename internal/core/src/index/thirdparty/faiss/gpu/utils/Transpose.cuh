/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/impl/FaissAssert.h>
#include <faiss/gpu/utils/Tensor.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/StaticUtils.h>
#include <cuda.h>

namespace faiss { namespace gpu {

template <typename T, typename IndexT>
struct TensorInfo {
  static constexpr int kMaxDims = 8;

  T* data;
  IndexT sizes[kMaxDims];
  IndexT strides[kMaxDims];
  int dims;
};

template <typename T, typename IndexT, int Dim>
struct TensorInfoOffset {
  __device__ inline static unsigned int get(const TensorInfo<T, IndexT>& info,
                                            IndexT linearId) {
    IndexT offset = 0;

#pragma unroll
    for (int i = Dim - 1; i >= 0; --i) {
      IndexT curDimIndex = linearId % info.sizes[i];
      IndexT curDimOffset = curDimIndex * info.strides[i];

      offset += curDimOffset;

      if (i > 0) {
        linearId /= info.sizes[i];
      }
    }

    return offset;
  }
};

template <typename T, typename IndexT>
struct TensorInfoOffset<T, IndexT, -1> {
  __device__ inline static unsigned int get(const TensorInfo<T, IndexT>& info,
                                            IndexT linearId) {
    return linearId;
  }
};

template <typename T, typename IndexT, int Dim>
TensorInfo<T, IndexT> getTensorInfo(const Tensor<T, Dim, true>& t) {
  TensorInfo<T, IndexT> info;

  for (int i = 0; i < Dim; ++i) {
    info.sizes[i] = (IndexT) t.getSize(i);
    info.strides[i] = (IndexT) t.getStride(i);
  }

  info.data = t.data();
  info.dims = Dim;

  return info;
}

template <typename T, typename IndexT, int DimInput, int DimOutput>
__global__ void transposeAny(TensorInfo<T, IndexT> input,
                             TensorInfo<T, IndexT> output,
                             IndexT totalSize) {
  for (IndexT i = blockIdx.x * blockDim.x + threadIdx.x;
       i < totalSize;
       i += gridDim.x + blockDim.x) {
    auto inputOffset = TensorInfoOffset<T, IndexT, DimInput>::get(input, i);
    auto outputOffset = TensorInfoOffset<T, IndexT, DimOutput>::get(output, i);

#if __CUDA_ARCH__ >= 350
    output.data[outputOffset] = __ldg(&input.data[inputOffset]);
#else
    output.data[outputOffset] = input.data[inputOffset];
#endif
  }
}

/// Performs an out-of-place transposition between any two dimensions.
/// Best performance is if the transposed dimensions are not
/// innermost, since the reads and writes will be coalesced.
/// Could include a shared memory transposition if the dimensions
/// being transposed are innermost, but would require support for
/// arbitrary rectangular matrices.
/// This linearized implementation seems to perform well enough,
/// especially for cases that we care about (outer dimension
/// transpositions).
template <typename T, int Dim>
void runTransposeAny(Tensor<T, Dim, true>& in,
                     int dim1, int dim2,
                     Tensor<T, Dim, true>& out,
                     cudaStream_t stream) {
  static_assert(Dim <= TensorInfo<T, unsigned int>::kMaxDims,
                "too many dimensions");

  FAISS_ASSERT(dim1 != dim2);
  FAISS_ASSERT(dim1 < Dim && dim2 < Dim);

  int outSize[Dim];

  for (int i = 0; i < Dim; ++i) {
    outSize[i] = in.getSize(i);
  }

  std::swap(outSize[dim1], outSize[dim2]);

  for (int i = 0; i < Dim; ++i) {
    FAISS_ASSERT(out.getSize(i) == outSize[i]);
  }

  size_t totalSize = in.numElements();
  size_t block = std::min((size_t) getMaxThreadsCurrentDevice(), totalSize);

  if (totalSize <= (size_t) std::numeric_limits<int>::max()) {
    // div/mod seems faster with unsigned types
    auto inInfo = getTensorInfo<T, unsigned int, Dim>(in);
    auto outInfo = getTensorInfo<T, unsigned int, Dim>(out);

    std::swap(inInfo.sizes[dim1], inInfo.sizes[dim2]);
    std::swap(inInfo.strides[dim1], inInfo.strides[dim2]);

    auto grid = std::min(utils::divUp(totalSize, block), (size_t) 4096);

    transposeAny<T, unsigned int, Dim, -1>
      <<<grid, block, 0, stream>>>(inInfo, outInfo, totalSize);
  } else {
    auto inInfo = getTensorInfo<T, unsigned long, Dim>(in);
    auto outInfo = getTensorInfo<T, unsigned long, Dim>(out);

    std::swap(inInfo.sizes[dim1], inInfo.sizes[dim2]);
    std::swap(inInfo.strides[dim1], inInfo.strides[dim2]);

    auto grid = std::min(utils::divUp(totalSize, block), (size_t) 4096);

    transposeAny<T, unsigned long, Dim, -1>
      <<<grid, block, 0, stream>>>(inInfo, outInfo, totalSize);
  }
  CUDA_TEST_ERROR();
}

} } // namespace
