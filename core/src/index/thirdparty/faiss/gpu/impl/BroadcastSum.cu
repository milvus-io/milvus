/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <algorithm>
#include <faiss/impl/FaissAssert.h>

#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/MathOperators.cuh>
#include <faiss/gpu/utils/Tensor.cuh>
#include <faiss/gpu/utils/StaticUtils.h>

namespace faiss { namespace gpu {

template <typename T, int kRowsPerBlock, int kRowUnroll, int kColLoad>
__global__ void sumAlongColumns(Tensor<T, 1, true> input,
                                Tensor<T, 2, true> output) {
  static_assert(kRowsPerBlock % kRowUnroll == 0, "must fit rows");

  // blockIdx.x: which chunk of rows we are responsible for updating
  // blockIdx.y: which chunk of columns we are responsible for
  // updating
  int rowStart = blockIdx.x * kRowsPerBlock;
  int rowEnd = rowStart + kRowsPerBlock;
  int colStart = blockIdx.y * blockDim.x * kColLoad;

  // FIXME: if we have exact multiples, don't need this
  bool endRow = (blockIdx.x == gridDim.x - 1);
  bool endCol = (blockIdx.y == gridDim.y - 1);

  if (endRow) {
    if (output.getSize(0) % kRowsPerBlock == 0) {
      endRow = false;
    }
  }

  if (endCol) {
    for (int col = colStart + threadIdx.x;
         col < input.getSize(0); col += blockDim.x) {
      T val = input[col];

      if (endRow) {
        for (int row = rowStart; row < output.getSize(0); ++row) {
          T out = output[row][col];
          out = Math<T>::add(out, val);
          output[row][col] = out;
        }
      } else {
        T rows[kRowUnroll];

        for (int row = rowStart; row < rowEnd; row += kRowUnroll) {
#pragma unroll
          for (int i = 0; i < kRowUnroll; ++i) {
            rows[i] = output[row + i][col];
          }

#pragma unroll
          for (int i = 0; i < kRowUnroll; ++i) {
            rows[i] = Math<T>::add(rows[i], val);
          }

#pragma unroll
          for (int i = 0; i < kRowUnroll; ++i) {
            output[row + i][col] = rows[i];
          }
        }
      }
    }
  } else {
    int col = colStart + threadIdx.x;

    T val[kColLoad];

#pragma unroll
    for (int i = 0; i < kColLoad; ++i) {
      val[i] = input[col + i * blockDim.x];
    }

    if (endRow) {
      for (int row = rowStart; row < output.getSize(0); ++row) {
#pragma unroll
        for (int i = 0; i < kColLoad; ++i) {
          T out = output[row][col + i * blockDim.x];
          out = Math<T>::add(out, val[i]);
          output[row][col + i * blockDim.x] = out;
        }
      }
    } else {
      T rows[kRowUnroll * kColLoad];

      for (int row = rowStart; row < rowEnd; row += kRowUnroll) {
#pragma unroll
        for (int i = 0; i < kRowUnroll; ++i) {
#pragma unroll
          for (int j = 0; j < kColLoad; ++j) {
            rows[i * kColLoad + j] =
              output[row + i][col + j * blockDim.x];
          }
        }

#pragma unroll
        for (int i = 0; i < kRowUnroll; ++i) {
#pragma unroll
          for (int j = 0; j < kColLoad; ++j) {
            rows[i * kColLoad + j] =
              Math<T>::add(rows[i * kColLoad + j], val[j]);
          }
        }

#pragma unroll
        for (int i = 0; i < kRowUnroll; ++i) {
#pragma unroll
          for (int j = 0; j < kColLoad; ++j) {
            output[row + i][col + j * blockDim.x] =
              rows[i * kColLoad + j];
          }
        }
      }
    }
  }
}

template <typename T, int kRowsPerBlock, int kRowUnroll, int kColLoad>
__global__ void assignAlongColumns(Tensor<T, 1, true> input,
                                   Tensor<T, 2, true> output) {
  static_assert(kRowsPerBlock % kRowUnroll == 0, "must fit rows");

  // blockIdx.x: which chunk of rows we are responsible for updating
  // blockIdx.y: which chunk of columns we are responsible for
  // updating
  int rowStart = blockIdx.x * kRowsPerBlock;
  int rowEnd = rowStart + kRowsPerBlock;
  int colStart = blockIdx.y * blockDim.x * kColLoad;

  // FIXME: if we have exact multiples, don't need this
  bool endRow = (blockIdx.x == gridDim.x - 1);
  bool endCol = (blockIdx.y == gridDim.y - 1);

  if (endRow) {
    if (output.getSize(0) % kRowsPerBlock == 0) {
      endRow = false;
    }
  }

  if (endCol) {
    for (int col = colStart + threadIdx.x;
         col < input.getSize(0); col += blockDim.x) {
      T val = input[col];

      if (endRow) {
        for (int row = rowStart; row < output.getSize(0); ++row) {
          output[row][col] = val;
        }
      } else {
        for (int row = rowStart; row < rowEnd; row += kRowUnroll) {
#pragma unroll
          for (int i = 0; i < kRowUnroll; ++i) {
            output[row + i][col] = val;
          }
        }
      }
    }
  } else {
    int col = colStart + threadIdx.x;

    T val[kColLoad];

#pragma unroll
    for (int i = 0; i < kColLoad; ++i) {
      val[i] = input[col + i * blockDim.x];
    }

    if (endRow) {
      for (int row = rowStart; row < output.getSize(0); ++row) {
#pragma unroll
        for (int i = 0; i < kColLoad; ++i) {
          output[row][col + i * blockDim.x] = val[i];
        }
      }
    } else {
      for (int row = rowStart; row < rowEnd; row += kRowUnroll) {
#pragma unroll
        for (int i = 0; i < kRowUnroll; ++i) {
#pragma unroll
          for (int j = 0; j < kColLoad; ++j) {
            output[row + i][col + j * blockDim.x] = val[j];
          }
        }
      }
    }
  }
}

template <typename T, bool ZeroClamp>
__global__ void sumAlongRows(Tensor<T, 1, true> input,
                             Tensor<T, 2, true> output) {
  __shared__ T sval;

  int row = blockIdx.x;

  if (threadIdx.x == 0) {
    sval = input[row];
  }

  __syncthreads();

  T val = sval;

  // FIXME: speed up
  for (int i = threadIdx.x; i < output.getSize(1); i += blockDim.x) {
    T out = output[row][i];
    out = Math<T>::add(out, val);
    out = Math<T>::lt(out, Math<T>::zero()) ? Math<T>::zero() : out;

    output[row][i] = out;
  }
}

template <typename T, typename TVec>
void runSumAlongColumns(Tensor<T, 1, true>& input,
                        Tensor<T, 2, true>& output,
                        cudaStream_t stream) {
  FAISS_ASSERT(input.getSize(0) == output.getSize(1));

  int threadsPerBlock = 256;
  constexpr int kRowUnroll = 4;
  constexpr int kRowsPerBlock = kRowUnroll * 4;
  constexpr int kColLoad = 4;

  auto block = dim3(threadsPerBlock);

  if (input.template canCastResize<TVec>() &&
      output.template canCastResize<TVec>()) {
    auto inputV = input.template castResize<TVec>();
    auto outputV = output.template castResize<TVec>();

    auto grid =
      dim3(utils::divUp(outputV.getSize(0), kRowsPerBlock),
           utils::divUp(outputV.getSize(1), threadsPerBlock * kColLoad));

    sumAlongColumns<TVec, kRowsPerBlock, kRowUnroll, kColLoad>
      <<<grid, block, 0, stream>>>(inputV, outputV);
  } else {
    auto grid =
      dim3(utils::divUp(output.getSize(0), kRowsPerBlock),
           utils::divUp(output.getSize(1), threadsPerBlock * kColLoad));

    sumAlongColumns<T, kRowsPerBlock, kRowUnroll, kColLoad>
      <<<grid, block, 0, stream>>>(input, output);
  }

  CUDA_TEST_ERROR();
}

void runSumAlongColumns(Tensor<float, 1, true>& input,
                        Tensor<float, 2, true>& output,
                        cudaStream_t stream) {
  runSumAlongColumns<float, float4>(input, output, stream);
}

void runSumAlongColumns(Tensor<half, 1, true>& input,
                        Tensor<half, 2, true>& output,
                        cudaStream_t stream) {
  runSumAlongColumns<half, half2>(input, output, stream);
}

template <typename T, typename TVec>
void runAssignAlongColumns(Tensor<T, 1, true>& input,
                           Tensor<T, 2, true>& output,
                           cudaStream_t stream) {
  FAISS_ASSERT(input.getSize(0) == output.getSize(1));

  int threadsPerBlock = 256;
  constexpr int kRowUnroll = 4;
  constexpr int kRowsPerBlock = kRowUnroll * 4;
  constexpr int kColLoad = 4;

  auto block = dim3(threadsPerBlock);

  if (input.template canCastResize<TVec>() &&
      output.template canCastResize<TVec>()) {
    auto inputV = input.template castResize<TVec>();
    auto outputV = output.template castResize<TVec>();

    auto grid =
      dim3(utils::divUp(outputV.getSize(0), kRowsPerBlock),
           utils::divUp(outputV.getSize(1), threadsPerBlock * kColLoad));

    assignAlongColumns<TVec, kRowsPerBlock, kRowUnroll, kColLoad>
      <<<grid, block, 0, stream>>>(inputV, outputV);
  } else {
    auto grid =
      dim3(utils::divUp(output.getSize(0), kRowsPerBlock),
           utils::divUp(output.getSize(1), threadsPerBlock * kColLoad));

    assignAlongColumns<T, kRowsPerBlock, kRowUnroll, kColLoad>
      <<<grid, block, 0, stream>>>(input, output);
  }

  CUDA_TEST_ERROR();
}

void runAssignAlongColumns(Tensor<float, 1, true>& input,
                           Tensor<float, 2, true>& output,
                           cudaStream_t stream) {
  runAssignAlongColumns<float, float4>(input, output, stream);
}

void runAssignAlongColumns(Tensor<half, 1, true>& input,
                           Tensor<half, 2, true>& output,
                           cudaStream_t stream) {
  runAssignAlongColumns<half, half2>(input, output, stream);
}

template <typename T>
void runSumAlongRows(Tensor<T, 1, true>& input,
                     Tensor<T, 2, true>& output,
                     bool zeroClamp,
                     cudaStream_t stream) {
  FAISS_ASSERT(input.getSize(0) == output.getSize(0));

  int threadsPerBlock =
    std::min(output.getSize(1), getMaxThreadsCurrentDevice());
  auto grid = dim3(output.getSize(0));
  auto block = dim3(threadsPerBlock);

  if (zeroClamp) {
    sumAlongRows<T, true><<<grid, block, 0, stream>>>(input, output);
  } else {
    sumAlongRows<T, false><<<grid, block, 0, stream>>>(input, output);
  }

  CUDA_TEST_ERROR();
}

void runSumAlongRows(Tensor<float, 1, true>& input,
                     Tensor<float, 2, true>& output,
                     bool zeroClamp,
                     cudaStream_t stream) {
  runSumAlongRows<float>(input, output, zeroClamp, stream);
}

void runSumAlongRows(Tensor<half, 1, true>& input,
                     Tensor<half, 2, true>& output,
                     bool zeroClamp,
                     cudaStream_t stream) {
  runSumAlongRows<half>(input, output, zeroClamp, stream);
}

} } // namespace
