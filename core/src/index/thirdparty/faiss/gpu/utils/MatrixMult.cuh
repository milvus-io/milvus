/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cublas_v2.h>
#include <faiss/gpu/utils/Tensor.cuh>

namespace faiss { namespace gpu {

class DeviceMemory;

/// C = alpha * A * B + beta * C
/// Expects row major layout, not fortran/blas column major!
void runMatrixMult(Tensor<float, 2, true>& c, bool transC,
                   Tensor<float, 2, true>& a, bool transA,
                   Tensor<float, 2, true>& b, bool transB,
                   float alpha,
                   float beta,
                   bool useHgemm, // ignored for float32
                   cublasHandle_t handle,
                   cudaStream_t stream);

/// C = alpha * A * B + beta * C
/// Expects row major layout, not fortran/blas column major!
void runMatrixMult(Tensor<half, 2, true>& c, bool transC,
                   Tensor<half, 2, true>& a, bool transA,
                   Tensor<half, 2, true>& b, bool transB,
                   float alpha,
                   float beta,
                   bool useHgemm,
                   cublasHandle_t handle,
                   cudaStream_t stream);

/// C_i = alpha * A_i * B_i + beta * C_i
/// where `i` is the outermost dimension, via iterated gemm
/// Expects row major layout, not fortran/blas column major!
void runIteratedMatrixMult(Tensor<float, 3, true>& c, bool transC,
                           Tensor<float, 3, true>& a, bool transA,
                           Tensor<float, 3, true>& b, bool transB,
                           float alpha,
                           float beta,
                           cublasHandle_t handle,
                           cudaStream_t stream);

/// C_i = alpha * A_i * B_i + beta * C_i
/// where `i` is the outermost dimension, via batched gemm
/// Expects row major layout, not fortran/blas column major!
void runBatchMatrixMult(Tensor<float, 3, true>& c, bool transC,
                        Tensor<float, 3, true>& a, bool transA,
                        Tensor<float, 3, true>& b, bool transB,
                        float alpha,
                        float beta,
                        DeviceMemory& mem,
                        cublasHandle_t handle,
                        cudaStream_t stream);

} } // namespace
