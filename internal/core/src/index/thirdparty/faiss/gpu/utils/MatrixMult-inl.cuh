/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cublas_v2.h>
#include <faiss/gpu/utils/Tensor.cuh>
#include <faiss/gpu/utils/DeviceTensor.cuh>
#include <faiss/gpu/utils/HostTensor.cuh>
#include <faiss/gpu/utils/Float16.cuh>

namespace faiss { namespace gpu {

class DeviceMemory;

template <typename T>
struct GetCudaType;

template <>
struct GetCudaType<float> {
  static constexpr cudaDataType_t Type = CUDA_R_32F;
};

template <>
struct GetCudaType<half> {
  static constexpr cudaDataType_t Type = CUDA_R_16F;
};

template <typename AT, typename BT>
cublasStatus_t
rawGemm(cublasHandle_t handle,
        cublasOperation_t transa,
        cublasOperation_t transb,
        int m,
        int n,
        int k,
        const float fAlpha,
        const AT *A,
        int lda,
        const BT *B,
        int ldb,
        const float fBeta,
        float *C,
        int ldc) {
  auto cAT = GetCudaType<AT>::Type;
  auto cBT = GetCudaType<BT>::Type;

  // Always accumulate in f32
  return cublasSgemmEx(handle, transa, transb, m, n, k,
                       &fAlpha, A, cAT, lda,
                       B, cBT, ldb,
                       &fBeta,
                       C, CUDA_R_32F, ldc);
}

template <typename AT, typename BT>
void
runMatrixMult(Tensor<float, 2, true>& c, bool transC,
              Tensor<AT, 2, true>& a, bool transA,
              Tensor<BT, 2, true>& b, bool transB,
              float alpha,
              float beta,
              cublasHandle_t handle,
              cudaStream_t stream) {
  cublasSetStream(handle, stream);

  // Check that we have (m x k) * (k x n) = (m x n)
  // using the input row-major layout
  int aM = transA ? a.getSize(1) : a.getSize(0);
  int aK = transA ? a.getSize(0) : a.getSize(1);

  int bK = transB ? b.getSize(1) : b.getSize(0);
  int bN = transB ? b.getSize(0) : b.getSize(1);

  int cM = transC ? c.getSize(1) : c.getSize(0);
  int cN = transC ? c.getSize(0) : c.getSize(1);

  FAISS_ASSERT(aM == cM);
  FAISS_ASSERT(aK == bK);
  FAISS_ASSERT(bN == cN);

  FAISS_ASSERT(a.getStride(1) == 1);
  FAISS_ASSERT(b.getStride(1) == 1);
  FAISS_ASSERT(c.getStride(1) == 1);

  // Now, we have to represent the matrix multiplication in
  // column-major layout
  float* pC = c.data();

  int m = c.getSize(1); // stride 1 size
  int n = c.getSize(0); // other size
  int k = transA ? a.getSize(0) : a.getSize(1);

  int lda = transC ? a.getStride(0) : b.getStride(0);
  int ldb = transC ? b.getStride(0) : a.getStride(0);
  int ldc = c.getStride(0);

  auto gemmTrA = transB ? CUBLAS_OP_T : CUBLAS_OP_N;
  auto gemmTrB = transA ? CUBLAS_OP_T : CUBLAS_OP_N;

  if (transC) {
    gemmTrA = transA ? CUBLAS_OP_N : CUBLAS_OP_T;
    gemmTrB = transB ? CUBLAS_OP_N : CUBLAS_OP_T;
  }

  cublasStatus_t err;

  if (transC) {
    err = rawGemm(handle,
                  gemmTrA, gemmTrB,
                  m, n, k, alpha,
                  a.data(), lda, b.data(), ldb, beta,
                  pC, ldc);
  } else {
    err = rawGemm(handle,
                  gemmTrA, gemmTrB,
                  m, n, k, alpha,
                  b.data(), lda, a.data(), ldb, beta,
                  pC, ldc);
  }

  FAISS_ASSERT_FMT(err == CUBLAS_STATUS_SUCCESS,
                   "cublas failed (%d): "
                   "(%d, %d)%s x (%d, %d)%s = (%d, %d)%s",
                   (int) err,
                   a.getSize(0), a.getSize(1), transA ? "'" : "",
                   b.getSize(0), b.getSize(1), transB ? "'" : "",
                   c.getSize(0), c.getSize(1), transC ? "'" : "");
  CUDA_TEST_ERROR();
}

template <typename AT, typename BT>
void runIteratedMatrixMult(Tensor<float, 3, true>& c, bool transC,
                           Tensor<AT, 3, true>& a, bool transA,
                           Tensor<BT, 3, true>& b, bool transB,
                           float alpha,
                           float beta,
                           cublasHandle_t handle,
                           cudaStream_t stream) {
  FAISS_ASSERT(c.getSize(0) == a.getSize(0));
  FAISS_ASSERT(a.getSize(0) == b.getSize(0));

  for (int i = 0; i < a.getSize(0); ++i) {
    auto cView = c[i].view();
    auto aView = a[i].view();
    auto bView = b[i].view();

    runMatrixMult(cView, transC,
                  aView, transA,
                  bView, transB,
                  alpha, beta, handle, stream);
  }
}

} } // namespace
