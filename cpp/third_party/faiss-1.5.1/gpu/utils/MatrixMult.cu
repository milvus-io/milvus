/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "MatrixMult.cuh"
#include "DeviceMemory.h"
#include "DeviceUtils.h" // CUDA_VERIFY
#include "DeviceTensor.cuh"
#include "HostTensor.cuh"

namespace faiss { namespace gpu {

template <typename T>
struct CublasGemm {
};

template <>
struct CublasGemm<float> {
  static cublasStatus_t gemm(cublasHandle_t handle,
                             cublasOperation_t transa,
                             cublasOperation_t transb,
                             int m,
                             int n,
                             int k,
                             float fAlpha,
                             const float *A,
                             int lda,
                             const float *B,
                             int ldb,
                             float fBeta,
                             float *C,
                             int ldc,
                             bool useHgemm) {
    return cublasSgemm(handle, transa, transb, m, n, k,
                       &fAlpha, A, lda, B, ldb, &fBeta, C, ldc);
  }
};

#ifdef FAISS_USE_FLOAT16
template <>
struct CublasGemm<half> {
  static cublasStatus_t gemm(cublasHandle_t handle,
                             cublasOperation_t transa,
                             cublasOperation_t transb,
                             int m,
                             int n,
                             int k,
                             const float fAlpha,
                             const half *A,
                             int lda,
                             const half *B,
                             int ldb,
                             const float fBeta,
                             half *C,
                             int ldc,
                             bool useHgemm) {
    if (getDeviceSupportsFloat16Math(getCurrentDevice()) && useHgemm) {
      half hAlpha = hostFloat2Half(fAlpha);
      half hBeta = hostFloat2Half(fBeta);

      return cublasHgemm(handle, transa, transb, m, n, k,
                         &hAlpha, A, lda, B, ldb, &hBeta, C, ldc);
    }

    // CUDA 8.0 changes the half datatype specifier
#if CUDA_VERSION == 7050
    auto halfType = CUBLAS_DATA_HALF;
#else
    auto halfType = CUDA_R_16F;
#endif // CUDA_VERSION

    return cublasSgemmEx(handle, transa, transb, m, n, k,
                         &fAlpha, A, halfType, lda,
                         B, halfType, ldb,
                         &fBeta,
                         C, halfType, ldc);
  }
};
#endif // FAISS_USE_FLOAT16


template <typename T>
void
runMatrixMult(Tensor<T, 2, true>& c, bool transC,
              Tensor<T, 2, true>& a, bool transA,
              Tensor<T, 2, true>& b, bool transB,
              float alpha,
              float beta,
              bool useHgemm,
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
  T* pA = transC ? a.data() : b.data();
  T* pB = transC ? b.data() : a.data();
  T* pC = c.data();

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

  auto err = CublasGemm<T>::gemm(handle,
                                 gemmTrA, gemmTrB,
                                 m, n, k, alpha,
                                 pA, lda, pB, ldb, beta,
                                 pC, ldc, useHgemm);

  FAISS_ASSERT_FMT(err == CUBLAS_STATUS_SUCCESS,
                   "cublas failed (%d): %s "
                   "(%d, %d)%s x (%d, %d)%s = (%d, %d)%s",
                   (int) err,
                   useHgemm ? "Hgemm" : "Sgemm",
                   a.getSize(0), a.getSize(1), transA ? "'" : "",
                   b.getSize(0), b.getSize(1), transB ? "'" : "",
                   c.getSize(0), c.getSize(1), transC ? "'" : "");
  CUDA_TEST_ERROR();
}

void runMatrixMult(Tensor<float, 2, true>& c, bool transC,
                   Tensor<float, 2, true>& a, bool transA,
                   Tensor<float, 2, true>& b, bool transB,
                   float alpha,
                   float beta,
                   bool useHgemm,
                   cublasHandle_t handle,
                   cudaStream_t stream) {
  return runMatrixMult<float>(c, transC, a, transA, b, transB,
                              alpha, beta, useHgemm, handle, stream);
}

#ifdef FAISS_USE_FLOAT16
void runMatrixMult(Tensor<half, 2, true>& c, bool transC,
                   Tensor<half, 2, true>& a, bool transA,
                   Tensor<half, 2, true>& b, bool transB,
                   float alpha,
                   float beta,
                   bool useHgemm,
                   cublasHandle_t handle,
                   cudaStream_t stream) {
  return runMatrixMult<half>(c, transC, a, transA, b, transB,
                             alpha, beta, useHgemm, handle, stream);
}
#endif

void
runIteratedMatrixMult(Tensor<float, 3, true>& c, bool transC,
                      Tensor<float, 3, true>& a, bool transA,
                      Tensor<float, 3, true>& b, bool transB,
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
                  alpha, beta, false, handle, stream);
  }
}

void
runBatchMatrixMult(Tensor<float, 3, true>& c, bool transC,
                   Tensor<float, 3, true>& a, bool transA,
                   Tensor<float, 3, true>& b, bool transB,
                   float alpha,
                   float beta,
                   DeviceMemory& mem,
                   cublasHandle_t handle,
                   cudaStream_t stream) {
  FAISS_ASSERT(c.getSize(0) == a.getSize(0));
  FAISS_ASSERT(a.getSize(0) == b.getSize(0));
  cublasSetStream(handle, stream);

  // Check that we have (m x k) * (k x n) = (m x n)
  // using the input row-major layout
  int aM = transA ? a.getSize(2) : a.getSize(1);
  int aK = transA ? a.getSize(1) : a.getSize(2);

  int bK = transB ? b.getSize(2) : b.getSize(1);
  int bN = transB ? b.getSize(1) : b.getSize(2);

  int cM = transC ? c.getSize(2) : c.getSize(1);
  int cN = transC ? c.getSize(1) : c.getSize(2);

  FAISS_ASSERT(aM == cM);
  FAISS_ASSERT(aK == bK);
  FAISS_ASSERT(bN == cN);

  // Now, we have to represent the matrix multiplication in
  // column-major layout
  float* pA = transC ? a.data() : b.data();
  float* pB = transC ? b.data() : a.data();
  float* pC = c.data();

  int m = c.getSize(2); // stride 1 size
  int n = c.getSize(1); // other size
  int k = transA ? a.getSize(1) : a.getSize(2);

  int lda = transC ? a.getStride(1) : b.getStride(1);
  int ldb = transC ? b.getStride(1) : a.getStride(1);
  int ldc = c.getStride(1);

  auto gemmTrA = transB ? CUBLAS_OP_T : CUBLAS_OP_N;
  auto gemmTrB = transA ? CUBLAS_OP_T : CUBLAS_OP_N;

  if (transC) {
    gemmTrA = transA ? CUBLAS_OP_N : CUBLAS_OP_T;
    gemmTrB = transB ? CUBLAS_OP_N : CUBLAS_OP_T;
  }

  HostTensor<float*, 1, true> hostA({a.getSize(0)});
  HostTensor<float*, 1, true> hostB({b.getSize(0)});
  HostTensor<float*, 1, true> hostC({c.getSize(0)});

  size_t aOffset = a.getStride(0);
  size_t bOffset = b.getStride(0);
  size_t cOffset = c.getStride(0);

  for (int i = 0; i < a.getSize(0); ++i) {
    hostA[i] = transC ? a.data() + i * aOffset : b.data() + i * bOffset;
    hostB[i] = transC ? b.data() + i * bOffset : a.data() + i * aOffset;
    hostC[i] = c.data() + i * cOffset;
  }

  DeviceTensor<float*, 1, true> deviceA(mem, hostA, stream);
  DeviceTensor<float*, 1, true> deviceB(mem, hostB, stream);
  DeviceTensor<float*, 1, true> deviceC(mem, hostC, stream);

  auto err =
    cublasSgemmBatched(handle,
                       gemmTrA, gemmTrB,
                       m, n, k, &alpha,
                       (const float**) deviceA.data(), lda,
                       (const float**) deviceB.data(), ldb, &beta,
                       deviceC.data(), ldc, a.getSize(0));
  FAISS_ASSERT_FMT(err == CUBLAS_STATUS_SUCCESS,
                   "cublasSgemmBatched failed (%d)", (int) err);
  CUDA_TEST_ERROR();
}

} } // namespace
