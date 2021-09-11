/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/utils/MatrixMult.cuh>
#include <faiss/gpu/utils/DeviceMemory.h>

namespace faiss { namespace gpu {

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
