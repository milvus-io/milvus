/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "PQCodeDistances.cuh"

#include "BroadcastSum.cuh"
#include "Distance.cuh"
#include "L2Norm.cuh"
#include "../utils/DeviceDefs.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/Float16.cuh"
#include "../utils/MatrixMult.cuh"
#include "../utils/PtxUtils.cuh"
#include "../utils/StaticUtils.h"
#include "../utils/Transpose.cuh"

namespace faiss { namespace gpu {

template <typename T>
struct Converter {
};

#ifdef FAISS_USE_FLOAT16
template <>
struct Converter<half> {
  inline static __device__ half to(float v) { return __float2half(v); }
};
#endif

template <>
struct Converter<float> {
  inline static __device__ float to(float v) { return v; }
};

// Kernel responsible for calculating distance from residual vector to
// each product quantizer code centroid
template <typename OutCodeT, int DimsPerSubQuantizer>
__global__ void
__launch_bounds__(288, 4)
pqCodeDistances(Tensor<float, 2, true> queries,
                int queriesPerBlock,
                Tensor<float, 2, true> coarseCentroids,
                Tensor<float, 3, true> pqCentroids,
                Tensor<int, 2, true> topQueryToCentroid,
                // (query id)(coarse)(subquantizer)(code) -> dist
                Tensor<OutCodeT, 4, true> outCodeDistances) {
  const auto numSubQuantizers = pqCentroids.getSize(0);
  const auto dimsPerSubQuantizer = pqCentroids.getSize(1);
  assert(DimsPerSubQuantizer == dimsPerSubQuantizer);
  const auto codesPerSubQuantizer = pqCentroids.getSize(2);

  bool isLoadingThread = threadIdx.x >= codesPerSubQuantizer;
  int loadingThreadId = threadIdx.x - codesPerSubQuantizer;

  extern __shared__ float smem[];

  // Each thread calculates a single code
  float subQuantizerData[DimsPerSubQuantizer];

  auto code = threadIdx.x;
  auto subQuantizer = blockIdx.y;

  // Each thread will load the pq centroid data for the code that it
  // is processing
#pragma unroll
  for (int i = 0; i < DimsPerSubQuantizer; ++i) {
    subQuantizerData[i] = pqCentroids[subQuantizer][i][code].ldg();
  }

  // Where we store our query vector
  float* smemQuery = smem;

  // Where we store our residual vector; this is double buffered so we
  // can be loading the next one while processing the current one
  float* smemResidual1 = &smemQuery[DimsPerSubQuantizer];
  float* smemResidual2 = &smemResidual1[DimsPerSubQuantizer];

  // Where we pre-load the coarse centroid IDs
  int* coarseIds = (int*) &smemResidual2[DimsPerSubQuantizer];

  // Each thread is calculating the distance for a single code,
  // performing the reductions locally

  // Handle multiple queries per block
  auto startQueryId = blockIdx.x * queriesPerBlock;
  auto numQueries = queries.getSize(0) - startQueryId;
  if (numQueries > queriesPerBlock) {
    numQueries = queriesPerBlock;
  }

  for (int query = 0; query < numQueries; ++query) {
    auto queryId = startQueryId + query;

    auto querySubQuantizer =
      queries[queryId][subQuantizer * DimsPerSubQuantizer].data();

    // Load current query vector
    for (int i = threadIdx.x; i < DimsPerSubQuantizer; i += blockDim.x) {
      smemQuery[i] = querySubQuantizer[i];
    }

    // Load list of coarse centroids found
    for (int i = threadIdx.x;
         i < topQueryToCentroid.getSize(1); i += blockDim.x) {
      coarseIds[i] = topQueryToCentroid[queryId][i];
    }

    // We need coarseIds below
    // FIXME: investigate loading separately, so we don't need this
    __syncthreads();

    // Preload first buffer of residual data
    if (isLoadingThread) {
      for (int i = loadingThreadId;
           i < DimsPerSubQuantizer;
           i += blockDim.x - codesPerSubQuantizer) {
        auto coarseId = coarseIds[0];
        // In case NaNs were in the original query data
        coarseId = coarseId == -1 ? 0 : coarseId;
        auto coarseCentroidSubQuantizer =
          coarseCentroids[coarseId][subQuantizer * dimsPerSubQuantizer].data();

        smemResidual1[i] = smemQuery[i] - coarseCentroidSubQuantizer[i];
      }
    }

    // The block walks the list for a single query
    for (int coarse = 0; coarse < topQueryToCentroid.getSize(1); ++coarse) {
      // Wait for smemResidual1 to be loaded
      __syncthreads();

      if (isLoadingThread) {
        // Preload second buffer of residual data
        for (int i = loadingThreadId;
             i < DimsPerSubQuantizer;
             i += blockDim.x - codesPerSubQuantizer) {
          // FIXME: try always making this centroid id 0 so we can
          // terminate
          if (coarse != (topQueryToCentroid.getSize(1) - 1)) {
            auto coarseId = coarseIds[coarse + 1];
            // In case NaNs were in the original query data
            coarseId = coarseId == -1 ? 0 : coarseId;

            auto coarseCentroidSubQuantizer =
              coarseCentroids[coarseId][subQuantizer * dimsPerSubQuantizer].data();

            smemResidual2[i] = smemQuery[i] - coarseCentroidSubQuantizer[i];
          }
        }
      } else {
        // These are the processing threads
        float dist = 0.0f;

        constexpr int kUnroll = 4;
        constexpr int kRemainder = DimsPerSubQuantizer % kUnroll;
        constexpr int kRemainderBase = DimsPerSubQuantizer - kRemainder;
        float vals[kUnroll];

        // Calculate residual - pqCentroid for each dim that we're
        // processing

        // Unrolled loop
#pragma unroll
        for (int i = 0; i < DimsPerSubQuantizer / kUnroll; ++i) {

#pragma unroll
          for (int j = 0; j < kUnroll; ++j) {
            vals[j] = smemResidual1[i * kUnroll + j];
          }

#pragma unroll
          for (int j = 0; j < kUnroll; ++j) {
            vals[j] -= subQuantizerData[i * kUnroll + j];
          }

#pragma unroll
          for (int j = 0; j < kUnroll; ++j) {
            vals[j] *= vals[j];
          }

#pragma unroll
          for (int j = 0; j < kUnroll; ++j) {
            dist += vals[j];
          }
        }

        // Remainder loop
#pragma unroll
        for (int j = 0; j < kRemainder; ++j) {
          vals[j] = smemResidual1[kRemainderBase + j];
        }

#pragma unroll
        for (int j = 0; j < kRemainder; ++j) {
          vals[j] -= subQuantizerData[kRemainderBase + j];
        }

#pragma unroll
        for (int j = 0; j < kRemainder; ++j) {
          vals[j] *= vals[j];
        }

#pragma unroll
        for (int j = 0; j < kRemainder; ++j) {
          dist += vals[j];
        }

        // We have the distance for our code; write it out
        outCodeDistances[queryId][coarse][subQuantizer][code] =
          Converter<OutCodeT>::to(dist);
      } // !isLoadingThread

      // Swap residual buffers
      float* tmp = smemResidual1;
      smemResidual1 = smemResidual2;
      smemResidual2 = tmp;
    }
  }
}

__global__ void
residualVector(Tensor<float, 2, true> queries,
               Tensor<float, 2, true> coarseCentroids,
               Tensor<int, 2, true> topQueryToCentroid,
               int numSubDim,
               // output is transposed:
               // (sub q)(query id)(centroid id)(sub dim)
               Tensor<float, 4, true> residual) {
  // block x is query id
  // block y is centroid id
  // thread x is dim
  auto queryId = blockIdx.x;
  auto centroidId = blockIdx.y;

  int realCentroidId = topQueryToCentroid[queryId][centroidId];

  for (int dim = threadIdx.x; dim < queries.getSize(1); dim += blockDim.x) {
    float q = queries[queryId][dim];
    float c = coarseCentroids[realCentroidId][dim];

    residual[dim / numSubDim][queryId][centroidId][dim % numSubDim] =
      q - c;
  }
}

void
runResidualVector(Tensor<float, 3, true>& pqCentroids,
                  Tensor<float, 2, true>& queries,
                  Tensor<float, 2, true>& coarseCentroids,
                  Tensor<int, 2, true>& topQueryToCentroid,
                  Tensor<float, 4, true>& residual,
                  cudaStream_t stream) {
  auto grid =
    dim3(topQueryToCentroid.getSize(0), topQueryToCentroid.getSize(1));
  auto block = dim3(std::min(queries.getSize(1), getMaxThreadsCurrentDevice()));

  residualVector<<<grid, block, 0, stream>>>(
    queries, coarseCentroids, topQueryToCentroid, pqCentroids.getSize(1),
    residual);

  CUDA_TEST_ERROR();
}

void
runPQCodeDistancesMM(Tensor<float, 3, true>& pqCentroids,
                     Tensor<float, 2, true>& queries,
                     Tensor<float, 2, true>& coarseCentroids,
                     Tensor<int, 2, true>& topQueryToCentroid,
                     NoTypeTensor<4, true>& outCodeDistances,
                     bool useFloat16Lookup,
                     DeviceMemory& mem,
                     cublasHandle_t handle,
                     cudaStream_t stream) {
  // Calculate (q - c) residual vector
  // (sub q)(query id)(centroid id)(sub dim)
  DeviceTensor<float, 4, true> residual(
    mem,
    {pqCentroids.getSize(0),
        topQueryToCentroid.getSize(0),
        topQueryToCentroid.getSize(1),
        pqCentroids.getSize(1)},
    stream);

  runResidualVector(pqCentroids, queries,
                    coarseCentroids, topQueryToCentroid,
                    residual, stream);

  // Calculate ||q - c||^2
  DeviceTensor<float, 1, true> residualNorms(
    mem,
    {pqCentroids.getSize(0) *
        topQueryToCentroid.getSize(0) *
        topQueryToCentroid.getSize(1)},
    stream);

  auto residualView2 = residual.view<2>(
    {pqCentroids.getSize(0) *
        topQueryToCentroid.getSize(0) *
        topQueryToCentroid.getSize(1),
        pqCentroids.getSize(1)});

  runL2Norm(residualView2, residualNorms, true, stream);

  // Perform a batch MM:
  // (sub q) x {(q * c)(sub dim) x (sub dim)(code)} =>
  // (sub q) x {(q * c)(code)}
  auto residualView3 = residual.view<3>(
    {pqCentroids.getSize(0),
        topQueryToCentroid.getSize(0) * topQueryToCentroid.getSize(1),
        pqCentroids.getSize(1)});

  DeviceTensor<float, 3, true> residualDistance(
    mem,
    {pqCentroids.getSize(0),
        topQueryToCentroid.getSize(0) * topQueryToCentroid.getSize(1),
        pqCentroids.getSize(2)},
    stream);

  runIteratedMatrixMult(residualDistance, false,
                        residualView3, false,
                        pqCentroids, false,
                        -2.0f, 0.0f,
                        handle,
                        stream);

  // Sum ||q - c||^2 along rows
  auto residualDistanceView2 = residualDistance.view<2>(
    {pqCentroids.getSize(0) *
        topQueryToCentroid.getSize(0) *
        topQueryToCentroid.getSize(1),
        pqCentroids.getSize(2)});

  runSumAlongRows(residualNorms, residualDistanceView2, false, stream);

  Tensor<float, 4, true> outCodeDistancesF;
  DeviceTensor<float, 4, true> outCodeDistancesFloatMem;

#ifdef FAISS_USE_FLOAT16
  if (useFloat16Lookup) {
    outCodeDistancesFloatMem = DeviceTensor<float, 4, true>(
      mem, {outCodeDistances.getSize(0),
          outCodeDistances.getSize(1),
          outCodeDistances.getSize(2),
          outCodeDistances.getSize(3)},
      stream);

    outCodeDistancesF = outCodeDistancesFloatMem;
  }
#endif

  if (!useFloat16Lookup) {
    outCodeDistancesF = outCodeDistances.toTensor<float>();
  }

  // Transpose -2(sub q)(q * c)(code) to -2(q * c)(sub q)(code) (which
  // is where we build our output distances)
  auto outCodeDistancesView = outCodeDistancesF.view<3>(
    {topQueryToCentroid.getSize(0) * topQueryToCentroid.getSize(1),
        outCodeDistances.getSize(2),
        outCodeDistances.getSize(3)});

  runTransposeAny(residualDistance, 0, 1, outCodeDistancesView, stream);

  // Calculate code norms per each sub-dim
  // (sub q)(sub dim)(code) is pqCentroids
  // transpose to (sub q)(code)(sub dim)
  DeviceTensor<float, 3, true> pqCentroidsTranspose(
    mem,
    {pqCentroids.getSize(0), pqCentroids.getSize(2), pqCentroids.getSize(1)},
    stream);

  runTransposeAny(pqCentroids, 1, 2, pqCentroidsTranspose, stream);

  auto pqCentroidsTransposeView = pqCentroidsTranspose.view<2>(
    {pqCentroids.getSize(0) * pqCentroids.getSize(2),
        pqCentroids.getSize(1)});

  DeviceTensor<float, 1, true> pqCentroidsNorm(
    mem,
    {pqCentroids.getSize(0) * pqCentroids.getSize(2)},
    stream);

  runL2Norm(pqCentroidsTransposeView, pqCentroidsNorm, true, stream);

  // View output as (q * c)(sub q * code), and add centroid norm to
  // each row
  auto outDistancesCodeViewCols = outCodeDistancesView.view<2>(
    {topQueryToCentroid.getSize(0) * topQueryToCentroid.getSize(1),
        outCodeDistances.getSize(2) * outCodeDistances.getSize(3)});

  runSumAlongColumns(pqCentroidsNorm, outDistancesCodeViewCols, stream);

#ifdef FAISS_USE_FLOAT16
  if (useFloat16Lookup) {
    // Need to convert back
    auto outCodeDistancesH = outCodeDistances.toTensor<half>();
    toHalf(stream, outCodeDistancesF, outCodeDistancesH);
  }
#endif
}

void
runPQCodeDistances(Tensor<float, 3, true>& pqCentroids,
                   Tensor<float, 2, true>& queries,
                   Tensor<float, 2, true>& coarseCentroids,
                   Tensor<int, 2, true>& topQueryToCentroid,
                   NoTypeTensor<4, true>& outCodeDistances,
                   bool useFloat16Lookup,
                   cudaStream_t stream) {
  const auto numSubQuantizers = pqCentroids.getSize(0);
  const auto dimsPerSubQuantizer = pqCentroids.getSize(1);
  const auto codesPerSubQuantizer = pqCentroids.getSize(2);

  // FIXME: tune
  // Reuse of pq centroid data is based on both # of queries * nprobe,
  // and we should really be tiling in both dimensions
  constexpr int kQueriesPerBlock = 8;

  auto grid = dim3(utils::divUp(queries.getSize(0), kQueriesPerBlock),
                   numSubQuantizers);

  // Reserve one block of threads for double buffering
  // FIXME: probably impractical for large # of dims?
  auto loadingThreads = utils::roundUp(dimsPerSubQuantizer, kWarpSize);
  auto block = dim3(codesPerSubQuantizer + loadingThreads);

  auto smem = (3 * dimsPerSubQuantizer) * sizeof(float)
    + topQueryToCentroid.getSize(1) * sizeof(int);

#ifdef FAISS_USE_FLOAT16
#define CODE_DISTANCE(DIMS)                                             \
  do {                                                                  \
    if (useFloat16Lookup) {                                             \
      auto outCodeDistancesT = outCodeDistances.toTensor<half>();       \
                                                                        \
      pqCodeDistances<half, DIMS><<<grid, block, smem, stream>>>(       \
        queries, kQueriesPerBlock,                                      \
        coarseCentroids, pqCentroids,                                   \
        topQueryToCentroid, outCodeDistancesT);                         \
    } else {                                                            \
      auto outCodeDistancesT = outCodeDistances.toTensor<float>();      \
                                                                        \
      pqCodeDistances<float, DIMS><<<grid, block, smem, stream>>>(      \
        queries, kQueriesPerBlock,                                      \
        coarseCentroids, pqCentroids,                                   \
        topQueryToCentroid, outCodeDistancesT);                         \
    }                                                                   \
  } while (0)
#else
#define CODE_DISTANCE(DIMS)                                             \
  do {                                                                  \
    if (!useFloat16Lookup) {                                            \
      auto outCodeDistancesT = outCodeDistances.toTensor<float>();      \
                                                                        \
      pqCodeDistances<float, DIMS><<<grid, block, smem, stream>>>(      \
        queries, kQueriesPerBlock,                                      \
        coarseCentroids, pqCentroids,                                   \
        topQueryToCentroid, outCodeDistancesT);                         \
    }                                                                   \
  } while (0)
#endif

  switch (dimsPerSubQuantizer) {
    case 1:
      CODE_DISTANCE(1);
      break;
    case 2:
      CODE_DISTANCE(2);
      break;
    case 3:
      CODE_DISTANCE(3);
      break;
    case 4:
      CODE_DISTANCE(4);
      break;
    case 6:
      CODE_DISTANCE(6);
      break;
    case 8:
      CODE_DISTANCE(8);
      break;
    case 10:
      CODE_DISTANCE(10);
      break;
    case 12:
      CODE_DISTANCE(12);
      break;
    case 16:
      CODE_DISTANCE(16);
      break;
    case 20:
      CODE_DISTANCE(20);
      break;
    case 24:
      CODE_DISTANCE(24);
      break;
    case 28:
      CODE_DISTANCE(28);
      break;
    case 32:
      CODE_DISTANCE(32);
      break;
      // FIXME: larger sizes require too many registers - we need the
      // MM implementation working
    default:
      FAISS_ASSERT(false);
      break;
  }

#undef CODE_DISTANCE

  CUDA_TEST_ERROR();
}

} } // namespace
