/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "IVFFlatScan.cuh"
#include "../GpuResources.h"
#include "IVFUtils.cuh"
#include "../utils/ConversionOperators.cuh"
#include "../utils/DeviceDefs.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/DeviceTensor.cuh"
#include "../utils/Float16.cuh"
#include "../utils/MathOperators.cuh"
#include "../utils/LoadStoreOperators.cuh"
#include "../utils/PtxUtils.cuh"
#include "../utils/Reductions.cuh"
#include "../utils/StaticUtils.h"
#include <thrust/host_vector.h>

namespace faiss { namespace gpu {

template <typename T>
inline __device__ typename Math<T>::ScalarType l2Distance(T a, T b) {
  a = Math<T>::sub(a, b);
  a = Math<T>::mul(a, a);
  return Math<T>::reduceAdd(a);
}

template <typename T>
inline __device__ typename Math<T>::ScalarType ipDistance(T a, T b) {
  return Math<T>::reduceAdd(Math<T>::mul(a, b));
}

// For list scanning, even if the input data is `half`, we perform all
// math in float32, because the code is memory b/w bound, and the
// added precision for accumulation is useful

/// The class that we use to provide scan specializations
template <int Dims, bool L2, typename T>
struct IVFFlatScan {
};

// Fallback implementation: works for any dimension size
template <bool L2, typename T>
struct IVFFlatScan<-1, L2, T> {
  static __device__ void scan(float* query,
                              void* vecData,
                              int numVecs,
                              int dim,
                              float* distanceOut) {
    extern __shared__ float smem[];
    T* vecs = (T*) vecData;

    for (int vec = 0; vec < numVecs; ++vec) {
      // Reduce in dist
      float dist = 0.0f;

      for (int d = threadIdx.x; d < dim; d += blockDim.x) {
        float vecVal = ConvertTo<float>::to(vecs[vec * dim + d]);
        float queryVal = query[d];
        float curDist;

        if (L2) {
          curDist = l2Distance(queryVal, vecVal);
        } else {
          curDist = ipDistance(queryVal, vecVal);
        }

        dist += curDist;
      }

      // Reduce distance within block
      dist = blockReduceAllSum<float, false, true>(dist, smem);

      if (threadIdx.x == 0) {
        distanceOut[vec] = dist;
      }
    }
  }
};

// implementation: works for # dims == blockDim.x
template <bool L2, typename T>
struct IVFFlatScan<0, L2, T> {
  static __device__ void scan(float* query,
                              void* vecData,
                              int numVecs,
                              int dim,
                              float* distanceOut) {
    extern __shared__ float smem[];
    T* vecs = (T*) vecData;

    float queryVal = query[threadIdx.x];

    constexpr int kUnroll = 4;
    int limit = utils::roundDown(numVecs, kUnroll);

    for (int i = 0; i < limit; i += kUnroll) {
      float vecVal[kUnroll];

#pragma unroll
      for (int j = 0; j < kUnroll; ++j) {
        vecVal[j] = ConvertTo<float>::to(vecs[(i + j) * dim + threadIdx.x]);
      }

#pragma unroll
      for (int j = 0; j < kUnroll; ++j) {
        if (L2) {
          vecVal[j] = l2Distance(queryVal, vecVal[j]);
        } else {
          vecVal[j] = ipDistance(queryVal, vecVal[j]);
        }
      }

      blockReduceAllSum<kUnroll, float, false, true>(vecVal, smem);

      if (threadIdx.x == 0) {
#pragma unroll
        for (int j = 0; j < kUnroll; ++j) {
          distanceOut[i + j] = vecVal[j];
        }
      }
    }

    // Handle remainder
    for (int i = limit; i < numVecs; ++i) {
      float vecVal = ConvertTo<float>::to(vecs[i * dim + threadIdx.x]);

      if (L2) {
        vecVal = l2Distance(queryVal, vecVal);
      } else {
        vecVal = ipDistance(queryVal, vecVal);
      }

      vecVal = blockReduceAllSum<float, false, true>(vecVal, smem);

      if (threadIdx.x == 0) {
        distanceOut[i] = vecVal;
      }
    }
  }
};

template <int Dims, bool L2, typename T>
__global__ void
ivfFlatScan(Tensor<float, 2, true> queries,
            Tensor<int, 2, true> listIds,
            void** allListData,
            int* listLengths,
            Tensor<int, 2, true> prefixSumOffsets,
            Tensor<float, 1, true> distance) {
  auto queryId = blockIdx.y;
  auto probeId = blockIdx.x;

  // This is where we start writing out data
  // We ensure that before the array (at offset -1), there is a 0 value
  int outBase = *(prefixSumOffsets[queryId][probeId].data() - 1);

  auto listId = listIds[queryId][probeId];
  // Safety guard in case NaNs in input cause no list ID to be generated
  if (listId == -1) {
    return;
  }

  auto query = queries[queryId].data();
  auto vecs = allListData[listId];
  auto numVecs = listLengths[listId];
  auto dim = queries.getSize(1);
  auto distanceOut = distance[outBase].data();

  IVFFlatScan<Dims, L2, T>::scan(query, vecs, numVecs, dim, distanceOut);
}

void
runIVFFlatScanTile(Tensor<float, 2, true>& queries,
                   Tensor<int, 2, true>& listIds,
                   thrust::device_vector<void*>& listData,
                   thrust::device_vector<void*>& listIndices,
                   IndicesOptions indicesOptions,
                   thrust::device_vector<int>& listLengths,
                   Tensor<char, 1, true>& thrustMem,
                   Tensor<int, 2, true>& prefixSumOffsets,
                   Tensor<float, 1, true>& allDistances,
                   Tensor<float, 3, true>& heapDistances,
                   Tensor<int, 3, true>& heapIndices,
                   int k,
                   bool l2Distance,
                   bool useFloat16,
                   Tensor<float, 2, true>& outDistances,
                   Tensor<long, 2, true>& outIndices,
                   cudaStream_t stream) {
  // Calculate offset lengths, so we know where to write out
  // intermediate results
  runCalcListOffsets(listIds, listLengths, prefixSumOffsets, thrustMem, stream);

  // Calculate distances for vectors within our chunk of lists
  constexpr int kMaxThreadsIVF = 512;

  // FIXME: if `half` and # dims is multiple of 2, halve the
  // threadblock size

  int dim = queries.getSize(1);
  int numThreads = std::min(dim, kMaxThreadsIVF);

  auto grid = dim3(listIds.getSize(1),
                   listIds.getSize(0));
  auto block = dim3(numThreads);
  // All exact dim kernels are unrolled by 4, hence the `4`
  auto smem = sizeof(float) * utils::divUp(numThreads, kWarpSize) * 4;

#define RUN_IVF_FLAT(DIMS, L2, T)                                       \
  do {                                                                  \
    ivfFlatScan<DIMS, L2, T>                                            \
      <<<grid, block, smem, stream>>>(                                  \
        queries,                                                        \
        listIds,                                                        \
        listData.data().get(),                                          \
        listLengths.data().get(),                                       \
        prefixSumOffsets,                                               \
        allDistances);                                                  \
  } while (0)

#ifdef FAISS_USE_FLOAT16

#define HANDLE_DIM_CASE(DIMS)                   \
  do {                                          \
    if (l2Distance) {                           \
      if (useFloat16) {                         \
        RUN_IVF_FLAT(DIMS, true, half);         \
      } else {                                  \
        RUN_IVF_FLAT(DIMS, true, float);        \
      }                                         \
    } else {                                    \
      if (useFloat16) {                         \
        RUN_IVF_FLAT(DIMS, false, half);        \
      } else {                                  \
        RUN_IVF_FLAT(DIMS, false, float);       \
      }                                         \
    }                                           \
  } while (0)
#else

#define HANDLE_DIM_CASE(DIMS)                   \
  do {                                          \
    if (l2Distance) {                           \
      if (useFloat16) {                         \
        FAISS_ASSERT(false);                    \
      } else {                                  \
        RUN_IVF_FLAT(DIMS, true, float);        \
      }                                         \
    } else {                                    \
      if (useFloat16) {                         \
        FAISS_ASSERT(false);                    \
      } else {                                  \
        RUN_IVF_FLAT(DIMS, false, float);       \
      }                                         \
    }                                           \
  } while (0)

#endif // FAISS_USE_FLOAT16

  if (dim <= kMaxThreadsIVF) {
    HANDLE_DIM_CASE(0);
  } else {
    HANDLE_DIM_CASE(-1);
  }

  CUDA_TEST_ERROR();

#undef HANDLE_DIM_CASE
#undef RUN_IVF_FLAT

  // k-select the output in chunks, to increase parallelism
  runPass1SelectLists(prefixSumOffsets,
                      allDistances,
                      listIds.getSize(1),
                      k,
                      !l2Distance, // L2 distance chooses smallest
                      heapDistances,
                      heapIndices,
                      stream);

  // k-select final output
  auto flatHeapDistances = heapDistances.downcastInner<2>();
  auto flatHeapIndices = heapIndices.downcastInner<2>();

  runPass2SelectLists(flatHeapDistances,
                      flatHeapIndices,
                      listIndices,
                      indicesOptions,
                      prefixSumOffsets,
                      listIds,
                      k,
                      !l2Distance, // L2 distance chooses smallest
                      outDistances,
                      outIndices,
                      stream);
}

void
runIVFFlatScan(Tensor<float, 2, true>& queries,
               Tensor<int, 2, true>& listIds,
               thrust::device_vector<void*>& listData,
               thrust::device_vector<void*>& listIndices,
               IndicesOptions indicesOptions,
               thrust::device_vector<int>& listLengths,
               int maxListLength,
               int k,
               bool l2Distance,
               bool useFloat16,
               // output
               Tensor<float, 2, true>& outDistances,
               // output
               Tensor<long, 2, true>& outIndices,
               GpuResources* res) {
  constexpr int kMinQueryTileSize = 8;
  constexpr int kMaxQueryTileSize = 128;
  constexpr int kThrustMemSize = 16384;

  int nprobe = listIds.getSize(1);

  auto& mem = res->getMemoryManagerCurrentDevice();
  auto stream = res->getDefaultStreamCurrentDevice();

  // Make a reservation for Thrust to do its dirty work (global memory
  // cross-block reduction space); hopefully this is large enough.
  DeviceTensor<char, 1, true> thrustMem1(
    mem, {kThrustMemSize}, stream);
  DeviceTensor<char, 1, true> thrustMem2(
    mem, {kThrustMemSize}, stream);
  DeviceTensor<char, 1, true>* thrustMem[2] =
    {&thrustMem1, &thrustMem2};

  // How much temporary storage is available?
  // If possible, we'd like to fit within the space available.
  size_t sizeAvailable = mem.getSizeAvailable();

  // We run two passes of heap selection
  // This is the size of the first-level heap passes
  constexpr int kNProbeSplit = 8;
  int pass2Chunks = std::min(nprobe, kNProbeSplit);

  size_t sizeForFirstSelectPass =
    pass2Chunks * k * (sizeof(float) + sizeof(int));

  // How much temporary storage we need per each query
  size_t sizePerQuery =
    2 * // # streams
    ((nprobe * sizeof(int) + sizeof(int)) + // prefixSumOffsets
     nprobe * maxListLength * sizeof(float) + // allDistances
     sizeForFirstSelectPass);

  int queryTileSize = (int) (sizeAvailable / sizePerQuery);

  if (queryTileSize < kMinQueryTileSize) {
    queryTileSize = kMinQueryTileSize;
  } else if (queryTileSize > kMaxQueryTileSize) {
    queryTileSize = kMaxQueryTileSize;
  }

  // FIXME: we should adjust queryTileSize to deal with this, since
  // indexing is in int32
  FAISS_ASSERT(queryTileSize * nprobe * maxListLength <
         std::numeric_limits<int>::max());

  // Temporary memory buffers
  // Make sure there is space prior to the start which will be 0, and
  // will handle the boundary condition without branches
  DeviceTensor<int, 1, true> prefixSumOffsetSpace1(
    mem, {queryTileSize * nprobe + 1}, stream);
  DeviceTensor<int, 1, true> prefixSumOffsetSpace2(
    mem, {queryTileSize * nprobe + 1}, stream);

  DeviceTensor<int, 2, true> prefixSumOffsets1(
    prefixSumOffsetSpace1[1].data(),
    {queryTileSize, nprobe});
  DeviceTensor<int, 2, true> prefixSumOffsets2(
    prefixSumOffsetSpace2[1].data(),
    {queryTileSize, nprobe});
  DeviceTensor<int, 2, true>* prefixSumOffsets[2] =
    {&prefixSumOffsets1, &prefixSumOffsets2};

  // Make sure the element before prefixSumOffsets is 0, since we
  // depend upon simple, boundary-less indexing to get proper results
  CUDA_VERIFY(cudaMemsetAsync(prefixSumOffsetSpace1.data(),
                              0,
                              sizeof(int),
                              stream));
  CUDA_VERIFY(cudaMemsetAsync(prefixSumOffsetSpace2.data(),
                              0,
                              sizeof(int),
                              stream));

  DeviceTensor<float, 1, true> allDistances1(
    mem, {queryTileSize * nprobe * maxListLength}, stream);
  DeviceTensor<float, 1, true> allDistances2(
    mem, {queryTileSize * nprobe * maxListLength}, stream);
  DeviceTensor<float, 1, true>* allDistances[2] =
    {&allDistances1, &allDistances2};

  DeviceTensor<float, 3, true> heapDistances1(
    mem, {queryTileSize, pass2Chunks, k}, stream);
  DeviceTensor<float, 3, true> heapDistances2(
    mem, {queryTileSize, pass2Chunks, k}, stream);
  DeviceTensor<float, 3, true>* heapDistances[2] =
    {&heapDistances1, &heapDistances2};

  DeviceTensor<int, 3, true> heapIndices1(
    mem, {queryTileSize, pass2Chunks, k}, stream);
  DeviceTensor<int, 3, true> heapIndices2(
    mem, {queryTileSize, pass2Chunks, k}, stream);
  DeviceTensor<int, 3, true>* heapIndices[2] =
    {&heapIndices1, &heapIndices2};

  auto streams = res->getAlternateStreamsCurrentDevice();
  streamWait(streams, {stream});

  int curStream = 0;

  for (int query = 0; query < queries.getSize(0); query += queryTileSize) {
    int numQueriesInTile =
      std::min(queryTileSize, queries.getSize(0) - query);

    auto prefixSumOffsetsView =
      prefixSumOffsets[curStream]->narrowOutermost(0, numQueriesInTile);

    auto listIdsView =
      listIds.narrowOutermost(query, numQueriesInTile);
    auto queryView =
      queries.narrowOutermost(query, numQueriesInTile);

    auto heapDistancesView =
      heapDistances[curStream]->narrowOutermost(0, numQueriesInTile);
    auto heapIndicesView =
      heapIndices[curStream]->narrowOutermost(0, numQueriesInTile);

    auto outDistanceView =
      outDistances.narrowOutermost(query, numQueriesInTile);
    auto outIndicesView =
      outIndices.narrowOutermost(query, numQueriesInTile);

    runIVFFlatScanTile(queryView,
                       listIdsView,
                       listData,
                       listIndices,
                       indicesOptions,
                       listLengths,
                       *thrustMem[curStream],
                       prefixSumOffsetsView,
                       *allDistances[curStream],
                       heapDistancesView,
                       heapIndicesView,
                       k,
                       l2Distance,
                       useFloat16,
                       outDistanceView,
                       outIndicesView,
                       streams[curStream]);

    curStream = (curStream + 1) % 2;
  }

  streamWait({stream}, streams);
}

} } // namespace
