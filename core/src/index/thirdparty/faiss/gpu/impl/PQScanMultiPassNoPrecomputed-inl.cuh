/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/impl/PQCodeDistances.cuh>
#include <faiss/gpu/impl/PQCodeLoad.cuh>
#include <faiss/gpu/impl/IVFUtils.cuh>
#include <faiss/gpu/utils/ConversionOperators.cuh>
#include <faiss/gpu/utils/DeviceTensor.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/Float16.cuh>
#include <faiss/gpu/utils/LoadStoreOperators.cuh>
#include <faiss/gpu/utils/NoTypeTensor.cuh>
#include <faiss/gpu/utils/StaticUtils.h>

#include <faiss/gpu/utils/HostTensor.cuh>

namespace faiss { namespace gpu {

// This must be kept in sync with PQCodeDistances.cu
inline bool isSupportedNoPrecomputedSubDimSize(int dims) {
  switch (dims) {
    case 1:
    case 2:
    case 3:
    case 4:
    case 6:
    case 8:
    case 10:
    case 12:
    case 16:
    case 20:
    case 24:
    case 28:
    case 32:
      return true;
    default:
      // FIXME: larger sizes require too many registers - we need the
      // MM implementation working
      return false;
  }
}

template <typename LookupT, typename LookupVecT>
struct LoadCodeDistances {
  static inline __device__ void load(LookupT* smem,
                                     LookupT* codes,
                                     int numCodes) {
    constexpr int kWordSize = sizeof(LookupVecT) / sizeof(LookupT);

    // We can only use the vector type if the data is guaranteed to be
    // aligned. The codes are innermost, so if it is evenly divisible,
    // then any slice will be aligned.
    if (numCodes % kWordSize == 0) {
      // Load the data by float4 for efficiency, and then handle any remainder
      // limitVec is the number of whole vec words we can load, in terms
      // of whole blocks performing the load
      constexpr int kUnroll = 2;
      int limitVec = numCodes / (kUnroll * kWordSize * blockDim.x);
      limitVec *= kUnroll * blockDim.x;

      LookupVecT* smemV = (LookupVecT*) smem;
      LookupVecT* codesV = (LookupVecT*) codes;

      for (int i = threadIdx.x; i < limitVec; i += kUnroll * blockDim.x) {
        LookupVecT vals[kUnroll];

#pragma unroll
        for (int j = 0; j < kUnroll; ++j) {
          vals[j] =
            LoadStore<LookupVecT>::load(&codesV[i + j * blockDim.x]);
        }

#pragma unroll
        for (int j = 0; j < kUnroll; ++j) {
          LoadStore<LookupVecT>::store(&smemV[i + j * blockDim.x], vals[j]);
        }
      }

      // This is where we start loading the remainder that does not evenly
      // fit into kUnroll x blockDim.x
      int remainder = limitVec * kWordSize;

      for (int i = remainder + threadIdx.x; i < numCodes; i += blockDim.x) {
        smem[i] = codes[i];
      }
    } else {
      // Potential unaligned load
      constexpr int kUnroll = 4;

      int limit = utils::roundDown(numCodes, kUnroll * blockDim.x);

      int i = threadIdx.x;
      for (; i < limit; i += kUnroll * blockDim.x) {
        LookupT vals[kUnroll];

#pragma unroll
        for (int j = 0; j < kUnroll; ++j) {
          vals[j] = codes[i + j * blockDim.x];
        }

#pragma unroll
        for (int j = 0; j < kUnroll; ++j) {
          smem[i + j * blockDim.x] = vals[j];
        }
      }

      for (; i < numCodes; i += blockDim.x) {
        smem[i] = codes[i];
      }
    }
  }
};

template <int NumSubQuantizers, typename LookupT, typename LookupVecT>
__global__ void
pqScanNoPrecomputedMultiPass(Tensor<float, 2, true> queries,
                             Tensor<float, 3, true> pqCentroids,
                             Tensor<int, 2, true> topQueryToCentroid,
                             Tensor<LookupT, 4, true> codeDistances,
                             void** listCodes,
                             int* listLengths,
                             Tensor<int, 2, true> prefixSumOffsets,
                             Tensor<float, 1, true> distance) {
  const auto codesPerSubQuantizer = pqCentroids.getSize(2);

  // Where the pq code -> residual distance is stored
  extern __shared__ char smemCodeDistances[];
  LookupT* codeDist = (LookupT*) smemCodeDistances;

  // Each block handles a single query
  auto queryId = blockIdx.y;
  auto probeId = blockIdx.x;

  // This is where we start writing out data
  // We ensure that before the array (at offset -1), there is a 0 value
  int outBase = *(prefixSumOffsets[queryId][probeId].data() - 1);
  float* distanceOut = distance[outBase].data();

  auto listId = topQueryToCentroid[queryId][probeId];
  // Safety guard in case NaNs in input cause no list ID to be generated
  if (listId == -1) {
    return;
  }

  unsigned char* codeList = (unsigned char*) listCodes[listId];
  int limit = listLengths[listId];

  constexpr int kNumCode32 = NumSubQuantizers <= 4 ? 1 :
    (NumSubQuantizers / 4);
  unsigned int code32[kNumCode32];
  unsigned int nextCode32[kNumCode32];

  // We double-buffer the code loading, which improves memory utilization
  if (threadIdx.x < limit) {
    LoadCode32<NumSubQuantizers>::load(code32, codeList, threadIdx.x);
  }

  LoadCodeDistances<LookupT, LookupVecT>::load(
    codeDist,
    codeDistances[queryId][probeId].data(),
    codeDistances.getSize(2) * codeDistances.getSize(3));

  // Prevent WAR dependencies
  __syncthreads();

  // Each thread handles one code element in the list, with a
  // block-wide stride
  for (int codeIndex = threadIdx.x;
       codeIndex < limit;
       codeIndex += blockDim.x) {
    // Prefetch next codes
    if (codeIndex + blockDim.x < limit) {
      LoadCode32<NumSubQuantizers>::load(
        nextCode32, codeList, codeIndex + blockDim.x);
    }

    float dist = 0.0f;

#pragma unroll
    for (int word = 0; word < kNumCode32; ++word) {
      constexpr int kBytesPerCode32 =
        NumSubQuantizers < 4 ? NumSubQuantizers : 4;

      if (kBytesPerCode32 == 1) {
        auto code = code32[0];
        dist = ConvertTo<float>::to(codeDist[code]);

      } else {
#pragma unroll
        for (int byte = 0; byte < kBytesPerCode32; ++byte) {
          auto code = getByte(code32[word], byte * 8, 8);

          auto offset =
            codesPerSubQuantizer * (word * kBytesPerCode32 + byte);

          dist += ConvertTo<float>::to(codeDist[offset + code]);
        }
      }
    }

    // Write out intermediate distance result
    // We do not maintain indices here, in order to reduce global
    // memory traffic. Those are recovered in the final selection step.
    distanceOut[codeIndex] = dist;

    // Rotate buffers
#pragma unroll
    for (int word = 0; word < kNumCode32; ++word) {
      code32[word] = nextCode32[word];
    }
  }
}

template <typename CentroidT>
void
runMultiPassTile(Tensor<float, 2, true>& queries,
                 Tensor<CentroidT, 2, true>& centroids,
                 Tensor<float, 3, true>& pqCentroidsInnermostCode,
                 NoTypeTensor<4, true>& codeDistances,
                 Tensor<int, 2, true>& topQueryToCentroid,
                 bool useFloat16Lookup,
                 int bytesPerCode,
                 int numSubQuantizers,
                 int numSubQuantizerCodes,
                 thrust::device_vector<void*>& listCodes,
                 thrust::device_vector<void*>& listIndices,
                 IndicesOptions indicesOptions,
                 thrust::device_vector<int>& listLengths,
                 Tensor<char, 1, true>& thrustMem,
                 Tensor<int, 2, true>& prefixSumOffsets,
                 Tensor<float, 1, true>& allDistances,
                 Tensor<float, 3, true>& heapDistances,
                 Tensor<int, 3, true>& heapIndices,
                 int k,
                 faiss::MetricType metric,
                 Tensor<float, 2, true>& outDistances,
                 Tensor<long, 2, true>& outIndices,
                 cudaStream_t stream) {
  // We only support two metrics at the moment
  FAISS_ASSERT(metric == MetricType::METRIC_INNER_PRODUCT ||
               metric == MetricType::METRIC_L2);

  bool l2Distance = metric == MetricType::METRIC_L2;

  // Calculate offset lengths, so we know where to write out
  // intermediate results
  runCalcListOffsets(topQueryToCentroid, listLengths, prefixSumOffsets,
                     thrustMem, stream);

  // Calculate residual code distances, since this is without
  // precomputed codes
  runPQCodeDistances(pqCentroidsInnermostCode,
                     queries,
                     centroids,
                     topQueryToCentroid,
                     codeDistances,
                     l2Distance,
                     useFloat16Lookup,
                     stream);

  // Convert all codes to a distance, and write out (distance,
  // index) values for all intermediate results
  {
    auto kThreadsPerBlock = 256;

    auto grid = dim3(topQueryToCentroid.getSize(1),
                     topQueryToCentroid.getSize(0));
    auto block = dim3(kThreadsPerBlock);

    // pq centroid distances
    auto smem = useFloat16Lookup ? sizeof(half) : sizeof(float);

    smem *= numSubQuantizers * numSubQuantizerCodes;
    FAISS_ASSERT(smem <= getMaxSharedMemPerBlockCurrentDevice());

#define RUN_PQ_OPT(NUM_SUB_Q, LOOKUP_T, LOOKUP_VEC_T)                   \
    do {                                                                \
      auto codeDistancesT = codeDistances.toTensor<LOOKUP_T>();         \
                                                                        \
      pqScanNoPrecomputedMultiPass<NUM_SUB_Q, LOOKUP_T, LOOKUP_VEC_T>   \
        <<<grid, block, smem, stream>>>(                                \
          queries,                                                      \
          pqCentroidsInnermostCode,                                     \
          topQueryToCentroid,                                           \
          codeDistancesT,                                               \
          listCodes.data().get(),                                       \
          listLengths.data().get(),                                     \
          prefixSumOffsets,                                             \
          allDistances);                                                \
    } while (0)

#define RUN_PQ(NUM_SUB_Q)                       \
    do {                                        \
      if (useFloat16Lookup) {                   \
        RUN_PQ_OPT(NUM_SUB_Q, half, Half8);     \
      } else {                                  \
        RUN_PQ_OPT(NUM_SUB_Q, float, float4);   \
      }                                         \
    } while (0)

    switch (bytesPerCode) {
      case 1:
        RUN_PQ(1);
        break;
      case 2:
        RUN_PQ(2);
        break;
      case 3:
        RUN_PQ(3);
        break;
      case 4:
        RUN_PQ(4);
        break;
      case 8:
        RUN_PQ(8);
        break;
      case 12:
        RUN_PQ(12);
        break;
      case 16:
        RUN_PQ(16);
        break;
      case 20:
        RUN_PQ(20);
        break;
      case 24:
        RUN_PQ(24);
        break;
      case 28:
        RUN_PQ(28);
        break;
      case 32:
        RUN_PQ(32);
        break;
      case 40:
        RUN_PQ(40);
        break;
      case 48:
        RUN_PQ(48);
        break;
      case 56:
        RUN_PQ(56);
        break;
      case 64:
        RUN_PQ(64);
        break;
      case 96:
        RUN_PQ(96);
        break;
      default:
        FAISS_ASSERT(false);
        break;
    }

#undef RUN_PQ
#undef RUN_PQ_OPT
  }

  CUDA_TEST_ERROR();

  // k-select the output in chunks, to increase parallelism
  runPass1SelectLists(prefixSumOffsets,
                      allDistances,
                      topQueryToCentroid.getSize(1),
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
                      topQueryToCentroid,
                      k,
                      !l2Distance, // L2 distance chooses smallest
                      outDistances,
                      outIndices,
                      stream);
}

template <typename CentroidT>
void
runPQScanMultiPassNoPrecomputed(Tensor<float, 2, true>& queries,
                                Tensor<CentroidT, 2, true>& centroids,
                                Tensor<float, 3, true>& pqCentroidsInnermostCode,
                                Tensor<int, 2, true>& topQueryToCentroid,
                                bool useFloat16Lookup,
                                int bytesPerCode,
                                int numSubQuantizers,
                                int numSubQuantizerCodes,
                                thrust::device_vector<void*>& listCodes,
                                thrust::device_vector<void*>& listIndices,
                                IndicesOptions indicesOptions,
                                thrust::device_vector<int>& listLengths,
                                int maxListLength,
                                int k,
                                faiss::MetricType metric,
                                // output
                                Tensor<float, 2, true>& outDistances,
                                // output
                                Tensor<long, 2, true>& outIndices,
                                GpuResources* res) {
  constexpr int kMinQueryTileSize = 8;
  constexpr int kMaxQueryTileSize = 128;
  constexpr int kThrustMemSize = 16384;

  int nprobe = topQueryToCentroid.getSize(1);

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
    2 * // streams
    ((nprobe * sizeof(int) + sizeof(int)) + // prefixSumOffsets
     nprobe * maxListLength * sizeof(float) + // allDistances
     // residual distances
     nprobe * numSubQuantizers * numSubQuantizerCodes * sizeof(float) +
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

  int codeDistanceTypeSize = useFloat16Lookup ? sizeof(half) : sizeof(float);

  int totalCodeDistancesSize =
    queryTileSize * nprobe * numSubQuantizers * numSubQuantizerCodes *
    codeDistanceTypeSize;

  DeviceTensor<char, 1, true> codeDistances1Mem(
    mem, {totalCodeDistancesSize}, stream);
  NoTypeTensor<4, true> codeDistances1(
    codeDistances1Mem.data(),
    codeDistanceTypeSize,
    {queryTileSize, nprobe, numSubQuantizers, numSubQuantizerCodes});

  DeviceTensor<char, 1, true> codeDistances2Mem(
    mem, {totalCodeDistancesSize}, stream);
  NoTypeTensor<4, true> codeDistances2(
    codeDistances2Mem.data(),
    codeDistanceTypeSize,
    {queryTileSize, nprobe, numSubQuantizers, numSubQuantizerCodes});

  NoTypeTensor<4, true>* codeDistances[2] =
    {&codeDistances1, &codeDistances2};

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

    auto codeDistancesView =
      codeDistances[curStream]->narrowOutermost(0, numQueriesInTile);
    auto coarseIndicesView =
      topQueryToCentroid.narrowOutermost(query, numQueriesInTile);
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

    runMultiPassTile(queryView,
                     centroids,
                     pqCentroidsInnermostCode,
                     codeDistancesView,
                     coarseIndicesView,
                     useFloat16Lookup,
                     bytesPerCode,
                     numSubQuantizers,
                     numSubQuantizerCodes,
                     listCodes,
                     listIndices,
                     indicesOptions,
                     listLengths,
                     *thrustMem[curStream],
                     prefixSumOffsetsView,
                     *allDistances[curStream],
                     heapDistancesView,
                     heapIndicesView,
                     k,
                     metric,
                     outDistanceView,
                     outIndicesView,
                     streams[curStream]);

    curStream = (curStream + 1) % 2;
  }

  streamWait({stream}, streams);
}

} } // namespace
