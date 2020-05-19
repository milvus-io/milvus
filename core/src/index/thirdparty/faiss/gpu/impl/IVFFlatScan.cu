/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/impl/IVFFlatScan.cuh>
#include <faiss/gpu/impl/DistanceUtils.cuh>
#include <faiss/gpu/impl/IVFUtils.cuh>
#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/utils/ConversionOperators.cuh>
#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/DeviceTensor.cuh>
#include <faiss/gpu/utils/Float16.cuh>
#include <faiss/gpu/utils/MathOperators.cuh>
#include <faiss/gpu/utils/LoadStoreOperators.cuh>
#include <faiss/gpu/utils/PtxUtils.cuh>
#include <faiss/gpu/utils/Reductions.cuh>
#include <faiss/gpu/utils/StaticUtils.h>
#include <thrust/host_vector.h>

namespace faiss { namespace gpu {

namespace {

/// Sort direction per each metric
inline bool metricToSortDirection(MetricType mt) {
  switch (mt) {
    case MetricType::METRIC_INNER_PRODUCT:
      // highest
      return true;
    case MetricType::METRIC_L2:
      // lowest
      return false;
    default:
      // unhandled metric
      FAISS_ASSERT(false);
      return false;
  }
}

}

// Number of warps we create per block of IVFFlatScan
constexpr int kIVFFlatScanWarps = 4;

// Works for any dimension size
template <typename Codec, typename Metric>
struct IVFFlatScan {
  static __device__ void scan(float* query,
                              bool useResidual,
                              float* residualBaseSlice,
                              void* vecData,
                              const Codec& codec,
                              const Metric& metric,
                              int numVecs,
                              int dim,
                              float* distanceOut) {
    // How many separate loading points are there for the decoder?
    int limit = utils::divDown(dim, Codec::kDimPerIter);

    // Each warp handles a separate chunk of vectors
    int warpId = threadIdx.x / kWarpSize;
    // FIXME: why does getLaneId() not work when we write out below!?!?!
    int laneId = threadIdx.x % kWarpSize; // getLaneId();

    // Divide the set of vectors among the warps
    int vecsPerWarp = utils::divUp(numVecs, kIVFFlatScanWarps);

    int vecStart = vecsPerWarp * warpId;
    int vecEnd = min(vecsPerWarp * (warpId + 1), numVecs);

    // Walk the list of vectors for this warp
    for (int vec = vecStart; vec < vecEnd; ++vec) {
      Metric dist = metric.zero();

      // Scan the dimensions availabe that have whole units for the decoder,
      // as the decoder may handle more than one dimension at once (leaving the
      // remainder to be handled separately)
      for (int d = laneId; d < limit; d += kWarpSize) {
        int realDim = d * Codec::kDimPerIter;
        float vecVal[Codec::kDimPerIter];

        // Decode the kDimPerIter dimensions
        codec.decode(vecData, vec, d, vecVal);

#pragma unroll
        for (int j = 0; j < Codec::kDimPerIter; ++j) {
          vecVal[j] += useResidual ? residualBaseSlice[realDim + j] : 0.0f;
        }

#pragma unroll
        for (int j = 0; j < Codec::kDimPerIter; ++j) {
          dist.handle(query[realDim + j], vecVal[j]);
        }
      }

      // Handle remainder by a single thread, if any
      // Not needed if we decode 1 dim per time
      if (Codec::kDimPerIter > 1) {
        int realDim = limit * Codec::kDimPerIter;

        // Was there any remainder?
        if (realDim < dim) {
          // Let the first threads in the block sequentially perform it
          int remainderDim = realDim + laneId;

          if (remainderDim < dim) {
            float vecVal =
              codec.decodePartial(vecData, vec, limit, laneId);
            vecVal += useResidual ? residualBaseSlice[remainderDim] : 0.0f;
            dist.handle(query[remainderDim], vecVal);
          }
        }
      }

      // Reduce distance within warp
      auto warpDist = warpReduceAllSum(dist.reduce());

      if (laneId == 0) {
        distanceOut[vec] = warpDist;
      }
    }
  }
};

template <typename Codec, typename Metric>
__global__ void
ivfFlatScan(Tensor<float, 2, true> queries,
            bool useResidual,
            Tensor<float, 3, true> residualBase,
            Tensor<int, 2, true> listIds,
            void** allListData,
            int* listLengths,
            Codec codec,
            Metric metric,
            Tensor<int, 2, true> prefixSumOffsets,
            Tensor<float, 1, true> distance) {
  extern __shared__ float smem[];

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

  auto residualBaseSlice = residualBase[queryId][probeId].data();

  codec.setSmem(smem, dim);

  IVFFlatScan<Codec, Metric>::scan(query,
                                   useResidual,
                                   residualBaseSlice,
                                   vecs,
                                   codec,
                                   metric,
                                   numVecs,
                                   dim,
                                   distanceOut);
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
                   faiss::MetricType metricType,
                   bool useResidual,
                   Tensor<float, 3, true>& residualBase,
                   GpuScalarQuantizer* scalarQ,
                   Tensor<float, 2, true>& outDistances,
                   Tensor<long, 2, true>& outIndices,
                   cudaStream_t stream) {
  int dim = queries.getSize(1);

  // Check the amount of shared memory per block available based on our type is
  // sufficient
  if (scalarQ &&
      (scalarQ->qtype == ScalarQuantizer::QuantizerType::QT_8bit ||
       scalarQ->qtype == ScalarQuantizer::QuantizerType::QT_4bit)) {
    int maxDim = getMaxSharedMemPerBlockCurrentDevice() /
      (sizeof(float) * 2);

    FAISS_THROW_IF_NOT_FMT(dim < maxDim,
                           "Insufficient shared memory available on the GPU "
                           "for QT_8bit or QT_4bit with %d dimensions; "
                           "maximum dimensions possible is %d", dim, maxDim);
  }


  // Calculate offset lengths, so we know where to write out
  // intermediate results
  runCalcListOffsets(listIds, listLengths, prefixSumOffsets, thrustMem, stream);

  auto grid = dim3(listIds.getSize(1), listIds.getSize(0));
  auto block = dim3(kWarpSize * kIVFFlatScanWarps);

#define RUN_IVF_FLAT                                                    \
  do {                                                                  \
    ivfFlatScan                                                         \
      <<<grid, block, codec.getSmemSize(dim), stream>>>(                \
        queries,                                                        \
        useResidual,                                                    \
        residualBase,                                                   \
        listIds,                                                        \
        listData.data().get(),                                          \
        listLengths.data().get(),                                       \
        codec,                                                          \
        metric,                                                         \
        prefixSumOffsets,                                               \
        allDistances);                                                  \
  } while (0)

#define HANDLE_METRICS                                  \
    do {                                                \
      if (metricType == MetricType::METRIC_L2) {        \
        L2Distance metric; RUN_IVF_FLAT;                \
      } else {                                          \
        IPDistance metric; RUN_IVF_FLAT;                \
      }                                                 \
    } while (0)

  if (!scalarQ) {
    CodecFloat codec(dim * sizeof(float));
    HANDLE_METRICS;
  } else {
    switch (scalarQ->qtype) {
      case ScalarQuantizer::QuantizerType::QT_8bit:
      {
        // FIXME: investigate 32 bit load perf issues
//        if (dim % 4 == 0) {
        if (false) {
          Codec<ScalarQuantizer::QuantizerType::QT_8bit, 4>
            codec(scalarQ->code_size,
                  scalarQ->gpuTrained.data(),
                  scalarQ->gpuTrained.data() + dim);
          HANDLE_METRICS;
        } else {
          Codec<ScalarQuantizer::QuantizerType::QT_8bit, 1>
            codec(scalarQ->code_size,
                  scalarQ->gpuTrained.data(),
                  scalarQ->gpuTrained.data() + dim);
          HANDLE_METRICS;
        }
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_8bit_uniform:
      {
        // FIXME: investigate 32 bit load perf issues
        if (false) {
//        if (dim % 4 == 0) {
          Codec<ScalarQuantizer::QuantizerType::QT_8bit_uniform, 4>
            codec(scalarQ->code_size, scalarQ->trained[0], scalarQ->trained[1]);
          HANDLE_METRICS;
        } else {
          Codec<ScalarQuantizer::QuantizerType::QT_8bit_uniform, 1>
            codec(scalarQ->code_size, scalarQ->trained[0], scalarQ->trained[1]);
          HANDLE_METRICS;
        }
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_fp16:
      {
        if (false) {
          // FIXME: investigate 32 bit load perf issues
//        if (dim % 2 == 0) {
          Codec<ScalarQuantizer::QuantizerType::QT_fp16, 2>
            codec(scalarQ->code_size);
          HANDLE_METRICS;
        } else {
          Codec<ScalarQuantizer::QuantizerType::QT_fp16, 1>
            codec(scalarQ->code_size);
          HANDLE_METRICS;
        }
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_8bit_direct:
      {
        Codec<ScalarQuantizer::QuantizerType::QT_8bit_direct, 1>
          codec(scalarQ->code_size);
        HANDLE_METRICS;
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_4bit:
      {
        Codec<ScalarQuantizer::QuantizerType::QT_4bit, 1>
          codec(scalarQ->code_size,
                scalarQ->gpuTrained.data(),
                scalarQ->gpuTrained.data() + dim);
        HANDLE_METRICS;
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_4bit_uniform:
      {
        Codec<ScalarQuantizer::QuantizerType::QT_4bit_uniform, 1>
          codec(scalarQ->code_size, scalarQ->trained[0], scalarQ->trained[1]);
        HANDLE_METRICS;
      }
      break;
      default:
        // unimplemented, should be handled at a higher level
        FAISS_ASSERT(false);
    }
  }

  CUDA_TEST_ERROR();

#undef HANDLE_METRICS
#undef RUN_IVF_FLAT

  // k-select the output in chunks, to increase parallelism
  runPass1SelectLists(prefixSumOffsets,
                      allDistances,
                      listIds.getSize(1),
                      k,
                      metricToSortDirection(metricType),
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
                      metricToSortDirection(metricType),
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
               faiss::MetricType metric,
               bool useResidual,
               Tensor<float, 3, true>& residualBase,
               GpuScalarQuantizer* scalarQ,
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
    auto residualBaseView =
      residualBase.narrowOutermost(query, numQueriesInTile);

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
                       metric,
                       useResidual,
                       residualBaseView,
                       scalarQ,
                       outDistanceView,
                       outIndicesView,
                       streams[curStream]);

    curStream = (curStream + 1) % 2;
  }

  streamWait({stream}, streams);
}

} } // namespace
