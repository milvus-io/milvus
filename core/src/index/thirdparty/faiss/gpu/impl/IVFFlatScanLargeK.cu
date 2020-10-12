/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/impl/IVFFlatScanLargeK.cuh>
#include <faiss/gpu/impl/DistanceUtils.cuh>
#include <faiss/gpu/impl/IVFUtils.cuh>
#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/utils/ConversionOperators.cuh>
#include <faiss/gpu/utils/CopyUtils.cuh>
#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/DeviceTensor.cuh>
#include <faiss/gpu/utils/Float16.cuh>
#include <faiss/gpu/utils/MathOperators.cuh>
#include <faiss/gpu/utils/LoadStoreOperators.cuh>
#include <faiss/gpu/utils/PtxUtils.cuh>
#include <faiss/gpu/utils/Reductions.cuh>
#include <faiss/gpu/utils/StaticUtils.h>
#include <faiss/impl/ScalarQuantizerOp.h>
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

__global__ void
copyMinDistancePerQuery(Tensor<float, 2, true> minDistances,
                        Tensor<float, 2, true> outDistances,
                        int k) {
    auto queryId = threadIdx.x;
    float minDistance = outDistances[queryId][k-1];
    minDistances[queryId][0] = minDistance;
}


void 
runIVFFlatScanTileSlice(
                   /* input */
                   Tensor<int, 2, true>& listIds,
                   thrust::device_vector<void*>& listIndices,
                   IndicesOptions indicesOptions,
                   Tensor<int, 2, true>& prefixSumOffsets,
                   Tensor<float, 1, true>& allDistances,
                   Tensor<float, 3, true>& heapDistances,
                   Tensor<int, 3, true>& heapIndices,
                   Tensor<float, 2, true>& minDistances,
                   int k,
                   faiss::MetricType metricType,
                   //float minDist,
                   /* output */
                   Tensor<float, 2, true>& outDistances,
                   Tensor<long, 2, true>& outIndices,
                   /* CUDA stream */
                   cudaStream_t stream,
                    GpuResources* res
                   ) {

    //cudaMemset(heapDistances.data(), 0, heapDistances.numElements() * sizeof(float));
    //cudaMemset(heapIndices.data(), 0, heapIndices.numElements() * sizeof(int));
  //auto& mem = res->getMemoryManagerCurrentDevice();
//    printf("heapDistances.shape: %d, %d, %d\n", heapDistances.getSize(0), heapDistances.getSize(1), heapDistances.getSize(2));
//    printf("heapIndices.shape: %d, %d, %d\n", heapIndices.getSize(0), heapIndices.getSize(1), heapIndices.getSize(2));

    // k-select the output in chunks, to increase parallelism
    runPass1SelectLists(prefixSumOffsets,
                        allDistances,
                        listIds.getSize(1),
                        k,
                        metricToSortDirection(metricType),
                        heapDistances,
                        heapIndices,
                        minDistances,
                        stream);

//    float *heap = (float*)malloc(sizeof(float) * 2 * k);
//    fromDevice<float, 3>(heapDistances, heap, stream);
//    int *heapindice = (int*)malloc(sizeof(int) * 2 * k);
//    fromDevice<int, 3>(heapIndices, heapindice, stream);
//    for (auto i = 0; i < 0 * k; ++i) 
//        printf("[%d %f]\t\t", heapindice[i], heap[i]);

    // k-select final output
    //DeviceTensor<float, 2, true> flatHeapDistances(mem, {1, 2 * k}, stream);
    //DeviceTensor<int, 2, true> flatHeapIndices(mem, {1, 2 * k}, stream);
    //fromDevice<float>(&heapDistances[0][0], &flatHeapDistances[0][0], sizeof(float)*k, stream);
    //fromDevice<float>(&heapDistances[0][1], &flatHeapDistances[0][k], sizeof(float)*k, stream);
    //fromDevice<int>(&heapIndices[0][0], &flatHeapIndices[0][0], sizeof(int)*k, stream);
    //fromDevice<int>(&heapIndices[0][1], &flatHeapIndices[0][k], sizeof(int)*k, stream);

    auto flatHeapDistances = heapDistances.downcastInner<2>();
    auto flatHeapIndices = heapIndices.downcastInner<2>();

//    printf("flatHeapDistances.shape: %d, %d\n", flatHeapDistances.getSize(0), flatHeapDistances.getSize(1));

//    if (flatHeapDistances.isContiguous()) printf("flatHeapDistances is contiguous\n");

//    auto nq = heapDistances.getSize(0);
//    auto np2 = heapDistances.getSize(1);
//
//    float *flat = (float*)malloc(sizeof(float) * nq * np2 * k);
//    fromDevice<float, 2>(flatHeapDistances, flat, stream);
//    int *flatindice = (int*)malloc(sizeof(int) * nq * np2 * k);
//    fromDevice<int, 2>(flatHeapIndices, flatindice, stream);

//    for (auto i = 0; i < 0 * np2 * k; ++i)  {
//        auto index = 1 * np2 * k + i;
//        printf("{%d %f}\t\t", flatindice[index], flat[index]);
//    }
//    printf("\n");


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

//    float *out = (float*)malloc(sizeof(float) * nq * k);
//    long *outindices = (long*)malloc(sizeof(long) * nq * k);
//
//    auto out0 = outDistances.narrow(0, 0, 1).downcastInner<1>();
//    auto outi0 = outIndices.narrow(0, 0, 1).downcastInner<1>();
//    fromDevice<float, 1>(out0, out, stream);
//    fromDevice<long, 1>(outi0, outindices, stream);
//
//    auto out1 = outDistances.narrow(0, 1, 1).downcastInner<1>();
//    auto outi1 = outIndices.narrow(0, 1, 1).downcastInner<1>();
//    fromDevice<float, 1>(out1, out+k, stream);
//    fromDevice<long, 1>(outi1, outindices+k, stream);
//
//    for (auto i = 0; i < 1 * k; ++i)  {
//        auto index = 1 * k + i;
//        printf("{%d %f}\t\t", outindices[index], out[index]);
//    }
//    printf("\n");

    // Copy the kth distance into minDistances
    auto nq = outDistances.getSize(0);
    copyMinDistancePerQuery<<<1, nq, 0, stream>>>(minDistances, outDistances, k);

//    auto min = (float*)malloc(sizeof(float) * nq);
//auto tmp = minDistances.downcastInner<1>();
//    fromDevice<float, 1>(tmp, min, stream);
//    for (auto i = 0; i < nq; ++i)
//        printf("minDistances[%d]: %f\n", i, min[i]);
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
                   Tensor<float, 3, true>& lastHeapDistances,
                   Tensor<int, 3, true>& lastHeapIndices,
                   Tensor<float, 2, true>& minDistances,
                   int k,
                   faiss::MetricType metricType,
                   bool useResidual,
                   Tensor<float, 3, true>& residualBase,
                   GpuScalarQuantizer* scalarQ,
                   Tensor<float, 2, true>& outDistances,
                   Tensor<long, 2, true>& outIndices,
                   cudaStream_t stream,
                   GpuResources* res) {
  int dim = queries.getSize(1);

  // Check the amount of shared memory per block available based on our type is
  // sufficient
  if (scalarQ &&
      (scalarQ->qtype == QuantizerType::QT_8bit ||
       scalarQ->qtype == QuantizerType::QT_4bit)) {
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
      case QuantizerType::QT_8bit:
      {
        // FIXME: investigate 32 bit load perf issues
//        if (dim % 4 == 0) {
        if (false) {
          Codec<(int)QuantizerType::QT_8bit, 4>
            codec(scalarQ->code_size,
                  scalarQ->gpuTrained.data(),
                  scalarQ->gpuTrained.data() + dim);
          HANDLE_METRICS;
        } else {
          Codec<(int)QuantizerType::QT_8bit, 1>
            codec(scalarQ->code_size,
                  scalarQ->gpuTrained.data(),
                  scalarQ->gpuTrained.data() + dim);
          HANDLE_METRICS;
        }
      }
      break;
      case QuantizerType::QT_8bit_uniform:
      {
        // FIXME: investigate 32 bit load perf issues
        if (false) {
//        if (dim % 4 == 0) {
          Codec<(int)QuantizerType::QT_8bit_uniform, 4>
            codec(scalarQ->code_size, scalarQ->trained[0], scalarQ->trained[1]);
          HANDLE_METRICS;
        } else {
          Codec<(int)QuantizerType::QT_8bit_uniform, 1>
            codec(scalarQ->code_size, scalarQ->trained[0], scalarQ->trained[1]);
          HANDLE_METRICS;
        }
      }
      break;
      case QuantizerType::QT_fp16:
      {
        if (false) {
          // FIXME: investigate 32 bit load perf issues
//        if (dim % 2 == 0) {
          Codec<(int)QuantizerType::QT_fp16, 2>
            codec(scalarQ->code_size);
          HANDLE_METRICS;
        } else {
          Codec<(int)QuantizerType::QT_fp16, 1>
            codec(scalarQ->code_size);
          HANDLE_METRICS;
        }
      }
      break;
      case QuantizerType::QT_8bit_direct:
      {
        Codec<(int)QuantizerType::QT_8bit_direct, 1>
          codec(scalarQ->code_size);
        HANDLE_METRICS;
      }
      break;
      case QuantizerType::QT_4bit:
      {
        Codec<(int)QuantizerType::QT_4bit, 1>
          codec(scalarQ->code_size,
                scalarQ->gpuTrained.data(),
                scalarQ->gpuTrained.data() + dim);
        HANDLE_METRICS;
      }
      break;
      case QuantizerType::QT_4bit_uniform:
      {
        Codec<(int)QuantizerType::QT_4bit_uniform, 1>
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

  GPU_FAISS_ASSERT_MSG(k > 2048, "must be K > 2048");

  const int64_t max_slice_size = 2048;
  int64_t slice_size = 2048;
  //float minDist = 0.0;
  for (int64_t slice_start = 0; slice_start < k; slice_start += slice_size) {
      if (slice_start + max_slice_size <= k) slice_size = max_slice_size;
      else slice_size = k - slice_start;

      //printf("k:%d, i: %ld, slice_size: %ld, minDist: %f\n", k, slice_start, slice_size, minDist);
      printf("k:%d, i: %ld, slice_size: %ld\n", k, slice_start, slice_size);

      auto outDistancesView = outDistances.narrow(1, slice_start, slice_size);
      auto outIndicesView = outIndices.narrow(1, slice_start, slice_size);

      if (slice_size < max_slice_size) {
        runIVFFlatScanTileSlice(
            listIds,
            listIndices,
            indicesOptions,
            prefixSumOffsets,
            allDistances,
            lastHeapDistances,
            lastHeapIndices,
            minDistances,
            slice_size,
            metricType,
            outDistancesView,
            outIndicesView,
            stream,
            res
            );
      } else {
        runIVFFlatScanTileSlice(
            listIds,
            listIndices,
            indicesOptions,
            prefixSumOffsets,
            allDistances,
            heapDistances,
            heapIndices,
            minDistances,
            slice_size,
            metricType,
            outDistancesView,
            outIndicesView,
            stream,
            res
            );
      }

//      auto nq = outDistancesView.getSize(0);
//      auto len = outDistancesView.getSize(1);
//      printf("outDistancesView.nq, .len: %d, %d\n", nq, len);
//      assert(nq == 2);
//      float* ViewDistances = (float*)malloc(sizeof(float) * len * nq);
//
//      auto flat = outDistancesView.narrow(0, 0, 1).downcastInner<1>();
//      fromDevice<float, 1>(flat, ViewDistances, stream);
//
//      auto flat1 = outDistancesView.narrow(0, 1, 1).downcastInner<1>();
//      fromDevice<float, 1>(flat1, ViewDistances+len, stream);
//
//      for (auto i = 0; i < nq; ++i) 
//        printf("\t\t\tfirst: %f, last: %f\n", ViewDistances[i * len], ViewDistances[ (i+1) * len - 1]);

//      printf("just run once loop\n");
//      break;
  }

  printf("runIVFFlatScanTile finish\n");

//  // auto min_dist = 2.58573;
//  auto min_dist = 0.0; 
//  auto nq = outDistances.getSize(0);
//  auto topk = outDistances.getSize(1);
//
//  for (size_t i = 0; i < 2; ++i) {
//    printf("i: %d, min_dist:%f\n", i, min_dist);
//
//    auto heapDistance_view = heapDistances.narrow(2, 0 + i * 2048, 2048);
//    auto heapIndices_view = heapIndices.narrow(2, 0 + i * 2048, 2048);
//
//    // k-select the output in chunks, to increase parallelism
//    runPass1SelectLists(prefixSumOffsets,
//                        allDistances,
//                        listIds.getSize(1),
//                        2048,
//                        metricToSortDirection(metricType),
//                        heapDistance_view,
//                        heapIndices_view,
//                        min_dist,
//                        stream);
//
//    // k-select final output
//    auto flatHeapDistances = heapDistances.downcastInner<2>();
//    auto flatHeapIndices = heapIndices.downcastInner<2>();
//
//    //printf("heap: %d %d\n", flatHeapDistances.sizes()[0], flatHeapDistances.sizes()[1]);
//    //printf("narrow: %d %d\n", 0 + i * 2048, 2048);
//
//    flatHeapDistances = flatHeapDistances.narrow(1, 0 + i * 2048, 2048);
//    flatHeapIndices = flatHeapIndices.narrow(1, 0 + i * 2048, 2048);
//
//
//    float *flat_distances = new float[2048];
//    fromDevice<float, 2>(flatHeapDistances, flat_distances, stream);
//    printf("heap: %f %f\n", flat_distances[0], flat_distances[2047]);
//
//
//    //printf("heap: %d %d\n", flatHeapDistances.sizes()[0], flatHeapDistances.sizes()[1]);
//
//    //printf("out: %d %d\n", outDistances.sizes()[0], outDistances.sizes()[1]);
//
//    auto outDistances_view = outDistances.narrow(1, 0 + i * 2048, 2048);
//    auto outIndices_view = outIndices.narrow(1, 0 + i * 2048, 2048);
//
//    //printf("outview: %d %d\n", outDistances_view.sizes()[0], outDistances_view.sizes()[1]);
//
//    runPass2SelectLists(flatHeapDistances,
//                        flatHeapIndices,
//                        listIndices,
//                        indicesOptions,
//                        prefixSumOffsets,
//                        listIds,
//                        2048,
//                        metricToSortDirection(metricType),
//                        outDistances_view,
//                        outIndices_view,
//                        stream);
//
//    float *distances = new float[topk];
//    fromDevice<float, 2>(outDistances_view, distances, stream);
//
//    //for (size_t j = 0; j < topk; j += 1024) {
//    //    printf("distances[%u]: %f\n", j, distances[j]);
//    //}
//
//    min_dist = distances[2048 - 1];
//    printf("topk dist: %f\n", min_dist);
//    delete [] distances;
//
//
//    if (metricToSortDirection(metricType)) {
//        printf("choose largest\n");
//    } else {
//        printf("choose smallest\n");
//    }
//  }
}

void
runIVFFlatScanLargeK(Tensor<float, 2, true>& queries,
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
  GPU_FAISS_ASSERT_MSG(k > 2048, "must be K > 2048");

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
  printf("pass2Chunks: %d\n", pass2Chunks);

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

  const int slice_k = 2048;
  const int last_k = k % 2048;

  DeviceTensor<float, 3, true> heapDistances1(
    mem, {queryTileSize, pass2Chunks, slice_k}, stream);
  DeviceTensor<float, 3, true> heapDistances2(
    mem, {queryTileSize, pass2Chunks, slice_k}, stream);
  DeviceTensor<float, 3, true>* heapDistances[2] =
    {&heapDistances1, &heapDistances2};

  DeviceTensor<int, 3, true> heapIndices1(
    mem, {queryTileSize, pass2Chunks, slice_k}, stream);
  DeviceTensor<int, 3, true> heapIndices2(
    mem, {queryTileSize, pass2Chunks, slice_k}, stream);
  DeviceTensor<int, 3, true>* heapIndices[2] =
    {&heapIndices1, &heapIndices2};

  DeviceTensor<float, 3, true> lastHeapDistances1(
    mem, {queryTileSize, pass2Chunks, last_k}, stream);
  DeviceTensor<float, 3, true> lastHeapDistances2(
    mem, {queryTileSize, pass2Chunks, last_k}, stream);
  DeviceTensor<float, 3, true>* lastHeapDistances[2] =
    {&lastHeapDistances1, &lastHeapDistances2};

  DeviceTensor<int, 3, true> lastHeapIndices1(
    mem, {queryTileSize, pass2Chunks, last_k}, stream);
  DeviceTensor<int, 3, true> lastHeapIndices2(
    mem, {queryTileSize, pass2Chunks, last_k}, stream);
  DeviceTensor<int, 3, true>* lastHeapIndices[2] =
    {&lastHeapIndices1, &lastHeapIndices2};

  DeviceTensor<float, 2, true> minDistances1(
    mem, {queryTileSize, 1}, stream);
  DeviceTensor<float, 2, true> minDistances2(
    mem, {queryTileSize, 1}, stream);
  DeviceTensor<float, 2, true>* minDistances[2] =
    {&minDistances1, &minDistances2};

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

    auto lastHeapDistancesView =
      lastHeapDistances[curStream]->narrowOutermost(0, numQueriesInTile);
    auto lastHeapIndicesView =
      lastHeapIndices[curStream]->narrowOutermost(0, numQueriesInTile);

    auto minDistancesView = 
      minDistances[curStream]->narrowOutermost(0, numQueriesInTile);
    minDistances[curStream]->zero(streams[curStream]);

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
                       lastHeapDistancesView,
                       lastHeapIndicesView,
                       minDistancesView,
                       k,
                       metric,
                       useResidual,
                       residualBaseView,
                       scalarQ,
                       outDistanceView,
                       outIndicesView,
                       streams[curStream],
                       res
                       );

    curStream = (curStream + 1) % 2;
  }

  streamWait({stream}, streams);
}

} } // namespace
