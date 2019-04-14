/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "Distance.cuh"
#include "BroadcastSum.cuh"
#include "L2Norm.cuh"
#include "L2Select.cuh"
#include "../../FaissAssert.h"
#include "../../AuxIndexStructures.h"
#include "../GpuResources.h"
#include "../utils/DeviceDefs.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/Limits.cuh"
#include "../utils/MatrixMult.cuh"
#include "../utils/BlockSelectKernel.cuh"

#include <memory>
#include <algorithm>
#include <thrust/fill.h>
#include <thrust/for_each.h>
#include <thrust/device_ptr.h>
#include <thrust/execution_policy.h>

namespace faiss { namespace gpu {

namespace {

template <typename T>
Tensor<T, 2, true> sliceCentroids(Tensor<T, 2, true>& centroids,
                                  Tensor<T, 2, true>* centroidsTransposed,
                                  int startCentroid,
                                  int num) {
  if (startCentroid == 0 && num == centroids.getSize(0)) {
    if (centroidsTransposed) {
      return *centroidsTransposed;
    } else {
      return centroids;
    }
  }

  if (centroidsTransposed) {
    // (dim, num)
    return centroidsTransposed->narrow(1, startCentroid, num);
  } else {
    return centroids.narrow(0, startCentroid, num);
  }
}

// For each chunk of k indices, increment the index by chunk * increment
template <typename T>
__global__ void incrementIndex(Tensor<T, 2, true> indices,
                               int k,
                               int increment) {
  for (int i = threadIdx.x; i < k; i += blockDim.x) {
    indices[blockIdx.y][blockIdx.x * k + i] += blockIdx.x * increment;
  }
}

// Used to update result indices in distance computation where the number of
// centroids is high, and is tiled
template <typename T>
void runIncrementIndex(Tensor<T, 2, true>& indices,
                       int k,
                       int increment,
                       cudaStream_t stream) {
  dim3 grid(indices.getSize(1) / k, indices.getSize(0));
  int block = std::min(k, 512);

  // should be exact
  FAISS_ASSERT(grid.x * k == indices.getSize(1));

  incrementIndex<<<grid, block, 0, stream>>>(indices, k, increment);

  cudaDeviceSynchronize();
}

// If the inner size (dim) of the vectors is small, we want a larger query tile
// size, like 1024

void chooseTileSize(int numQueries,
                    int numCentroids,
                    int dim,
                    int elementSize,
                    size_t tempMemAvailable,
                    int& tileRows,
                    int& tileCols) {
  // The matrix multiplication should be large enough to be efficient, but if it
  // is too large, we seem to lose efficiency as opposed to double-streaming.
  // Each tile size here defines 1/2 of the memory use due to double streaming.
  // We ignore available temporary memory, as that is adjusted independently by
  // the user and can thus meet these requirements (or not).
  // For <= 4 GB GPUs, prefer 512 MB of usage.
  // For <= 8 GB GPUs, prefer 768 MB of usage.
  // Otherwise, prefer 1 GB of usage.
  auto totalMem = getCurrentDeviceProperties().totalGlobalMem;

  int targetUsage = 0;

  if (totalMem <= ((size_t) 4) * 1024 * 1024 * 1024) {
    targetUsage = 512 * 1024 * 1024;
  } else if (totalMem <= ((size_t) 8) * 1024 * 1024 * 1024) {
    targetUsage = 768 * 1024 * 1024;
  } else {
    targetUsage = 1024 * 1024 * 1024;
  }

  targetUsage /= 2 * elementSize;

  // 512 seems to be a batch size sweetspot for float32.
  // If we are on float16, increase to 512.
  // If the k size (vec dim) of the matrix multiplication is small (<= 32),
  // increase to 1024.
  int preferredTileRows = 512;
  if (dim <= 32) {
    preferredTileRows = 1024;
  }

  tileRows = std::min(preferredTileRows, numQueries);

  // tileCols is the remainder size
  tileCols = std::min(targetUsage / preferredTileRows, numCentroids);
}

}

template <typename T>
void runDistance(bool computeL2,
                 GpuResources* resources,
                 Tensor<T, 2, true>& centroids,
                 Tensor<T, 2, true>* centroidsTransposed,
                 Tensor<T, 1, true>* centroidNorms,
                 Tensor<T, 2, true>& queries,
                 int k,
                 Tensor<T, 2, true>& outDistances,
                 Tensor<int, 2, true>& outIndices,
                 bool useHgemm,
                 bool ignoreOutDistances) {
  FAISS_ASSERT(outDistances.getSize(0) == queries.getSize(0));
  FAISS_ASSERT(outIndices.getSize(0) == queries.getSize(0));
  FAISS_ASSERT(outDistances.getSize(1) == k);
  FAISS_ASSERT(outIndices.getSize(1) == k);

  auto& mem = resources->getMemoryManagerCurrentDevice();
  auto defaultStream = resources->getDefaultStreamCurrentDevice();

  // If we're quering against a 0 sized set, just return empty results
  if (centroids.numElements() == 0) {
    thrust::fill(thrust::cuda::par.on(defaultStream),
                 outDistances.data(), outDistances.end(),
                 Limits<T>::getMax());

    thrust::fill(thrust::cuda::par.on(defaultStream),
                 outIndices.data(), outIndices.end(),
                 -1);

    return;
  }

  // L2: If ||c||^2 is not pre-computed, calculate it
  DeviceTensor<T, 1, true> cNorms;
  if (computeL2 && !centroidNorms) {
    cNorms = std::move(DeviceTensor<T, 1, true>(
                       mem,
                       {centroids.getSize(0)}, defaultStream));
    runL2Norm(centroids, cNorms, true, defaultStream);
    centroidNorms = &cNorms;
  }

  //
  // Prepare norm vector ||q||^2; ||c||^2 is already pre-computed
  //
  int qNormSize[1] = {queries.getSize(0)};
  DeviceTensor<T, 1, true> queryNorms(mem, qNormSize, defaultStream);

  // ||q||^2
  if (computeL2) {
    runL2Norm(queries, queryNorms, true, defaultStream);
  }

  // By default, aim to use up to 512 MB of memory for the processing, with both
  // number of queries and number of centroids being at least 512.
  int tileRows = 0;
  int tileCols = 0;
  chooseTileSize(queries.getSize(0),
                 centroids.getSize(0),
                 queries.getSize(1),
                 sizeof(T),
                 mem.getSizeAvailable(),
                 tileRows,
                 tileCols);

  int numColTiles = utils::divUp(centroids.getSize(0), tileCols);

  // We can have any number of vectors to query against, even less than k, in
  // which case we'll return -1 for the index
  FAISS_ASSERT(k <= GPU_MAX_SELECTION_K); // select limitation

  // Temporary output memory space we'll use
  DeviceTensor<T, 2, true> distanceBuf1(
    mem, {tileRows, tileCols}, defaultStream);
  DeviceTensor<T, 2, true> distanceBuf2(
    mem, {tileRows, tileCols}, defaultStream);
  DeviceTensor<T, 2, true>* distanceBufs[2] =
    {&distanceBuf1, &distanceBuf2};

  DeviceTensor<T, 2, true> outDistanceBuf1(
    mem, {tileRows, numColTiles * k}, defaultStream);
  DeviceTensor<T, 2, true> outDistanceBuf2(
    mem, {tileRows, numColTiles * k}, defaultStream);
  DeviceTensor<T, 2, true>* outDistanceBufs[2] =
    {&outDistanceBuf1, &outDistanceBuf2};

  DeviceTensor<int, 2, true> outIndexBuf1(
    mem, {tileRows, numColTiles * k}, defaultStream);
  DeviceTensor<int, 2, true> outIndexBuf2(
    mem, {tileRows, numColTiles * k}, defaultStream);
  DeviceTensor<int, 2, true>* outIndexBufs[2] =
    {&outIndexBuf1, &outIndexBuf2};

  auto streams = resources->getAlternateStreamsCurrentDevice();
  streamWait(streams, {defaultStream});

  int curStream = 0;
  bool interrupt = false;

  // Tile over the input queries
  for (int i = 0; i < queries.getSize(0); i += tileRows) {

    if (interrupt || InterruptCallback::is_interrupted()) {
      interrupt = true;
      break;
    }

    int curQuerySize = std::min(tileRows, queries.getSize(0) - i);

    auto outDistanceView =
      outDistances.narrow(0, i, curQuerySize);
    auto outIndexView =
      outIndices.narrow(0, i, curQuerySize);

    auto queryView =
      queries.narrow(0, i, curQuerySize);
    auto queryNormNiew =
      queryNorms.narrow(0, i, curQuerySize);

    auto outDistanceBufRowView =
      outDistanceBufs[curStream]->narrow(0, 0, curQuerySize);
    auto outIndexBufRowView =
      outIndexBufs[curStream]->narrow(0, 0, curQuerySize);

    // Tile over the centroids
    for (int j = 0; j < centroids.getSize(0); j += tileCols) {

      if (InterruptCallback::is_interrupted()) {
        interrupt = true;
        break;
      }

      int curCentroidSize = std::min(tileCols, centroids.getSize(0) - j);

      int curColTile = j / tileCols;

      auto centroidsView =
        sliceCentroids(centroids, centroidsTransposed, j, curCentroidSize);

      auto distanceBufView = distanceBufs[curStream]->
        narrow(0, 0, curQuerySize).narrow(1, 0, curCentroidSize);

      auto outDistanceBufColView =
        outDistanceBufRowView.narrow(1, k * curColTile, k);
      auto outIndexBufColView =
        outIndexBufRowView.narrow(1, k * curColTile, k);

      // L2: distance is ||c||^2 - 2qc + ||q||^2, we compute -2qc
      // IP: just compute qc
      // (query id x dim) x (centroid id, dim)' = (query id, centroid id)
      runMatrixMult(distanceBufView, false,
                    queryView, false,
                    centroidsView,
                    centroidsTransposed ? false : true,
                    computeL2 ? -2.0f : 1.0f, 0.0f, useHgemm,
                    resources->getBlasHandleCurrentDevice(),
                    streams[curStream]);

      if (computeL2) {
        // For L2 distance, we use this fused kernel that performs both
        // adding ||c||^2 to -2qc and k-selection, so we only need two
        // passes (one write by the gemm, one read here) over the huge
        // region of output memory
        //
        // If we aren't tiling along the number of centroids, we can perform the
        // output work directly
        if (tileCols == centroids.getSize(0)) {
          // Write into the final output
          runL2SelectMin(distanceBufView,
                         *centroidNorms,
                         outDistanceView,
                         outIndexView,
                         k,
                         streams[curStream]);

          if (!ignoreOutDistances) {
            // expand (query id) to (query id, k) by duplicating along rows
            // top-k ||c||^2 - 2qc + ||q||^2 in the form (query id, k)
            runSumAlongRows(queryNormNiew,
                            outDistanceView,
                            true, // L2 distances should not go below zero due
                                  // to roundoff error
                            streams[curStream]);
          }
        } else {
          auto centroidNormsView =
            centroidNorms->narrow(0, j, curCentroidSize);

          // Write into our intermediate output
          runL2SelectMin(distanceBufView,
                         centroidNormsView,
                         outDistanceBufColView,
                         outIndexBufColView,
                         k,
                         streams[curStream]);

          if (!ignoreOutDistances) {
            // expand (query id) to (query id, k) by duplicating along rows
            // top-k ||c||^2 - 2qc + ||q||^2 in the form (query id, k)
            runSumAlongRows(queryNormNiew,
                            outDistanceBufColView,
                            true, // L2 distances should not go below zero due
                                  // to roundoff error
                            streams[curStream]);
          }
        }
      } else {
        // For IP, just k-select the output for this tile
        if (tileCols == centroids.getSize(0)) {
          // Write into the final output
          runBlockSelect(distanceBufView,
                         outDistanceView,
                         outIndexView,
                         true, k, streams[curStream]);
        } else {
          // Write into the intermediate output
          runBlockSelect(distanceBufView,
                         outDistanceBufColView,
                         outIndexBufColView,
                         true, k, streams[curStream]);
        }
      }


    }

    // As we're finished with processing a full set of centroids, perform the
    // final k-selection
    if (tileCols != centroids.getSize(0)) {
      // The indices are tile-relative; for each tile of k, we need to add
      // tileCols to the index
      runIncrementIndex(outIndexBufRowView, k, tileCols, streams[curStream]);

      runBlockSelectPair(outDistanceBufRowView,
                         outIndexBufRowView,
                         outDistanceView,
                         outIndexView,
                         computeL2 ? false : true, k, streams[curStream]);
    }

    curStream = (curStream + 1) % 2;
  }

  // Have the desired ordering stream wait on the multi-stream
  streamWait({defaultStream}, streams);

  if (interrupt) {
    FAISS_THROW_MSG("interrupted");
  }
}

template <typename T>
void runL2Distance(GpuResources* resources,
                   Tensor<T, 2, true>& centroids,
                   Tensor<T, 2, true>* centroidsTransposed,
                   Tensor<T, 1, true>* centroidNorms,
                   Tensor<T, 2, true>& queries,
                   int k,
                   Tensor<T, 2, true>& outDistances,
                   Tensor<int, 2, true>& outIndices,
                   bool useHgemm,
                   bool ignoreOutDistances = false) {
  runDistance<T>(true, // L2
                 resources,
                 centroids,
                 centroidsTransposed,
                 centroidNorms,
                 queries,
                 k,
                 outDistances,
                 outIndices,
                 useHgemm,
                 ignoreOutDistances);
}

template <typename T>
void runIPDistance(GpuResources* resources,
                   Tensor<T, 2, true>& centroids,
                   Tensor<T, 2, true>* centroidsTransposed,
                   Tensor<T, 2, true>& queries,
                   int k,
                   Tensor<T, 2, true>& outDistances,
                   Tensor<int, 2, true>& outIndices,
                   bool useHgemm) {
  runDistance<T>(false, // IP
                 resources,
                 centroids,
                 centroidsTransposed,
                 nullptr,
                 queries,
                 k,
                 outDistances,
                 outIndices,
                 useHgemm,
                 false);
}

//
// Instantiations of the distance templates
//

void
runIPDistance(GpuResources* resources,
              Tensor<float, 2, true>& vectors,
              Tensor<float, 2, true>* vectorsTransposed,
              Tensor<float, 2, true>& queries,
              int k,
              Tensor<float, 2, true>& outDistances,
              Tensor<int, 2, true>& outIndices) {
  runIPDistance<float>(resources,
                       vectors,
                       vectorsTransposed,
                       queries,
                       k,
                       outDistances,
                       outIndices,
                       false);
}

#ifdef FAISS_USE_FLOAT16
void
runIPDistance(GpuResources* resources,
              Tensor<half, 2, true>& vectors,
              Tensor<half, 2, true>* vectorsTransposed,
              Tensor<half, 2, true>& queries,
              int k,
              Tensor<half, 2, true>& outDistances,
              Tensor<int, 2, true>& outIndices,
              bool useHgemm) {
  runIPDistance<half>(resources,
                      vectors,
                      vectorsTransposed,
                      queries,
                      k,
                      outDistances,
                      outIndices,
                      useHgemm);
}
#endif

void
runL2Distance(GpuResources* resources,
              Tensor<float, 2, true>& vectors,
              Tensor<float, 2, true>* vectorsTransposed,
              Tensor<float, 1, true>* vectorNorms,
              Tensor<float, 2, true>& queries,
              int k,
              Tensor<float, 2, true>& outDistances,
              Tensor<int, 2, true>& outIndices,
              bool ignoreOutDistances) {
  runL2Distance<float>(resources,
                       vectors,
                       vectorsTransposed,
                       vectorNorms,
                       queries,
                       k,
                       outDistances,
                       outIndices,
                       false,
                       ignoreOutDistances);
}

#ifdef FAISS_USE_FLOAT16
void
runL2Distance(GpuResources* resources,
              Tensor<half, 2, true>& vectors,
              Tensor<half, 2, true>* vectorsTransposed,
              Tensor<half, 1, true>* vectorNorms,
              Tensor<half, 2, true>& queries,
              int k,
              Tensor<half, 2, true>& outDistances,
              Tensor<int, 2, true>& outIndices,
              bool useHgemm,
              bool ignoreOutDistances) {
  runL2Distance<half>(resources,
                      vectors,
                      vectorsTransposed,
                      vectorNorms,
                      queries,
                      k,
                      outDistances,
                      outIndices,
                      useHgemm,
                      ignoreOutDistances);
}
#endif

} } // namespace
