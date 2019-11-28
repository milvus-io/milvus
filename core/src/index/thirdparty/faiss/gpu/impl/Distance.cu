/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/impl/Distance.cuh>
#include <faiss/gpu/impl/BroadcastSum.cuh>
#include <faiss/gpu/impl/L2Norm.cuh>
#include <faiss/gpu/impl/L2Select.cuh>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/Limits.cuh>
#include <faiss/gpu/utils/MatrixMult.cuh>
#include <faiss/gpu/utils/BlockSelectKernel.cuh>

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
                                  bool centroidsRowMajor,
                                  int startCentroid,
                                  int num) {
  // Row major is (num, dim)
  // Col major is (dim, num)
  if (startCentroid == 0 &&
      num == centroids.getSize(centroidsRowMajor ? 0 : 1)) {
    return centroids;
  }

  return centroids.narrow(centroidsRowMajor ? 0 : 1, startCentroid, num);
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
                 bool centroidsRowMajor,
                 Tensor<T, 1, true>* centroidNorms,
                 Tensor<T, 2, true>& queries,
                 bool queriesRowMajor,
                 int k,
                 Tensor<T, 2, true>& outDistances,
                 Tensor<int, 2, true>& outIndices,
                 bool useHgemm,
                 bool ignoreOutDistances) {
  // The # of centroids in `centroids` based on memory layout
  auto numCentroids = centroids.getSize(centroidsRowMajor ? 0 : 1);

  // The # of queries in `queries` based on memory layout
  auto numQueries = queries.getSize(queriesRowMajor ? 0 : 1);

  // The dimensions of the vectors to consider
  auto dim = queries.getSize(queriesRowMajor ? 1 : 0);
  FAISS_ASSERT((numQueries == 0 || numCentroids == 0) ||
               dim == centroids.getSize(centroidsRowMajor ? 1 : 0));

  FAISS_ASSERT(outDistances.getSize(0) == numQueries);
  FAISS_ASSERT(outIndices.getSize(0) == numQueries);
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
    cNorms =
      std::move(DeviceTensor<T, 1, true>(mem,
                                         {numCentroids}, defaultStream));
    runL2Norm(centroids, centroidsRowMajor, cNorms, true, defaultStream);
    centroidNorms = &cNorms;
  }

  //
  // Prepare norm vector ||q||^2; ||c||^2 is already pre-computed
  //
  int qNormSize[1] = {numQueries};
  DeviceTensor<T, 1, true> queryNorms(mem, qNormSize, defaultStream);

  // ||q||^2
  if (computeL2) {
    runL2Norm(queries, queriesRowMajor, queryNorms, true, defaultStream);
  }

  // By default, aim to use up to 512 MB of memory for the processing, with both
  // number of queries and number of centroids being at least 512.
  int tileRows = 0;
  int tileCols = 0;
  chooseTileSize(numQueries,
                 numCentroids,
                 dim,
                 sizeof(T),
                 mem.getSizeAvailable(),
                 tileRows,
                 tileCols);

  int numColTiles = utils::divUp(numCentroids, tileCols);

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
  for (int i = 0; i < numQueries; i += tileRows) {
    if (interrupt || InterruptCallback::is_interrupted()) {
      interrupt = true;
      break;
    }

    int curQuerySize = std::min(tileRows, numQueries - i);

    auto outDistanceView =
      outDistances.narrow(0, i, curQuerySize);
    auto outIndexView =
      outIndices.narrow(0, i, curQuerySize);

    auto queryView =
      queries.narrow(queriesRowMajor ? 0 : 1, i, curQuerySize);
    auto queryNormNiew =
      queryNorms.narrow(0, i, curQuerySize);

    auto outDistanceBufRowView =
      outDistanceBufs[curStream]->narrow(0, 0, curQuerySize);
    auto outIndexBufRowView =
      outIndexBufs[curStream]->narrow(0, 0, curQuerySize);

    // Tile over the centroids
    for (int j = 0; j < numCentroids; j += tileCols) {
      if (InterruptCallback::is_interrupted()) {
        interrupt = true;
        break;
      }

      int curCentroidSize = std::min(tileCols, numCentroids - j);
      int curColTile = j / tileCols;

      auto centroidsView =
        sliceCentroids(centroids, centroidsRowMajor, j, curCentroidSize);

      auto distanceBufView = distanceBufs[curStream]->
        narrow(0, 0, curQuerySize).narrow(1, 0, curCentroidSize);

      auto outDistanceBufColView =
        outDistanceBufRowView.narrow(1, k * curColTile, k);
      auto outIndexBufColView =
        outIndexBufRowView.narrow(1, k * curColTile, k);

      // L2: distance is ||c||^2 - 2qc + ||q||^2, we compute -2qc
      // IP: just compute qc
      // (query id x dim) x (centroid id, dim)' = (query id, centroid id)
      runMatrixMult(distanceBufView,
                    false, // not transposed
                    queryView,
                    !queriesRowMajor, // transposed MM if col major
                    centroidsView,
                    centroidsRowMajor, // transposed MM if row major
                    computeL2 ? -2.0f : 1.0f,
                    0.0f,
                    useHgemm,
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
        if (tileCols == numCentroids) {
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
          auto centroidNormsView = centroidNorms->narrow(0, j, curCentroidSize);

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
        if (tileCols == numCentroids) {
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
    if (tileCols != numCentroids) {
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
                   bool centroidsRowMajor,
                   Tensor<T, 1, true>* centroidNorms,
                   Tensor<T, 2, true>& queries,
                   bool queriesRowMajor,
                   int k,
                   Tensor<T, 2, true>& outDistances,
                   Tensor<int, 2, true>& outIndices,
                   bool useHgemm,
                   bool ignoreOutDistances = false) {
  runDistance<T>(true, // L2
                 resources,
                 centroids,
                 centroidsRowMajor,
                 centroidNorms,
                 queries,
                 queriesRowMajor,
                 k,
                 outDistances,
                 outIndices,
                 useHgemm,
                 ignoreOutDistances);
}

template <typename T>
void runIPDistance(GpuResources* resources,
                   Tensor<T, 2, true>& centroids,
                   bool centroidsRowMajor,
                   Tensor<T, 2, true>& queries,
                   bool queriesRowMajor,
                   int k,
                   Tensor<T, 2, true>& outDistances,
                   Tensor<int, 2, true>& outIndices,
                   bool useHgemm) {
  runDistance<T>(false, // IP
                 resources,
                 centroids,
                 centroidsRowMajor,
                 nullptr, // no centroid norms provided
                 queries,
                 queriesRowMajor,
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
              bool vectorsRowMajor,
              Tensor<float, 2, true>& queries,
              bool queriesRowMajor,
              int k,
              Tensor<float, 2, true>& outDistances,
              Tensor<int, 2, true>& outIndices) {
  runIPDistance<float>(resources,
                       vectors,
                       vectorsRowMajor,
                       queries,
                       queriesRowMajor,
                       k,
                       outDistances,
                       outIndices,
                       false);
}

void
runIPDistance(GpuResources* resources,
              Tensor<half, 2, true>& vectors,
              bool vectorsRowMajor,
              Tensor<half, 2, true>& queries,
              bool queriesRowMajor,
              int k,
              Tensor<half, 2, true>& outDistances,
              Tensor<int, 2, true>& outIndices,
              bool useHgemm) {
  runIPDistance<half>(resources,
                      vectors,
                      vectorsRowMajor,
                      queries,
                      queriesRowMajor,
                      k,
                      outDistances,
                      outIndices,
                      useHgemm);
}

void
runL2Distance(GpuResources* resources,
              Tensor<float, 2, true>& vectors,
              bool vectorsRowMajor,
              Tensor<float, 1, true>* vectorNorms,
              Tensor<float, 2, true>& queries,
              bool queriesRowMajor,
              int k,
              Tensor<float, 2, true>& outDistances,
              Tensor<int, 2, true>& outIndices,
              bool ignoreOutDistances) {
  runL2Distance<float>(resources,
                       vectors,
                       vectorsRowMajor,
                       vectorNorms,
                       queries,
                       queriesRowMajor,
                       k,
                       outDistances,
                       outIndices,
                       false,
                       ignoreOutDistances);
}

void
runL2Distance(GpuResources* resources,
              Tensor<half, 2, true>& vectors,
              bool vectorsRowMajor,
              Tensor<half, 1, true>* vectorNorms,
              Tensor<half, 2, true>& queries,
              bool queriesRowMajor,
              int k,
              Tensor<half, 2, true>& outDistances,
              Tensor<int, 2, true>& outIndices,
              bool useHgemm,
              bool ignoreOutDistances) {
  runL2Distance<half>(resources,
                      vectors,
                      vectorsRowMajor,
                      vectorNorms,
                      queries,
                      queriesRowMajor,
                      k,
                      outDistances,
                      outIndices,
                      useHgemm,
                      ignoreOutDistances);
}

} } // namespace
