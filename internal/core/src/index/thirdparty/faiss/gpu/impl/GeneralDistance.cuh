/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/MetricType.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/gpu/impl/DistanceUtils.cuh>
#include <faiss/gpu/utils/DeviceTensor.cuh>
#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/Select.cuh>
#include <faiss/gpu/utils/BlockSelectKernel.cuh>

#include <memory>
#include <algorithm>
#include <thrust/fill.h>
#include <thrust/for_each.h>
#include <thrust/device_ptr.h>
#include <thrust/execution_policy.h>
#include <iostream>

//
// Kernels for non-L2 / inner product distances
//

namespace faiss { namespace gpu {

// Reduction tree operator
template <typename DistanceOp, int N>
struct ReduceDistanceOp {
  __device__ static DistanceOp reduce(DistanceOp ops[N]) {
    DistanceOp vals[N/2];
#pragma unroll
    for (int i = 0; i < N / 2; ++i) {
      vals[i] = ops[i * 2];
      vals[i].combine(ops[i * 2 + 1]);
    }

    return ReduceDistanceOp<DistanceOp, N/2>::reduce(vals);
  }
};

template <typename DistanceOp>
struct ReduceDistanceOp<DistanceOp, 1> {
  __device__ static DistanceOp reduce(DistanceOp ops[1]) {
    return ops[0];
  }
};

// Implements a pairwise reduction tree
template <typename T, int Unroll, int DimMultiple, typename DistanceOp>
inline __device__ DistanceOp
reduce(const DistanceOp& in,
       const T queryTile[kWarpSize][DimMultiple * kWarpSize + 1],
       const T vecTile[kWarpSize][DimMultiple * kWarpSize + 1]) {
  DistanceOp accs[Unroll];
#pragma unroll
  for (int i = 0; i < Unroll; ++i) {
    accs[i] = in.zero();
  }

  auto vecTileBase = vecTile[threadIdx.x];
  auto queryTileBase = queryTile[threadIdx.y];

#pragma unroll
  for (int i = 0; i < Unroll; ++i) {
#pragma unroll
    for (int j = 0; j < (kWarpSize * DimMultiple / Unroll); ++j) {
      int idx = i * (kWarpSize * DimMultiple / Unroll) + j;
      accs[i].handle(ConvertTo<float>::to(queryTileBase[idx]),
                     ConvertTo<float>::to(vecTileBase[idx]));
    }
  }

  return ReduceDistanceOp<DistanceOp, Unroll>::reduce(accs);
}

// Our general distance matrix "multiplication" kernel
template <typename T, typename DistanceOp, bool InnerContig>
__launch_bounds__(kWarpSize * kWarpSize)
__global__ void
generalDistance(Tensor<T, 2, InnerContig> query, // m x k
                Tensor<T, 2, InnerContig> vec, // n x k
                DistanceOp op,
                Tensor<float, 2, true> out) { // m x n
  constexpr int kDimMultiple = 1;

  __shared__ T queryTile[kWarpSize][kWarpSize * kDimMultiple + 1];
  __shared__ T vecTile[kWarpSize][kWarpSize * kDimMultiple + 1];

  // block y -> query
  // block x -> vector

  int queryBlock = blockIdx.y * kWarpSize;
  int queryThread = queryBlock + threadIdx.y;

  int vecBlock = blockIdx.x * kWarpSize;
  int vecThreadLoad = vecBlock + threadIdx.y;
  int vecThreadSave = vecBlock + threadIdx.x;

  DistanceOp acc = op.zero();

  auto queryTileBase = queryTile[threadIdx.y];
  auto vecTileBase = vecTile[threadIdx.y];

  auto queryBase = query[queryThread];
  auto vecBase = vec[vecThreadLoad];

  if ((blockIdx.x != (gridDim.x - 1)) && (blockIdx.y != (gridDim.y - 1))) {
    //
    // Interior tile
    //
    int limit = utils::roundDown(query.getSize(1), kWarpSize * kDimMultiple);

    for (int k = threadIdx.x; k < limit; k += kWarpSize * kDimMultiple) {
      // Load query tile
#pragma unroll
      for (int i = 0; i < kDimMultiple; ++i) {
        queryTileBase[threadIdx.x + i * kWarpSize] =
          queryBase[k + i * kWarpSize];
        vecTileBase[threadIdx.x + i * kWarpSize] =
          vecBase[k + i * kWarpSize];
      }

      __syncthreads();

      // thread (y, x) does (query y, vec x)
      acc.combine(
        reduce<T, 8, kDimMultiple, DistanceOp>(op, queryTile, vecTile));

      __syncthreads();
    }

    // Handle remainder
    if (limit < query.getSize(1)) {
#pragma unroll
      for (int i = 0; i < kDimMultiple; ++i) {
        int k = limit + threadIdx.x + i * kWarpSize;
        bool kInBounds = k < query.getSize(1);

        queryTileBase[threadIdx.x + i * kWarpSize] =
                             kInBounds ?
                             queryBase[k] : (T) 0; //DistanceOp::kIdentityData;

        vecTileBase[threadIdx.x + i * kWarpSize] =
                             kInBounds ?
                             vecBase[k] : (T) 0; // DistanceOp::kIdentityData;
      }

      __syncthreads();

      int remainder = query.getSize(1) - limit;

      // thread (y, x) does (query y, vec x)
#pragma unroll
      for (int i = 0; i < remainder; ++i) {
        acc.handle(ConvertTo<float>::to(queryTileBase[i]),
                   ConvertTo<float>::to(vecTile[threadIdx.x][i]));
      }
    }

    // Write out results
    out[queryThread][vecThreadSave] = acc.reduce();
  } else {
    //
    // Otherwise, we're an exterior tile
    //

    bool queryThreadInBounds = queryThread < query.getSize(0);
    bool vecThreadInBoundsLoad = vecThreadLoad < vec.getSize(0);
    bool vecThreadInBoundsSave = vecThreadSave < vec.getSize(0);
    int limit = utils::roundDown(query.getSize(1), kWarpSize);

    for (int k = threadIdx.x; k < limit; k += kWarpSize) {
      // Load query tile
      queryTileBase[threadIdx.x] =
        queryThreadInBounds ?
        queryBase[k] : (T) 0; // DistanceOp::kIdentityData;

      vecTileBase[threadIdx.x] =
        vecThreadInBoundsLoad ?
        vecBase[k] : (T) 0; // DistanceOp::kIdentityData;

      __syncthreads();

      // thread (y, x) does (query y, vec x)
#pragma unroll
      for (int i = 0; i < kWarpSize; ++i) {
        acc.handle(ConvertTo<float>::to(queryTileBase[i]),
                   ConvertTo<float>::to(vecTile[threadIdx.x][i]));
      }

      __syncthreads();
    }

    // Handle remainder
    if (limit < query.getSize(1)) {
      int k = limit + threadIdx.x;
      bool kInBounds = k < query.getSize(1);

      // Load query tile
      queryTileBase[threadIdx.x] =
                           queryThreadInBounds && kInBounds ?
                           queryBase[k] : (T) 0; // DistanceOp::kIdentityData;

      vecTileBase[threadIdx.x] =
                           vecThreadInBoundsLoad && kInBounds ?
                           vecBase[k] : (T) 0; // DistanceOp::kIdentityData;

      __syncthreads();

      int remainder = query.getSize(1) - limit;

      // thread (y, x) does (query y, vec x)
      for (int i = 0; i < remainder; ++i) {
        acc.handle(ConvertTo<float>::to(queryTileBase[i]),
                   ConvertTo<float>::to(vecTile[threadIdx.x][i]));
      }
    }

    // Write out results
    if (queryThreadInBounds && vecThreadInBoundsSave) {
      out[queryThread][vecThreadSave] = acc.reduce();
    }
  }
}


template <typename T, typename DistanceOp, bool InnerContig>
void runGeneralDistanceKernel(Tensor<T, 2, InnerContig>& vecs,
                              Tensor<T, 2, InnerContig>& query,
                              Tensor<float, 2, true>& out,
                              const DistanceOp& op,
                              cudaStream_t stream) {
  FAISS_ASSERT(vecs.getSize(1) == query.getSize(1));
  FAISS_ASSERT(out.getSize(0) == query.getSize(0));
  FAISS_ASSERT(out.getSize(1) == vecs.getSize(0));

  dim3 grid(utils::divUp(vecs.getSize(0), kWarpSize),
            utils::divUp(query.getSize(0), kWarpSize));
  dim3 block(kWarpSize, kWarpSize);

  generalDistance<<<grid, block, 0, stream>>>(query, vecs, op, out);
}

template <typename T, typename DistanceOp, bool InnerContig>
void runGeneralDistance(GpuResources* resources,
                        Tensor<T, 2, InnerContig>& centroids,
                        Tensor<T, 2, InnerContig>& queries,
                        Tensor<uint8_t, 1, true>& bitset,
                        int k,
                        const DistanceOp& op,
                        Tensor<float, 2, true>& outDistances,
                        Tensor<int, 2, true>& outIndices) {
  // The # of centroids in `centroids` based on memory layout
  auto numCentroids = centroids.getSize(0);

  // The # of queries in `queries` based on memory layout
  auto numQueries = queries.getSize(0);

  // The dimensions of the vectors to consider
  auto dim = queries.getSize(1);
  FAISS_ASSERT((numQueries == 0 || numCentroids == 0) ||
               dim == centroids.getSize(1));

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
  DeviceTensor<float, 2, true> distanceBuf1(
    mem, {tileRows, tileCols}, defaultStream);
  DeviceTensor<float, 2, true> distanceBuf2(
    mem, {tileRows, tileCols}, defaultStream);
  DeviceTensor<float, 2, true>* distanceBufs[2] =
    {&distanceBuf1, &distanceBuf2};

  DeviceTensor<float, 2, true> outDistanceBuf1(
    mem, {tileRows, numColTiles * k}, defaultStream);
  DeviceTensor<float, 2, true> outDistanceBuf2(
    mem, {tileRows, numColTiles * k}, defaultStream);
  DeviceTensor<float, 2, true>* outDistanceBufs[2] =
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
      queries.narrow(0, i, curQuerySize);

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
        sliceCentroids(centroids, true, j, curCentroidSize);

      auto distanceBufView = distanceBufs[curStream]->
        narrow(0, 0, curQuerySize).narrow(1, 0, curCentroidSize);

      auto outDistanceBufColView =
        outDistanceBufRowView.narrow(1, k * curColTile, k);
      auto outIndexBufColView =
        outIndexBufRowView.narrow(1, k * curColTile, k);

      runGeneralDistanceKernel(centroidsView,
                               queryView,
                               distanceBufView,
                               op,
                               streams[curStream]);

      // For IP, just k-select the output for this tile
      if (tileCols == numCentroids) {
        // Write into the final output
        runBlockSelect(distanceBufView,
                       bitset,
                       outDistanceView,
                       outIndexView,
                       DistanceOp::kDirection, k, streams[curStream]);
      } else {
        // Write into the intermediate output
        runBlockSelect(distanceBufView,
                       bitset,
                       outDistanceBufColView,
                       outIndexBufColView,
                       DistanceOp::kDirection, k, streams[curStream]);
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
                         bitset,
                         outDistanceView,
                         outIndexView,
                         DistanceOp::kDirection, k, streams[curStream]);
    }

    curStream = (curStream + 1) % 2;
  }

  // Have the desired ordering stream wait on the multi-stream
  streamWait({defaultStream}, streams);

  if (interrupt) {
    FAISS_THROW_MSG("interrupted");
  }

  CUDA_TEST_ERROR();
}

} } // namespace
