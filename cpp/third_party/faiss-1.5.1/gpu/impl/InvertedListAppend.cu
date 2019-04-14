/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "InvertedListAppend.cuh"
#include "../../FaissAssert.h"
#include "../utils/Float16.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/Tensor.cuh"
#include "../utils/StaticUtils.h"

namespace faiss { namespace gpu {

__global__ void
runUpdateListPointers(Tensor<int, 1, true> listIds,
                      Tensor<int, 1, true> newListLength,
                      Tensor<void*, 1, true> newCodePointers,
                      Tensor<void*, 1, true> newIndexPointers,
                      int* listLengths,
                      void** listCodes,
                      void** listIndices) {
  int index = blockIdx.x * blockDim.x + threadIdx.x;

  if (index >= listIds.getSize(0)) {
    return;
  }

  int listId = listIds[index];
  listLengths[listId] = newListLength[index];
  listCodes[listId] = newCodePointers[index];
  listIndices[listId] = newIndexPointers[index];
}

void
runUpdateListPointers(Tensor<int, 1, true>& listIds,
                      Tensor<int, 1, true>& newListLength,
                      Tensor<void*, 1, true>& newCodePointers,
                      Tensor<void*, 1, true>& newIndexPointers,
                      thrust::device_vector<int>& listLengths,
                      thrust::device_vector<void*>& listCodes,
                      thrust::device_vector<void*>& listIndices,
                      cudaStream_t stream) {
  int numThreads = std::min(listIds.getSize(0), getMaxThreadsCurrentDevice());
  int numBlocks = utils::divUp(listIds.getSize(0), numThreads);

  dim3 grid(numBlocks);
  dim3 block(numThreads);

  runUpdateListPointers<<<grid, block, 0, stream>>>(
    listIds, newListLength, newCodePointers, newIndexPointers,
    listLengths.data().get(),
    listCodes.data().get(),
    listIndices.data().get());

  CUDA_TEST_ERROR();
}

template <IndicesOptions Opt>
__global__ void
ivfpqInvertedListAppend(Tensor<int, 1, true> listIds,
                        Tensor<int, 1, true> listOffset,
                        Tensor<int, 2, true> encodings,
                        Tensor<long, 1, true> indices,
                        void** listCodes,
                        void** listIndices) {
  int encodingToAdd = blockIdx.x * blockDim.x + threadIdx.x;

  if (encodingToAdd >= listIds.getSize(0)) {
    return;
  }

  int listId = listIds[encodingToAdd];
  int offset = listOffset[encodingToAdd];

  // Add vector could be invalid (contains NaNs etc)
  if (listId == -1 || offset == -1) {
    return;
  }

  auto encoding = encodings[encodingToAdd];
  long index = indices[encodingToAdd];

  if (Opt == INDICES_32_BIT) {
    // FIXME: there could be overflow here, but where should we check this?
    ((int*) listIndices[listId])[offset] = (int) index;
  } else if (Opt == INDICES_64_BIT) {
    ((long*) listIndices[listId])[offset] = (long) index;
  } else {
    // INDICES_CPU or INDICES_IVF; no indices are being stored
  }

  unsigned char* codeStart =
    ((unsigned char*) listCodes[listId]) + offset * encodings.getSize(1);

  // FIXME: slow
  for (int i = 0; i < encodings.getSize(1); ++i) {
    codeStart[i] = (unsigned char) encoding[i];
  }
}

void
runIVFPQInvertedListAppend(Tensor<int, 1, true>& listIds,
                           Tensor<int, 1, true>& listOffset,
                           Tensor<int, 2, true>& encodings,
                           Tensor<long, 1, true>& indices,
                           thrust::device_vector<void*>& listCodes,
                           thrust::device_vector<void*>& listIndices,
                           IndicesOptions indicesOptions,
                           cudaStream_t stream) {
  int numThreads = std::min(listIds.getSize(0), getMaxThreadsCurrentDevice());
  int numBlocks = utils::divUp(listIds.getSize(0), numThreads);

  dim3 grid(numBlocks);
  dim3 block(numThreads);

#define RUN_APPEND(IND)                                         \
  do {                                                          \
    ivfpqInvertedListAppend<IND><<<grid, block, 0, stream>>>(   \
      listIds, listOffset, encodings, indices,                  \
      listCodes.data().get(),                                   \
      listIndices.data().get());                                \
  } while (0)

  if ((indicesOptions == INDICES_CPU) || (indicesOptions == INDICES_IVF)) {
    // no need to maintain indices on the GPU
    RUN_APPEND(INDICES_IVF);
  } else if (indicesOptions == INDICES_32_BIT) {
    RUN_APPEND(INDICES_32_BIT);
  } else if (indicesOptions == INDICES_64_BIT) {
    RUN_APPEND(INDICES_64_BIT);
  } else {
    // unknown index storage type
    FAISS_ASSERT(false);
  }

  CUDA_TEST_ERROR();

#undef RUN_APPEND
}

template <IndicesOptions Opt, bool Exact, bool Float16>
__global__ void
ivfFlatInvertedListAppend(Tensor<int, 1, true> listIds,
                          Tensor<int, 1, true> listOffset,
                          Tensor<float, 2, true> vecs,
                          Tensor<long, 1, true> indices,
                          void** listData,
                          void** listIndices) {
  int vec = blockIdx.x;

  int listId = listIds[vec];
  int offset = listOffset[vec];

  // Add vector could be invalid (contains NaNs etc)
  if (listId == -1 || offset == -1) {
    return;
  }

  if (threadIdx.x == 0) {
    long index = indices[vec];

    if (Opt == INDICES_32_BIT) {
      // FIXME: there could be overflow here, but where should we check this?
      ((int*) listIndices[listId])[offset] = (int) index;
    } else if (Opt == INDICES_64_BIT) {
      ((long*) listIndices[listId])[offset] = (long) index;
    } else {
      // INDICES_CPU or INDICES_IVF; no indices are being stored
    }
  }

#ifdef FAISS_USE_FLOAT16
  // FIXME: should use half2 for better memory b/w
  if (Float16) {
    half* vecStart = ((half*) listData[listId]) + offset * vecs.getSize(1);

    if (Exact) {
      vecStart[threadIdx.x] = __float2half(vecs[vec][threadIdx.x]);
    } else {
      for (int i = threadIdx.x; i < vecs.getSize(1); i += blockDim.x) {
        vecStart[i] = __float2half(vecs[vec][i]);
      }
    }
  }
#else
  static_assert(!Float16, "float16 unsupported");
#endif

  if (!Float16) {
    float* vecStart = ((float*) listData[listId]) + offset * vecs.getSize(1);

    if (Exact) {
      vecStart[threadIdx.x] = vecs[vec][threadIdx.x];
    } else {
      for (int i = threadIdx.x; i < vecs.getSize(1); i += blockDim.x) {
        vecStart[i] = vecs[vec][i];
      }
    }
  }
}

void
runIVFFlatInvertedListAppend(Tensor<int, 1, true>& listIds,
                             Tensor<int, 1, true>& listOffset,
                             Tensor<float, 2, true>& vecs,
                             Tensor<long, 1, true>& indices,
                             bool useFloat16,
                             thrust::device_vector<void*>& listData,
                             thrust::device_vector<void*>& listIndices,
                             IndicesOptions indicesOptions,
                             cudaStream_t stream) {
  int maxThreads = getMaxThreadsCurrentDevice();
  bool exact = vecs.getSize(1) <= maxThreads;

  // Each block will handle appending a single vector
  dim3 grid(vecs.getSize(0));
  dim3 block(std::min(vecs.getSize(1), maxThreads));

#define RUN_APPEND_OPT(OPT, EXACT, FLOAT16)                             \
  do {                                                                  \
    ivfFlatInvertedListAppend<OPT, EXACT, FLOAT16>                      \
      <<<grid, block, 0, stream>>>(                                     \
        listIds, listOffset, vecs, indices,                             \
        listData.data().get(),                                          \
        listIndices.data().get());                                      \
  } while (0)                                                           \

#define RUN_APPEND(EXACT, FLOAT16)                                      \
  do {                                                                  \
    if ((indicesOptions == INDICES_CPU) || (indicesOptions == INDICES_IVF)) { \
      /* no indices are maintained on the GPU */                        \
      RUN_APPEND_OPT(INDICES_IVF, EXACT, FLOAT16);                      \
    } else if (indicesOptions == INDICES_32_BIT) {                      \
      RUN_APPEND_OPT(INDICES_32_BIT, EXACT, FLOAT16);                   \
    } else if (indicesOptions == INDICES_64_BIT) {                      \
      RUN_APPEND_OPT(INDICES_64_BIT, EXACT, FLOAT16);                   \
    } else {                                                            \
      FAISS_ASSERT(false);                                              \
    }                                                                   \
  } while (0);

  if (useFloat16) {
#ifdef FAISS_USE_FLOAT16
    if (exact) {
      RUN_APPEND(true, true);
    } else {
      RUN_APPEND(false, true);
    }
#else
    // no float16 support
    FAISS_ASSERT(false);
#endif
  } else {
    if (exact) {
      RUN_APPEND(true, false);
    } else {
      RUN_APPEND(false, false);
    }
  }

  CUDA_TEST_ERROR();

#undef RUN_APPEND
#undef RUN_APPEND_OPT
}

} } // namespace
