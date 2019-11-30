/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/impl/IVFAppend.cuh>
#include <faiss/impl/FaissAssert.h>
#include <faiss/gpu/utils/Float16.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/Tensor.cuh>
#include <faiss/gpu/utils/StaticUtils.h>

namespace faiss { namespace gpu {

//
// IVF list length update
//

__global__ void
runUpdateListPointers(Tensor<int, 1, true> listIds,
                      Tensor<int, 1, true> newListLength,
                      Tensor<void*, 1, true> newCodePointers,
                      Tensor<void*, 1, true> newIndexPointers,
                      int* listLengths,
                      void** listCodes,
                      void** listIndices) {
  int i = blockIdx.x * blockDim.x + threadIdx.x;

  if (i < listIds.getSize(0)) {
    int listId = listIds[i];
    listLengths[listId] = newListLength[i];
    listCodes[listId] = newCodePointers[i];
    listIndices[listId] = newIndexPointers[i];
  }
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

//
// IVF PQ append
//

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

//
// IVF flat append
//

__global__ void
ivfFlatIndicesAppend(Tensor<int, 1, true> listIds,
                     Tensor<int, 1, true> listOffset,
                     Tensor<long, 1, true> indices,
                     IndicesOptions opt,
                     void** listIndices) {
  int vec = blockIdx.x * blockDim.x + threadIdx.x;

  if (vec >= listIds.getSize(0)) {
    return;
  }

  int listId = listIds[vec];
  int offset = listOffset[vec];

  // Add vector could be invalid (contains NaNs etc)
  if (listId == -1 || offset == -1) {
    return;
  }

  long index = indices[vec];

  if (opt == INDICES_32_BIT) {
    // FIXME: there could be overflow here, but where should we check this?
    ((int*) listIndices[listId])[offset] = (int) index;
  } else if (opt == INDICES_64_BIT) {
    ((long*) listIndices[listId])[offset] = (long) index;
  }
}

template <typename Codec>
__global__ void
ivfFlatInvertedListAppend(Tensor<int, 1, true> listIds,
                          Tensor<int, 1, true> listOffset,
                          Tensor<float, 2, true> vecs,
                          void** listData,
                          Codec codec) {
  int vec = blockIdx.x;

  int listId = listIds[vec];
  int offset = listOffset[vec];

  // Add vector could be invalid (contains NaNs etc)
  if (listId == -1 || offset == -1) {
    return;
  }

  // Handle whole encoding (only thread 0 will handle the remainder)
  int limit = utils::divDown(vecs.getSize(1), Codec::kDimPerIter);

  int i;
  for (i = threadIdx.x; i < limit; i += blockDim.x) {
    int realDim = i * Codec::kDimPerIter;
    float toEncode[Codec::kDimPerIter];

#pragma unroll
    for (int j = 0; j < Codec::kDimPerIter; ++j) {
      toEncode[j] = vecs[vec][realDim + j];
    }

    codec.encode(listData[listId], offset, i, toEncode);
  }

  // Handle remainder with a single thread, if any
  if (Codec::kDimPerIter > 1) {
    int realDim = limit * Codec::kDimPerIter;

    // Was there any remainder?
    if (realDim < vecs.getSize(1)) {
      if (threadIdx.x == 0) {
        float toEncode[Codec::kDimPerIter];

        // How many remaining that we need to encode
        int remaining = vecs.getSize(1) - realDim;

#pragma unroll
        for (int j = 0; j < Codec::kDimPerIter; ++j) {
          int idx = realDim + j;
          toEncode[j] = idx < vecs.getSize(1) ? vecs[vec][idx] : 0.0f;
        }

        codec.encodePartial(listData[listId], offset, i, remaining, toEncode);
      }
    }
  }
}

void
runIVFFlatInvertedListAppend(Tensor<int, 1, true>& listIds,
                             Tensor<int, 1, true>& listOffset,
                             Tensor<float, 2, true>& vecs,
                             Tensor<long, 1, true>& indices,
                             bool useResidual,
                             Tensor<float, 2, true>& residuals,
                             GpuScalarQuantizer* scalarQ,
                             thrust::device_vector<void*>& listData,
                             thrust::device_vector<void*>& listIndices,
                             IndicesOptions indicesOptions,
                             cudaStream_t stream) {
  int dim = vecs.getSize(1);
  int maxThreads = getMaxThreadsCurrentDevice();

  // First, append the indices that we're about to add, if any
  if (indicesOptions != INDICES_CPU && indicesOptions != INDICES_IVF) {
    int blocks = utils::divUp(vecs.getSize(0), maxThreads);

    ivfFlatIndicesAppend<<<blocks, maxThreads, 0, stream>>>(
      listIds,
      listOffset,
      indices,
      indicesOptions,
      listIndices.data().get());
  }

  // Each block will handle appending a single vector
#define RUN_APPEND                                                      \
  do {                                                                  \
    dim3 grid(vecs.getSize(0));                                         \
    dim3 block(std::min(dim / codec.kDimPerIter, maxThreads));          \
                                                                        \
    ivfFlatInvertedListAppend                                           \
      <<<grid, block, 0, stream>>>(                                     \
        listIds,                                                        \
        listOffset,                                                     \
        useResidual ? residuals : vecs,                                 \
        listData.data().get(),                                          \
        codec);                                                         \
  } while (0)

  if (!scalarQ) {
    CodecFloat codec(dim * sizeof(float));
    RUN_APPEND;
  } else {
    switch (scalarQ->qtype) {
      case ScalarQuantizer::QuantizerType::QT_8bit:
      {
        if (false) {
//        if (dim % 4 == 0) {
          Codec<ScalarQuantizer::QuantizerType::QT_8bit, 4>
            codec(scalarQ->code_size,
                  scalarQ->gpuTrained.data(),
                  scalarQ->gpuTrained.data() + dim);
          RUN_APPEND;
        } else {
          Codec<ScalarQuantizer::QuantizerType::QT_8bit, 1>
            codec(scalarQ->code_size,
                  scalarQ->gpuTrained.data(),
                  scalarQ->gpuTrained.data() + dim);
          RUN_APPEND;
        }
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_8bit_uniform:
      {
//        if (dim % 4 == 0) {
        if (false) {
          Codec<ScalarQuantizer::QuantizerType::QT_8bit_uniform, 4>
            codec(scalarQ->code_size, scalarQ->trained[0], scalarQ->trained[1]);
          RUN_APPEND;
        } else {
          Codec<ScalarQuantizer::QuantizerType::QT_8bit_uniform, 1>
            codec(scalarQ->code_size, scalarQ->trained[0], scalarQ->trained[1]);
          RUN_APPEND;
        }
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_fp16:
      {
//        if (dim % 2 == 0) {
        if (false) {
          Codec<ScalarQuantizer::QuantizerType::QT_fp16, 2>
            codec(scalarQ->code_size);
          RUN_APPEND;
        } else {
          Codec<ScalarQuantizer::QuantizerType::QT_fp16, 1>
            codec(scalarQ->code_size);
          RUN_APPEND;
        }
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_8bit_direct:
      {
        Codec<ScalarQuantizer::QuantizerType::QT_8bit_direct, 1>
          codec(scalarQ->code_size);
        RUN_APPEND;
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_4bit:
      {
        Codec<ScalarQuantizer::QuantizerType::QT_4bit, 1>
          codec(scalarQ->code_size,
                scalarQ->gpuTrained.data(),
                scalarQ->gpuTrained.data() + dim);
        RUN_APPEND;
      }
      break;
      case ScalarQuantizer::QuantizerType::QT_4bit_uniform:
      {
        Codec<ScalarQuantizer::QuantizerType::QT_4bit_uniform, 1>
          codec(scalarQ->code_size, scalarQ->trained[0], scalarQ->trained[1]);
        RUN_APPEND;
      }
      break;
      default:
        // unimplemented, should be handled at a higher level
        FAISS_ASSERT(false);
    }
  }

  CUDA_TEST_ERROR();

#undef RUN_APPEND
}

} } // namespace
