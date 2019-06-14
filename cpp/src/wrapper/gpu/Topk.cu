////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "faiss/FaissAssert.h"
#include "faiss/gpu/utils/Limits.cuh"
#include "Arithmetic.h"


namespace faiss {
namespace gpu {

constexpr bool kBoolMax = zilliz::milvus::engine::kBoolMax;
constexpr bool kBoolMin = zilliz::milvus::engine::kBoolMin;

template<>
struct Limits<bool> {
    static __device__ __host__
    inline bool getMin() {
        return kBoolMin;
    }
    static __device__ __host__
    inline bool getMax() {
        return kBoolMax;
    }
};

constexpr int8_t kInt8Max = zilliz::milvus::engine::kInt8Max;
constexpr int8_t kInt8Min = zilliz::milvus::engine::kInt8Min;

template<>
struct Limits<int8_t> {
    static __device__ __host__
    inline int8_t getMin() {
        return kInt8Min;
    }
    static __device__ __host__
    inline int8_t getMax() {
        return kInt8Max;
    }
};

constexpr int16_t kInt16Max = zilliz::milvus::engine::kInt16Max;
constexpr int16_t kInt16Min = zilliz::milvus::engine::kInt16Min;

template<>
struct Limits<int16_t> {
    static __device__ __host__
    inline int16_t getMin() {
        return kInt16Min;
    }
    static __device__ __host__
    inline int16_t getMax() {
        return kInt16Max;
    }
};

constexpr int64_t kInt64Max = zilliz::milvus::engine::kInt64Max;
constexpr int64_t kInt64Min = zilliz::milvus::engine::kInt64Min;

template<>
struct Limits<int64_t> {
    static __device__ __host__
    inline int64_t getMin() {
        return kInt64Min;
    }
    static __device__ __host__
    inline int64_t getMax() {
        return kInt64Max;
    }
};

constexpr double kDoubleMax = zilliz::milvus::engine::kDoubleMax;
constexpr double kDoubleMin = zilliz::milvus::engine::kDoubleMin;

template<>
struct Limits<double> {
    static __device__ __host__
    inline double getMin() {
        return kDoubleMin;
    }
    static __device__ __host__
    inline double getMax() {
        return kDoubleMax;
    }
};

}
}

#include "faiss/gpu/utils/DeviceUtils.h"
#include "faiss/gpu/utils/MathOperators.cuh"
#include "faiss/gpu/utils/Pair.cuh"
#include "faiss/gpu/utils/Reductions.cuh"
#include "faiss/gpu/utils/Select.cuh"
#include "faiss/gpu/utils/Tensor.cuh"
#include "faiss/gpu/utils/StaticUtils.h"

#include "Topk.h"


namespace zilliz {
namespace milvus {
namespace engine {
namespace gpu {

constexpr int kWarpSize = 32;

template<typename T, int Dim, bool InnerContig>
using Tensor = faiss::gpu::Tensor<T, Dim, InnerContig>;

template<typename T, typename U>
using Pair = faiss::gpu::Pair<T, U>;


// select kernel for k == 1
template<typename T, int kRowsPerBlock, int kBlockSize>
__global__ void topkSelectMin1(Tensor<T, 2, true> productDistances,
                               Tensor<T, 2, true> outDistances,
                               Tensor<int64_t, 2, true> outIndices) {
    // Each block handles kRowsPerBlock rows of the distances (results)
    Pair<T, int64_t> threadMin[kRowsPerBlock];
    __shared__
    Pair<T, int64_t> blockMin[kRowsPerBlock * (kBlockSize / kWarpSize)];

    T distance[kRowsPerBlock];

#pragma unroll
    for (int i = 0; i < kRowsPerBlock; ++i) {
        threadMin[i].k = faiss::gpu::Limits<T>::getMax();
        threadMin[i].v = -1;
    }

    // blockIdx.x: which chunk of rows we are responsible for updating
    int rowStart = blockIdx.x * kRowsPerBlock;

    // FIXME: if we have exact multiples, don't need this
    bool endRow = (blockIdx.x == gridDim.x - 1);

    if (endRow) {
        if (productDistances.getSize(0) % kRowsPerBlock == 0) {
            endRow = false;
        }
    }

    if (endRow) {
        for (int row = rowStart; row < productDistances.getSize(0); ++row) {
            for (int col = threadIdx.x; col < productDistances.getSize(1);
                 col += blockDim.x) {
                distance[0] = productDistances[row][col];

                if (faiss::gpu::Math<T>::lt(distance[0], threadMin[0].k)) {
                    threadMin[0].k = distance[0];
                    threadMin[0].v = col;
                }
            }

            // Reduce within the block
            threadMin[0] =
                faiss::gpu::blockReduceAll<Pair<T, int64_t>, faiss::gpu::Min<Pair<T, int64_t> >, false, false>(
                    threadMin[0], faiss::gpu::Min<Pair<T, int64_t> >(), blockMin);

            if (threadIdx.x == 0) {
                outDistances[row][0] = threadMin[0].k;
                outIndices[row][0] = threadMin[0].v;
            }

            // so we can use the shared memory again
            __syncthreads();

            threadMin[0].k = faiss::gpu::Limits<T>::getMax();
            threadMin[0].v = -1;
        }
    } else {
        for (int col = threadIdx.x; col < productDistances.getSize(1);
             col += blockDim.x) {

#pragma unroll
            for (int row = 0; row < kRowsPerBlock; ++row) {
                distance[row] = productDistances[rowStart + row][col];
            }

#pragma unroll
            for (int row = 0; row < kRowsPerBlock; ++row) {
                if (faiss::gpu::Math<T>::lt(distance[row], threadMin[row].k)) {
                    threadMin[row].k = distance[row];
                    threadMin[row].v = col;
                }
            }
        }

        // Reduce within the block
        faiss::gpu::blockReduceAll<kRowsPerBlock, Pair<T, int64_t>, faiss::gpu::Min<Pair<T, int64_t> >, false, false>(
            threadMin, faiss::gpu::Min<Pair<T, int64_t> >(), blockMin);

        if (threadIdx.x == 0) {
#pragma unroll
            for (int row = 0; row < kRowsPerBlock; ++row) {
                outDistances[rowStart + row][0] = threadMin[row].k;
                outIndices[rowStart + row][0] = threadMin[row].v;
            }
        }
    }
}

// L2 + select kernel for k > 1, no re-use of ||c||^2
template<typename T, int NumWarpQ, int NumThreadQ, int ThreadsPerBlock>
__global__ void topkSelectMinK(Tensor<T, 2, true> productDistances,
                               Tensor<T, 2, true> outDistances,
                               Tensor<int64_t, 2, true> outIndices,
                               int k, T initK) {
    // Each block handles a single row of the distances (results)
    constexpr int kNumWarps = ThreadsPerBlock / kWarpSize;

    __shared__
    T smemK[kNumWarps * NumWarpQ];
    __shared__
    int64_t smemV[kNumWarps * NumWarpQ];

    faiss::gpu::BlockSelect<T, int64_t, false, faiss::gpu::Comparator<T>,
                            NumWarpQ, NumThreadQ, ThreadsPerBlock>
        heap(initK, -1, smemK, smemV, k);

    int row = blockIdx.x;

    // Whole warps must participate in the selection
    int limit = faiss::gpu::utils::roundDown(productDistances.getSize(1), kWarpSize);
    int i = threadIdx.x;

    for (; i < limit; i += blockDim.x) {
        T v = productDistances[row][i];
        heap.add(v, i);
    }

    if (i < productDistances.getSize(1)) {
        T v = productDistances[row][i];
        heap.addThreadQ(v, i);
    }

    heap.reduce();
    for (int i = threadIdx.x; i < k; i += blockDim.x) {
        outDistances[row][i] = smemK[i];
        outIndices[row][i] = smemV[i];
    }
}

// FIXME: no TVec specialization
template<typename T>
void runTopKSelectMin(Tensor<T, 2, true> &productDistances,
                      Tensor<T, 2, true> &outDistances,
                      Tensor<int64_t, 2, true> &outIndices,
                      int k,
                      cudaStream_t stream) {
    FAISS_ASSERT(productDistances.getSize(0) == outDistances.getSize(0));
    FAISS_ASSERT(productDistances.getSize(0) == outIndices.getSize(0));
    FAISS_ASSERT(outDistances.getSize(1) == k);
    FAISS_ASSERT(outIndices.getSize(1) == k);
    FAISS_ASSERT(k <= 1024);

    if (k == 1) {
        constexpr int kThreadsPerBlock = 256;
        constexpr int kRowsPerBlock = 8;

        auto block = dim3(kThreadsPerBlock);
        auto grid = dim3(faiss::gpu::utils::divUp(outDistances.getSize(0), kRowsPerBlock));

        topkSelectMin1<T, kRowsPerBlock, kThreadsPerBlock>
            << < grid, block, 0, stream >> > (productDistances, outDistances, outIndices);
    } else {
        constexpr int kThreadsPerBlock = 128;

        auto block = dim3(kThreadsPerBlock);
        auto grid = dim3(outDistances.getSize(0));

#define RUN_TOPK_SELECT_MIN(NUM_WARP_Q, NUM_THREAD_Q)                         \
    do {                                                                \
      topkSelectMinK<T, NUM_WARP_Q, NUM_THREAD_Q, kThreadsPerBlock>       \
        <<<grid, block, 0, stream>>>(productDistances, \
                                     outDistances, outIndices,          \
                                     k, faiss::gpu::Limits<T>::getMax());           \
    } while (0)

        if (k <= 32) {
            RUN_TOPK_SELECT_MIN(32, 2);
        } else if (k <= 64) {
            RUN_TOPK_SELECT_MIN(64, 3);
        } else if (k <= 128) {
            RUN_TOPK_SELECT_MIN(128, 3);
        } else if (k <= 256) {
            RUN_TOPK_SELECT_MIN(256, 4);
        } else if (k <= 512) {
            RUN_TOPK_SELECT_MIN(512, 8);
        } else if (k <= 1024) {
            RUN_TOPK_SELECT_MIN(1024, 8);
        } else {
            FAISS_ASSERT(false);
        }
    }

    CUDA_TEST_ERROR();
}

////////////////////////////////////////////////////////////
// select kernel for k == 1
template<typename T, int kRowsPerBlock, int kBlockSize>
__global__ void topkSelectMax1(Tensor<T, 2, true> productDistances,
                               Tensor<T, 2, true> outDistances,
                               Tensor<int64_t, 2, true> outIndices) {
    // Each block handles kRowsPerBlock rows of the distances (results)
    Pair<T, int64_t> threadMax[kRowsPerBlock];
    __shared__
    Pair<T, int64_t> blockMax[kRowsPerBlock * (kBlockSize / kWarpSize)];

    T distance[kRowsPerBlock];

#pragma unroll
    for (int i = 0; i < kRowsPerBlock; ++i) {
        threadMax[i].k = faiss::gpu::Limits<T>::getMin();
        threadMax[i].v = -1;
    }

    // blockIdx.x: which chunk of rows we are responsible for updating
    int rowStart = blockIdx.x * kRowsPerBlock;

    // FIXME: if we have exact multiples, don't need this
    bool endRow = (blockIdx.x == gridDim.x - 1);

    if (endRow) {
        if (productDistances.getSize(0) % kRowsPerBlock == 0) {
            endRow = false;
        }
    }

    if (endRow) {
        for (int row = rowStart; row < productDistances.getSize(0); ++row) {
            for (int col = threadIdx.x; col < productDistances.getSize(1);
                 col += blockDim.x) {
                distance[0] = productDistances[row][col];

                if (faiss::gpu::Math<T>::gt(distance[0], threadMax[0].k)) {
                    threadMax[0].k = distance[0];
                    threadMax[0].v = col;
                }
            }

            // Reduce within the block
            threadMax[0] =
                faiss::gpu::blockReduceAll<Pair<T, int64_t>, faiss::gpu::Max<Pair<T, int64_t> >, false, false>(
                    threadMax[0], faiss::gpu::Max<Pair<T, int64_t> >(), blockMax);

            if (threadIdx.x == 0) {
                outDistances[row][0] = threadMax[0].k;
                outIndices[row][0] = threadMax[0].v;
            }

            // so we can use the shared memory again
            __syncthreads();

            threadMax[0].k = faiss::gpu::Limits<T>::getMin();
            threadMax[0].v = -1;
        }
    } else {
        for (int col = threadIdx.x; col < productDistances.getSize(1);
             col += blockDim.x) {

#pragma unroll
            for (int row = 0; row < kRowsPerBlock; ++row) {
                distance[row] = productDistances[rowStart + row][col];
            }

#pragma unroll
            for (int row = 0; row < kRowsPerBlock; ++row) {
                if (faiss::gpu::Math<T>::gt(distance[row], threadMax[row].k)) {
                    threadMax[row].k = distance[row];
                    threadMax[row].v = col;
                }
            }
        }

        // Reduce within the block
        faiss::gpu::blockReduceAll<kRowsPerBlock, Pair<T, int64_t>, faiss::gpu::Max<Pair<T, int64_t> >, false, false>(
            threadMax, faiss::gpu::Max<Pair<T, int64_t> >(), blockMax);

        if (threadIdx.x == 0) {
#pragma unroll
            for (int row = 0; row < kRowsPerBlock; ++row) {
                outDistances[rowStart + row][0] = threadMax[row].k;
                outIndices[rowStart + row][0] = threadMax[row].v;
            }
        }
    }
}

// L2 + select kernel for k > 1, no re-use of ||c||^2
template<typename T, int NumWarpQ, int NumThreadQ, int ThreadsPerBlock>
__global__ void topkSelectMaxK(Tensor<T, 2, true> productDistances,
                               Tensor<T, 2, true> outDistances,
                               Tensor<int64_t, 2, true> outIndices,
                               int k, T initK) {
    // Each block handles a single row of the distances (results)
    constexpr int kNumWarps = ThreadsPerBlock / kWarpSize;

    __shared__
    T smemK[kNumWarps * NumWarpQ];
    __shared__
    int64_t smemV[kNumWarps * NumWarpQ];

    faiss::gpu::BlockSelect<T, int64_t, true, faiss::gpu::Comparator<T>,
                            NumWarpQ, NumThreadQ, ThreadsPerBlock>
        heap(initK, -1, smemK, smemV, k);

    int row = blockIdx.x;

    // Whole warps must participate in the selection
    int limit = faiss::gpu::utils::roundDown(productDistances.getSize(1), kWarpSize);
    int i = threadIdx.x;

    for (; i < limit; i += blockDim.x) {
        T v = productDistances[row][i];
        heap.add(v, i);
    }

    if (i < productDistances.getSize(1)) {
        T v = productDistances[row][i];
        heap.addThreadQ(v, i);
    }

    heap.reduce();
    for (int i = threadIdx.x; i < k; i += blockDim.x) {
        outDistances[row][i] = smemK[i];
        outIndices[row][i] = smemV[i];
    }
}

// FIXME: no TVec specialization
template<typename T>
void runTopKSelectMax(Tensor<T, 2, true> &productDistances,
                      Tensor<T, 2, true> &outDistances,
                      Tensor<int64_t, 2, true> &outIndices,
                      int k,
                      cudaStream_t stream) {
    FAISS_ASSERT(productDistances.getSize(0) == outDistances.getSize(0));
    FAISS_ASSERT(productDistances.getSize(0) == outIndices.getSize(0));
    FAISS_ASSERT(outDistances.getSize(1) == k);
    FAISS_ASSERT(outIndices.getSize(1) == k);
    FAISS_ASSERT(k <= 1024);

    if (k == 1) {
        constexpr int kThreadsPerBlock = 256;
        constexpr int kRowsPerBlock = 8;

        auto block = dim3(kThreadsPerBlock);
        auto grid = dim3(faiss::gpu::utils::divUp(outDistances.getSize(0), kRowsPerBlock));

        topkSelectMax1<T, kRowsPerBlock, kThreadsPerBlock>
            << < grid, block, 0, stream >> > (productDistances, outDistances, outIndices);
    } else {
        constexpr int kThreadsPerBlock = 128;

        auto block = dim3(kThreadsPerBlock);
        auto grid = dim3(outDistances.getSize(0));

#define RUN_TOPK_SELECT_MAX(NUM_WARP_Q, NUM_THREAD_Q)                         \
    do {                                                                \
      topkSelectMaxK<T, NUM_WARP_Q, NUM_THREAD_Q, kThreadsPerBlock>       \
        <<<grid, block, 0, stream>>>(productDistances, \
                                     outDistances, outIndices,          \
                                     k, faiss::gpu::Limits<T>::getMin());           \
    } while (0)

        if (k <= 32) {
            RUN_TOPK_SELECT_MAX(32, 2);
        } else if (k <= 64) {
            RUN_TOPK_SELECT_MAX(64, 3);
        } else if (k <= 128) {
            RUN_TOPK_SELECT_MAX(128, 3);
        } else if (k <= 256) {
            RUN_TOPK_SELECT_MAX(256, 4);
        } else if (k <= 512) {
            RUN_TOPK_SELECT_MAX(512, 8);
        } else if (k <= 1024) {
            RUN_TOPK_SELECT_MAX(1024, 8);
        } else {
            FAISS_ASSERT(false);
        }
    }

    CUDA_TEST_ERROR();
}
//////////////////////////////////////////////////////////////

template<typename T>
void runTopKSelect(Tensor<T, 2, true> &productDistances,
                   Tensor<T, 2, true> &outDistances,
                   Tensor<int64_t, 2, true> &outIndices,
                   bool dir,
                   int k,
                   cudaStream_t stream) {
    if (dir) {
        runTopKSelectMax<T>(productDistances,
                            outDistances,
                            outIndices,
                            k,
                            stream);
    } else {
        runTopKSelectMin<T>(productDistances,
                            outDistances,
                            outIndices,
                            k,
                            stream);
    }
}

template<typename T>
void TopK(T *input,
          int length,
          int k,
          T *output,
          int64_t *idx,
//          Ordering order_flag,
          cudaStream_t stream) {

//    bool dir = (order_flag == Ordering::kAscending ? false : true);
    bool dir = 0;

    Tensor<T, 2, true> t_input(input, {1, length});
    Tensor<T, 2, true> t_output(output, {1, k});
    Tensor<int64_t, 2, true> t_idx(idx, {1, k});

    runTopKSelect<T>(t_input, t_output, t_idx, dir, k, stream);
}

//INSTANTIATION_TOPK_2(bool);
//INSTANTIATION_TOPK_2(int8_t);
//INSTANTIATION_TOPK_2(int16_t);
INSTANTIATION_TOPK_2(int32_t);
//INSTANTIATION_TOPK_2(int64_t);
INSTANTIATION_TOPK_2(float);
//INSTANTIATION_TOPK_2(double);
//INSTANTIATION_TOPK(TimeInterval);
//INSTANTIATION_TOPK(Float128);
//INSTANTIATION_TOPK(char);

}

void TopK(float *host_input,
          int length,
          int k,
          float *output,
          int64_t *indices) {
    float *device_input, *device_output;
    int64_t *ids;

    cudaMalloc((void **) &device_input, sizeof(float) * length);
    cudaMalloc((void **) &device_output, sizeof(float) * k);
    cudaMalloc((void **) &ids, sizeof(int64_t) * k);

    cudaMemcpy(device_input, host_input, sizeof(float) * length, cudaMemcpyHostToDevice);

    gpu::TopK<float>(device_input, length, k, device_output, ids, nullptr);

    cudaMemcpy(output, device_output, sizeof(float) * k, cudaMemcpyDeviceToHost);
    cudaMemcpy(indices, ids, sizeof(int64_t) * k, cudaMemcpyDeviceToHost);

    cudaFree(device_input);
    cudaFree(device_output);
    cudaFree(ids);
}

}
}
}
