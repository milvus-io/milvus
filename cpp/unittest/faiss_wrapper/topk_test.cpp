////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include "wrapper/Topk.h"


using namespace zilliz::vecwise::engine;

constexpr float threshhold = 0.00001;

template<typename T>
void TopK_check(T *data,
                int length,
                int k,
                T *result) {

    std::vector<T> arr(data, data + length);
    sort(arr.begin(), arr.end(), std::less<T>());

    for (int i = 0; i < k; ++i) {
        ASSERT_TRUE(fabs(arr[i] - result[i]) < threshhold);
    }
}

TEST(wrapper_topk, Wrapper_Test) {
    int length = 100000;
    int k = 1000;

    float *host_input, *host_output;
    int64_t *ids;

    host_input = (float *) malloc(length * sizeof(float));
    host_output = (float *) malloc(k * sizeof(float));
    ids = (int64_t *) malloc(k * sizeof(int64_t));

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(-1.0, 1.0);
    for (int i = 0; i < length; ++i) {
        host_input[i] = 1.0 * dis(gen);
    }

    TopK(host_input, length, k, host_output, ids);
    TopK_check(host_input, length, k, host_output);
}

template<typename T>
void TopK_Test(T factor) {
    int length = 1000000;     // data length
    int k = 100;

    T *data, *out;
    int64_t *idx;
    cudaMallocManaged((void **) &data, sizeof(T) * length);
    cudaMallocManaged((void **) &out, sizeof(T) * k);
    cudaMallocManaged((void **) &idx, sizeof(int64_t) * k);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(-1.0, 1.0);

    for (int i = 0; i < length; i++) {
        data[i] = factor * dis(gen);
    }

    cudaMemAdvise(data, sizeof(T) * length, cudaMemAdviseSetReadMostly, 0);

    cudaMemPrefetchAsync(data, sizeof(T) * length, 0);

    gpu::TopK<T>(data, length, k, out, idx, nullptr);
    TopK_check<T>(data, length, k, out);

//    order_flag = Ordering::kDescending;
//    TopK<T>(data, length, k, out, idx, nullptr);
//    TopK_check<T>(data, length, k, out);

    cudaFree(data);
    cudaFree(out);
    cudaFree(idx);
}

TEST(topk_test, Wrapper_Test) {
    TopK_Test<float>(1.0);
}
