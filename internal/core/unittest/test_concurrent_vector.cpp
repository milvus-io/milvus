// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "segcore/ConcurrentVector.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/AckResponder.h"
#include "common/Utils.h"

using namespace milvus::segcore;
using std::vector;

TEST(ConcurrentVector, TestSingle) {
    auto dim = 8;
    ConcurrentVectorImpl<int, false> c_vec(dim, 32);
    std::default_random_engine e(42);
    int data = 0;
    auto total_count = 0;
    for (int i = 0; i < 10000; ++i) {
        int insert_size = e() % 150;
        vector<int> vec(insert_size * dim);
        for (auto& x : vec) {
            x = data++;
        }
        c_vec.grow_to_at_least(total_count + insert_size);
        c_vec.set_data(total_count, vec.data(), insert_size);
        total_count += insert_size;
    }
    ASSERT_EQ(c_vec.num_chunk(), (total_count + 31) / 32);
    for (int i = 0; i < total_count; ++i) {
        for (int d = 0; d < dim; ++d) {
            auto std_data = d + i * dim;
            ASSERT_EQ(c_vec.get_element(i)[d], std_data);
        }
    }
}

TEST(ConcurrentVector, TestMultithreads) {
    auto dim = 8;
    constexpr int threads = 16;
    std::vector<int64_t> total_counts(threads);

    ConcurrentVectorImpl<int64_t, false> c_vec(dim, 32);
    std::atomic<int64_t> ack_counter = 0;

    auto executor = [&](int thread_id) {
        std::default_random_engine e(42 + thread_id);
        int64_t data = 0;
        int64_t total_count = 0;
        for (int i = 0; i < 2000; ++i) {
            int insert_size = e() % 150;
            vector<int64_t> vec(insert_size * dim);
            for (auto& x : vec) {
                x = data++ * threads + thread_id;
            }
            auto offset = ack_counter.fetch_add(insert_size);
            c_vec.grow_to_at_least(offset + insert_size);
            c_vec.set_data(offset, vec.data(), insert_size);
            total_count += insert_size;
        }
        assert(data == total_count * dim);
        total_counts[thread_id] = total_count;
    };
    std::vector<std::thread> pool;
    for (int i = 0; i < threads; ++i) {
        pool.emplace_back(executor, i);
    }
    for (auto& thread : pool) {
        thread.join();
    }

    std::vector<int64_t> counts(threads);
    auto N = ack_counter.load();
    for (int64_t i = 0; i < N; ++i) {
        for (int d = 0; d < dim; ++d) {
            auto data = c_vec.get_element(i)[d];
            auto thread_id = data % threads;
            auto raw_data = data / threads;
            auto std_data = counts[thread_id]++;
            ASSERT_EQ(raw_data, std_data) << data;
        }
    }
}

TEST(ConcurrentVector, TestAckSingle) {
    std::vector<std::tuple<int64_t, int64_t, int64_t>> raw_data;
    std::default_random_engine e(42);
    AckResponder ack;
    int N = 10000;
    for (int i = 0; i < 10000; ++i) {
        auto weight = i + e() % 100;
        raw_data.emplace_back(weight, i, (i + 1));
    }
    std::sort(raw_data.begin(), raw_data.end());
    for (auto [_, b, e] : raw_data) {
        EXPECT_LE(ack.GetAck(), b);
        ack.AddSegment(b, e);
        auto seg = ack.GetAck();
        EXPECT_GE(seg + 100, b);
    }
    EXPECT_EQ(ack.GetAck(), N);
}

template <typename T>
bool
eq(const boost::container::vector<T>& v1, const boost::container::vector<T>& v2)  {
    if (v1.size() != v2.size()) {
        return false;
    }
    auto l = v1.size();
    for (int i = 0; i < l; i++) {
        if (v1[i] != v2[i]) {
            return false;
        }
    }
    return true;
}

TEST(ConcurrentVectorImpl, getChunks) {
    auto size_per_chunk = 8;
    auto size_per_index_chunk = size_per_chunk * 2;

    auto dim = 4;
    auto n = size_per_index_chunk * 2;

    auto num_chunk = milvus::upper_div(n, size_per_chunk);
    std::vector<int64_t> chunks(num_chunk);
    for (auto i = 0; i < num_chunk; i++) {
        chunks[i] = i;
    }

    std::default_random_engine e(42);

    boost::container::vector<int64_t> int64_raw_data(n);
    boost::container::vector<float> fvec_raw_data(dim * n);
    boost::container::vector<std::string> str_raw_data(n);

    for (auto x = n - 1; x >= 0; x--) {
        int64_raw_data[x] = x;
        str_raw_data[x] = std::to_string(x);
    }

    for (int i = 0; i < n * dim; i++) {
        fvec_raw_data[i] = e();
    }

    {
        // POD.
        ConcurrentVector<int64_t> vec(size_per_chunk);
        vec.grow_to_at_least(n);
        for (int i = 0; i < num_chunk; i++) {
            auto offset = i * size_per_chunk;
            vec.set_data_raw(offset, int64_raw_data.data() + offset, size_per_chunk);
        }
        auto big_chunk = vec.get_chunks(chunks);

        ASSERT_TRUE(eq(int64_raw_data, big_chunk));
    }

    {
        // Float Vector.
        ConcurrentVector<milvus::FloatVector> vec(dim, size_per_chunk);
        vec.grow_to_at_least(n);
        for (int i = 0; i < num_chunk; i++) {
            auto offset = i * size_per_chunk;
            vec.set_data_raw(offset, fvec_raw_data.data() + offset * dim, size_per_chunk);
        }
        auto big_chunk = vec.get_chunks(chunks);

        ASSERT_TRUE(eq(fvec_raw_data, big_chunk));
    }

    {
        // string.
        ConcurrentVector<std::string> vec(size_per_chunk);
        vec.grow_to_at_least(n);
        for (int i = 0; i < num_chunk; i++) {
            auto offset = i * size_per_chunk;
            vec.set_data_raw(offset, str_raw_data.data() + offset, size_per_chunk);
        }
        auto big_chunk = vec.get_chunks(chunks);

        ASSERT_TRUE(eq(str_raw_data, big_chunk));
    }
}
