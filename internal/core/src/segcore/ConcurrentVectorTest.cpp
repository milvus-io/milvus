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
        c_vec.set_data_raw(total_count, vec.data(), insert_size);
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
            c_vec.set_data_raw(offset, vec.data(), insert_size);
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

namespace {
void
AppendValid(ThreadSafeValidData& valid,
            const milvus::FieldMeta& field_meta,
            const std::vector<bool>& bits) {
    milvus::DataArray data;
    for (bool b : bits) {
        data.add_valid_data(b);
    }
    valid.set_data_raw(bits.size(), &data, field_meta);
}
}  // namespace

TEST(ThreadSafeValidData, ChunkedLayout) {
    const int64_t size_per_chunk = 4;
    milvus::FieldMeta field_meta(milvus::FieldName("f"),
                                 milvus::FieldId(100),
                                 milvus::DataType::INT64,
                                 true,
                                 std::nullopt);
    ThreadSafeValidData valid(size_per_chunk);
    ASSERT_TRUE(valid.empty());

    // Appends that start mid-chunk and span chunk boundaries.
    std::vector<bool> expected;
    std::default_random_engine e(42);
    for (int round = 0; round < 20; ++round) {
        std::vector<bool> bits(e() % 11);
        for (size_t i = 0; i < bits.size(); ++i) {
            bits[i] = (e() & 1) != 0;
        }
        AppendValid(valid, field_meta, bits);
        expected.insert(expected.end(), bits.begin(), bits.end());
    }
    ASSERT_FALSE(valid.empty());

    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(valid.is_valid(i), expected[i]) << "offset " << i;
    }

    auto flat = valid.get_data();
    ASSERT_EQ(flat.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(flat[i], expected[i]) << "offset " << i;
    }

    std::vector<int64_t> offsets = {0,
                                    3,
                                    4,
                                    static_cast<int64_t>(expected.size()) - 1,
                                    -1,
                                    static_cast<int64_t>(expected.size())};
    std::unique_ptr<bool[]> out(new bool[offsets.size()]);
    valid.bulk_is_valid(offsets.data(), offsets.size(), out.get());
    for (size_t i = 0; i < offsets.size(); ++i) {
        auto offset = offsets[i];
        bool want = offset >= 0 &&
                    offset < static_cast<int64_t>(expected.size()) &&
                    expected[offset];
        ASSERT_EQ(out[i], want) << "offset " << offset;
    }
}

TEST(ThreadSafeValidData, ChunkPointerStableAcrossAppends) {
    const int64_t size_per_chunk = 8;
    milvus::FieldMeta field_meta(milvus::FieldName("f"),
                                 milvus::FieldId(100),
                                 milvus::DataType::INT64,
                                 true,
                                 std::nullopt);
    ThreadSafeValidData valid(size_per_chunk);
    AppendValid(valid, field_meta, std::vector<bool>(size_per_chunk, true));

    // A borrowed pointer into the first chunk must stay valid after many
    // appends: existing chunk buffers are never relocated.
    bool* first_chunk = valid.get_chunk_data(0);
    for (int i = 0; i < 1000; ++i) {
        AppendValid(valid, field_meta, std::vector<bool>(7, false));
    }
    ASSERT_EQ(valid.get_chunk_data(0), first_chunk);
    for (int64_t i = 0; i < size_per_chunk; ++i) {
        ASSERT_TRUE(first_chunk[i]);
    }
}
