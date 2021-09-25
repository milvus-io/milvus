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
#include "segcore/SegmentGrowing.h"
#include "test_utils/DataGen.h"

using namespace milvus;

namespace {
auto
generate_data(int N) {
    std::vector<char> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    std::default_random_engine er(42);
    std::normal_distribution<> distribution(0.0, 1.0);
    std::default_random_engine ei(42);

    for (int i = 0; i < N; ++i) {
        uids.push_back(10 * N + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for (auto& x : vec) {
            x = distribution(er);
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = ei() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}
}  // namespace

TEST(SegmentCoreTest, TestABI) {
    using namespace milvus::engine;
    using namespace milvus::segcore;
    ASSERT_EQ(TestABI(), 42);
    assert(true);
}

TEST(SegmentCoreTest, NormalDistributionTest) {
    using namespace milvus::segcore;
    using namespace milvus::engine;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
    schema->AddDebugField("age", DataType::INT32);
    int N = 100 * 1000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto segment = CreateGrowingSegment(schema);
    segment->PreInsert(N);
    segment->PreDelete(N);
}

TEST(SegmentCoreTest, MockTest) {
    using namespace milvus::segcore;
    using namespace milvus::engine;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
    schema->AddDebugField("age", DataType::INT32);
    std::vector<char> raw_data;
    std::vector<Timestamp> timestamps;
    std::vector<int64_t> uids;
    int N = 10000;
    std::default_random_engine e(67);
    for (int i = 0; i < N; ++i) {
        uids.push_back(100000 + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for (auto& x : vec) {
            x = e() % 2000 * 0.001 - 1.0;
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }
    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
    assert(raw_data.size() == line_sizeof * N);

    // auto index_meta = std::make_shared<IndexMeta>(schema);
    auto segment = CreateGrowingSegment(schema);

    RowBasedRawData data_chunk{raw_data.data(), (int)line_sizeof, N};
    auto offset = segment->PreInsert(N);
    segment->Insert(offset, N, uids.data(), timestamps.data(), data_chunk);
    SearchResult search_result;
    // segment->Query(nullptr, 0, query_result);
    // segment->BuildIndex();
    int i = 0;
    i++;
}

TEST(SegmentCoreTest, SmallIndex) {
    using namespace milvus::segcore;
    using namespace milvus::engine;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
    schema->AddDebugField("age", DataType::INT32);
}
