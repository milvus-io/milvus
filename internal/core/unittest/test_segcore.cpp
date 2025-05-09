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

#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"

using namespace milvus;

namespace {
static constexpr int64_t seg_id = 101;
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
        raw_data.insert(raw_data.end(),
                        (const char*)std::begin(vec),
                        (const char*)std::end(vec));
        int age = ei() % 100;
        raw_data.insert(raw_data.end(),
                        (const char*)&age,
                        ((const char*)&age) + sizeof(age));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}
}  // namespace

TEST(SegmentCoreTest, NormalDistributionTest) {
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::INT32);
    int N = 100 * 1000;
    auto [raw_data, timestamps, uids] = generate_data(N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
}

// Test insert column-based data
TEST(SegmentCoreTest, MockTest2) {
    using namespace milvus::segcore;

    // schema
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    int N = 10000;  // number of records
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto reserved_begin = segment->PreInsert(N);
    segment->Insert(reserved_begin,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
}

TEST(SegmentCoreTest, SmallIndex) {
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::INT32);
}
