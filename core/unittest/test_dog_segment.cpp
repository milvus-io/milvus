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

#include <iostream>
#include <string>

// #include "knowhere/index/vector_index/helpers/IndexParameter.h"
// #include "segment/SegmentReader.h"
// #include "segment/SegmentWriter.h"
#include "dog_segment/SegmentBase.h"
// #include "utils/Json.h"
#include <random>
using std::cin;
using std::cout;
using std::endl;


TEST(DogSegmentTest, TestABI) {
    using namespace milvus::engine;
    using namespace milvus::dog_segment;
    ASSERT_EQ(TestABI(), 42);
    assert(true);
}


TEST(DogSegmentTest, MockTest) {
    using namespace milvus::dog_segment;
    using namespace milvus::engine;
    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    schema->AddField("age", DataType::INT32);
    std::vector<char> raw_data;
    std::vector<Timestamp> timestamps;
    std::vector<int64_t> uids;
    int N = 10000;
    std::default_random_engine e(67);
    for(int i = 0; i < N; ++i) {
        uids.push_back(100000 + i);
        timestamps.push_back(0);
        // append vec
        float vec[16];
        for(auto &x: vec) {
            x = e() % 2000 * 0.001 - 1.0;
        }
        raw_data.insert(raw_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
        int age = e() % 100;
        raw_data.insert(raw_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
    }
    auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
    assert(raw_data.size() == line_sizeof * N);


    // auto index_meta = std::make_shared<IndexMeta>(schema);
    auto segment = CreateSegment(schema, nullptr);

    DogDataChunk data_chunk{raw_data.data(), (int)line_sizeof, N};
    auto offset = segment->PreInsert(N);
    segment->Insert(offset, N, uids.data(), timestamps.data(), data_chunk);
    QueryResult query_result;
//    segment->Query(nullptr, 0, query_result);
    segment->Close();
//    segment->BuildIndex();
    int i = 0;
    i++;
}

