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

// Test insert row-based data
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

// Test insert column-based data
TEST(SegmentCoreTest, MockTest2) {
   using namespace milvus::segcore;
   using namespace milvus::engine;

   // schema
   auto schema = std::make_shared<Schema>();
   schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
   schema->AddDebugField("age", DataType::INT32);

   // generate random row-based data
   std::vector<char> row_data;
   std::vector<Timestamp> timestamps;
   std::vector<int64_t> uids;
   int N = 10000; // number of records
   std::default_random_engine e(67);
   for (int i = 0; i < N; ++i) {
       uids.push_back(100000 + i);
       timestamps.push_back(0);
       // append vec
       float vec[16];
       for (auto& x : vec) {
           x = e() % 2000 * 0.001 - 1.0;
       }
       row_data.insert(row_data.end(), (const char*)std::begin(vec), (const char*)std::end(vec));
       int age = e() % 100;
       row_data.insert(row_data.end(), (const char*)&age, ((const char*)&age) + sizeof(age));
   }
   auto line_sizeof = (sizeof(int) + sizeof(float) * 16);
   assert(row_data.size() == line_sizeof * N);

   int64_t size = N;
   const int64_t* uids_raw = uids.data();
   const Timestamp* timestamps_raw = timestamps.data();
   std::vector<std::tuple<Timestamp, idx_t, int64_t>> ordering(size); // timestamp, pk, order_index
   for (int i = 0; i < size; ++i) {
      ordering[i] = std::make_tuple(timestamps_raw[i], uids_raw[i], i);
   }
   std::sort(ordering.begin(), ordering.end()); // sort according to timestamp

   // convert row-based data to column-based data accordingly
   auto sizeof_infos = schema->get_sizeof_infos();
   std::vector<int> offset_infos(schema->size() + 1, 0);
   std::partial_sum(sizeof_infos.begin(), sizeof_infos.end(), offset_infos.begin() + 1);
   std::vector<aligned_vector<uint8_t>> entities(schema->size());

   for (int fid = 0; fid < schema->size(); ++fid) {
      auto len = sizeof_infos[fid];
      entities[fid].resize(len * size);
   }

   auto raw_data = row_data.data();
   std::vector<idx_t> sorted_uids(size);
   std::vector<Timestamp> sorted_timestamps(size);
   for (int index = 0; index < size; ++index) {
      auto [t, uid, order_index] = ordering[index];
      sorted_timestamps[index] = t;
      sorted_uids[index] = uid;
      for (int fid = 0; fid < schema->size(); ++fid) {
         auto len = sizeof_infos[fid];
         auto offset = offset_infos[fid];
         auto src = raw_data + order_index * line_sizeof + offset;
         auto dst = entities[fid].data() + index * len;
         memcpy(dst, src, len);
      }
   }

   // insert column-based data
   ColumnBasedRawData data_chunk{entities, N};
   auto segment = CreateGrowingSegment(schema);
   auto reserved_begin = segment->PreInsert(N);
   segment->Insert(reserved_begin, size, sorted_uids.data(), sorted_timestamps.data(), data_chunk);
}

TEST(SegmentCoreTest, SmallIndex) {
    using namespace milvus::segcore;
    using namespace milvus::engine;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
    schema->AddDebugField("age", DataType::INT32);
}
