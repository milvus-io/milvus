// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <faiss/utils/distances.h>
#include "segcore/ConcurrentVector.h"
#include "segcore/SegmentBase.h"
// #include "knowhere/index/vector_index/helpers/IndexParameter.h"

#include "segcore/SegmentBase.h"
#include "segcore/AckResponder.h"
#include <knowhere/index/vector_index/VecIndex.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <knowhere/index/vector_index/IndexIVF.h>
#include <algorithm>
#include <chrono>
#include "test_utils/Timer.h"
#include "segcore/Reduce.h"
#include "test_utils/DataGen.h"
#include "query/BruteForceSearch.h"

using std::cin;
using std::cout;
using std::endl;
using namespace milvus::engine;
using namespace milvus::segcore;
using std::vector;
using namespace milvus;

namespace {
template <int DIM>
auto
generate_data(int N) {
    std::vector<float> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    std::default_random_engine er(42);
    std::uniform_real_distribution<> distribution(0.0, 1.0);
    std::default_random_engine ei(42);
    for (int i = 0; i < N; ++i) {
        uids.push_back(10 * N + i);
        timestamps.push_back(0);
        // append vec
        vector<float> vec(DIM);
        for (auto& x : vec) {
            x = distribution(er);
        }
        raw_data.insert(raw_data.end(), std::begin(vec), std::end(vec));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}
}  // namespace

TEST(Indexing, SmartBruteForce) {
    // how to ?
    // I'd know
    constexpr int N = 100000;
    constexpr int DIM = 16;
    constexpr int TOPK = 10;

    auto bitmap = std::make_shared<faiss::ConcurrentBitset>(N);
    // exclude the first
    for (int i = 0; i < N / 2; ++i) {
        bitmap->set(i);
    }

    auto [raw_data, timestamps, uids] = generate_data<DIM>(N);
    auto total_count = DIM * TOPK;
    auto raw = (const float*)raw_data.data();
    AssertInfo(raw, "wtf");

    constexpr int64_t queries = 3;
    auto heap = faiss::float_maxheap_array_t{};

    auto query_data = raw;

    vector<int64_t> final_uids(total_count, -1);
    vector<float> final_dis(total_count, std::numeric_limits<float>::max());

    for (int beg = 0; beg < N; beg += DefaultElementPerChunk) {
        vector<int64_t> buf_uids(total_count, -1);
        vector<float> buf_dis(total_count, std::numeric_limits<float>::max());
        faiss::float_maxheap_array_t buf = {queries, TOPK, buf_uids.data(), buf_dis.data()};
        auto end = beg + DefaultElementPerChunk;
        if (end > N) {
            end = N;
        }
        auto nsize = end - beg;
        auto src_data = raw + beg * DIM;

        faiss::knn_L2sqr(query_data, src_data, DIM, queries, nsize, &buf, nullptr);
        for (auto& x : buf_uids) {
            x = uids[x + beg];
        }
        merge_into(queries, TOPK, final_dis.data(), final_uids.data(), buf_dis.data(), buf_uids.data());
    }

    for (int qn = 0; qn < queries; ++qn) {
        for (int kn = 0; kn < TOPK; ++kn) {
            auto index = qn * TOPK + kn;
            cout << final_uids[index] << "->" << final_dis[index] << endl;
        }
        cout << endl;
    }
}

TEST(Indexing, DISABLED_Naive) {
    constexpr int N = 10000;
    constexpr int DIM = 16;
    constexpr int TOPK = 10;

    auto [raw_data, timestamps, uids] = generate_data<DIM>(N);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
                                                                         knowhere::IndexMode::MODE_CPU);

    auto conf = milvus::knowhere::Config{
        {knowhere::meta::DIM, DIM},
        {knowhere::meta::TOPK, TOPK},
        {knowhere::IndexParams::nlist, 100},
        {knowhere::IndexParams::nprobe, 4},
        {knowhere::IndexParams::m, 4},
        {knowhere::IndexParams::nbits, 8},
        {knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
        {knowhere::meta::DEVICEID, 0},
    };

    //    auto ds = knowhere::GenDataset(N, DIM, raw_data.data());
    //    auto ds2 = knowhere::GenDatasetWithIds(N / 2, DIM, raw_data.data() +
    //    sizeof(float[DIM]) * N / 2, uids.data() + N / 2);
    // NOTE: you must train first and then add
    //    index->Train(ds, conf);
    //    index->Train(ds2, conf);
    //    index->AddWithoutIds(ds, conf);
    //    index->Add(ds2, conf);

    std::vector<knowhere::DatasetPtr> datasets;
    std::vector<std::vector<float>> ftrashs;
    auto raw = raw_data.data();
    for (int beg = 0; beg < N; beg += DefaultElementPerChunk) {
        auto end = beg + DefaultElementPerChunk;
        if (end > N) {
            end = N;
        }
        std::vector<float> ft(raw + DIM * beg, raw + DIM * end);

        auto ds = knowhere::GenDataset(end - beg, DIM, ft.data());
        datasets.push_back(ds);
        ftrashs.push_back(std::move(ft));

        // // NOTE: you must train first and then add
        // index->Train(ds, conf);
        // index->Add(ds, conf);
    }

    for (auto& ds : datasets) {
        index->Train(ds, conf);
    }
    for (auto& ds : datasets) {
        index->AddWithoutIds(ds, conf);
    }

    auto bitmap = std::make_shared<faiss::ConcurrentBitset>(N);
    // exclude the first
    for (int i = 0; i < N / 2; ++i) {
        bitmap->set(i);
    }

    //    index->SetBlacklist(bitmap);
    auto query_ds = knowhere::GenDataset(1, DIM, raw_data.data());

    auto final = index->Query(query_ds, conf, bitmap);
    auto ids = final->Get<idx_t*>(knowhere::meta::IDS);
    auto distances = final->Get<float*>(knowhere::meta::DISTANCE);
    for (int i = 0; i < TOPK; ++i) {
        if (ids[i] < N / 2) {
            cout << "WRONG: ";
        }
        cout << ids[i] << "->" << distances[i] << endl;
    }
    int i = 1 + 1;
}

TEST(Indexing, IVFFlatNM) {
    // hello, world
    constexpr auto DIM = 16;
    constexpr auto K = 10;

    auto N = 1024 * 1024 * 10;
    auto num_query = 100;
    Timer timer;
    auto [raw_data, timestamps, uids] = generate_data<DIM>(N);
    std::cout << "generate data: " << timer.get_step_seconds() << " seconds" << endl;
    auto indexing = std::make_shared<knowhere::IVF>();
    auto conf = knowhere::Config{{knowhere::meta::DIM, DIM},
                                 {knowhere::meta::TOPK, K},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 4},
                                 {knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto database = knowhere::GenDataset(N, DIM, raw_data.data());
    std::cout << "init ivf " << timer.get_step_seconds() << " seconds" << endl;
    indexing->Train(database, conf);
    std::cout << "train ivf " << timer.get_step_seconds() << " seconds" << endl;
    indexing->AddWithoutIds(database, conf);
    std::cout << "insert ivf " << timer.get_step_seconds() << " seconds" << endl;

    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), DIM);
    auto query_dataset = knowhere::GenDataset(num_query, DIM, raw_data.data() + DIM * 4200);

    auto result = indexing->Query(query_dataset, conf, nullptr);
    std::cout << "query ivf " << timer.get_step_seconds() << " seconds" << endl;

    auto ids = result->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto dis = result->Get<float*>(milvus::knowhere::meta::DISTANCE);
    for (int i = 0; i < std::min(num_query * K, 100); ++i) {
        cout << ids[i] << "->" << dis[i] << endl;
    }
}

TEST(Indexing, DISABLED_BinaryBruteForce) {
    int64_t N = 100000;
    int64_t num_queries = 10;
    int64_t topk = 5;
    int64_t dim = 64;
    auto result_count = topk * num_queries;
    auto schema = std::make_shared<Schema>();
    schema->AddField("vecbin", DataType::VECTOR_BINARY, dim);
    schema->AddField("age", DataType::INT64);
    auto dataset = DataGen(schema, N, 10);
    vector<float> distances(result_count);
    vector<int64_t> ids(result_count);
    auto bin_vec = dataset.get_col<uint8_t>(0);
    auto line_sizeof = schema->operator[](0).get_sizeof();
    auto query_data = 1024 * line_sizeof + bin_vec.data();
    query::BinarySearchBruteForce(faiss::MetricType::METRIC_Jaccard, line_sizeof, bin_vec.data(), N, topk, num_queries,
                                  query_data, distances.data(), ids.data());
    QueryResult qr;
    qr.num_queries_ = num_queries;
    qr.topK_ = topk;
    qr.internal_seg_offsets_ = ids;
    qr.result_distances_ = distances;

    auto json = QueryResultToJson(qr);
    auto ref = Json::parse(R"(
[
  [
    [
      "1024->0.000000",
      "86966->0.395349",
      "24843->0.404762",
      "13806->0.416667",
      "44313->0.421053"
    ],
    [
      "1025->0.000000",
      "14226->0.348837",
      "1488->0.365854",
      "47337->0.377778",
      "20913->0.377778"
    ],
    [
      "1026->0.000000",
      "81882->0.386364",
      "9215->0.409091",
      "95024->0.409091",
      "54987->0.414634"
    ],
    [
      "1027->0.000000",
      "68981->0.394737",
      "75528->0.404762",
      "68794->0.405405",
      "21975->0.425000"
    ],
    [
      "1028->0.000000",
      "90290->0.375000",
      "34309->0.394737",
      "58559->0.400000",
      "33865->0.400000"
    ],
    [
      "1029->0.000000",
      "62722->0.388889",
      "89070->0.394737",
      "18528->0.414634",
      "94971->0.421053"
    ],
    [
      "1030->0.000000",
      "67402->0.333333",
      "3988->0.347826",
      "86376->0.354167",
      "84381->0.361702"
    ],
    [
      "1031->0.000000",
      "81569->0.325581",
      "12715->0.347826",
      "40332->0.363636",
      "21037->0.372093"
    ],
    [
      "1032->0.000000",
      "60536->0.428571",
      "93293->0.432432",
      "70969->0.435897",
      "64048->0.450000"
    ],
    [
      "1033->0.000000",
      "99022->0.394737",
      "11763->0.405405",
      "50073->0.428571",
      "97118->0.428571"
    ]
  ]
]
)");
    ASSERT_EQ(json, ref);
}
