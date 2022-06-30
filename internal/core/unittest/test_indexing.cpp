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
#include <vector>

#include "faiss/utils/distances.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_offset_index/IndexIVF_NM.h"
#include "query/SearchBruteForce.h"
#include "segcore/Reduce.h"
#include "test_utils/DataGen.h"
#include "test_utils/Timer.h"

using namespace milvus;
using namespace milvus::segcore;

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
        std::vector<float> vec(DIM);
        for (auto& x : vec) {
            x = distribution(er);
        }
        raw_data.insert(raw_data.end(), std::begin(vec), std::end(vec));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}
}  // namespace

Status
merge_into(int64_t queries,
           int64_t topk,
           float* distances,
           int64_t* uids,
           const float* new_distances,
           const int64_t* new_uids) {
    for (int64_t qn = 0; qn < queries; ++qn) {
        auto base = qn * topk;
        auto src2_dis = distances + base;
        auto src2_uids = uids + base;

        auto src1_dis = new_distances + base;
        auto src1_uids = new_uids + base;

        std::vector<float> buf_dis(topk);
        std::vector<int64_t> buf_uids(topk);

        auto it1 = 0;
        auto it2 = 0;

        for (auto buf = 0; buf < topk; ++buf) {
            if (src1_dis[it1] <= src2_dis[it2]) {
                buf_dis[buf] = src1_dis[it1];
                buf_uids[buf] = src1_uids[it1];
                ++it1;
            } else {
                buf_dis[buf] = src2_dis[it2];
                buf_uids[buf] = src2_uids[it2];
                ++it2;
            }
        }
        std::copy_n(buf_dis.data(), topk, src2_dis);
        std::copy_n(buf_uids.data(), topk, src2_uids);
    }
    return Status::OK();
}

TEST(Indexing, SmartBruteForce) {
    constexpr int N = 100000;
    constexpr int DIM = 16;
    constexpr int TOPK = 10;

    auto [raw_data, timestamps, uids] = generate_data<DIM>(N);
    auto total_count = DIM * TOPK;
    auto raw = (const float*)raw_data.data();
    AssertInfo(raw, "wtf");

    constexpr int64_t queries = 3;
    auto heap = faiss::float_maxheap_array_t{};

    auto query_data = raw;

    std::vector<int64_t> final_uids(total_count, -1);
    std::vector<float> final_dis(total_count, std::numeric_limits<float>::max());

    for (int beg = 0; beg < N; beg += TestChunkSize) {
        std::vector<int64_t> buf_uids(total_count, -1);
        std::vector<float> buf_dis(total_count, std::numeric_limits<float>::max());
        faiss::float_maxheap_array_t buf = {queries, TOPK, buf_uids.data(), buf_dis.data()};
        auto end = beg + TestChunkSize;
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
            std::cout << final_uids[index] << "->" << final_dis[index] << std::endl;
        }
        std::cout << std::endl;
    }
}

TEST(Indexing, Naive) {
    constexpr int N = 10000;
    constexpr int DIM = 16;
    constexpr int TOPK = 10;

    auto [raw_data, timestamps, uids] = generate_data<DIM>(N);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
                                                                         knowhere::IndexMode::MODE_CPU);

    auto conf = knowhere::Config{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
        {knowhere::meta::DIM, DIM},
        {knowhere::meta::TOPK, TOPK},
        {knowhere::indexparam::NLIST, 100},
        {knowhere::indexparam::NPROBE, 4},
        {knowhere::indexparam::M, 4},
        {knowhere::indexparam::NBITS, 8},
        {knowhere::meta::DEVICE_ID, 0},
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
    for (int beg = 0; beg < N; beg += TestChunkSize) {
        auto end = beg + TestChunkSize;
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

    auto bitmap = BitsetType(N, false);
    // exclude the first
    for (int i = 0; i < N / 2; ++i) {
        bitmap.set(i);
    }

    //    index->SetBlacklist(bitmap);
    BitsetView view = bitmap;
    auto query_ds = knowhere::GenDataset(1, DIM, raw_data.data());
    auto final = index->Query(query_ds, conf, view);
    auto ids = knowhere::GetDatasetIDs(final);
    auto distances = knowhere::GetDatasetDistance(final);
    for (int i = 0; i < TOPK; ++i) {
        if (ids[i] < N / 2) {
            std::cout << "WRONG: ";
        }
        std::cout << ids[i] << "->" << distances[i] << std::endl;
    }
}

TEST(Indexing, IVFFlat) {
    constexpr int N = 100000;
    constexpr int NQ = 10;
    constexpr int DIM = 16;
    constexpr int TOPK = 5;
    constexpr int NLIST = 128;
    constexpr int NPROBE = 16;

    Timer timer;
    auto [raw_data, timestamps, uids] = generate_data<DIM>(N);
    std::cout << "generate data: " << timer.get_step_seconds() << " seconds" << std::endl;
    auto indexing = std::make_shared<knowhere::IVF>();
    auto conf = knowhere::Config{
            {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
            {knowhere::meta::DIM, DIM},
            {knowhere::meta::TOPK, TOPK},
            {knowhere::indexparam::NLIST, NLIST},
            {knowhere::indexparam::NPROBE, NPROBE},
            {knowhere::meta::DEVICE_ID, 0}
    };

    auto database = knowhere::GenDataset(N, DIM, raw_data.data());
    std::cout << "init ivf " << timer.get_step_seconds() << " seconds" << std::endl;
    indexing->Train(database, conf);
    std::cout << "train ivf " << timer.get_step_seconds() << " seconds" << std::endl;
    indexing->AddWithoutIds(database, conf);
    std::cout << "insert ivf " << timer.get_step_seconds() << " seconds" << std::endl;

    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), DIM);
    auto dataset = knowhere::GenDataset(NQ, DIM, raw_data.data() + DIM * 4200);

    auto result = indexing->Query(dataset, conf, nullptr);
    std::cout << "query ivf " << timer.get_step_seconds() << " seconds" << std::endl;

    auto ids = knowhere::GetDatasetIDs(result);
    auto dis = knowhere::GetDatasetDistance(result);
    for (int i = 0; i < std::min(NQ * TOPK, 100); ++i) {
        std::cout << ids[i] << "->" << dis[i] << std::endl;
    }
}

TEST(Indexing, IVFFlatNM) {
    constexpr int N = 100000;
    constexpr int NQ = 10;
    constexpr int DIM = 16;
    constexpr int TOPK = 5;
    constexpr int NLIST = 128;
    constexpr int NPROBE = 16;

    Timer timer;
    auto [raw_data, timestamps, uids] = generate_data<DIM>(N);
    std::cout << "generate data: " << timer.get_step_seconds() << " seconds" << std::endl;
    auto indexing = std::make_shared<knowhere::IVF_NM>();
    auto conf = knowhere::Config{
            {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
            {knowhere::meta::DIM, DIM},
            {knowhere::meta::TOPK, TOPK},
            {knowhere::indexparam::NLIST, NLIST},
            {knowhere::indexparam::NPROBE, NPROBE},
            {knowhere::meta::DEVICE_ID, 0}
    };

    auto database = knowhere::GenDataset(N, DIM, raw_data.data());
    std::cout << "init ivf_nm " << timer.get_step_seconds() << " seconds" << std::endl;
    indexing->Train(database, conf);
    std::cout << "train ivf_nm " << timer.get_step_seconds() << " seconds" << std::endl;
    indexing->AddWithoutIds(database, conf);
    std::cout << "insert ivf_nm " << timer.get_step_seconds() << " seconds" << std::endl;

    knowhere::BinarySet bs = indexing->Serialize(conf);

    knowhere::BinaryPtr bptr = std::make_shared<knowhere::Binary>();
    bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)raw_data.data(), [&](uint8_t*) {});
    bptr->size = DIM * N * sizeof(float);
    bs.Append(RAW_DATA, bptr);
    indexing->Load(bs);

    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), DIM);
    auto dataset = knowhere::GenDataset(NQ, DIM, raw_data.data() + DIM * 4200);

    auto result = indexing->Query(dataset, conf, nullptr);
    std::cout << "query ivf_nm " << timer.get_step_seconds() << " seconds" << std::endl;

    auto ids = knowhere::GetDatasetIDs(result);
    auto dis = knowhere::GetDatasetDistance(result);
    for (int i = 0; i < std::min(NQ * TOPK, 100); ++i) {
        std::cout << ids[i] << "->" << dis[i] << std::endl;
    }
}

TEST(Indexing, BinaryBruteForce) {
    int64_t N = 100000;
    int64_t num_queries = 10;
    int64_t topk = 5;
    int64_t round_decimal = 3;
    int64_t dim = 8192;
    auto metric_type = knowhere::metric::JACCARD;
    auto result_count = topk * num_queries;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("vecbin", DataType::VECTOR_BINARY, dim, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    auto dataset = DataGen(schema, N, 10);
    auto bin_vec = dataset.get_col<uint8_t>(vec_fid);
    auto query_data = 1024 * dim / 8 + bin_vec.data();
    query::dataset::SearchDataset search_dataset{
        metric_type,    //
        num_queries,    //
        topk,           //
        round_decimal,
        dim,        //
        query_data  //
    };

    auto sub_result = query::BinarySearchBruteForce(search_dataset, bin_vec.data(), N, nullptr);

    SearchResult sr;
    sr.total_nq_ = num_queries;
    sr.unity_topK_ = topk;
    sr.seg_offsets_ = std::move(sub_result.mutable_seg_offsets());
    sr.distances_ = std::move(sub_result.mutable_distances());

    auto json = SearchResultToJson(sr);
    std::cout << json.dump(2);
#ifdef __linux__
    auto ref = json::parse(R"(
[
  [
    [ "1024->0.000000", "48942->0.642000", "18494->0.644000", "68225->0.644000", "93557->0.644000" ],
    [ "1025->0.000000", "73557->0.641000", "53086->0.643000", "9737->0.643000", "62855->0.644000" ],
    [ "1026->0.000000", "62904->0.644000", "46758->0.644000", "57969->0.645000", "98113->0.646000" ],
    [ "1027->0.000000", "92446->0.638000", "96034->0.640000", "92129->0.644000", "45887->0.644000" ],
    [ "1028->0.000000", "22992->0.643000", "73903->0.644000", "19969->0.645000", "65178->0.645000" ],
    [ "1029->0.000000", "19776->0.641000", "15166->0.642000", "85470->0.642000", "16730->0.643000" ],
    [ "1030->0.000000", "55939->0.640000", "84253->0.643000", "31958->0.644000", "11667->0.646000" ],
    [ "1031->0.000000", "89536->0.637000", "61622->0.638000", "9275->0.639000", "91403->0.640000" ],
    [ "1032->0.000000", "69504->0.642000", "23414->0.644000", "48770->0.645000", "23231->0.645000" ],
    [ "1033->0.000000", "33540->0.636000", "25310->0.640000", "18576->0.640000", "73729->0.642000" ]
  ]
]
)");
#else  // for mac
    auto ref = json::parse(R"(
[
  [
    [ "1024->0.000000", "59169->0.645000", "98548->0.646000", "3356->0.646000", "90373->0.647000" ],
    [ "1025->0.000000", "61245->0.638000", "95271->0.639000", "31087->0.639000", "31549->0.640000" ],
    [ "1026->0.000000", "65225->0.648000", "35750->0.648000", "14971->0.649000", "75385->0.649000" ],
    [ "1027->0.000000", "70158->0.640000", "27076->0.640000", "3407->0.641000", "59527->0.641000" ],
    [ "1028->0.000000", "45757->0.645000", "3356->0.645000", "77230->0.646000", "28690->0.647000" ],
    [ "1029->0.000000", "13291->0.642000", "24960->0.643000", "83770->0.643000", "88244->0.643000" ],
    [ "1030->0.000000", "96807->0.641000", "39920->0.643000", "62943->0.644000", "12603->0.644000" ],
    [ "1031->0.000000", "65769->0.648000", "60493->0.648000", "48738->0.648000", "4353->0.648000" ],
    [ "1032->0.000000", "57827->0.637000", "8213->0.638000", "22221->0.639000", "23328->0.640000" ],
    [ "1033->0.000000", "676->0.645000", "91430->0.646000", "85353->0.646000", "6014->0.646000" ]
  ]
]
)");
#endif
    auto json_str = json.dump(2);
    auto ref_str = ref.dump(2);
    ASSERT_EQ(json_str, ref_str);
}
