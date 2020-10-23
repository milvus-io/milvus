#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <faiss/utils/distances.h>
#include "dog_segment/ConcurrentVector.h"
#include "dog_segment/SegmentBase.h"
// #include "knowhere/index/vector_index/helpers/IndexParameter.h"

#include "dog_segment/SegmentBase.h"
#include "dog_segment/AckResponder.h"
#include <knowhere/index/vector_index/VecIndex.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <knowhere/index/vector_index/IndexIVF.h>
#include <algorithm>
#include <chrono>
#include "test_utils/Timer.h"

using std::cin;
using std::cout;
using std::endl;
using namespace milvus::engine;
using namespace milvus::dog_segment;
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
        float vec[DIM];
        for (auto& x : vec) {
            x = distribution(er);
        }
        raw_data.insert(raw_data.end(), std::begin(vec), std::end(vec));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}
}  // namespace

void
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
}

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

    constexpr int64_t queries = 3;
    auto heap = faiss::float_maxheap_array_t{};

    auto query_data = raw;

    vector<int64_t> final_uids(total_count);
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
        if (beg == 0) {
            final_uids = buf_uids;
            final_dis = buf_dis;
        } else {
            merge_into(queries, TOPK, final_dis.data(), final_uids.data(), buf_dis.data(), buf_uids.data());
        }
    }

    for (int qn = 0; qn < queries; ++qn) {
        for (int kn = 0; kn < TOPK; ++kn) {
            auto index = qn * TOPK + kn;
            cout << final_uids[index] << "->" << final_dis[index] << endl;
        }
        cout << endl;
    }
}

TEST(Indexing, Naive) {
    constexpr int N = 100000;
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
    auto num_query = 1000;
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
