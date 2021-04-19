#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "dog_segment/ConcurrentVector.h"
#include "dog_segment/SegmentBase.h"
// #include "knowhere/index/vector_index/helpers/IndexParameter.h"

#include "dog_segment/SegmentBase.h"
#include "dog_segment/AckResponder.h"

#include <knowhere/index/vector_index/VecIndex.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>

using std::cin;
using std::cout;
using std::endl;
using namespace milvus::engine;
using namespace milvus::dog_segment;
using std::vector;
using namespace milvus;

namespace {
    template<int DIM>
    auto generate_data(int N) {
        std::vector<char> raw_data;
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
            for (auto &x: vec) {
                x = distribution(er);
            }
            raw_data.insert(raw_data.end(), (const char *) std::begin(vec), (const char *) std::end(vec));
//            int age = ei() % 100;
//            raw_data.insert(raw_data.end(), (const char *) &age, ((const char *) &age) + sizeof(age));
        }
        return std::make_tuple(raw_data, timestamps, uids);
    }
}

TEST(TestIndex, Naive) {
    constexpr int N = 100000;
    constexpr int DIM = 16;
    constexpr int TOPK = 10;

    auto[raw_data, timestamps, uids] = generate_data<DIM>(N);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
                                                                         knowhere::IndexMode::MODE_CPU);
    auto conf = milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM,           DIM},
            {milvus::knowhere::meta::TOPK,          TOPK},
            {milvus::knowhere::IndexParams::nlist,  100},
            {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::IndexParams::m,      4},
            {milvus::knowhere::IndexParams::nbits,  8},
            {milvus::knowhere::Metric::TYPE,        milvus::knowhere::Metric::L2},
            {milvus::knowhere::meta::DEVICEID,      0},
    };

    auto ds = knowhere::GenDatasetWithIds(N / 2, DIM, raw_data.data(), uids.data());
    auto ds2 = knowhere::GenDatasetWithIds(N / 2, DIM, raw_data.data() + sizeof(float[DIM]) * N / 2, uids.data() + N / 2);
    // NOTE: you must train first and then add
    index->Train(ds, conf);
    index->Train(ds2, conf);
    index->Add(ds, conf);
    index->Add(ds2, conf);

    auto query_ds = knowhere::GenDataset(1, DIM, raw_data.data());
    auto final = index->Query(query_ds, conf);
    auto mmm = final->data();
    cout << endl;
    for(auto [k, v]: mmm) {
        cout << k << endl;
    }
    auto ids = final->Get<idx_t*>(knowhere::meta::IDS);
    auto distances = final->Get<float*>(knowhere::meta::DISTANCE);
    for(int i = 0; i < TOPK; ++i) {
        cout << ids[i] << "->" << distances[i] << endl;
    }
    int i = 1+1;
}
