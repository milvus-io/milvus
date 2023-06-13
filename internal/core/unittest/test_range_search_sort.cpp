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
#include <queue>
#include <random>
#include <vector>
#include <iostream>

#include "common/RangeSearchHelper.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/Schema.h"
#include "test_utils/indexbuilder_test_utils.h"

bool
greater(std::pair<float, int64_t> a, std::pair<float, int64_t> b) {
    return a.first > b.first;
}

bool
less(std::pair<float, int64_t> a, std::pair<float, int64_t> b) {
    return a.first < b.first;
}

auto
RangeSearchSortResultBF(milvus::DatasetPtr data_set,
                        int64_t topk,
                        size_t nq,
                        std::string& metric_type) {
    auto lims = milvus::GetDatasetLims(data_set);
    auto id = milvus::GetDatasetIDs(data_set);
    auto dist = milvus::GetDatasetDistance(data_set);
    auto p_id = new int64_t[topk * nq];
    memset(p_id, -1, sizeof(int64_t) * topk * nq);
    auto p_dist = new float[topk * nq];
    std::fill_n(p_dist, topk * nq, std::numeric_limits<float>::max());

    auto cmp_func = (milvus::PositivelyRelated(metric_type)) ? greater : less;

    //  cnt means the subscript of p_id and p_dist
    for (int i = 0; i < nq; i++) {
        auto capacity = std::min<int64_t>(lims[i + 1] - lims[i], topk);

        // sort each layer
        std::vector<std::pair<float, int64_t>> list;
        for (int j = lims[i]; j < lims[i + 1]; j++) {
            list.emplace_back(dist[j], id[j]);
        }
        std::sort(list.begin(), list.end(), cmp_func);

        for (int k = 0; k < capacity; k++) {
            p_dist[i * topk + k] = list[k].first;
            p_id[i * topk + k] = list[k].second;
        }
    }
    return std::make_tuple(p_id, p_dist);
}

milvus::DatasetPtr
genResultDataset(const int64_t nq,
                 const int64_t* ids,
                 const float* distance,
                 const size_t* lims) {
    auto ret_ds = std::make_shared<milvus::Dataset>();
    ret_ds->SetRows(nq);
    ret_ds->SetIds(ids);
    ret_ds->SetDistance(distance);
    ret_ds->SetLims(lims);
    ret_ds->SetIsOwner(true);
    return ret_ds;
}

void
CheckRangeSearchSortResult(int64_t* p_id,
                           float* p_dist,
                           milvus::DatasetPtr dataset,
                           int64_t n) {
    auto id = milvus::GetDatasetIDs(dataset);
    auto dist = milvus::GetDatasetDistance(dataset);
    for (int i = 0; i < n; i++) {
        AssertInfo(id[i] == p_id[i], "id of range search result not same");
        AssertInfo(dist[i] == p_dist[i],
                   "distance of range search result not same");
    }
}

auto
GenRangeSearchResult(int64_t* ids,
                     float* distances,
                     size_t* lims,
                     int64_t N,
                     int64_t id_min,
                     int64_t id_max,
                     float distance_min,
                     float distance_max,
                     int seed = 42) {
    std::mt19937 e(seed);
    std::uniform_int_distribution<> uniform_num(0, N);
    std::uniform_int_distribution<> uniform_ids(id_min, id_max);
    std::uniform_real_distribution<> uniform_distance(distance_min,
                                                      distance_max);

    lims = new size_t[N + 1];
    // alloc max memory
    distances = new float[N * N];
    ids = new int64_t[N * N];
    lims[0] = 0;
    for (int64_t i = 0; i < N; i++) {
        int64_t num = uniform_num(e);
        for (int64_t j = 0; j < num; j++) {
            auto id = uniform_ids(e);
            auto dis = uniform_distance(e);
            ids[lims[i] + j] = id;
            distances[lims[i] + j] = dis;
        }
        lims[i + 1] = lims[i] + num;
    }
    return genResultDataset(N, ids, distances, lims);
}

class RangeSearchSortTest
    : public ::testing::TestWithParam<knowhere::MetricType> {
 protected:
    void
    SetUp() override {
        metric_type = GetParam();
        dataset = GenRangeSearchResult(
            ids, distances, lims, N, id_min, id_max, dist_min, dist_max);
    }

    void
    TearDown() override {
        delete[] ids;
        delete[] distances;
        delete[] lims;
    }

 protected:
    knowhere::MetricType metric_type;
    milvus::DatasetPtr dataset = nullptr;
    int64_t N = 100;
    int64_t TOPK = 10;
    int64_t DIM = 16;
    int64_t* ids = nullptr;
    float* distances = nullptr;
    size_t* lims = nullptr;
    int64_t id_min = 0, id_max = 10000;
    float dist_min = 0.0, dist_max = 100.0;
};

INSTANTIATE_TEST_CASE_P(RangeSearchSortParameters,
                        RangeSearchSortTest,
                        ::testing::Values(knowhere::metric::L2,
                                          knowhere::metric::IP,
                                          knowhere::metric::JACCARD,
                                          knowhere::metric::TANIMOTO,
                                          knowhere::metric::HAMMING));

TEST_P(RangeSearchSortTest, CheckRangeSearchSort) {
    auto res = milvus::ReGenRangeSearchResult(dataset, TOPK, N, metric_type);
    auto [p_id, p_dist] =
        RangeSearchSortResultBF(dataset, TOPK, N, metric_type);
    CheckRangeSearchSortResult(p_id, p_dist, res, N * TOPK);
    delete[] p_id;
    delete[] p_dist;
}
