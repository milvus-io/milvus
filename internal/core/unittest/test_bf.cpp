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
#include <random>

#include "common/Utils.h"

#include "query/SearchBruteForce.h"
#include "test_utils/Distance.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::query;

namespace {

auto
GenFloatVecs(int dim,
             int n,
             const knowhere::MetricType& metric,
             int seed = 42) {
    auto schema = std::make_shared<Schema>();
    auto fvec =
        schema->AddDebugField("fvec", DataType::VECTOR_FLOAT, dim, metric);
    auto dataset = DataGen(schema, n, seed);
    return dataset.get_col<float>(fvec);
}

// (offset, distance)
std::vector<std::tuple<int, float>>
Distances(const float* base,
          const float* query,  // one query.
          int nb,
          int dim,
          const knowhere::MetricType& metric) {
    if (milvus::IsMetricType(metric, knowhere::metric::L2)) {
        std::vector<std::tuple<int, float>> res;
        for (int i = 0; i < nb; i++) {
            res.emplace_back(i, L2(base + i * dim, query, dim));
        }
        return res;
    } else if (milvus::IsMetricType(metric, knowhere::metric::IP)) {
        std::vector<std::tuple<int, float>> res;
        for (int i = 0; i < nb; i++) {
            res.emplace_back(i, IP(base + i * dim, query, dim));
        }
        return res;
    } else {
        PanicInfo(MetricTypeInvalid, "invalid metric type");
    }
}

std::vector<int>
GetOffsets(const std::vector<std::tuple<int, float>>& tuples, int k) {
    std::vector<int> offsets;
    for (int i = 0; i < k; i++) {
        auto [offset, distance] = tuples[i];
        offsets.push_back(offset);
    }
    return offsets;
}

// offsets
std::vector<int>
Ref(const float* base,
    const float* query,  // one query.
    int nb,
    int dim,
    int topk,
    const knowhere::MetricType& metric) {
    auto res = Distances(base, query, nb, dim, metric);
    std::sort(res.begin(), res.end());
    if (milvus::IsMetricType(metric, knowhere::metric::L2)) {
        // do nothing
    } else if (milvus::IsMetricType(metric, knowhere::metric::IP)) {
        std::reverse(res.begin(), res.end());
    } else {
        PanicInfo(MetricTypeInvalid, "invalid metric type");
    }
    return GetOffsets(res, topk);
}

bool
AssertMatch(const std::vector<int>& ref, const int64_t* ans) {
    for (int i = 0; i < ref.size(); i++) {
        if (ref[i] != ans[i]) {
            return false;
        }
    }
    return true;
}

bool
is_supported_float_metric(const std::string& metric) {
    return milvus::IsMetricType(metric, knowhere::metric::L2) ||
           milvus::IsMetricType(metric, knowhere::metric::IP);
}

}  // namespace

class TestFloatSearchBruteForce : public ::testing::Test {
 public:
    void
    Run(int nb,
        int nq,
        int topk,
        int dim,
        const knowhere::MetricType& metric_type) {
        auto bitset = std::make_shared<BitsetType>();
        bitset->resize(nb);
        auto bitset_view = BitsetView(*bitset);

        auto base = GenFloatVecs(dim, nb, metric_type);
        auto query = GenFloatVecs(dim, nq, metric_type);
        auto index_info = std::map<std::string, std::string>{};

        dataset::SearchDataset query_dataset{
            metric_type, nq, topk, -1, dim, query.data()};
        if (!is_supported_float_metric(metric_type)) {
            // Memory leak in knowhere.
            // ASSERT_ANY_THROW(BruteForceSearch(dataset, base.data(), nb, bitset_view));
            return;
        }
        SearchInfo search_info;
        search_info.topk_ = topk;
        search_info.metric_type_ = metric_type;

        auto raw_dataset = query::dataset::RawDataset{0, dim, nb, base.data()};
        auto result = BruteForceSearch(query_dataset,
                                       raw_dataset,
                                       search_info,
                                       index_info,
                                       bitset_view,
                                       DataType::VECTOR_FLOAT);
        for (int i = 0; i < nq; i++) {
            auto ref = Ref(base.data(),
                           query.data() + i * dim,
                           nb,
                           dim,
                           topk,
                           metric_type);
            auto ans = result.get_seg_offsets() + i * topk;
            AssertMatch(ref, ans);
        }
    }
};

TEST_F(TestFloatSearchBruteForce, L2) {
    Run(100, 10, 5, 128, "L2");
    Run(100, 10, 5, 128, "l2");
}

TEST_F(TestFloatSearchBruteForce, IP) {
    Run(100, 10, 5, 128, "IP");
    Run(100, 10, 5, 128, "ip");
}

TEST_F(TestFloatSearchBruteForce, NotSupported) {
    Run(100, 10, 5, 128, "aaaaaaaaaaaa");
}
