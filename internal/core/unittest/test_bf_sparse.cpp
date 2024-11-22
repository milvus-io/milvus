// Copyright (C) 2019-2024 Zilliz. All rights reserved.
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
#include <map>
#include <random>

#include "common/Utils.h"

#include "query/SearchBruteForce.h"
#include "test_utils/Constants.h"
#include "test_utils/Distance.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::query;

namespace {

std::vector<int>
SearchRef(const knowhere::sparse::SparseRow<float>* base,
          const knowhere::sparse::SparseRow<float>& query,
          int nb,
          int topk) {
    std::vector<std::tuple<float, int>> res;
    for (int i = 0; i < nb; i++) {
        auto& row = base[i];
        auto distance = row.dot(query);
        res.emplace_back(-distance, i);
    }
    std::sort(res.begin(), res.end());
    std::vector<int> offsets;
    for (int i = 0; i < topk; i++) {
        auto [distance, offset] = res[i];
        if (distance == 0) {
            distance = std::numeric_limits<float>::quiet_NaN();
            offset = -1;
        }
        offsets.push_back(offset);
    }
    return offsets;
}

std::vector<int>
RangeSearchRef(const knowhere::sparse::SparseRow<float>* base,
               const knowhere::sparse::SparseRow<float>& query,
               int nb,
               float radius,
               float range_filter,
               int topk) {
    std::vector<int> offsets;
    for (int i = 0; i < nb; i++) {
        auto& row = base[i];
        auto distance = row.dot(query);
        if (distance <= range_filter && distance > radius) {
            offsets.push_back(i);
        }
    }
    // select and sort top k on the range filter side
    std::sort(offsets.begin(), offsets.end(), [&](int a, int b) {
        return base[a].dot(query) > base[b].dot(query);
    });
    if (offsets.size() > topk) {
        offsets.resize(topk);
    }
    return offsets;
}

void
AssertMatch(const std::vector<int>& expected, const int64_t* actual) {
    for (int i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], actual[i]);
    }
}

bool
is_supported_sparse_float_metric(const std::string& metric) {
    return milvus::IsMetricType(metric, knowhere::metric::IP);
}

}  // namespace

class TestSparseFloatSearchBruteForce : public ::testing::Test {
 public:
    void
    Run(int nb, int nq, int topk, const knowhere::MetricType& metric_type) {
        auto bitset = std::make_shared<BitsetType>();
        bitset->resize(nb);
        auto bitset_view = BitsetView(*bitset);

        auto base = milvus::segcore::GenerateRandomSparseFloatVector(nb);
        auto query = milvus::segcore::GenerateRandomSparseFloatVector(nq);
        auto index_info = std::map<std::string, std::string>{};
        SearchInfo search_info;
        search_info.topk_ = topk;
        search_info.metric_type_ = metric_type;
        dataset::SearchDataset query_dataset{
            metric_type, nq, topk, -1, kTestSparseDim, query.get()};
        auto raw_dataset =
            query::dataset::RawDataset{0, kTestSparseDim, nb, base.get()};
        if (!is_supported_sparse_float_metric(metric_type)) {
            ASSERT_ANY_THROW(BruteForceSearch(query_dataset,
                                              raw_dataset,
                                              search_info,
                                              index_info,
                                              bitset_view,
                                              DataType::VECTOR_SPARSE_FLOAT));
            return;
        }
        auto result = BruteForceSearch(query_dataset,
                                       raw_dataset,
                                       search_info,
                                       index_info,
                                       bitset_view,
                                       DataType::VECTOR_SPARSE_FLOAT);
        for (int i = 0; i < nq; i++) {
            auto ref = SearchRef(base.get(), *(query.get() + i), nb, topk);
            auto ans = result.get_seg_offsets() + i * topk;
            AssertMatch(ref, ans);
        }

        search_info.search_params_[RADIUS] = 0.1;
        search_info.search_params_[RANGE_FILTER] = 0.5;
        auto result2 = BruteForceSearch(query_dataset,
                                        raw_dataset,
                                        search_info,
                                        index_info,
                                        bitset_view,
                                        DataType::VECTOR_SPARSE_FLOAT);
        for (int i = 0; i < nq; i++) {
            auto ref = RangeSearchRef(
                base.get(), *(query.get() + i), nb, 0.1, 0.5, topk);
            auto ans = result2.get_seg_offsets() + i * topk;
            AssertMatch(ref, ans);
        }

        auto result3 = PackBruteForceSearchIteratorsIntoSubResult(
            query_dataset,
            raw_dataset,
            search_info,
            index_info,
            bitset_view,
            DataType::VECTOR_SPARSE_FLOAT);
        auto iterators = result3.chunk_iterators();
        for (int i = 0; i < nq; i++) {
            auto it = iterators[i];
            auto q = *(query.get() + i);
            auto last_dis = std::numeric_limits<float>::max();
            // we should see strict decreasing distances for brute force iterator.
            while (it->HasNext()) {
                auto [offset, dis] = it->Next();
                ASSERT_LE(dis, last_dis);
                last_dis = dis;
                ASSERT_FLOAT_EQ(dis, base[offset].dot(q));
            }
        }
    }
};

TEST_F(TestSparseFloatSearchBruteForce, NotSupported) {
    Run(100, 10, 5, "L2");
    Run(100, 10, 5, "l2");
    Run(100, 10, 5, "lxxx");
}

TEST_F(TestSparseFloatSearchBruteForce, IP) {
    Run(100, 10, 5, "IP");
    Run(100, 10, 5, "ip");
}
