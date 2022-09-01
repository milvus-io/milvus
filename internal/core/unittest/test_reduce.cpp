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

#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "query/SubSearchResult.h"

using namespace milvus;
using namespace milvus::query;

using SubSearchResultUniq = std::unique_ptr<SubSearchResult>;

std::default_random_engine e(42);

SubSearchResultUniq
GenSubSearchResult(const int64_t nq,
                   const int64_t topk,
                   const knowhere::MetricType &metric_type,
                   const int64_t round_decimal) {
    constexpr int64_t limit = 1000000L;
    bool is_ip = (metric_type == knowhere::metric::IP);
    SubSearchResultUniq sub_result = std::make_unique<SubSearchResult>(nq, topk, metric_type, round_decimal);
    std::vector<int64_t> ids;
    std::vector<float> distances;
    for (auto n = 0; n < nq; ++n) {
        for (auto k = 0; k < topk; ++k) {
            auto gen_x = e() % limit;
            ids.push_back(gen_x);
            distances.push_back(gen_x);
        }
        if (is_ip) {
            std::sort(ids.begin() + n * topk, ids.begin() + (n + 1) * topk, std::greater<int64_t>());
            std::sort(distances.begin() + n * topk, distances.begin() + (n + 1) * topk, std::greater<float>());
        } else {
            std::sort(ids.begin() + n * topk, ids.begin() + (n + 1) * topk);
            std::sort(distances.begin() + n * topk, distances.begin() + (n + 1) * topk);
        }
    }
    sub_result->mutable_distances() = std::move(distances);
    sub_result->mutable_seg_offsets() = std::move(ids);
    return sub_result;
}

template<class queue_type>
void
CheckSubSearchResult(const int64_t nq,
                     const int64_t topk,
                     SubSearchResult& result,
                     std::vector<queue_type>& result_ref) {
    ASSERT_EQ(result_ref.size(), nq);
    for (int n = 0; n < nq; ++n) {
        ASSERT_EQ(result_ref[n].size(), topk);
        for (int k = 0; k < topk; ++k) {
            auto ref_x = result_ref[n].top();
            result_ref[n].pop();
            auto index = n * topk + topk - 1 - k;
            auto id = result.get_seg_offsets()[index];
            auto distance = result.get_distances()[index];
            ASSERT_EQ(id, ref_x);
            ASSERT_EQ(distance, ref_x);
        }
    }
}

template<class queue_type>
void
TestSubSearchResultMerge(const knowhere::MetricType& metric_type,
                         const int64_t iteration,
                         const int64_t nq,
                         const int64_t topk) {
    const int64_t round_decimal = 3;

    std::vector<queue_type> result_ref(nq);

    SubSearchResult final_result(nq, topk, metric_type, round_decimal);
    for (int i = 0; i < iteration; ++i) {
        SubSearchResultUniq sub_result = GenSubSearchResult(nq, topk, metric_type, round_decimal);
        auto ids = sub_result->get_ids();
        for (int n = 0; n < nq; ++n) {
            for (int k = 0; k < topk; ++k) {
                int64_t x = ids[n * topk + k];
                result_ref[n].push(x);
                if (result_ref[n].size() > topk) {
                    result_ref[n].pop();
                }
            }
        }
        final_result.merge(*sub_result);
    }
    CheckSubSearchResult<queue_type>(nq, topk, final_result, result_ref);
}

TEST(Reduce, SubSearchResult) {
    using queue_type_l2 = std::priority_queue<int64_t, std::vector<int64_t>, std::less<int64_t>>;
    using queue_type_ip = std::priority_queue<int64_t, std::vector<int64_t>, std::greater<int64_t>>;

    TestSubSearchResultMerge<queue_type_l2>(knowhere::metric::L2, 1, 1, 1);
    TestSubSearchResultMerge<queue_type_l2>(knowhere::metric::L2, 1, 1, 10);
    TestSubSearchResultMerge<queue_type_l2>(knowhere::metric::L2, 1, 16, 1);
    TestSubSearchResultMerge<queue_type_l2>(knowhere::metric::L2, 1, 16, 10);
    TestSubSearchResultMerge<queue_type_l2>(knowhere::metric::L2, 4, 1, 1);
    TestSubSearchResultMerge<queue_type_l2>(knowhere::metric::L2, 4, 1, 10);
    TestSubSearchResultMerge<queue_type_l2>(knowhere::metric::L2, 4, 16, 1);
    TestSubSearchResultMerge<queue_type_l2>(knowhere::metric::L2, 4, 16, 10);

    TestSubSearchResultMerge<queue_type_ip>(knowhere::metric::IP, 1, 1, 1);
    TestSubSearchResultMerge<queue_type_ip>(knowhere::metric::IP, 1, 1, 10);
    TestSubSearchResultMerge<queue_type_ip>(knowhere::metric::IP, 1, 16, 1);
    TestSubSearchResultMerge<queue_type_ip>(knowhere::metric::IP, 1, 16, 10);
    TestSubSearchResultMerge<queue_type_ip>(knowhere::metric::IP, 4, 1, 1);
    TestSubSearchResultMerge<queue_type_ip>(knowhere::metric::IP, 4, 1, 10);
    TestSubSearchResultMerge<queue_type_ip>(knowhere::metric::IP, 4, 16, 1);
    TestSubSearchResultMerge<queue_type_ip>(knowhere::metric::IP, 4, 16, 10);
}