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

#include <folly/FBVector.h>
#include <stdint.h>
#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/common.h"
#include "common/BitsetView.h"
#include "common/EasyAssert.h"
#include "common/QueryInfo.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "query/SearchBruteForce.h"
#include "query/SubSearchResult.h"
#include "query/helper.h"
#include "segcore/Collection.h"
#include "test_utils/DataGen.h"
#include "test_utils/Distance.h"

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
        ThrowInfo(MetricTypeInvalid, "invalid metric type");
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
        ThrowInfo(MetricTypeInvalid, "invalid metric type");
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
                                       DataType::VECTOR_FLOAT,
                                       DataType::NONE,
                                       nullptr);
        for (int i = 0; i < nq; i++) {
            auto ref = Ref(base.data(),
                           query.data() + i * dim,
                           nb,
                           dim,
                           topk,
                           metric_type);
            auto ans = result.get_offsets() + i * topk;
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

TEST(PrepareBFSearchParams, BM25ParamsFromIndexInfo) {
    SearchInfo search_info;
    search_info.metric_type_ = knowhere::metric::BM25;
    search_info.search_params_[knowhere::meta::BM25_AVGDL] = 5.0;
    std::map<std::string, std::string> index_info{
        {knowhere::meta::BM25_K1, "2.0"},
        {knowhere::meta::BM25_B, "0.5"},
    };
    auto cfg = PrepareBFSearchParams(search_info, index_info);
    ASSERT_FLOAT_EQ(cfg[knowhere::meta::BM25_K1].get<float>(), 2.0f);
    ASSERT_FLOAT_EQ(cfg[knowhere::meta::BM25_B].get<float>(), 0.5f);
    ASSERT_DOUBLE_EQ(cfg[knowhere::meta::BM25_AVGDL].get<double>(), 5.0);
}

// A field added by add_function_field leaves the per-segment index_info empty;
// k1/b then come from brute_force_index_params_ (sourced from the collection
// metadata at plan creation), not the stale segment meta, and must not throw.
TEST(PrepareBFSearchParams, BM25ParamsFromCollectionMetaWhenIndexInfoEmpty) {
    SearchInfo search_info;
    search_info.metric_type_ = knowhere::metric::BM25;
    search_info.search_params_[knowhere::meta::BM25_AVGDL] = 5.0;
    search_info.brute_force_index_params_.bm25_k1_ = 1.2f;
    search_info.brute_force_index_params_.bm25_b_ = 0.75f;
    std::map<std::string, std::string> empty_index_info;
    knowhere::Json cfg;
    ASSERT_NO_THROW(cfg = PrepareBFSearchParams(search_info, empty_index_info));
    ASSERT_FLOAT_EQ(cfg[knowhere::meta::BM25_K1].get<float>(), 1.2f);
    ASSERT_FLOAT_EQ(cfg[knowhere::meta::BM25_B].get<float>(), 0.75f);
}

TEST(PrepareBFSearchParams, MinHashParams) {
    // present per-segment params are parsed from index_info
    SearchInfo search_info;
    search_info.metric_type_ = knowhere::metric::MHJACCARD;
    std::map<std::string, std::string> index_info{
        {knowhere::indexparam::MH_LSH_BAND, "8"},
        {knowhere::indexparam::MH_ELEMENT_BIT_WIDTH, "16"},
    };
    auto cfg = PrepareBFSearchParams(search_info, index_info);
    ASSERT_EQ(cfg[knowhere::indexparam::MH_LSH_BAND].get<int>(), 8);
    ASSERT_EQ(cfg[knowhere::indexparam::MH_ELEMENT_BIT_WIDTH].get<int>(), 16);

    // a stale segment leaves index_info empty; band/width then come from
    // brute_force_index_params_ (collection metadata), not knowhere defaults.
    SearchInfo stale_info;
    stale_info.metric_type_ = knowhere::metric::MHJACCARD;
    stale_info.brute_force_index_params_.minhash_lsh_band_ = 4;
    stale_info.brute_force_index_params_.minhash_element_bit_width_ = 32;
    std::map<std::string, std::string> empty_index_info;
    knowhere::Json cfg2;
    ASSERT_NO_THROW(cfg2 = PrepareBFSearchParams(stale_info, empty_index_info));
    ASSERT_EQ(cfg2[knowhere::indexparam::MH_LSH_BAND].get<int>(), 4);
    ASSERT_EQ(cfg2[knowhere::indexparam::MH_ELEMENT_BIT_WIDTH].get<int>(), 32);
}

// plan-creation side: params are sourced from the collection-level field index
// meta into brute_force_index_params_.
TEST(PopulateBruteForceIndexParams, BM25) {
    SearchInfo search_info;
    search_info.metric_type_ = knowhere::metric::BM25;
    FieldIndexMeta meta(
        FieldId(100),
        {{knowhere::meta::BM25_K1, "2.0"}, {knowhere::meta::BM25_B, "0.5"}},
        {});
    PopulateBruteForceIndexParams(search_info, meta);
    const auto& p = search_info.brute_force_index_params_;
    ASSERT_TRUE(p.bm25_k1_.has_value());
    ASSERT_FLOAT_EQ(p.bm25_k1_.value(), 2.0f);
    ASSERT_TRUE(p.bm25_b_.has_value());
    ASSERT_FLOAT_EQ(p.bm25_b_.value(), 0.5f);
}

TEST(PopulateBruteForceIndexParams, MinHashDefaultsWhenAbsent) {
    SearchInfo search_info;
    search_info.metric_type_ = knowhere::metric::MHJACCARD;
    FieldIndexMeta meta(FieldId(100), {}, {});
    PopulateBruteForceIndexParams(search_info, meta);
    const auto& p = search_info.brute_force_index_params_;
    ASSERT_EQ(p.minhash_lsh_band_.value(), 1);
    ASSERT_EQ(p.minhash_element_bit_width_.value(), 8);
}

TEST(PopulateBruteForceIndexParams, MinHashOverridesFromMeta) {
    SearchInfo search_info;
    search_info.metric_type_ = knowhere::metric::MHJACCARD;
    FieldIndexMeta meta(FieldId(100),
                        {{knowhere::indexparam::MH_LSH_BAND, "4"},
                         {knowhere::indexparam::MH_ELEMENT_BIT_WIDTH, "32"}},
                        {});
    PopulateBruteForceIndexParams(search_info, meta);
    const auto& p = search_info.brute_force_index_params_;
    ASSERT_EQ(p.minhash_lsh_band_.value(), 4);
    ASSERT_EQ(p.minhash_element_bit_width_.value(), 32);
}
