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

#include <cstdint>
#include <cmath>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "knowhere/config.h"
#include "nlohmann/json.hpp"
#include "query/SearchBruteForce.h"
#include "query/SubSearchResult.h"
#include "query/helper.h"

using namespace milvus;

namespace {
nlohmann::json
ResultJson(const SearchResult& result, int64_t round_decimal) {
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.total_nq_);
    const float multiplier = std::pow(10.0, round_decimal);
    for (int64_t query = 0; query < result.total_nq_; ++query) {
        std::vector<std::string> neighbors;
        neighbors.reserve(result.unity_topK_);
        for (int64_t hit = 0; hit < result.unity_topK_; ++hit) {
            const auto offset = query * result.unity_topK_ + hit;
            const auto distance =
                std::round(result.distances_[offset] * multiplier) / multiplier;
            neighbors.push_back(std::to_string(result.seg_offsets_[offset]) +
                                "->" + std::to_string(distance));
        }
        rows.push_back(std::move(neighbors));
    }
    return nlohmann::json{rows};
}
}  // namespace

TEST(SearchBruteForceBinaryTest, JaccardResultsMatchReference) {
    int64_t N = 100000;
    int64_t num_queries = 10;
    int64_t topk = 5;
    int64_t round_decimal = 3;
    int64_t dim = 8192;
    Config search_params_ = {};
    auto metric_type = knowhere::metric::JACCARD;
    // Preserve the original binary fixture's seed and byte-generation order.
    std::default_random_engine random(10);
    std::vector<uint8_t> bin_vec(N * dim / 8);
    for (auto& value : bin_vec) {
        value = static_cast<uint8_t>(random());
    }
    auto query_data = 1024 * dim / 8 + bin_vec.data();
    query::dataset::SearchDataset search_dataset{
        metric_type,  //
        num_queries,  //
        topk,         //
        round_decimal,
        dim,        //
        query_data  //
    };

    SearchInfo search_info;
    auto index_info = std::map<std::string, std::string>{};
    search_info.topk_ = topk;
    search_info.round_decimal_ = round_decimal;
    search_info.metric_type_ = metric_type;
    auto base_dataset = query::dataset::RawDataset{
        int64_t(0), dim, N, (const void*)bin_vec.data()};
    auto sub_result = query::BruteForceSearch(search_dataset,
                                              base_dataset,
                                              search_info,
                                              index_info,
                                              nullptr,
                                              DataType::VECTOR_BINARY,
                                              DataType::NONE,
                                              nullptr);

    SearchResult sr;
    sr.total_nq_ = num_queries;
    sr.unity_topK_ = topk;
    sr.seg_offsets_ = std::move(sub_result.mutable_offsets());
    sr.distances_ = std::move(sub_result.mutable_distances());

    auto json = ResultJson(sr, round_decimal);
#ifdef __linux__
    auto ref = nlohmann::json::parse(R"(
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
    auto ref = nlohmann::json::parse(R"(
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
