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
#include <string>

#include "common/Utils.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"

TEST(Util_Common, GetCommonPrefix) {
    std::string str1 = "";
    std::string str2 = "milvus";
    auto common_prefix = milvus::GetCommonPrefix(str1, str2);
    EXPECT_STREQ(common_prefix.c_str(), "");

    str1 = "milvus";
    str2 = "milvus is great";
    common_prefix = milvus::GetCommonPrefix(str1, str2);
    EXPECT_STREQ(common_prefix.c_str(), "milvus");

    str1 = "milvus";
    str2 = "";
    common_prefix = milvus::GetCommonPrefix(str1, str2);
    EXPECT_STREQ(common_prefix.c_str(), "");
}

TEST(SimilarityCorelation, Naive) {
    ASSERT_TRUE(milvus::PositivelyRelated(knowhere::metric::IP));
    ASSERT_TRUE(milvus::PositivelyRelated(knowhere::metric::COSINE));

    ASSERT_FALSE(milvus::PositivelyRelated(knowhere::metric::L2));
    ASSERT_FALSE(milvus::PositivelyRelated(knowhere::metric::HAMMING));
    ASSERT_FALSE(milvus::PositivelyRelated(knowhere::metric::JACCARD));
    ASSERT_FALSE(milvus::PositivelyRelated(knowhere::metric::SUBSTRUCTURE));
    ASSERT_FALSE(milvus::PositivelyRelated(knowhere::metric::SUPERSTRUCTURE));
}

TEST(SimilarityCorelation, MaxSimMetrics) {
    // MAX_SIM, MAX_SIM_IP, MAX_SIM_COSINE are positively related
    // (higher distance = better similarity)
    ASSERT_TRUE(milvus::PositivelyRelated(knowhere::metric::MAX_SIM));
    ASSERT_TRUE(milvus::PositivelyRelated(knowhere::metric::MAX_SIM_IP));
    ASSERT_TRUE(milvus::PositivelyRelated(knowhere::metric::MAX_SIM_COSINE));

    // MAX_SIM_L2, MAX_SIM_HAMMING, MAX_SIM_JACCARD are negatively related
    // (lower distance = better similarity)
    ASSERT_FALSE(milvus::PositivelyRelated(knowhere::metric::MAX_SIM_L2));
    ASSERT_FALSE(milvus::PositivelyRelated(knowhere::metric::MAX_SIM_HAMMING));
    ASSERT_FALSE(milvus::PositivelyRelated(knowhere::metric::MAX_SIM_JACCARD));
}
