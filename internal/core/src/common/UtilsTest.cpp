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

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "common/Utils.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"

TEST(Util_Common, SparseRowsRejectMisalignedSizes) {
    for (bool validate : {true, false}) {
        for (size_t size : {9, 1, 7, 17, 25}) {
            SCOPED_TRACE(size);
            SCOPED_TRACE(validate);
            std::string bytes(size, '\0');
            try {
                (void)milvus::CopyAndWrapSparseRow(
                    bytes.data(), bytes.size(), validate);
                FAIL() << "expected invalid sparse row length";
            } catch (const milvus::SegcoreError& error) {
                EXPECT_EQ(error.get_error_code(),
                          milvus::ErrorCode::DataFormatBroken);
            }
        }
    }
}

TEST(Util_Common, SparseRowsPreserveAlignedData) {
    const std::string bytes("\x01\0\0\0\0\0\x80\x3f", 8);
    for (bool validate : {true, false}) {
        std::vector<std::string> rows{"", bytes, bytes};
        auto result = milvus::SparseBytesToRows(rows, validate);
        EXPECT_EQ(result[0].size(), 0);
        for (size_t i = 1; i < rows.size(); ++i) {
            ASSERT_EQ(result[i].size(), 1);
            EXPECT_EQ(result[i][0].id, 1);
            EXPECT_FLOAT_EQ(result[i][0].val, 1.0f);
            EXPECT_EQ(std::string(static_cast<const char*>(result[i].data()),
                                  result[i].data_byte_size()),
                      bytes);
        }
    }
}

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

TEST(Util_Common, CheckPlusOverflowKeepsSystemClassification) {
    try {
        (void)milvus::checkPlus<int64_t>(std::numeric_limits<int64_t>::max(),
                                         1);
        FAIL() << "expected integer overflow";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::ErrorCode::UnexpectedError);
    }
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
