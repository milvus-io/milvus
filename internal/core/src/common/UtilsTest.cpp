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

TEST(Util_Common, CheckPlusOverflowKeepsSystemClassification) {
    try {
        (void)milvus::checkPlus<int64_t>(std::numeric_limits<int64_t>::max(),
                                         1);
        FAIL() << "expected integer overflow";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::ErrorCode::UnexpectedError);
    }
}

TEST(Util_Common, SparseRowValidationAndCopy) {
    using SparseRow = knowhere::sparse::SparseRow<milvus::SparseValueType>;
    SparseRow source(std::vector<std::pair<uint32_t, float>>{
        {0, 0.0f},
        {std::numeric_limits<uint32_t>::max() - 1,
         std::numeric_limits<float>::max()}});
    std::string unaligned(source.data_byte_size() + 1, '\0');
    std::memcpy(unaligned.data() + 1, source.data(), source.data_byte_size());
    auto* data = unaligned.data() + 1;
    EXPECT_EQ(milvus::ValidateSparseRow(data, source.data_byte_size()),
              nullptr);
    auto copy =
        milvus::CopyAndWrapSparseRow(data, source.data_byte_size(), true);
    ASSERT_EQ(copy.size(), source.size());
    for (size_t i = 0; i < source.size(); ++i) {
        EXPECT_EQ(copy[i].id, source[i].id);
        EXPECT_EQ(copy[i].val, source[i].val);
    }
    std::memset(data, 0, source.data_byte_size());
    EXPECT_EQ(copy[1].id, source[1].id);
    EXPECT_EQ(copy[1].val, source[1].val);

    EXPECT_EQ(milvus::ValidateSparseRow(nullptr, 0), nullptr);
    EXPECT_EQ(milvus::CopyAndWrapSparseRow("", 0, true).size(), 0);
}

TEST(Util_Common, SparseRowValidationRejectsInvalidData) {
    using SparseRow = knowhere::sparse::SparseRow<milvus::SparseValueType>;
    auto check_invalid = [](const void* data, size_t size, const char* reason) {
        EXPECT_STREQ(milvus::ValidateSparseRow(data, size), reason);
        try {
            milvus::CopyAndWrapSparseRow(data, size, true);
            FAIL() << "expected rejection: " << reason;
        } catch (const milvus::SegcoreError& e) {
            EXPECT_EQ(e.get_error_code(), milvus::UnexpectedError);
            EXPECT_NE(std::string(e.what()).find(reason), std::string::npos);
        }
    };
    // Invalid lengths must fail before accessing or copying the buffer.
    check_invalid(nullptr, 1, "Invalid size for sparse row data");
    check_invalid(nullptr,
                  SparseRow::element_size() + 1,
                  "Invalid size for sparse row data");
    const std::vector<
        std::pair<std::vector<std::pair<uint32_t, float>>, const char*>>
        cases = {{{{2, 1}, {2, 2}},
                  "Invalid sparse row: id should be strict ascending"},
                 {{{3, 1}, {2, 2}},
                  "Invalid sparse row: id should be strict ascending"},
                 {{{std::numeric_limits<uint32_t>::max(), 1}},
                  "Invalid sparse row: id should be smaller than uint32 max"},
                 {{{1, -1}}, "Invalid sparse row: negative value"},
                 {{{1, std::numeric_limits<float>::infinity()}},
                  "Invalid sparse row: NaN or Inf value"},
                 {{{1, std::numeric_limits<float>::quiet_NaN()}},
                  "Invalid sparse row: NaN or Inf value"}};
    for (const auto& [entries, reason] : cases) {
        SparseRow source(entries);
        check_invalid(source.data(), source.data_byte_size(), reason);
        // Trusted callers retain the existing opt-out from validation.
        EXPECT_NO_THROW(milvus::CopyAndWrapSparseRow(
            source.data(), source.data_byte_size(), false));
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
