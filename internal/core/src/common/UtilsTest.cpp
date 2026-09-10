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

TEST(Util_Common, FieldDataRowValidData) {
    milvus::DataArray field_data;
    field_data.add_valid_data(true);
    field_data.add_valid_data(false);

    EXPECT_EQ(milvus::GetFieldDataRowValidData(field_data).size(), 2);
    EXPECT_TRUE(milvus::GetFieldDataRowValidData(field_data)[0]);
    EXPECT_FALSE(milvus::GetFieldDataRowValidData(field_data)[1]);

    field_data.clear_valid_data();
    auto* scalar_valid_data =
        field_data.mutable_scalars()->mutable_valid_data();
    scalar_valid_data->Add(false);
    scalar_valid_data->Add(true);

    const auto& current = milvus::GetFieldDataRowValidData(field_data);
    EXPECT_EQ(current.size(), 2);
    EXPECT_FALSE(current[0]);
    EXPECT_TRUE(current[1]);
}

TEST(Util_Common, MutableFieldDataRowValidData) {
    milvus::DataArray scalar_field;
    scalar_field.add_valid_data(true);
    scalar_field.set_type(milvus::proto::schema::DataType::Int64);

    auto* scalar_valid_data =
        milvus::MutableFieldDataRowValidData(&scalar_field);
    scalar_valid_data->Add(false);
    EXPECT_TRUE(scalar_field.valid_data().empty());
    ASSERT_EQ(scalar_field.scalars().valid_data_size(), 1);
    EXPECT_FALSE(scalar_field.scalars().valid_data(0));

    milvus::DataArray vector_field;
    vector_field.add_valid_data(false);
    vector_field.set_type(milvus::proto::schema::DataType::FloatVector);

    auto* vector_valid_data =
        milvus::MutableFieldDataRowValidData(&vector_field);
    vector_valid_data->Add(true);
    EXPECT_TRUE(vector_field.valid_data().empty());
    ASSERT_EQ(vector_field.vectors().valid_data_size(), 1);
    EXPECT_TRUE(vector_field.vectors().valid_data(0));
}

TEST(Util_Common, SaturatingAddReturnsExactSum) {
    EXPECT_EQ(milvus::SaturatingAdd(uint32_t{40}, uint32_t{2}), uint32_t{42});
}

TEST(Util_Common, SaturatingAddClampsOnOverflow) {
    constexpr auto max = std::numeric_limits<uint64_t>::max();
    EXPECT_EQ(milvus::SaturatingAdd(max - 1, uint64_t{2}), max);
}

TEST(Util_Common, SaturatingMultiplyReturnsExactProduct) {
    EXPECT_EQ(milvus::SaturatingMultiply(uint32_t{6}, uint32_t{7}),
              uint32_t{42});
}

TEST(Util_Common, SaturatingMultiplyClampsOnOverflow) {
    constexpr auto max = std::numeric_limits<uint64_t>::max();
    EXPECT_EQ(milvus::SaturatingMultiply(max / 2 + 1, uint64_t{2}), max);
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
