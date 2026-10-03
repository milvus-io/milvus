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
            EXPECT_EQ(e.get_error_code(), milvus::DataFormatBroken);
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
