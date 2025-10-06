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

#include <arrow/array/builder_binary.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <memory>

#include "SkipIndex.h"
#include "common/Schema.h"
#include "arrow/type_fwd.h"
#include "common/FieldDataInterface.h"

namespace milvus {
class SkipIndexTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        std::vector<int64_t> pks = {1, 2, 3, 4, 5, 6, 7, 8, 9, 12};
        std::vector<int32_t> int32s = {2, 2, 3, 4, 5, 6, 7, 8, 9, 12};
        std::vector<int16_t> int16s = {2, 2, 3, 4, 5, 6, 7, 8, 9, 12};
        std::vector<int8_t> int8s = {2, 2, 3, 4, 5, 6, 7, 8, 9, 12};
        std::vector<float> floats = {
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
        std::vector<double> doubles = {
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
        std::vector<std::string> strings = {
            "e", "f", "g", "g", "j", "l", "o", "q", "r", "t"};
        std::vector<std::string> strings_for_match = {
            "hello world", "milvus data", "vector search"};

        std::vector<bool> bools_mixed = {
            true, true, false, true, false, false, true, true, false, false};
        std::vector<bool> bools_all_true(10, true);
        std::vector<bool> bools_all_false(10, false);

        std::vector<std::int64_t> int64_low_cardinality;
        std::vector<std::int64_t> int64_high_cardinality;
        std::vector<std::string> strings_low_card;
        std::vector<std::string> strings_high_card;
        int64_low_cardinality.reserve(100);
        int64_high_cardinality.reserve(100);
        strings_low_card.reserve(100);
        strings_high_card.reserve(100);
        for (int i = 0; i < 100; i++) {
            strings_high_card.push_back("str_" + std::to_string(i));
            strings_low_card.push_back("str_" + std::to_string(i % 2));
            int64_high_cardinality.push_back(i);
            int64_low_cardinality.push_back(i % 2);
        }

        arrow::Int64Builder pk_builder;
        arrow::Int32Builder i32_builder;
        arrow::Int16Builder i16_builder;
        arrow::Int8Builder i8_builder;
        arrow::FloatBuilder float_builder;
        arrow::DoubleBuilder double_builder;
        arrow::StringBuilder string_builder;
        arrow::StringBuilder string_for_match_builder;

        arrow::Int64Builder i64_nullable_builder;
        arrow::Int64Builder i64_low_cardinality_builder;

        arrow::BooleanBuilder bool_builder;
        arrow::BooleanBuilder bool_all_true_builder;
        arrow::BooleanBuilder bool_all_false_builder;

        arrow::Int64Builder i64_high_cardinality_builder;
        arrow::StringBuilder string_low_cardinality_builder;
        arrow::StringBuilder string_high_cardinality_builder;

        pk_builder.AppendValues(pks);
        i32_builder.AppendValues(int32s);
        i16_builder.AppendValues(int16s);
        i8_builder.AppendValues(int8s);
        float_builder.AppendValues(floats);
        double_builder.AppendValues(doubles);
        string_builder.AppendValues(strings);
        string_for_match_builder.AppendValues(strings_for_match);

        bool_builder.AppendValues(bools_mixed);
        bool_all_true_builder.AppendValues(bools_all_true);
        bool_all_false_builder.AppendValues(bools_all_false);

        i64_low_cardinality_builder.AppendValues(int64_low_cardinality);
        i64_high_cardinality_builder.AppendValues(int64_high_cardinality);
        string_low_cardinality_builder.AppendValues(strings_low_card);
        string_high_cardinality_builder.AppendValues(strings_high_card);

        for (int i = 0; i < 10; i++) {
            if (i > 4) {
                i64_nullable_builder.AppendNull();
            } else {
                i64_nullable_builder.Append(pks[i]);
            }
        }

        pk_builder.Finish(&pk_array_).ok();
        i32_builder.Finish(&i32_array_).ok();
        i16_builder.Finish(&i16_array_).ok();
        i8_builder.Finish(&i8_array_).ok();
        float_builder.Finish(&float_array_).ok();
        double_builder.Finish(&double_array_).ok();
        string_builder.Finish(&string_array_).ok();

        string_for_match_builder.Finish(&string_for_match_array_).ok();
        i64_nullable_builder.Finish(&i64_nullable_array_).ok();

        bool_builder.Finish(&bool_array_).ok();
        bool_all_true_builder.Finish(&bool_all_true_array_).ok();
        bool_all_false_builder.Finish(&bool_all_false_array_).ok();

        i64_low_cardinality_builder.Finish(&i64_low_cardinality_array_).ok();
        i64_high_cardinality_builder.Finish(&i64_high_cardinality_array_).ok();

        string_low_cardinality_builder.Finish(&string_low_cardinality_array_)
            .ok();
        string_high_cardinality_builder.Finish(&string_high_cardinality_array_)
            .ok();
    }

    std::unique_ptr<SkipIndex>
    BuildSkipIndex(SchemaPtr schema,
                   std::vector<std::shared_ptr<arrow::Array>> arrays) {
        if (arrays.size() == 0) {
            return nullptr;
        }
        auto arrow_schema = schema->ConvertToArrowSchema();
        auto record_batch =
            arrow::RecordBatch::Make(arrow_schema, arrays[0]->length(), arrays);
        auto batches =
            std::vector<std::shared_ptr<arrow::RecordBatch>>{record_batch};
        auto chunk_skipindex = std::make_unique<ChunkSkipIndex>(batches);
        auto chunk_skipindex_vec = std::vector<std::unique_ptr<ChunkSkipIndex>>{
            std::move(chunk_skipindex)};
        auto skip_index = std::make_unique<SkipIndex>();
        skip_index->LoadSkipIndex(chunk_skipindex_vec);
        return skip_index;
    }

 protected:
    std::shared_ptr<arrow::Array> pk_array_;
    std::shared_ptr<arrow::Array> i32_array_;
    std::shared_ptr<arrow::Array> i16_array_;
    std::shared_ptr<arrow::Array> i8_array_;
    std::shared_ptr<arrow::Array> float_array_;
    std::shared_ptr<arrow::Array> double_array_;
    std::shared_ptr<arrow::Array> string_array_;

    std::shared_ptr<arrow::Array> string_for_match_array_;
    std::shared_ptr<arrow::Array> i64_nullable_array_;

    std::shared_ptr<arrow::Array> bool_array_;
    std::shared_ptr<arrow::Array> bool_all_true_array_;
    std::shared_ptr<arrow::Array> bool_all_false_array_;

    std::shared_ptr<arrow::Array> i64_low_cardinality_array_;
    std::shared_ptr<arrow::Array> i64_high_cardinality_array_;
    std::shared_ptr<arrow::Array> string_low_cardinality_array_;
    std::shared_ptr<arrow::Array> string_high_cardinality_array_;
};

TEST_F(SkipIndexTest, SkipUnaryRangeNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("int64_field", DataType::INT64, true);
    auto skip_index = BuildSkipIndex(schema, {i64_nullable_array_});

    // test for int64
    bool equal_6_skip =
        skip_index->CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::Equal, 6);
    bool equal_7_skip =
        skip_index->CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::Equal, 7);
    bool equal_2_skip =
        skip_index->CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::Equal, 2);
    bool equal_1_skip =
        skip_index->CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::Equal, 1);
    ASSERT_TRUE(equal_6_skip);
    ASSERT_TRUE(equal_7_skip);
    ASSERT_FALSE(equal_2_skip);
    ASSERT_FALSE(equal_1_skip);
    bool less_than_1_skip =
        skip_index->CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::LessThan, 1);
    bool less_than_7_skip =
        skip_index->CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::LessThan, 7);
    ASSERT_TRUE(less_than_1_skip);
    ASSERT_FALSE(less_than_7_skip);
    bool less_equal_than_1_skip = skip_index->CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::LessEqual, 1);
    bool less_equal_than_15_skip = skip_index->CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::LessThan, 15);
    ASSERT_FALSE(less_equal_than_1_skip);
    ASSERT_FALSE(less_equal_than_15_skip);
    bool greater_than_10_skip = skip_index->CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterThan, 12);
    bool greater_than_7_skip = skip_index->CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterThan, 7);
    bool greater_than_6_skip = skip_index->CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterThan, 6);
    bool greater_than_1_skip = skip_index->CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterThan, 1);
    ASSERT_TRUE(greater_than_10_skip);
    ASSERT_TRUE(greater_than_7_skip);
    ASSERT_TRUE(greater_than_6_skip);
    ASSERT_FALSE(greater_than_1_skip);
    bool greater_equal_than_6_skip = skip_index->CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterEqual, 6);
    bool greater_equal_than_2_skip = skip_index->CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterEqual, 2);
    ASSERT_TRUE(greater_equal_than_6_skip);
    ASSERT_FALSE(greater_equal_than_2_skip);
}

TEST_F(SkipIndexTest, SkipUnaryRange) {
    auto schema = std::make_shared<Schema>();
    FieldId pk_fid = schema->AddDebugField("pk", DataType::INT64);
    FieldId i32_fid = schema->AddDebugField("int32_field", DataType::INT32);
    FieldId i16_fid = schema->AddDebugField("int16_field", DataType::INT16);
    FieldId i8_fid = schema->AddDebugField("int8_field", DataType::INT8);
    FieldId float_fid = schema->AddDebugField("float_field", DataType::FLOAT);
    FieldId double_fid =
        schema->AddDebugField("double_field", DataType::DOUBLE);
    FieldId string_fid =
        schema->AddDebugField("string_field", DataType::VARCHAR);
    auto skip_index = BuildSkipIndex(schema,
                                     {pk_array_,
                                      i32_array_,
                                      i16_array_,
                                      i8_array_,
                                      float_array_,
                                      double_array_,
                                      string_array_});

    // test for int64
    ASSERT_FALSE(
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::Equal, 5));
    ASSERT_FALSE(
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::Equal, 12));
    ASSERT_TRUE(
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::Equal, 10));
    ASSERT_TRUE(
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessThan, 1));
    ASSERT_TRUE(
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessThan, 5));
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::LessEqual, 1));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::LessEqual, 15));
    skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessThan, 15);
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, 10));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, 5));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterEqual, 10));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterEqual, 5));

    // test for int32
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<int32_t>(
        i32_fid, 0, OpType::LessThan, 1));

    // test for int16
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<int16_t>(
        i16_fid, 0, OpType::LessThan, 12));

    // test for int8
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<int8_t>(
        i8_fid, 0, OpType::GreaterThan, 12));

    // test for float
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<float>(
        float_fid, 0, OpType::GreaterThan, 10.0));

    // test for double
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<double>(
        double_fid, 0, OpType::GreaterThan, 10.0));
}

TEST_F(SkipIndexTest, SkipUnaryRangeString) {
    auto schema = std::make_shared<Schema>();
    FieldId string_fid =
        schema->AddDebugField("string_field", DataType::VARCHAR);
    auto skip_index = BuildSkipIndex(schema, {string_array_});

    //test for string
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::Equal, "w"));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::Equal, "e"));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::Equal, "j"));

    ASSERT_TRUE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::LessThan, "e"));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::LessEqual, "e"));

    ASSERT_TRUE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::GreaterThan, "t"));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::GreaterEqual, "t"));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<int64_t>(
        string_fid, 0, OpType::GreaterEqual, 1));
}

TEST_F(SkipIndexTest, SkipUnaryRangeStringMatch) {
    auto schema = std::make_shared<Schema>();
    auto string_fid =
        schema->AddDebugField("string_match_field", DataType::VARCHAR);
    auto skip_index = BuildSkipIndex(schema, {string_for_match_array_});

    ASSERT_TRUE(skip_index->HasMetric(
        string_fid, 0, FieldChunkMetricType::NGRAM_FILTER));

    // 1. Pattern contains an n-gram that is definitely not in the data.
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::InnerMatch, "xyz"));
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::PrefixMatch, "xyz"));
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::PostfixMatch, "xyz"));

    // Pattern is shorter than NGRAM_SIZE. Cannot skip.
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::InnerMatch, "ve"));

    // Pattern's n-grams might exist in the data. Cannot skip.
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::InnerMatch, "vec"));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::PostfixMatch, "rld"));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::PrefixMatch, "mil"));

    // Pattern contains n-grams that exist, but the full string doesn't match.
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<std::string>(
        string_fid, 0, OpType::InnerMatch, "datorch"));
}

TEST_F(SkipIndexTest, SkipBinaryRangeNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("int64_field", DataType::INT64, true);
    auto skip_index = BuildSkipIndex(schema, {i64_nullable_array_});

    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, -3, 1, true, true));
    ASSERT_TRUE(skip_index->CanSkipBinaryRange<int64_t>(
        i64_fid, 0, -3, 1, true, false));

    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, 1, 8, true, true));
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, 1, 5, true, false));
    ASSERT_TRUE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, 5, 8, false, true));
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, 5, 8, true, true));
}

TEST_F(SkipIndexTest, SkipBinaryRange) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto skip_index = BuildSkipIndex(schema, {pk_array_});

    // test for int64
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, -3, 1, true, true));
    ASSERT_TRUE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, -3, 1, true, false));

    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, 7, 9, true, true));
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, 8, 12, true, false));

    ASSERT_TRUE(skip_index->CanSkipBinaryRange<int64_t>(
        pk_fid, 0, 12, 14, false, true));
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, 12, 14, true, true));
}

TEST_F(SkipIndexTest, SkipBinaryRangeString) {
    auto schema = std::make_shared<Schema>();
    FieldId string_fid =
        schema->AddDebugField("string_field", DataType::VARCHAR);
    auto skip_index = BuildSkipIndex(schema, {string_array_});

    ASSERT_TRUE(skip_index->CanSkipBinaryRange<std::string>(
        string_fid, 0, "a", "c", true, true));
    ASSERT_TRUE(skip_index->CanSkipBinaryRange<std::string>(
        string_fid, 0, "c", "e", true, false));
    ASSERT_FALSE(skip_index->CanSkipBinaryRange<std::string>(
        string_fid, 0, "c", "e", true, true));
    ASSERT_FALSE(skip_index->CanSkipBinaryRange<std::string>(
        string_fid, 0, "e", "z", false, true));
    ASSERT_FALSE(skip_index->CanSkipBinaryRange<std::string>(
        string_fid, 0, "t", "z", true, true));
    ASSERT_TRUE(skip_index->CanSkipBinaryRange<std::string>(
        string_fid, 0, "t", "z", false, true));
    ASSERT_FALSE(skip_index->CanSkipBinaryRange<int64_t>(
        string_fid, 0, 1, 2, false, true));
}

TEST_F(SkipIndexTest, SkipInQueryNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("int64_field", DataType::INT64, true);
    auto skip_index = BuildSkipIndex(schema, {i64_nullable_array_});

    std::vector<int64_t> values1 = {6, 7, 8};
    ASSERT_TRUE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, values1));
    std::vector<int64_t> values2 = {4, 5, 6};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, values2));
    std::vector<int64_t> values3 = {1, 2, 3};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, values3));
}

TEST_F(SkipIndexTest, SkipInQueryBool) {
    auto schema = std::make_shared<Schema>();
    auto bool_mixed_fid = schema->AddDebugField("bool_mixed", DataType::BOOL);
    auto bool_all_true_fid =
        schema->AddDebugField("bool_all_true", DataType::BOOL);
    auto bool_all_false_fid =
        schema->AddDebugField("bool_all_false", DataType::BOOL);
    auto skip_index = BuildSkipIndex(
        schema, {bool_array_, bool_all_true_array_, bool_all_false_array_});

    // The query vector for bool is {query_contains_true, query_contains_false}
    std::vector<bool> q_true_only = {true, false};
    std::vector<bool> q_false_only = {false, true};
    std::vector<bool> q_both = {true, true};

    // 1. Test with a chunk containing both true and false
    // Chunk has both, so no query should be skippable
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<bool>(bool_mixed_fid, 0, q_true_only));
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<bool>(bool_mixed_fid, 0, q_false_only));
    ASSERT_FALSE(skip_index->CanSkipInQuery<bool>(bool_mixed_fid, 0, q_both));

    // 2. Test with a chunk containing only true
    // Chunk only has true, so a query for only false can be skipped
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<bool>(bool_all_true_fid, 1, q_true_only));
    ASSERT_TRUE(
        skip_index->CanSkipInQuery<bool>(bool_all_true_fid, 1, q_false_only));
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<bool>(bool_all_true_fid, 1, q_both));

    // 3. Test with a chunk containing only false
    // Chunk only has false, so a query for only true can be skipped
    ASSERT_TRUE(
        skip_index->CanSkipInQuery<bool>(bool_all_false_fid, 2, q_true_only));
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<bool>(bool_all_false_fid, 2, q_false_only));
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<bool>(bool_all_false_fid, 2, q_both));
}

TEST_F(SkipIndexTest, SkipInQueryInt) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto skip_index = BuildSkipIndex(schema, {pk_array_});

    std::vector<int64_t> values1 = {11, 12, 13};
    ASSERT_TRUE(skip_index->CanSkipInQuery<int64_t>(pk_fid, 0, values1));
    std::vector<int64_t> values2 = {9, 10, 11};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(pk_fid, 0, values2));
    std::vector<int64_t> values3 = {1, 2, 3};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(pk_fid, 0, values3));
}

TEST_F(SkipIndexTest, SkipInQueryLowCardinalityInt) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid =
        schema->AddDebugField("i64_low_cardinality", DataType::INT64);
    auto skip_index = BuildSkipIndex(schema, {i64_low_cardinality_array_});

    // Low cardinality data should build a Set metric, not a BloomFilter
    ASSERT_TRUE(skip_index->HasMetric(i64_fid, 0, FieldChunkMetricType::SET));
    ASSERT_FALSE(
        skip_index->HasMetric(i64_fid, 0, FieldChunkMetricType::BLOOM_FILTER));

    // Test cases for IN query
    // 1. All values in query are not in the data. Should skip.
    // The data contains only {0, 1}.
    std::vector<int64_t> q_all_miss = {2, 3, 4};
    ASSERT_TRUE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, q_all_miss));

    // 2. Some values in query are in the data. Should not skip.
    std::vector<int64_t> q_some_hit = {1, 2, 4};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, q_some_hit));

    // 3. All values in query are in the data. Should not skip.
    std::vector<int64_t> q_all_hit = {0, 1};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, q_all_hit));

    // 4. Empty query. Should not skip.
    std::vector<int64_t> q_empty = {};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, q_empty));
}

TEST_F(SkipIndexTest, SkipInQueryHighCardinalityInt) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid =
        schema->AddDebugField("i64_high_cardinality", DataType::INT64);
    auto skip_index = BuildSkipIndex(schema, {i64_high_cardinality_array_});

    // High cardinality data should build a BloomFilter metric, not a Set
    ASSERT_TRUE(
        skip_index->HasMetric(i64_fid, 0, FieldChunkMetricType::BLOOM_FILTER));
    ASSERT_FALSE(skip_index->HasMetric(i64_fid, 0, FieldChunkMetricType::SET));

    // Test cases for IN query
    // 1. All values in query are not in the data. Should skip.
    // The data contains {0, 1, ..., 99}.
    std::vector<int64_t> q_all_miss = {101, 102, 103};
    ASSERT_TRUE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, q_all_miss));

    // 2. Some values in query are in the data. Should not skip.
    std::vector<int64_t> q_some_hit = {1, 101, 102};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, q_some_hit));

    // 3. All values in query are in the data. Should not skip.
    std::vector<int64_t> q_all_hit = {1, 50, 99};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, q_all_hit));

    // 4. Empty query. Should not skip.
    std::vector<int64_t> q_empty = {};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, q_empty));
}

TEST_F(SkipIndexTest, SkipInQueryFloat) {
    auto schema = std::make_shared<Schema>();
    auto float_fid = schema->AddDebugField("float_field", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double_field", DataType::DOUBLE);
    auto skip_index = BuildSkipIndex(schema, {float_array_, double_array_});

    // For float types, only MinMax metric should be created.
    ASSERT_TRUE(
        skip_index->HasMetric(float_fid, 0, FieldChunkMetricType::MINMAX));
    ASSERT_FALSE(
        skip_index->HasMetric(float_fid, 0, FieldChunkMetricType::SET));
    ASSERT_FALSE(skip_index->HasMetric(
        float_fid, 0, FieldChunkMetricType::BLOOM_FILTER));

    ASSERT_TRUE(
        skip_index->HasMetric(double_fid, 0, FieldChunkMetricType::MINMAX));
    ASSERT_FALSE(
        skip_index->HasMetric(double_fid, 0, FieldChunkMetricType::SET));
    ASSERT_FALSE(skip_index->HasMetric(
        double_fid, 0, FieldChunkMetricType::BLOOM_FILTER));

    // Test data range is [1.0, 10.0] for both float and double arrays.

    // 1. Query range is completely outside the data range. Should skip.
    std::vector<float> q_float_miss = {11.0f, 12.5f, 15.0f};
    ASSERT_TRUE(skip_index->CanSkipInQuery<float>(float_fid, 0, q_float_miss));

    std::vector<double> q_double_miss = {-5.0, -2.5, 0.5};
    ASSERT_TRUE(
        skip_index->CanSkipInQuery<double>(double_fid, 0, q_double_miss));

    // 2. Query range overlaps with the data range. Should not skip.
    std::vector<float> q_float_overlap = {9.5f, 10.5f, 11.5f};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<float>(float_fid, 0, q_float_overlap));

    std::vector<double> q_double_overlap = {0.5, 1.5, 2.5};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<double>(double_fid, 0, q_double_overlap));

    // 3. Query range is completely inside the data range. Should not skip.
    std::vector<float> q_float_hit = {2.0f, 5.0f, 8.0f};
    ASSERT_FALSE(skip_index->CanSkipInQuery<float>(float_fid, 0, q_float_hit));

    std::vector<double> q_double_hit = {3.0, 6.0, 9.0};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<double>(double_fid, 0, q_double_hit));

    // 4. Empty query. Should not skip.
    std::vector<float> q_float_empty = {};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<float>(float_fid, 0, q_float_empty));
}

TEST_F(SkipIndexTest, SkipInQueryString) {
    auto schema = std::make_shared<Schema>();
    FieldId str_fid = schema->AddDebugField("str_field", DataType::VARCHAR);
    auto skip_index = BuildSkipIndex(schema, {string_array_});

    std::vector<std::string> str_values1 = {"u", "v", "w"};
    ASSERT_TRUE(
        skip_index->CanSkipInQuery<std::string>(str_fid, 0, str_values1));
    std::vector<std::string> str_values2 = {"s", "t", "u"};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<std::string>(str_fid, 0, str_values2));
    std::vector<std::string> str_values3 = {"e", "f", "g"};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<std::string>(str_fid, 0, str_values3));
}

TEST_F(SkipIndexTest, SkipInQueryLowCardinalityString) {
    auto schema = std::make_shared<Schema>();
    auto string_fid =
        schema->AddDebugField("string_low_cardinality", DataType::VARCHAR);
    auto skip_index = BuildSkipIndex(schema, {string_low_cardinality_array_});

    // Low cardinality string data should build a Set metric.
    ASSERT_TRUE(
        skip_index->HasMetric(string_fid, 0, FieldChunkMetricType::SET));

    // Test cases for IN query
    // The data contains only {"str_0", "str_1"}.
    // 1. All values in query are not in the data. Should skip.
    std::vector<std::string> q_all_miss = {"str_2", "str_3", "str_4"};
    ASSERT_TRUE(
        skip_index->CanSkipInQuery<std::string>(string_fid, 0, q_all_miss));

    // 2. Some values in query are in the data. Should not skip.
    std::vector<std::string> q_some_hit = {"str_0", "str_2", "str_3"};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<std::string>(string_fid, 0, q_some_hit));

    // 3. All values in query are in the data. Should not skip.
    std::vector<std::string> q_all_hit = {"str_0", "str_1"};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<std::string>(string_fid, 0, q_all_hit));

    // 4. Empty query. Should not skip.
    std::vector<std::string> q_empty = {};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<std::string>(string_fid, 0, q_empty));
}

TEST_F(SkipIndexTest, SkipInQueryHighCardinalityString) {
    auto schema = std::make_shared<Schema>();
    auto string_fid =
        schema->AddDebugField("string_high_cardinality", DataType::VARCHAR);
    auto skip_index = BuildSkipIndex(schema, {string_high_cardinality_array_});

    // High cardinality string data should build a BloomFilter metric.
    ASSERT_TRUE(skip_index->HasMetric(
        string_fid, 0, FieldChunkMetricType::BLOOM_FILTER));

    // Test cases for IN query
    // The data contains {"str_0", "str_1", ..., "str_99"}.
    // 1. All values in query are not in the data. Should skip.
    std::vector<std::string> q_all_miss = {"str_100", "str_101", "str_102"};
    ASSERT_TRUE(
        skip_index->CanSkipInQuery<std::string>(string_fid, 0, q_all_miss));

    // 2. Some values in query are in the data. Should not skip.
    std::vector<std::string> q_some_hit = {"str_0", "str_100", "str_101"};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<std::string>(string_fid, 0, q_some_hit));

    // 3. All values in query are in the data. Should not skip.
    std::vector<std::string> q_all_hit = {"str_10", "str_20", "str_30"};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<std::string>(string_fid, 0, q_all_hit));

    // 4. Empty query. Should not skip.
    std::vector<std::string> q_empty = {};
    ASSERT_FALSE(
        skip_index->CanSkipInQuery<std::string>(string_fid, 0, q_empty));
}

TEST_F(SkipIndexTest, SkipBinaryArithRange) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto i32_fid = schema->AddDebugField("int32_field", DataType::INT32);
    auto i16_fid = schema->AddDebugField("int16_field", DataType::INT16);
    auto i8_fid = schema->AddDebugField("int8_field", DataType::INT8);
    auto float_fid = schema->AddDebugField("float_field", DataType::FLOAT);
    auto skip_index = BuildSkipIndex(
        schema, {pk_array_, i32_array_, i16_array_, i8_array_, float_array_});

    // --- Test for Add: field + C op V  =>  field op V - C ---
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Add, 20, 5));
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Add, 15, 5));
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::LessThan, ArithOpType::Add, 0, 5));

    // --- Test for Sub: field - C op V  =>  field op V + C ---
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::LessThan, ArithOpType::Sub, -10, 5));
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Sub, 5, 5));

    // --- Test for Mul: field * C op V ---
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Mul, 20, 2));
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Mul, 21, 2));
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Mul, 0, -2));
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Mul, 10, 0));

    // --- Test for Div: field / C op V ---
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::LessThan, ArithOpType::Div, 0, 2));
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Div, 10, 2));
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Div, 0, -2));
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Div, 10, 0));

    // --- Test with float data ---
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<float>(
        float_fid, 0, OpType::GreaterEqual, ArithOpType::Mul, -2.0, -2.0));
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<float>(
        float_fid, 0, OpType::GreaterThan, ArithOpType::Mul, -2.0, -2.0));

    // --- Test for Overflow/Underflow cases ---
    // For int32_t, data range is [2, 12]
    // Test overflow: field + (-10) > INT32_MAX => field > INT32_MAX + 10
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int32_t>(
        i32_fid,
        0,
        OpType::GreaterThan,
        ArithOpType::Add,
        std::numeric_limits<int32_t>::max(),
        -10));
    // Test underflow: field + 10 > INT32_MIN => field < INT32_MIN - 10
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int32_t>(
        i32_fid,
        0,
        OpType::GreaterThan,
        ArithOpType::Add,
        std::numeric_limits<int32_t>::min(),
        10));

    // For int16_t, data range is [2, 12]
    // Test overflow: field - 1 > INT16_MAX => field > INT16_MAX + 1
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int16_t>(
        i16_fid,
        0,
        OpType::GreaterThan,
        ArithOpType::Sub,
        std::numeric_limits<int16_t>::max(),
        1));

    // For int8_t, data range is [2, 12]
    // Test underflow: field - (-1) < INT8_MIN => field < INT8_MIN - 1
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int8_t>(
        i8_fid,
        0,
        OpType::LessThan,
        ArithOpType::Sub,
        std::numeric_limits<int8_t>::min(),
        -1));
}

TEST(BloomFilter, BloomFilterBasicFunctionality) {
    // Test with integers
    std::vector<int64_t> int_items = {1, 2, 3, 5, 8, 13, 21};
    auto int_bf = BloomFilter::Build(int_items);
    ASSERT_NE(int_bf, nullptr);

    for (const auto& item : int_items) {
        ASSERT_TRUE(int_bf->MightContain<int64_t>(item));
    }
    ASSERT_FALSE(int_bf->MightContain<int64_t>(0));
    ASSERT_FALSE(int_bf->MightContain<int64_t>(4));
    ASSERT_FALSE(int_bf->MightContain<int64_t>(100));

    // Test with strings
    std::vector<std::string> string_items = {"apple", "banana", "cherry"};
    auto string_bf = BloomFilter::Build(string_items);
    ASSERT_NE(string_bf, nullptr);

    for (const auto& item : string_items) {
        ASSERT_TRUE(string_bf->MightContain<std::string>(item));
    }
    ASSERT_FALSE(string_bf->MightContain<std::string>("grape"));
    ASSERT_FALSE(string_bf->MightContain<std::string>("milvus"));
}

TEST(BloomFilter, BloomFilterSerialization) {
    std::vector<int64_t> items = {10, 20, 30, 40, 50};
    auto bf1 = BloomFilter::Build(items);
    ASSERT_NE(bf1, nullptr);

    std::string serialized_data = bf1->Serialize();
    ASSERT_FALSE(serialized_data.empty());

    auto bf2 = BloomFilter::Deserialize(serialized_data);
    ASSERT_NE(bf2, nullptr);

    // Check properties
    ASSERT_EQ(bf1->GetHashCount(), bf2->GetHashCount());
    ASSERT_EQ(bf1->GetBitSize(), bf2->GetBitSize());
    ASSERT_EQ(bf1->GetBitArray(), bf2->GetBitArray());

    // Check behavior
    for (int64_t i = 0; i < 100; ++i) {
        ASSERT_EQ(bf1->MightContain<int64_t>(i), bf2->MightContain<int64_t>(i));
    }
}

TEST(BloomFilter, BloomFilterEdgeCases) {
    // Empty items vector
    std::vector<int> empty_items = {};
    auto empty_bf = BloomFilter::Build(empty_items);
    ASSERT_EQ(empty_bf, nullptr);

    // Single item
    std::vector<std::string> single_item = {"one"};
    auto single_item_bf = BloomFilter::Build(single_item);
    ASSERT_NE(single_item_bf, nullptr);
    ASSERT_TRUE(single_item_bf->MightContain<std::string>("one"));
    ASSERT_FALSE(single_item_bf->MightContain<std::string>("two"));
}

TEST(BloomFilter, BloomFilterBuildFromSet) {
    ankerl::unordered_dense::set<int64_t> item_set = {100, 200, 300};
    auto bf = BloomFilter::Build(item_set);
    ASSERT_NE(bf, nullptr);

    ASSERT_TRUE(bf->MightContain<int64_t>(100));
    ASSERT_TRUE(bf->MightContain<int64_t>(200));
    ASSERT_TRUE(bf->MightContain<int64_t>(300));
    ASSERT_FALSE(bf->MightContain<int64_t>(400));
}

TEST(MinMaxMetric, IntegerFunctionality) {
    MetricsInfo<int64_t> info;
    info.min_ = 10;
    info.max_ = 20;
    MinMaxFieldChunkMetric<int64_t> metric(info);
    ASSERT_TRUE(metric.hasValue_);

    // Test CanSkipUnaryRange
    ASSERT_TRUE(metric.CanSkipUnaryRange(OpType::Equal, 9));   // val < min
    ASSERT_TRUE(metric.CanSkipUnaryRange(OpType::Equal, 21));  // val > max
    ASSERT_FALSE(
        metric.CanSkipUnaryRange(OpType::Equal, 15));  // min <= val <= max
    ASSERT_TRUE(metric.CanSkipUnaryRange(OpType::LessThan, 10));  // val <= min
    ASSERT_FALSE(metric.CanSkipUnaryRange(OpType::LessThan, 11));
    ASSERT_TRUE(metric.CanSkipUnaryRange(OpType::LessEqual, 9));  // val < min
    ASSERT_FALSE(metric.CanSkipUnaryRange(OpType::LessEqual, 10));
    ASSERT_TRUE(
        metric.CanSkipUnaryRange(OpType::GreaterThan, 20));  // val >= max
    ASSERT_FALSE(metric.CanSkipUnaryRange(OpType::GreaterThan, 19));
    ASSERT_TRUE(
        metric.CanSkipUnaryRange(OpType::GreaterEqual, 21));  // val > max
    ASSERT_FALSE(metric.CanSkipUnaryRange(OpType::GreaterEqual, 20));

    // Test CanSkipIn
    ASSERT_TRUE(metric.CanSkipIn({1, 2, 3}));     // all values < min
    ASSERT_TRUE(metric.CanSkipIn({21, 22, 23}));  // all values > max
    ASSERT_FALSE(metric.CanSkipIn({9, 10, 11}));  // some values overlap
    ASSERT_FALSE(metric.CanSkipIn({15, 16}));     // all values inside
    ASSERT_FALSE(metric.CanSkipIn({}));           // empty query
    ASSERT_FALSE(metric.CanSkipIn({10, 20}));     // boundary values

    // Test CanSkipBinaryRange
    ASSERT_TRUE(metric.CanSkipBinaryRange(1, 9, true, true));     // range < min
    ASSERT_TRUE(metric.CanSkipBinaryRange(21, 30, true, true));   // range > max
    ASSERT_FALSE(metric.CanSkipBinaryRange(5, 15, true, true));   // overlap
    ASSERT_FALSE(metric.CanSkipBinaryRange(12, 18, true, true));  // inside
    ASSERT_FALSE(metric.CanSkipBinaryRange(10, 20, true, true));  // same range
    ASSERT_TRUE(metric.CanSkipBinaryRange(
        5, 10, true, false));  // upper bound not inclusive
    ASSERT_FALSE(
        metric.CanSkipBinaryRange(5, 10, true, true));  // upper bound inclusive
}

TEST(MinMaxMetric, StringFunctionality) {
    MetricsInfo<std::string> info;
    info.min_ = "e";
    info.max_ = "p";
    MinMaxFieldChunkMetric<std::string> metric(info);
    ASSERT_TRUE(metric.hasValue_);

    // Test CanSkipUnaryRange
    ASSERT_TRUE(metric.CanSkipUnaryRange(OpType::Equal, "a"));
    ASSERT_TRUE(metric.CanSkipUnaryRange(OpType::Equal, "q"));
    ASSERT_FALSE(metric.CanSkipUnaryRange(OpType::Equal, "f"));
    ASSERT_TRUE(metric.CanSkipUnaryRange(OpType::LessThan, "e"));
    ASSERT_FALSE(metric.CanSkipUnaryRange(OpType::GreaterThan, "p"));

    // Test CanSkipIn
    ASSERT_TRUE(metric.CanSkipIn({"a", "b", "c"}));
    ASSERT_TRUE(metric.CanSkipIn({"q", "r", "s"}));
    ASSERT_FALSE(metric.CanSkipIn({"d", "e", "f"}));
    ASSERT_FALSE(metric.CanSkipIn({"f", "g"}));

    // Test CanSkipBinaryRange
    ASSERT_TRUE(metric.CanSkipBinaryRange("a", "d", true, true));
    ASSERT_TRUE(metric.CanSkipBinaryRange("q", "z", true, true));
    ASSERT_FALSE(metric.CanSkipBinaryRange("d", "f", true, true));
}

TEST(MinMaxMetric, Serialization) {
    // Integer
    MetricsInfo<int32_t> int_info;
    int_info.min_ = -100;
    int_info.max_ = 100;
    MinMaxFieldChunkMetric<int32_t> int_metric1(int_info);
    std::string int_data = int_metric1.Serialize();
    MinMaxFieldChunkMetric<int32_t> int_metric2(int_data);
    ASSERT_TRUE(int_metric2.hasValue_);
    ASSERT_FALSE(int_metric2.CanSkipUnaryRange(OpType::Equal, 0));
    ASSERT_TRUE(int_metric2.CanSkipUnaryRange(OpType::Equal, 101));

    // String
    MetricsInfo<std::string> str_info;
    str_info.min_ = "milvus";
    str_info.max_ = "zilliz";
    MinMaxFieldChunkMetric<std::string> str_metric1(str_info);
    std::string str_data = str_metric1.Serialize();
    MinMaxFieldChunkMetric<std::string> str_metric2(str_data);
    ASSERT_TRUE(str_metric2.hasValue_);
    ASSERT_FALSE(str_metric2.CanSkipUnaryRange(OpType::Equal, "vector"));
    ASSERT_TRUE(str_metric2.CanSkipUnaryRange(OpType::Equal, "apple"));
}

TEST(MinMaxMetric, EdgeCases) {
    // Empty data
    MinMaxFieldChunkMetric<int64_t> empty_metric("");
    ASSERT_FALSE(empty_metric.hasValue_);
    ASSERT_FALSE(empty_metric.CanSkipUnaryRange(OpType::Equal, 10));
    ASSERT_FALSE(empty_metric.CanSkipIn({10, 20}));
    ASSERT_FALSE(empty_metric.CanSkipBinaryRange(10, 20, true, true));

    // Single value data
    MetricsInfo<int64_t> info;
    info.min_ = 5;
    info.max_ = 5;
    MinMaxFieldChunkMetric<int64_t> single_val_metric(info);
    ASSERT_TRUE(single_val_metric.hasValue_);
    ASSERT_FALSE(single_val_metric.CanSkipUnaryRange(OpType::Equal, 5));
    ASSERT_TRUE(single_val_metric.CanSkipUnaryRange(OpType::Equal, 6));
    ASSERT_TRUE(single_val_metric.CanSkipUnaryRange(OpType::LessThan, 5));
    ASSERT_FALSE(single_val_metric.CanSkipUnaryRange(OpType::LessEqual, 5));
}

TEST(SetMetric, IntegerFunctionality) {
    MetricsInfo<int64_t> info;
    info.unique_values_ = {10, 20, 30};
    SetFieldChunkMetric<int64_t> metric(info);

    ASSERT_TRUE(metric.hasValue_);
    ASSERT_FALSE(metric.CanSkipEqual(10));
    ASSERT_TRUE(metric.CanSkipEqual(15));
    ASSERT_FALSE(metric.CanSkipNotEqual(15));
    ASSERT_TRUE(metric.CanSkipNotEqual(10));

    ASSERT_FALSE(metric.CanSkipIn({10, 15}));
    ASSERT_TRUE(metric.CanSkipIn({15, 25}));
    ASSERT_FALSE(metric.CanSkipIn({}));
}

TEST(SetMetric, StringFunctionality) {
    MetricsInfo<std::string> info;
    info.unique_values_ = {"apple", "banana", "cherry"};
    SetFieldChunkMetric<std::string> metric(info);

    ASSERT_TRUE(metric.hasValue_);
    ASSERT_FALSE(metric.CanSkipEqual("apple"));
    ASSERT_TRUE(metric.CanSkipEqual("orange"));
    ASSERT_FALSE(metric.CanSkipNotEqual("orange"));
    ASSERT_TRUE(metric.CanSkipNotEqual("apple"));

    ASSERT_FALSE(metric.CanSkipIn({"apple", "orange"}));
    ASSERT_TRUE(metric.CanSkipIn({"orange", "grape"}));
    ASSERT_FALSE(metric.CanSkipIn({}));
}

TEST(SetMetric, BoolFunctionality) {
    // Case 1: Contains both true and false
    MetricsInfo<bool> info_both;
    info_both.contains_true_ = true;
    info_both.contains_false_ = true;
    SetFieldChunkMetric<bool> metric_both(info_both);
    ASSERT_FALSE(metric_both.CanSkipEqual(true));
    ASSERT_FALSE(metric_both.CanSkipEqual(false));
    ASSERT_FALSE(metric_both.CanSkipIn({true, false}));  // Query for true
    ASSERT_FALSE(metric_both.CanSkipIn({false, true}));  // Query for false

    // Case 2: Contains only true
    MetricsInfo<bool> info_true;
    info_true.contains_true_ = true;
    SetFieldChunkMetric<bool> metric_true(info_true);
    ASSERT_FALSE(metric_true.CanSkipEqual(true));
    ASSERT_TRUE(metric_true.CanSkipEqual(false));
    ASSERT_FALSE(metric_true.CanSkipIn({true, false}));  // Query for true
    ASSERT_TRUE(metric_true.CanSkipIn({false, true}));   // Query for false

    // Case 3: Contains only false
    MetricsInfo<bool> info_false;
    info_false.contains_false_ = true;
    SetFieldChunkMetric<bool> metric_false(info_false);
    ASSERT_TRUE(metric_false.CanSkipEqual(true));
    ASSERT_FALSE(metric_false.CanSkipEqual(false));
    ASSERT_TRUE(metric_false.CanSkipIn({true, false}));   // Query for true
    ASSERT_FALSE(metric_false.CanSkipIn({false, true}));  // Query for false
}

TEST(SetMetric, Serialization) {
    // Integer
    MetricsInfo<int64_t> info_int;
    info_int.unique_values_ = {100, 200, 300};
    SetFieldChunkMetric<int64_t> metric_int(info_int);
    auto serialized_int = metric_int.Serialize();
    SetFieldChunkMetric<int64_t> deserialized_int(serialized_int);
    ASSERT_TRUE(deserialized_int.hasValue_);
    ASSERT_FALSE(deserialized_int.CanSkipEqual(100));
    ASSERT_TRUE(deserialized_int.CanSkipEqual(150));

    // String
    MetricsInfo<std::string> info_str;
    info_str.unique_values_ = {"a", "b", "c"};
    SetFieldChunkMetric<std::string> metric_str(info_str);
    auto serialized_str = metric_str.Serialize();
    SetFieldChunkMetric<std::string> deserialized_str(serialized_str);
    ASSERT_TRUE(deserialized_str.hasValue_);
    ASSERT_FALSE(deserialized_str.CanSkipEqual("a"));
    ASSERT_TRUE(deserialized_str.CanSkipEqual("d"));

    // Bool
    MetricsInfo<bool> info_bool;
    info_bool.contains_true_ = true;
    info_bool.contains_false_ = false;
    SetFieldChunkMetric<bool> metric_bool(info_bool);
    auto serialized_bool = metric_bool.Serialize();
    SetFieldChunkMetric<bool> deserialized_bool(serialized_bool);
    ASSERT_TRUE(deserialized_bool.hasValue_);
    ASSERT_FALSE(deserialized_bool.CanSkipEqual(true));
    ASSERT_TRUE(deserialized_bool.CanSkipEqual(false));
}

TEST(SetMetric, EdgeCases) {
    // Empty data for deserialization
    SetFieldChunkMetric<int64_t> empty_metric_int("");
    ASSERT_FALSE(empty_metric_int.hasValue_);
    SetFieldChunkMetric<std::string> empty_metric_str("");
    ASSERT_FALSE(empty_metric_str.hasValue_);
    SetFieldChunkMetric<bool> empty_metric_bool("");
    ASSERT_FALSE(empty_metric_bool.hasValue_);

    // Empty MetricsInfo
    MetricsInfo<int64_t> empty_info;
    SetFieldChunkMetric<int64_t> metric_from_empty_info(empty_info);
    ASSERT_TRUE(metric_from_empty_info.hasValue_);
    ASSERT_TRUE(metric_from_empty_info.CanSkipEqual(10));
    ASSERT_TRUE(metric_from_empty_info.CanSkipIn({10, 20}));
}

TEST(BloomFilterMetric, IntegerFunctionality) {
    MetricsInfo<int64_t> info;
    info.unique_values_ = {1, 2, 3, 5, 8, 13, 21};
    BloomFilterFieldChunkMetric<int64_t> metric(info);

    ASSERT_TRUE(metric.hasValue_);

    // Values present in the set should not be skippable
    ASSERT_FALSE(metric.CanSkipEqual(1));
    ASSERT_FALSE(metric.CanSkipEqual(21));

    // Values not in the set should be skippable
    ASSERT_TRUE(metric.CanSkipEqual(0));
    ASSERT_TRUE(metric.CanSkipEqual(100));

    // Test CanSkipNotEqual
    ASSERT_TRUE(metric.CanSkipNotEqual(1));
    ASSERT_FALSE(metric.CanSkipNotEqual(100));

    // Test CanSkipIn
    ASSERT_TRUE(
        metric.CanSkipIn({100, 101, 102}));  // All values can be skipped
    ASSERT_FALSE(
        metric.CanSkipIn({1, 100, 101}));  // One value cannot be skipped
    ASSERT_FALSE(metric.CanSkipIn({}));    // Empty query
}

TEST(BloomFilterMetric, StringFunctionality) {
    MetricsInfo<std::string> info;
    info.unique_values_ = {"apple", "banana", "cherry"};
    BloomFilterFieldChunkMetric<std::string> metric(info);

    ASSERT_TRUE(metric.hasValue_);

    // Values present in the set should not be skippable
    ASSERT_FALSE(metric.CanSkipEqual("apple"));
    ASSERT_FALSE(metric.CanSkipEqual("cherry"));

    // Values not in the set should be skippable
    ASSERT_TRUE(metric.CanSkipEqual("grape"));
    ASSERT_TRUE(metric.CanSkipEqual("milvus"));

    // Test CanSkipNotEqual
    ASSERT_TRUE(metric.CanSkipNotEqual("apple"));
    ASSERT_FALSE(metric.CanSkipNotEqual("milvus"));

    // Test CanSkipIn
    ASSERT_TRUE(metric.CanSkipIn({"grape", "orange"}));
    ASSERT_FALSE(metric.CanSkipIn({"apple", "grape"}));
}

TEST(BloomFilterMetric, Serialization) {
    // Integer
    MetricsInfo<int64_t> info_int;
    for (int i = 0; i < 20; ++i) {
        info_int.unique_values_.insert(i);
    }
    BloomFilterFieldChunkMetric<int64_t> metric_int(info_int);
    auto serialized_int = metric_int.Serialize();
    BloomFilterFieldChunkMetric<int64_t> deserialized_int(serialized_int);

    ASSERT_TRUE(deserialized_int.hasValue_);
    ASSERT_FALSE(deserialized_int.CanSkipEqual(10));
    ASSERT_TRUE(deserialized_int.CanSkipEqual(100));

    // String
    MetricsInfo<std::string> info_str;
    info_str.unique_values_ = {"a", "b", "c"};
    BloomFilterFieldChunkMetric<std::string> metric_str(info_str);
    auto serialized_str = metric_str.Serialize();
    BloomFilterFieldChunkMetric<std::string> deserialized_str(serialized_str);

    ASSERT_TRUE(deserialized_str.hasValue_);
    ASSERT_FALSE(deserialized_str.CanSkipEqual("a"));
    ASSERT_TRUE(deserialized_str.CanSkipEqual("d"));
}

TEST(BloomFilterMetric, EdgeCases) {
    // Empty data for deserialization
    BloomFilterFieldChunkMetric<int64_t> empty_metric_int("");
    ASSERT_FALSE(empty_metric_int.hasValue_);
    ASSERT_FALSE(empty_metric_int.CanSkipEqual(10));
    ASSERT_FALSE(empty_metric_int.CanSkipIn({10, 20}));

    // Empty MetricsInfo
    MetricsInfo<int64_t> empty_info;
    BloomFilterFieldChunkMetric<int64_t> metric_from_empty_info(empty_info);
    ASSERT_FALSE(metric_from_empty_info.hasValue_);
    ASSERT_FALSE(metric_from_empty_info.CanSkipEqual(10));
    ASSERT_FALSE(metric_from_empty_info.CanSkipIn({10, 20}));
}

TEST(NgramFilterMetric, Functionality) {
    MetricsInfo<std::string> info;
    // Original strings might be "milvus" and "vector"
    // NGRAM_SIZE is 3
    info.ngram_values_ = {
        "mil", "ilv", "lvu", "vus", "vec", "ect", "cto", "tor"};
    NgramFilterFieldChunkMetric metric(info);

    ASSERT_TRUE(metric.hasValue_);

    // 1. Pattern contains an n-gram that is definitely not in the data. Should skip.
    // "xyz" is not in the filter.
    ASSERT_TRUE(metric.CanSkipSubstringMatch(std::string("xyzabc")));
    // "dat" is not in the filter.
    ASSERT_TRUE(metric.CanSkipSubstringMatch(std::string("database")));

    // 2. Pattern is shorter than NGRAM_SIZE. Cannot skip.
    ASSERT_FALSE(metric.CanSkipSubstringMatch(std::string("ve")));

    // 3. All of the pattern's n-grams might exist in the data. Cannot skip.
    ASSERT_FALSE(metric.CanSkipSubstringMatch(std::string("mil")));
    ASSERT_FALSE(metric.CanSkipSubstringMatch(std::string("vec")));
    ASSERT_FALSE(metric.CanSkipSubstringMatch(std::string("vector")));
    // "vus" and "vec" are in the filter.
    ASSERT_FALSE(metric.CanSkipSubstringMatch(std::string("vusvec")));
}

TEST(NgramFilterMetric, Serialization) {
    MetricsInfo<std::string> info;
    info.ngram_values_ = {"abc", "bcd", "cde"};
    NgramFilterFieldChunkMetric metric1(info);
    auto serialized_data = metric1.Serialize();
    NgramFilterFieldChunkMetric metric2(serialized_data);

    ASSERT_TRUE(metric2.hasValue_);
    ASSERT_FALSE(metric2.CanSkipSubstringMatch(std::string("abc")));
    ASSERT_FALSE(metric2.CanSkipSubstringMatch(std::string("bcd")));
    ASSERT_TRUE(metric2.CanSkipSubstringMatch(std::string("efg")));
}

TEST(NgramFilterMetric, EdgeCases) {
    // Empty data for deserialization
    NgramFilterFieldChunkMetric empty_metric("");
    ASSERT_FALSE(empty_metric.hasValue_);
    ASSERT_FALSE(empty_metric.CanSkipSubstringMatch(std::string("abc")));

    // Empty MetricsInfo
    MetricsInfo<std::string> empty_info;
    NgramFilterFieldChunkMetric metric_from_empty_info(empty_info);
    ASSERT_FALSE(metric_from_empty_info.hasValue_);
    ASSERT_FALSE(
        metric_from_empty_info.CanSkipSubstringMatch(std::string("abc")));
}

}  // namespace milvus