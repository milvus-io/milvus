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
        std::vector<int64_t> pks = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        std::vector<int32_t> int32s = {2, 2, 3, 4, 5, 6, 7, 8, 9, 12};
        std::vector<int16_t> int16s = {2, 2, 3, 4, 5, 6, 7, 8, 9, 12};
        std::vector<int8_t> int8s = {2, 2, 3, 4, 5, 6, 7, 8, 9, 12};
        std::vector<float> floats = {
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
        std::vector<double> doubles = {
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
        std::vector<std::string> strings = {
            "e", "f", "g", "g", "j", "l", "o", "q", "r", "t"};

        arrow::Int64Builder pk_builder;
        arrow::Int32Builder i32_builder;
        arrow::Int16Builder i16_builder;
        arrow::Int8Builder i8_builder;
        arrow::FloatBuilder float_builder;
        arrow::DoubleBuilder double_builder;
        arrow::StringBuilder string_builder;

        arrow::Int64Builder i64_nullable_builder;

        pk_builder.AppendValues(pks);
        i32_builder.AppendValues(int32s);
        i16_builder.AppendValues(int16s);
        i8_builder.AppendValues(int8s);
        float_builder.AppendValues(floats);
        double_builder.AppendValues(doubles);
        string_builder.AppendValues(strings);
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
        i64_nullable_builder.Finish(&i64_nullable_array_).ok();
    }

    std::unique_ptr<SkipIndex>
    BuildSkipIndex(ArrowSchemaPtr schema, std::vector<std::shared_ptr<arrow::Array>> arrays) {
        if (arrays.size() == 0) {
            return nullptr;
        }
        auto record_batch =
        arrow::RecordBatch::Make(schema, arrays[0]->length(), arrays);
        auto batches = std::vector<std::shared_ptr<arrow::RecordBatch>>{record_batch};
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
    std::shared_ptr<arrow::Array> i64_nullable_array_;
};

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
    auto arrow_schema = schema->ConvertToArrowSchema();

    auto skip_index = BuildSkipIndex(arrow_schema, {pk_array_, i32_array_, i16_array_, i8_array_, float_array_, double_array_, string_array_});

    // test for int64
    bool equal_5_skip =
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::Equal, 5);
    bool equal_12_skip =
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::Equal, 12);
    bool equal_10_skip =
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::Equal, 10);
    ASSERT_FALSE(equal_5_skip);
    ASSERT_TRUE(equal_12_skip);
    ASSERT_FALSE(equal_10_skip);
    bool less_than_1_skip =
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessThan, 1);
    bool less_than_5_skip =
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessThan, 5);
    ASSERT_TRUE(less_than_1_skip);
    ASSERT_FALSE(less_than_5_skip);
    bool less_equal_than_1_skip =
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessEqual, 1);
    bool less_equal_than_15_skip =
        skip_index->CanSkipUnaryRange<int64_t>(pk_fid, 0, OpType::LessThan, 15);
    ASSERT_FALSE(less_equal_than_1_skip);
    ASSERT_FALSE(less_equal_than_15_skip);
    bool greater_than_10_skip = skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, 10);
    bool greater_than_5_skip = skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, 5);
    ASSERT_TRUE(greater_than_10_skip);
    ASSERT_FALSE(greater_than_5_skip);
    bool greater_equal_than_10_skip = skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterEqual, 10);
    bool greater_equal_than_5_skip = skip_index->CanSkipUnaryRange<int64_t>(
        pk_fid, 0, OpType::GreaterEqual, 5);
    ASSERT_FALSE(greater_equal_than_10_skip);
    ASSERT_FALSE(greater_equal_than_5_skip);

    // test for int32
    less_than_1_skip =
        skip_index->CanSkipUnaryRange<int32_t>(i32_fid, 0, OpType::LessThan, 1);
    ASSERT_TRUE(less_than_1_skip);

    // test for int16
    bool less_than_12_skip =
        skip_index->CanSkipUnaryRange<int16_t>(i16_fid, 0, OpType::LessThan, 12);
    ASSERT_FALSE(less_than_12_skip);

    // test for int8
    bool greater_than_12_skip = skip_index->CanSkipUnaryRange<int8_t>(
        i8_fid, 0, OpType::GreaterThan, 12);
    ASSERT_TRUE(greater_than_12_skip);

    // test for float
    greater_than_10_skip = skip_index->CanSkipUnaryRange<float>(
        float_fid, 0, OpType::GreaterThan, 10.0);
    ASSERT_TRUE(greater_than_10_skip);

    // test for double
    greater_than_10_skip = skip_index->CanSkipUnaryRange<double>(
        double_fid, 0, OpType::GreaterThan, 10.0);
    ASSERT_TRUE(greater_than_10_skip);
}

TEST_F(SkipIndexTest, SkipBinaryRange) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto arrow_schema = schema->ConvertToArrowSchema();
    auto skip_index = BuildSkipIndex(arrow_schema, {pk_array_});

    // test for int64
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, -3, 1, true, true));
    ASSERT_TRUE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, -3, 1, true, false));

    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, 7, 9, true, true));
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, 8, 12, true, false));

    ASSERT_TRUE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, 10, 12, false, true));
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(pk_fid, 0, 10, 12, true, true));
}

TEST_F(SkipIndexTest, SkipUnaryRangeNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("int64_field", DataType::INT64, true);
    auto arrow_schema = schema->ConvertToArrowSchema();
    auto skip_index = BuildSkipIndex(arrow_schema, {i64_nullable_array_});

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
    bool less_equal_than_1_skip =
        skip_index->CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::LessEqual, 1);
    bool less_equal_than_15_skip =
        skip_index->CanSkipUnaryRange<int64_t>(i64_fid, 0, OpType::LessThan, 15);
    ASSERT_FALSE(less_equal_than_1_skip);
    ASSERT_FALSE(less_equal_than_15_skip);
    bool greater_than_10_skip = skip_index->CanSkipUnaryRange<int64_t>(
        i64_fid, 0, OpType::GreaterThan, 10);
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

TEST_F(SkipIndexTest, SkipBinaryRangeNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("int64_field", DataType::INT64, true);
    auto arrow_schema = schema->ConvertToArrowSchema();
    auto skip_index = BuildSkipIndex(arrow_schema, {i64_nullable_array_});

    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, -3, 1, true, true));
    ASSERT_TRUE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, -3, 1, true, false));

    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, 1, 8, true, true));
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, 1, 5, true, false));

    ASSERT_TRUE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, 5, 8, false, true));
    ASSERT_FALSE(
        skip_index->CanSkipBinaryRange<int64_t>(i64_fid, 0, 5, 8, true, true));
}

TEST_F(SkipIndexTest, SkipStringRange) {
    auto schema = std::make_shared<Schema>();
    FieldId string_fid =
        schema->AddDebugField("string_field", DataType::VARCHAR);
    auto arrow_schema = schema->ConvertToArrowSchema();
    auto skip_index = BuildSkipIndex(arrow_schema, {string_array_});

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

TEST_F(SkipIndexTest, SkipInQueryInt) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto arrow_schema = schema->ConvertToArrowSchema();
    auto skip_index = BuildSkipIndex(arrow_schema, {pk_array_});


    std::vector<int64_t> values1 = {11, 12, 13};
    ASSERT_TRUE(skip_index->CanSkipInQuery<int64_t>(pk_fid, 0, values1));
    std::vector<int64_t> values2 = {9, 10, 11};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(pk_fid, 0, values2));
    std::vector<int64_t> values3 = {1, 2, 3};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(pk_fid, 0, values3));
}

TEST_F(SkipIndexTest, SkipInQueryString) {
    auto schema = std::make_shared<Schema>();
    FieldId str_fid = schema->AddDebugField("str_field", DataType::VARCHAR);
    auto arrow_schema = schema->ConvertToArrowSchema();
    auto skip_index = BuildSkipIndex(arrow_schema, {string_array_});

    std::vector<std::string> str_values1 = {"u", "v", "w"};
    ASSERT_TRUE(skip_index->CanSkipInQuery<std::string>(str_fid, 0, str_values1));
    std::vector<std::string> str_values2 = {"s", "t", "u"};
    ASSERT_FALSE(skip_index->CanSkipInQuery<std::string>(str_fid, 0, str_values2));
    std::vector<std::string> str_values3 = {"e", "f", "g"};
    ASSERT_FALSE(skip_index->CanSkipInQuery<std::string>(str_fid, 0, str_values3));
}

TEST_F(SkipIndexTest, SkipInQueryNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("int64_field", DataType::INT64, true);
    auto arrow_schema = schema->ConvertToArrowSchema();
    auto skip_index = BuildSkipIndex(arrow_schema, {i64_nullable_array_});

    std::vector<int64_t> values1 = {6, 7, 8};
    ASSERT_TRUE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, values1));
    std::vector<int64_t> values2 = {4, 5, 6};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, values2));
    std::vector<int64_t> values3 = {1, 2, 3};
    ASSERT_FALSE(skip_index->CanSkipInQuery<int64_t>(i64_fid, 0, values3));
}

TEST_F(SkipIndexTest, SkipBinaryArithRange) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto arrow_schema = schema->ConvertToArrowSchema();
    auto skip_index = BuildSkipIndex(arrow_schema, {pk_array_});

    // --- Test for Add: field + C op V  =>  field op V - C ---
    // field + 5 > 20  =>  field > 15. Can skip since 15 > max(10).
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Add, 20, 5));
    // field + 5 > 15  =>  field > 10. Cannot skip since 10 is the max.
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Add, 15, 5));
    // field + 5 < 0  =>  field < -5. Can skip since -5 < min(1).
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::LessThan, ArithOpType::Add, 0, 5));

    // --- Test for Sub: field - C op V  =>  field op V + C ---
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::LessThan, ArithOpType::Sub, -10, 5));
    // field - 5 > 5  =>  field > 10. Cannot skip since 10 is the max.
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
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Div, 20, 0.5));
    ASSERT_TRUE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Div, 0, -2));
    ASSERT_FALSE(skip_index->CanSkipBinaryArithRange<int64_t>(
        pk_fid, 0, OpType::GreaterThan, ArithOpType::Div, 10, 0));

    // --- Test with float data ---
    auto float_fid = schema->AddDebugField("float_field", DataType::FLOAT);
    auto float_skip_index = BuildSkipIndex(arrow_schema, {float_array_});
    ASSERT_FALSE(float_skip_index->CanSkipBinaryArithRange<float>(
        float_fid, 0, OpType::GreaterEqual, ArithOpType::Mul, -2.0, -2.0));
    ASSERT_TRUE(float_skip_index->CanSkipBinaryArithRange<float>(
        float_fid, 0, OpType::GreaterThan, ArithOpType::Mul, -2.0, -2.0));
}

}  // namespace milvus