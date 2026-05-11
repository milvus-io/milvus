// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "storage/Util.h"

using milvus::storage::CanonicalizeArrowVariants;
using milvus::storage::CoerceToBinary;
using milvus::storage::CoerceToList;

namespace {

std::shared_ptr<arrow::Array>
MakeStringViewArray(const std::vector<std::string>& vals,
                    const std::vector<bool>& valid) {
    arrow::StringViewBuilder b;
    for (size_t i = 0; i < vals.size(); ++i) {
        if (!valid[i]) {
            EXPECT_TRUE(b.AppendNull().ok());
        } else {
            EXPECT_TRUE(b.Append(vals[i]).ok());
        }
    }
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}

std::shared_ptr<arrow::Array>
MakeLargeStringArray(const std::vector<std::string>& vals,
                     const std::vector<bool>& valid) {
    arrow::LargeStringBuilder b;
    for (size_t i = 0; i < vals.size(); ++i) {
        if (!valid[i]) {
            EXPECT_TRUE(b.AppendNull().ok());
        } else {
            EXPECT_TRUE(b.Append(vals[i]).ok());
        }
    }
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}

std::shared_ptr<arrow::Array>
MakeBinaryViewArray(const std::vector<std::string>& vals,
                    const std::vector<bool>& valid) {
    arrow::BinaryViewBuilder b;
    for (size_t i = 0; i < vals.size(); ++i) {
        if (!valid[i]) {
            EXPECT_TRUE(b.AppendNull().ok());
        } else {
            EXPECT_TRUE(
                b.Append(reinterpret_cast<const uint8_t*>(vals[i].data()),
                         vals[i].size())
                    .ok());
        }
    }
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}

std::shared_ptr<arrow::Array>
MakeLargeBinaryArray(const std::vector<std::string>& vals,
                     const std::vector<bool>& valid) {
    arrow::LargeBinaryBuilder b;
    for (size_t i = 0; i < vals.size(); ++i) {
        if (!valid[i]) {
            EXPECT_TRUE(b.AppendNull().ok());
        } else {
            EXPECT_TRUE(
                b.Append(reinterpret_cast<const uint8_t*>(vals[i].data()),
                         vals[i].size())
                    .ok());
        }
    }
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}

}  // namespace

// ===== CanonicalizeArrowVariants =====

TEST(CanonicalizeArrowVariants, StringViewToString) {
    auto in = MakeStringViewArray({"a", "bb", "ccc"}, {true, true, true});
    auto out = CanonicalizeArrowVariants(in);
    ASSERT_EQ(out->type_id(), arrow::Type::STRING);
    auto sa = std::static_pointer_cast<arrow::StringArray>(out);
    ASSERT_EQ(sa->length(), 3);
    EXPECT_EQ(sa->GetString(0), "a");
    EXPECT_EQ(sa->GetString(1), "bb");
    EXPECT_EQ(sa->GetString(2), "ccc");
}

TEST(CanonicalizeArrowVariants, StringViewWithNull) {
    auto in = MakeStringViewArray({"x", "", "z"}, {true, false, true});
    auto out = CanonicalizeArrowVariants(in);
    ASSERT_EQ(out->type_id(), arrow::Type::STRING);
    auto sa = std::static_pointer_cast<arrow::StringArray>(out);
    EXPECT_EQ(sa->null_count(), 1);
    EXPECT_TRUE(sa->IsNull(1));
    EXPECT_EQ(sa->GetString(0), "x");
    EXPECT_EQ(sa->GetString(2), "z");
}

TEST(CanonicalizeArrowVariants, LargeStringToString) {
    auto in = MakeLargeStringArray({"hello", "world"}, {true, true});
    auto out = CanonicalizeArrowVariants(in);
    ASSERT_EQ(out->type_id(), arrow::Type::STRING);
    auto sa = std::static_pointer_cast<arrow::StringArray>(out);
    EXPECT_EQ(sa->GetString(0), "hello");
    EXPECT_EQ(sa->GetString(1), "world");
}

TEST(CanonicalizeArrowVariants, BinaryViewToBinary) {
    std::string b1{"\x01\x02", 2};
    std::string b2{"\xff\xfe\xfd", 3};
    auto in = MakeBinaryViewArray({b1, b2}, {true, true});
    auto out = CanonicalizeArrowVariants(in);
    ASSERT_EQ(out->type_id(), arrow::Type::BINARY);
    auto ba = std::static_pointer_cast<arrow::BinaryArray>(out);
    EXPECT_EQ(ba->GetString(0), b1);
    EXPECT_EQ(ba->GetString(1), b2);
}

TEST(CanonicalizeArrowVariants, LargeBinaryToBinary) {
    auto in = MakeLargeBinaryArray({"abc", "de"}, {true, true});
    auto out = CanonicalizeArrowVariants(in);
    ASSERT_EQ(out->type_id(), arrow::Type::BINARY);
    auto ba = std::static_pointer_cast<arrow::BinaryArray>(out);
    EXPECT_EQ(ba->GetString(0), "abc");
    EXPECT_EQ(ba->GetString(1), "de");
}

TEST(CanonicalizeArrowVariants, CanonicalStringPassthrough) {
    arrow::StringBuilder b;
    ASSERT_TRUE(b.Append("foo").ok());
    std::shared_ptr<arrow::Array> in;
    ASSERT_TRUE(b.Finish(&in).ok());
    auto out = CanonicalizeArrowVariants(in);
    EXPECT_EQ(out.get(), in.get());  // zero-copy passthrough
}

TEST(CanonicalizeArrowVariants, CanonicalIntPassthrough) {
    arrow::Int32Builder b;
    ASSERT_TRUE(b.Append(42).ok());
    std::shared_ptr<arrow::Array> in;
    ASSERT_TRUE(b.Finish(&in).ok());
    auto out = CanonicalizeArrowVariants(in);
    EXPECT_EQ(out.get(), in.get());
}

TEST(CanonicalizeArrowVariants, LargeListToList) {
    auto values =
        MakeLargeStringArray({"a", "b", "c", "d"}, {true, true, true, true});
    // Build LargeListArray with offsets [0, 2, 2, 4] -> [[a,b], [], [c,d]]
    arrow::Int64Builder ob;
    ASSERT_TRUE(ob.AppendValues({0, 2, 2, 4}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(ob.Finish(&offsets).ok());
    auto large_list_result =
        arrow::LargeListArray::FromArrays(*offsets, *values);
    ASSERT_TRUE(large_list_result.ok());
    auto in = *large_list_result;

    auto out = CanonicalizeArrowVariants(in);
    ASSERT_EQ(out->type_id(), arrow::Type::LIST);
    auto la = std::static_pointer_cast<arrow::ListArray>(out);
    ASSERT_EQ(la->length(), 3);
    // Inner values must be canonical STRING
    EXPECT_EQ(la->values()->type_id(), arrow::Type::STRING);
    auto inner = std::static_pointer_cast<arrow::StringArray>(la->values());
    EXPECT_EQ(la->value_length(0), 2);
    EXPECT_EQ(la->value_length(1), 0);
    EXPECT_EQ(la->value_length(2), 2);
    EXPECT_EQ(inner->GetString(la->value_offset(0) + 0), "a");
    EXPECT_EQ(inner->GetString(la->value_offset(0) + 1), "b");
    EXPECT_EQ(inner->GetString(la->value_offset(2) + 0), "c");
    EXPECT_EQ(inner->GetString(la->value_offset(2) + 1), "d");
}

TEST(CanonicalizeArrowVariants, ListWithStringViewInnerRecurses) {
    auto values = MakeStringViewArray({"x", "y", "z"}, {true, true, true});
    arrow::Int32Builder ob;
    ASSERT_TRUE(ob.AppendValues({0, 1, 3}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(ob.Finish(&offsets).ok());
    auto list_result = arrow::ListArray::FromArrays(*offsets, *values);
    ASSERT_TRUE(list_result.ok());
    auto in = *list_result;

    auto out = CanonicalizeArrowVariants(in);
    ASSERT_EQ(out->type_id(), arrow::Type::LIST);
    auto la = std::static_pointer_cast<arrow::ListArray>(out);
    EXPECT_EQ(la->values()->type_id(), arrow::Type::STRING);
    EXPECT_EQ(la->length(), 2);
}

TEST(CanonicalizeArrowVariants, FixedSizeListWithViewInnerRecurses) {
    auto values =
        MakeStringViewArray({"a", "b", "c", "d"}, {true, true, true, true});
    auto fsl = std::make_shared<arrow::FixedSizeListArray>(
        arrow::fixed_size_list(values->type(), 2), 2, values);
    auto out = CanonicalizeArrowVariants(fsl);
    ASSERT_EQ(out->type_id(), arrow::Type::FIXED_SIZE_LIST);
    auto fsla = std::static_pointer_cast<arrow::FixedSizeListArray>(out);
    EXPECT_EQ(fsla->values()->type_id(), arrow::Type::STRING);
}

TEST(CanonicalizeArrowVariants, EmptyArray) {
    auto in = MakeStringViewArray({}, {});
    auto out = CanonicalizeArrowVariants(in);
    ASSERT_EQ(out->type_id(), arrow::Type::STRING);
    EXPECT_EQ(out->length(), 0);
}

// ===== CoerceToBinary =====

TEST(CoerceToBinary, StringToBinaryZeroCopy) {
    arrow::StringBuilder b;
    ASSERT_TRUE(b.Append("hello").ok());
    std::shared_ptr<arrow::Array> in;
    ASSERT_TRUE(b.Finish(&in).ok());
    auto out = CoerceToBinary({in})[0];
    ASSERT_EQ(out->type_id(), arrow::Type::BINARY);
    auto ba = std::static_pointer_cast<arrow::BinaryArray>(out);
    EXPECT_EQ(ba->GetString(0), "hello");
}

TEST(CoerceToBinary, BinaryPassthrough) {
    arrow::BinaryBuilder b;
    ASSERT_TRUE(b.Append(reinterpret_cast<const uint8_t*>("xy"), 2).ok());
    std::shared_ptr<arrow::Array> in;
    ASSERT_TRUE(b.Finish(&in).ok());
    auto out = CoerceToBinary({in})[0];
    EXPECT_EQ(out.get(), in.get());
}

TEST(CoerceToBinary, AllVariantsToBinary) {
    auto sv = MakeStringViewArray({"a"}, {true});
    auto ls = MakeLargeStringArray({"bb"}, {true});
    auto bv = MakeBinaryViewArray({"ccc"}, {true});
    auto lb = MakeLargeBinaryArray({"dddd"}, {true});
    for (const auto& in : {sv, ls, bv, lb}) {
        auto out = CoerceToBinary({in})[0];
        ASSERT_EQ(out->type_id(), arrow::Type::BINARY);
    }
}

namespace {
milvus::FieldMeta
MakeExternalFieldMetaForTest(milvus::DataType data_type,
                             int64_t dim,
                             bool nullable,
                             milvus::DataType element_type) {
    const auto name = milvus::FieldName("field");
    const auto field_id = milvus::FieldId(1000);
    const auto external_field = "external_field";
    if (data_type == milvus::DataType::VECTOR_ARRAY) {
        return milvus::FieldMeta(name,
                                 field_id,
                                 data_type,
                                 element_type,
                                 dim,
                                 std::nullopt,
                                 external_field);
    }
    if (milvus::IsVectorDataType(data_type)) {
        return milvus::FieldMeta(name,
                                 field_id,
                                 data_type,
                                 dim,
                                 std::nullopt,
                                 nullable,
                                 std::nullopt,
                                 external_field);
    }
    if (milvus::IsStringDataType(data_type)) {
        return milvus::FieldMeta(name,
                                 field_id,
                                 data_type,
                                 65535,
                                 nullable,
                                 std::nullopt,
                                 external_field);
    }
    if (milvus::IsArrayDataType(data_type)) {
        return milvus::FieldMeta(name,
                                 field_id,
                                 data_type,
                                 element_type,
                                 nullable,
                                 std::nullopt,
                                 external_field);
    }
    return milvus::FieldMeta(
        name, field_id, data_type, nullable, std::nullopt, external_field);
}

std::shared_ptr<arrow::Array>
NormalizeVectorArraysToFixedSizeBinaryForTest(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays,
    milvus::DataType data_type,
    int64_t dim) {
    AssertInfo(arrays.size() == 1, "test helper expects one array");
    auto field_meta = MakeExternalFieldMetaForTest(
        data_type, dim, false, milvus::DataType::NONE);
    return milvus::storage::NormalizeExternalArrow(arrays.front(), field_meta);
}

std::shared_ptr<arrow::Array>
MakeInt32Array(const std::vector<int32_t>& vals,
               const std::vector<bool>& valid) {
    arrow::Int32Builder b;
    for (size_t i = 0; i < vals.size(); ++i) {
        if (!valid[i]) {
            EXPECT_TRUE(b.AppendNull().ok());
        } else {
            EXPECT_TRUE(b.Append(vals[i]).ok());
        }
    }
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}

std::shared_ptr<arrow::Array>
MakeInt64Array(const std::vector<int64_t>& vals,
               const std::vector<bool>& valid) {
    arrow::Int64Builder b;
    for (size_t i = 0; i < vals.size(); ++i) {
        if (!valid[i]) {
            EXPECT_TRUE(b.AppendNull().ok());
        } else {
            EXPECT_TRUE(b.Append(vals[i]).ok());
        }
    }
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}

std::shared_ptr<arrow::Array>
MakeDoubleArray(const std::vector<double>& vals,
                const std::vector<bool>& valid) {
    arrow::DoubleBuilder b;
    for (size_t i = 0; i < vals.size(); ++i) {
        if (!valid[i]) {
            EXPECT_TRUE(b.AppendNull().ok());
        } else {
            EXPECT_TRUE(b.Append(vals[i]).ok());
        }
    }
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}
}  // namespace

// ===== Scalar numeric mismatch rejection (via NormalizeExternalArrow) =====
// External Arrow numeric types must match the Milvus scalar type exactly.
// Wider integer inputs are rejected instead of implicitly narrowed.

TEST(ScalarNumericMismatch, Int32RejectsInt8) {
    auto in =
        MakeInt32Array({0, 1, 99, -128, 127}, {true, true, true, true, true});
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::INT8, 0, false, milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(in, field_meta),
                 std::exception);
}

TEST(ScalarNumericMismatch, Int32RejectsInt16) {
    auto in =
        MakeInt32Array({0, 1000, -32768, 32767}, {true, true, true, true});
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::INT16, 0, false, milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(in, field_meta),
                 std::exception);
}

TEST(ScalarNumericMismatch, Int32RejectsInt8WithNulls) {
    auto in = MakeInt32Array({5, 0, 7}, {true, false, true});
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::INT8, 0, true, milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(in, field_meta),
                 std::exception);
}

TEST(ScalarNumericMismatch, Int32RejectsInt8EvenWhenInRange) {
    auto in = MakeInt32Array({5}, {true});
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::INT8, 0, false, milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(in, field_meta),
                 std::exception);
}

TEST(ScalarNumericMismatch, ExactMatchPassesThrough) {
    arrow::Int8Builder b;
    ASSERT_TRUE(b.Append(int8_t{5}).ok());
    std::shared_ptr<arrow::Array> in;
    ASSERT_TRUE(b.Finish(&in).ok());
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::INT8, 0, false, milvus::DataType::NONE);
    auto out = milvus::storage::NormalizeExternalArrow(in, field_meta);
    EXPECT_EQ(out.get(), in.get());
}

TEST(NormalizeExternalArrow, Int64RejectsString) {
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.AppendValues({"1", "2"}).ok());
    std::shared_ptr<arrow::Array> input;
    ASSERT_TRUE(builder.Finish(&input).ok());

    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::INT64, 0, false, milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, Issue49392ScalarMismatchesReject) {
    arrow::StringBuilder string_builder;
    ASSERT_TRUE(string_builder.AppendValues({"abcd", "efgh"}).ok());
    std::shared_ptr<arrow::Array> string_input;
    ASSERT_TRUE(string_builder.Finish(&string_input).ok());

    for (auto data_type : {milvus::DataType::BOOL,
                           milvus::DataType::INT8,
                           milvus::DataType::INT16,
                           milvus::DataType::INT32,
                           milvus::DataType::INT64,
                           milvus::DataType::FLOAT,
                           milvus::DataType::DOUBLE,
                           milvus::DataType::TIMESTAMPTZ}) {
        auto field_meta = MakeExternalFieldMetaForTest(
            data_type, 0, false, milvus::DataType::NONE);
        EXPECT_THROW(
            milvus::storage::NormalizeExternalArrow(string_input, field_meta),
            std::exception);
    }

    auto int64_input = MakeInt64Array({1, 2}, {true, true});
    auto float_field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::FLOAT, 0, false, milvus::DataType::NONE);
    EXPECT_THROW(
        milvus::storage::NormalizeExternalArrow(int64_input, float_field_meta),
        std::exception);
    auto double_field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::DOUBLE, 0, false, milvus::DataType::NONE);
    EXPECT_THROW(
        milvus::storage::NormalizeExternalArrow(int64_input, double_field_meta),
        std::exception);

    auto double_input = MakeDoubleArray({1.0, 2.0}, {true, true});
    auto int64_field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::INT64, 0, false, milvus::DataType::NONE);
    EXPECT_THROW(
        milvus::storage::NormalizeExternalArrow(double_input, int64_field_meta),
        std::exception);
}

TEST(NormalizeExternalArrow, TimestamptzAcceptsTimestampAndInt64) {
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO),
                                    arrow::default_memory_pool());
    ASSERT_TRUE(builder.AppendValues({1000, 2000}).ok());
    std::shared_ptr<arrow::Array> timestamp_input;
    ASSERT_TRUE(builder.Finish(&timestamp_input).ok());

    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::TIMESTAMPTZ, 0, false, milvus::DataType::NONE);
    auto timestamp_out =
        milvus::storage::NormalizeExternalArrow(timestamp_input, field_meta);
    ASSERT_EQ(timestamp_out->type_id(), arrow::Type::INT64);

    auto int64_input = MakeInt64Array({1000, 2000}, {true, true});
    auto int64_out =
        milvus::storage::NormalizeExternalArrow(int64_input, field_meta);
    EXPECT_EQ(int64_out.get(), int64_input.get());
}

TEST(NormalizeExternalArrow, StringLikeFieldsRejectInt64) {
    auto int64_input = MakeInt64Array({1, 2}, {true, true});
    for (auto data_type : {milvus::DataType::VARCHAR,
                           milvus::DataType::STRING,
                           milvus::DataType::TEXT,
                           milvus::DataType::JSON,
                           milvus::DataType::GEOMETRY}) {
        auto field_meta = MakeExternalFieldMetaForTest(
            data_type, 0, false, milvus::DataType::NONE);
        EXPECT_THROW(
            milvus::storage::NormalizeExternalArrow(int64_input, field_meta),
            std::exception);
    }
}

TEST(NormalizeVectorArraysToFixedSizeBinary, ListDimMismatchAsserts) {
    arrow::FloatBuilder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1.0f, 2.0f, 3.0f}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 3}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    auto input = *arrow::ListArray::FromArrays(*offsets, *values);
    EXPECT_THROW(NormalizeVectorArraysToFixedSizeBinaryForTest(
                     {input}, milvus::DataType::VECTOR_FLOAT, 2),
                 std::exception);
}

TEST(NormalizeVectorArraysToFixedSizeBinary, ListElementTypeMismatchAsserts) {
    arrow::Int64Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    auto input = *arrow::ListArray::FromArrays(*offsets, *values);
    EXPECT_THROW(NormalizeVectorArraysToFixedSizeBinaryForTest(
                     {input}, milvus::DataType::VECTOR_FLOAT, 2),
                 std::exception);
}

TEST(NormalizeVectorArraysToFixedSizeBinary, ListNullElementAsserts) {
    arrow::FloatBuilder values_builder;
    ASSERT_TRUE(values_builder.Append(1.0f).ok());
    ASSERT_TRUE(values_builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    auto input = *arrow::ListArray::FromArrays(*offsets, *values);
    EXPECT_THROW(NormalizeVectorArraysToFixedSizeBinaryForTest(
                     {input}, milvus::DataType::VECTOR_FLOAT, 2),
                 std::exception);
}

TEST(NormalizeVectorArraysToFixedSizeBinary, FixedSizeListDimMismatchAsserts) {
    arrow::FloatBuilder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1.0f, 2.0f, 3.0f}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto input = std::make_shared<arrow::FixedSizeListArray>(
        arrow::fixed_size_list(arrow::float32(), 3), 1, values);
    EXPECT_THROW(NormalizeVectorArraysToFixedSizeBinaryForTest(
                     {input}, milvus::DataType::VECTOR_FLOAT, 2),
                 std::exception);
}

TEST(NormalizeVectorArraysToFixedSizeBinary,
     FixedSizeListElementTypeMismatchAsserts) {
    arrow::Int64Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto input = std::make_shared<arrow::FixedSizeListArray>(
        arrow::fixed_size_list(arrow::int64(), 2), 1, values);
    EXPECT_THROW(NormalizeVectorArraysToFixedSizeBinaryForTest(
                     {input}, milvus::DataType::VECTOR_FLOAT, 2),
                 std::exception);
}

TEST(NormalizeVectorArraysToFixedSizeBinary, BinaryVectorRejectsFixedSizeList) {
    arrow::Int8Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1, 0, 1, 0, 1, 0, 1, 0}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto input = std::make_shared<arrow::FixedSizeListArray>(
        arrow::fixed_size_list(arrow::int8(), 8), 1, values);
    EXPECT_THROW(NormalizeVectorArraysToFixedSizeBinaryForTest(
                     {input}, milvus::DataType::VECTOR_BINARY, 8),
                 std::exception);
}

TEST(NormalizeVectorArraysToFixedSizeBinary,
     BFloat16VectorRejectsFixedSizeList) {
    arrow::Int16Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto input = std::make_shared<arrow::FixedSizeListArray>(
        arrow::fixed_size_list(arrow::int16(), 2), 1, values);
    EXPECT_THROW(NormalizeVectorArraysToFixedSizeBinaryForTest(
                     {input}, milvus::DataType::VECTOR_BFLOAT16, 2),
                 std::exception);
}

TEST(NormalizeVectorArraysToFixedSizeBinary,
     FixedSizeBinaryWidthMismatchAsserts) {
    auto fsb_type = arrow::fixed_size_binary(3 * sizeof(float));
    arrow::FixedSizeBinaryBuilder builder(fsb_type);
    float values[3] = {1.0f, 2.0f, 3.0f};
    ASSERT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(values)).ok());
    std::shared_ptr<arrow::Array> input;
    ASSERT_TRUE(builder.Finish(&input).ok());

    EXPECT_THROW(NormalizeVectorArraysToFixedSizeBinaryForTest(
                     {input}, milvus::DataType::VECTOR_FLOAT, 2),
                 std::exception);
}

TEST(NormalizeVectorArrays, MixedChunkTypesNormalizeIndependently) {
    auto fsb_type = arrow::fixed_size_binary(2 * sizeof(float));
    arrow::FixedSizeBinaryBuilder fsb_builder(fsb_type);
    float fsb_values[2] = {1.0f, 2.0f};
    ASSERT_TRUE(
        fsb_builder.Append(reinterpret_cast<const uint8_t*>(fsb_values)).ok());
    std::shared_ptr<arrow::Array> fsb_input;
    ASSERT_TRUE(fsb_builder.Finish(&fsb_input).ok());

    arrow::FloatBuilder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({3.0f, 4.0f}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    auto list_input = *arrow::ListArray::FromArrays(*offsets, *values);
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::VECTOR_FLOAT, 2, false, milvus::DataType::NONE);
    auto out = milvus::storage::NormalizeArrowForChunkWriter(
        {fsb_input, list_input}, field_meta);

    ASSERT_EQ(out.size(), 2);
    EXPECT_EQ(out[0]->type_id(), arrow::Type::FIXED_SIZE_BINARY);
    EXPECT_EQ(out[1]->type_id(), arrow::Type::FIXED_SIZE_BINARY);
    EXPECT_EQ(std::static_pointer_cast<arrow::FixedSizeBinaryArray>(out[0])
                  ->byte_width(),
              2 * sizeof(float));
    EXPECT_EQ(std::static_pointer_cast<arrow::FixedSizeBinaryArray>(out[1])
                  ->byte_width(),
              2 * sizeof(float));
}

TEST(NormalizeExternalArrow, NullableFloatVectorRejectsBinaryWidthMismatch) {
    arrow::BinaryBuilder builder;
    float values[3] = {1.0f, 2.0f, 3.0f};
    ASSERT_TRUE(
        builder.Append(reinterpret_cast<const uint8_t*>(values), sizeof(values))
            .ok());
    std::shared_ptr<arrow::Array> input;
    ASSERT_TRUE(builder.Finish(&input).ok());

    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::VECTOR_FLOAT, 2, true, milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, FloatVectorRejectsFixedSizeBinaryWidthMismatch) {
    auto fsb_type = arrow::fixed_size_binary(3 * sizeof(float));
    arrow::FixedSizeBinaryBuilder builder(fsb_type);
    float values[3] = {1.0f, 2.0f, 3.0f};
    ASSERT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(values)).ok());
    std::shared_ptr<arrow::Array> input;
    ASSERT_TRUE(builder.Finish(&input).ok());

    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::VECTOR_FLOAT, 2, false, milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow,
     NullableFloatVectorRejectsFixedSizeBinaryWidthMismatch) {
    auto fsb_type = arrow::fixed_size_binary(3 * sizeof(float));
    arrow::FixedSizeBinaryBuilder builder(fsb_type);
    float values[3] = {1.0f, 2.0f, 3.0f};
    ASSERT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(values)).ok());
    std::shared_ptr<arrow::Array> input;
    ASSERT_TRUE(builder.Finish(&input).ok());

    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::VECTOR_FLOAT, 2, true, milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, FloatVectorRejectsFixedSizeListInt64) {
    arrow::Int64Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    auto input = std::make_shared<arrow::FixedSizeListArray>(
        arrow::fixed_size_list(arrow::int64(), 2), 1, values);
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::VECTOR_FLOAT, 2, false, milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, VectorArrayRejectsFixedSizeBinaryWidthMismatch) {
    auto fsb_type = arrow::fixed_size_binary(3 * sizeof(float));
    arrow::FixedSizeBinaryBuilder values_builder(fsb_type);
    float values[3] = {1.0f, 2.0f, 3.0f};
    ASSERT_TRUE(
        values_builder.Append(reinterpret_cast<const uint8_t*>(values)).ok());
    std::shared_ptr<arrow::Array> values_array;
    ASSERT_TRUE(values_builder.Finish(&values_array).ok());

    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 1}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    auto input = *arrow::ListArray::FromArrays(*offsets, *values_array);
    auto field_meta =
        MakeExternalFieldMetaForTest(milvus::DataType::VECTOR_ARRAY,
                                     2,
                                     false,
                                     milvus::DataType::VECTOR_FLOAT);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, VectorArrayRejectsNonListInput) {
    auto input = MakeInt64Array({1, 2}, {true, true});
    auto field_meta =
        MakeExternalFieldMetaForTest(milvus::DataType::VECTOR_ARRAY,
                                     2,
                                     false,
                                     milvus::DataType::VECTOR_FLOAT);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, VectorArrayRejectsOuterNullRow) {
    arrow::FloatBuilder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1.0f, 2.0f}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    arrow::Int32Builder inner_offsets_builder;
    ASSERT_TRUE(inner_offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> inner_offsets;
    ASSERT_TRUE(inner_offsets_builder.Finish(&inner_offsets).ok());
    auto inner_list = *arrow::ListArray::FromArrays(*inner_offsets, *values);

    arrow::Int32Builder outer_offsets_builder;
    ASSERT_TRUE(outer_offsets_builder.AppendValues({0, 1, 1}).ok());
    std::shared_ptr<arrow::Array> outer_offsets;
    ASSERT_TRUE(outer_offsets_builder.Finish(&outer_offsets).ok());

    arrow::TypedBufferBuilder<bool> null_bitmap_builder;
    ASSERT_TRUE(null_bitmap_builder.Reserve(2).ok());
    null_bitmap_builder.UnsafeAppend(true);
    null_bitmap_builder.UnsafeAppend(false);
    std::shared_ptr<arrow::Buffer> null_bitmap;
    ASSERT_TRUE(null_bitmap_builder.Finish(&null_bitmap).ok());

    auto outer_offsets_values =
        std::static_pointer_cast<arrow::Int32Array>(outer_offsets)->values();
    auto input =
        std::make_shared<arrow::ListArray>(arrow::list(inner_list->type()),
                                           2,
                                           outer_offsets_values,
                                           inner_list,
                                           null_bitmap,
                                           1);

    auto field_meta =
        MakeExternalFieldMetaForTest(milvus::DataType::VECTOR_ARRAY,
                                     2,
                                     false,
                                     milvus::DataType::VECTOR_FLOAT);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, SparseVectorRejectsString) {
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.AppendValues({"not_sparse"}).ok());
    std::shared_ptr<arrow::Array> input;
    ASSERT_TRUE(builder.Finish(&input).ok());

    auto field_meta =
        MakeExternalFieldMetaForTest(milvus::DataType::VECTOR_SPARSE_U32_F32,
                                     0,
                                     false,
                                     milvus::DataType::NONE);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, ArrayInt64RejectsStringList) {
    arrow::StringBuilder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({"1", "2"}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    auto input = *arrow::ListArray::FromArrays(*offsets, *values);
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::ARRAY, 0, false, milvus::DataType::INT64);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, ArrayInt64RejectsNullElement) {
    arrow::Int64Builder values_builder;
    ASSERT_TRUE(values_builder.Append(1).ok());
    ASSERT_TRUE(values_builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    auto input = *arrow::ListArray::FromArrays(*offsets, *values);
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::ARRAY, 0, false, milvus::DataType::INT64);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, ArrayVarcharRejectsInt64List) {
    arrow::Int64Builder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    auto input = *arrow::ListArray::FromArrays(*offsets, *values);
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::ARRAY, 0, false, milvus::DataType::VARCHAR);
    EXPECT_THROW(milvus::storage::NormalizeExternalArrow(input, field_meta),
                 std::exception);
}

TEST(NormalizeExternalArrow, ArrayVarcharAcceptsStringList) {
    arrow::StringBuilder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({"a", "b"}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(values_builder.Finish(&values).ok());

    arrow::Int32Builder offsets_builder;
    ASSERT_TRUE(offsets_builder.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(offsets_builder.Finish(&offsets).ok());

    auto input = *arrow::ListArray::FromArrays(*offsets, *values);
    auto field_meta = MakeExternalFieldMetaForTest(
        milvus::DataType::ARRAY, 0, false, milvus::DataType::VARCHAR);
    auto out = milvus::storage::NormalizeExternalArrow(input, field_meta);
    ASSERT_EQ(out->type_id(), arrow::Type::BINARY);
    ASSERT_EQ(out->length(), 1);
    EXPECT_FALSE(out->IsNull(0));
}

// ===== CoerceToList =====

TEST(CoerceToList, LargeListToList) {
    arrow::Int32Builder vb;
    ASSERT_TRUE(vb.AppendValues({1, 2, 3, 4}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(vb.Finish(&values).ok());

    arrow::Int64Builder ob;
    ASSERT_TRUE(ob.AppendValues({0, 2, 4}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(ob.Finish(&offsets).ok());
    auto large_list = *arrow::LargeListArray::FromArrays(*offsets, *values);

    auto out = CoerceToList({large_list})[0];
    ASSERT_EQ(out->type_id(), arrow::Type::LIST);
    auto la = std::static_pointer_cast<arrow::ListArray>(out);
    EXPECT_EQ(la->length(), 2);
    EXPECT_EQ(la->value_length(0), 2);
    EXPECT_EQ(la->value_length(1), 2);
}

// ===== Sliced-input nullability =====

TEST(CanonicalizeArrowVariants, SlicedLargeListNullsPreserved) {
    // values: [a, b, c, d, e, f]
    auto values = MakeLargeStringArray({"a", "b", "c", "d", "e", "f"},
                                       {true, true, true, true, true, true});
    // 4 rows: [a,b], NULL, [c,d], [e,f]
    arrow::Int64Builder ob;
    ASSERT_TRUE(ob.AppendValues({0, 2, 2, 4, 6}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(ob.Finish(&offsets).ok());
    // null bitmap for the LargeListArray: row 1 is null
    arrow::TypedBufferBuilder<bool> nb;
    ASSERT_TRUE(nb.Reserve(4).ok());
    nb.UnsafeAppend(true);
    nb.UnsafeAppend(false);
    nb.UnsafeAppend(true);
    nb.UnsafeAppend(true);
    std::shared_ptr<arrow::Buffer> null_buf;
    ASSERT_TRUE(nb.Finish(&null_buf).ok());
    auto large_list = std::make_shared<arrow::LargeListArray>(
        arrow::large_list(values->type()),
        4,
        std::static_pointer_cast<arrow::Int64Array>(offsets)->values(),
        values,
        null_buf,
        1,
        0);

    // Slice from offset 1, length 3: should yield [NULL, [c,d], [e,f]]
    auto sliced = large_list->Slice(1, 3);
    ASSERT_TRUE(sliced->IsNull(0));
    ASSERT_FALSE(sliced->IsNull(1));

    auto out = CanonicalizeArrowVariants(sliced);
    ASSERT_EQ(out->type_id(), arrow::Type::LIST);
    ASSERT_EQ(out->length(), 3);
    EXPECT_EQ(out->null_count(), 1);
    EXPECT_TRUE(out->IsNull(0));
    EXPECT_FALSE(out->IsNull(1));
    EXPECT_FALSE(out->IsNull(2));
    auto la = std::static_pointer_cast<arrow::ListArray>(out);
    EXPECT_EQ(la->value_length(0), 0);  // null row → empty range
    EXPECT_EQ(la->value_length(1), 2);
    EXPECT_EQ(la->value_length(2), 2);
}

TEST(CanonicalizeArrowVariants, SlicedFixedSizeListNullsPreserved) {
    auto values = MakeStringViewArray({"a", "b", "c", "d", "e", "f"},
                                      {true, true, true, true, true, true});
    arrow::TypedBufferBuilder<bool> nb;
    ASSERT_TRUE(nb.Reserve(3).ok());
    nb.UnsafeAppend(true);
    nb.UnsafeAppend(false);
    nb.UnsafeAppend(true);
    std::shared_ptr<arrow::Buffer> null_buf;
    ASSERT_TRUE(nb.Finish(&null_buf).ok());
    auto fsl = std::make_shared<arrow::FixedSizeListArray>(
        arrow::fixed_size_list(values->type(), 2), 3, values, null_buf, 1, 0);

    auto sliced = fsl->Slice(1, 2);
    ASSERT_TRUE(sliced->IsNull(0));
    ASSERT_FALSE(sliced->IsNull(1));

    auto out = CanonicalizeArrowVariants(sliced);
    ASSERT_EQ(out->type_id(), arrow::Type::FIXED_SIZE_LIST);
    EXPECT_EQ(out->length(), 2);
    EXPECT_EQ(out->null_count(), 1);
    EXPECT_TRUE(out->IsNull(0));
    EXPECT_FALSE(out->IsNull(1));
    auto fsla = std::static_pointer_cast<arrow::FixedSizeListArray>(out);
    EXPECT_EQ(fsla->values()->type_id(), arrow::Type::STRING);
}

TEST(CoerceToList, SlicedLargeListNullsPreserved) {
    arrow::Int32Builder vb;
    ASSERT_TRUE(vb.AppendValues({1, 2, 3, 4, 5, 6}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(vb.Finish(&values).ok());
    arrow::Int64Builder ob;
    ASSERT_TRUE(ob.AppendValues({0, 2, 2, 4, 6}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(ob.Finish(&offsets).ok());
    arrow::TypedBufferBuilder<bool> nb;
    ASSERT_TRUE(nb.Reserve(4).ok());
    nb.UnsafeAppend(true);
    nb.UnsafeAppend(false);
    nb.UnsafeAppend(true);
    nb.UnsafeAppend(true);
    std::shared_ptr<arrow::Buffer> null_buf;
    ASSERT_TRUE(nb.Finish(&null_buf).ok());
    auto ll = std::make_shared<arrow::LargeListArray>(
        arrow::large_list(values->type()),
        4,
        std::static_pointer_cast<arrow::Int64Array>(offsets)->values(),
        values,
        null_buf,
        1,
        0);
    auto sliced = ll->Slice(1, 3);

    auto out = CoerceToList({sliced})[0];
    ASSERT_EQ(out->type_id(), arrow::Type::LIST);
    EXPECT_EQ(out->length(), 3);
    EXPECT_EQ(out->null_count(), 1);
    EXPECT_TRUE(out->IsNull(0));
}

TEST(CanonicalizeArrowVariants, AllNullStringView) {
    auto in = MakeStringViewArray({"x", "y", "z"}, {false, false, false});
    auto out = CanonicalizeArrowVariants(in);
    ASSERT_EQ(out->type_id(), arrow::Type::STRING);
    EXPECT_EQ(out->null_count(), 3);
    EXPECT_TRUE(out->IsNull(0));
    EXPECT_TRUE(out->IsNull(1));
    EXPECT_TRUE(out->IsNull(2));
}

TEST(CoerceToList, ListPassthrough) {
    arrow::Int32Builder vb;
    ASSERT_TRUE(vb.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(vb.Finish(&values).ok());
    arrow::Int32Builder ob;
    ASSERT_TRUE(ob.AppendValues({0, 2}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(ob.Finish(&offsets).ok());
    auto in = *arrow::ListArray::FromArrays(*offsets, *values);
    auto out = CoerceToList({in})[0];
    EXPECT_EQ(out.get(), in.get());
}
