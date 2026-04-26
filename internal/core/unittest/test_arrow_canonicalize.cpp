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
#include <string>
#include <vector>

#include "common/EasyAssert.h"
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
            EXPECT_TRUE(b
                           .Append(reinterpret_cast<const uint8_t*>(
                                       vals[i].data()),
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
            EXPECT_TRUE(b
                           .Append(reinterpret_cast<const uint8_t*>(
                                       vals[i].data()),
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
    auto values = MakeLargeStringArray({"a", "b", "c", "d"},
                                       {true, true, true, true});
    // Build LargeListArray with offsets [0, 2, 2, 4] -> [[a,b], [], [c,d]]
    arrow::Int64Builder ob;
    ASSERT_TRUE(ob.AppendValues({0, 2, 2, 4}).ok());
    std::shared_ptr<arrow::Array> offsets;
    ASSERT_TRUE(ob.Finish(&offsets).ok());
    auto large_list_result = arrow::LargeListArray::FromArrays(*offsets,
                                                               *values);
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
    auto values =
        MakeStringViewArray({"x", "y", "z"}, {true, true, true});
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
    ASSERT_TRUE(
        b.Append(reinterpret_cast<const uint8_t*>("xy"), 2).ok());
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

// ===== Integer narrowing (via NormalizeExternalArrow) =====
// MaybeNarrowInt is exercised through NormalizeExternalArrow because the
// helper itself is static. These tests build wider arrow ints and assert the
// returned array is the narrower target type with values intact.

#include "common/FieldMeta.h"

namespace {
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
}  // namespace

TEST(IntegerNarrowing, Int32ToInt8) {
    auto in = MakeInt32Array({0, 1, 99, -128, 127}, {true, true, true, true, true});
    auto out = milvus::storage::NormalizeExternalArrow(
        in, milvus::DataType::INT8, 0, false, milvus::DataType::NONE);
    ASSERT_EQ(out->type_id(), arrow::Type::INT8);
    auto ia = std::static_pointer_cast<arrow::Int8Array>(out);
    EXPECT_EQ(ia->Value(0), 0);
    EXPECT_EQ(ia->Value(1), 1);
    EXPECT_EQ(ia->Value(2), 99);
    EXPECT_EQ(ia->Value(3), -128);
    EXPECT_EQ(ia->Value(4), 127);
}

TEST(IntegerNarrowing, Int32ToInt16) {
    auto in = MakeInt32Array({0, 1000, -32768, 32767}, {true, true, true, true});
    auto out = milvus::storage::NormalizeExternalArrow(
        in, milvus::DataType::INT16, 0, false, milvus::DataType::NONE);
    ASSERT_EQ(out->type_id(), arrow::Type::INT16);
    auto ia = std::static_pointer_cast<arrow::Int16Array>(out);
    EXPECT_EQ(ia->Value(2), -32768);
    EXPECT_EQ(ia->Value(3), 32767);
}

TEST(IntegerNarrowing, Int32ToInt8WithNulls) {
    auto in = MakeInt32Array({5, 0, 7}, {true, false, true});
    auto out = milvus::storage::NormalizeExternalArrow(
        in, milvus::DataType::INT8, 0, true, milvus::DataType::NONE);
    ASSERT_EQ(out->type_id(), arrow::Type::INT8);
    EXPECT_EQ(out->null_count(), 1);
    EXPECT_TRUE(out->IsNull(1));
}

TEST(IntegerNarrowing, OverflowAsserts) {
    auto in = MakeInt32Array({999}, {true});  // > INT8_MAX
    EXPECT_THROW(
        milvus::storage::NormalizeExternalArrow(
            in, milvus::DataType::INT8, 0, false, milvus::DataType::NONE),
        std::exception);
}

TEST(IntegerNarrowing, NoNarrowOnExactMatch) {
    arrow::Int8Builder b;
    ASSERT_TRUE(b.Append(int8_t{5}).ok());
    std::shared_ptr<arrow::Array> in;
    ASSERT_TRUE(b.Finish(&in).ok());
    auto out = milvus::storage::NormalizeExternalArrow(
        in, milvus::DataType::INT8, 0, false, milvus::DataType::NONE);
    EXPECT_EQ(out.get(), in.get());
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
        arrow::large_list(values->type()), 4,
        std::static_pointer_cast<arrow::Int64Array>(offsets)->values(),
        values, null_buf, 1, 0);

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
    auto values =
        MakeStringViewArray({"a", "b", "c", "d", "e", "f"},
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
        arrow::large_list(values->type()), 4,
        std::static_pointer_cast<arrow::Int64Array>(offsets)->values(), values,
        null_buf, 1, 0);
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
