// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Tests for FieldDataToArrow's zero-copy aliasing path.
//
// The retrieve transport builds a DataArray and then converts it to Arrow. For
// every type whose protobuf payload already has Arrow's exact value-buffer
// layout that conversion was a full copy of the payload for nothing, which on
// large results is the single largest cost in the export. Passing an owner lets
// the Arrow array point at the protobuf memory instead.
//
// Two things need proving, and value equality alone proves neither:
//   - the alias is actually an alias (same address, not a copy that matches)
//   - the owner keeps the storage alive after every other handle is gone
// and three need proving in the negative, because aliasing them would be
// silently wrong: nullable columns (compacted, so the layout diverges), BOOL
// (byte-per-element vs bit-packed) and INT8/INT16 (widened to int32).

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "segcore/arrow_field_utils.h"

namespace {

using milvus::DataArray;
using milvus::segcore::FieldDataToArrow;
using milvus::segcore::ProtoOwner;

// ColumnOwner is the shape a real caller passes: a ProtoStorageOwner that owns
// the DataArray whose buffers the Arrow array will alias. ProtoOwner only
// accepts a type that declared itself one, so an unrelated shared_ptr no longer
// compiles -- which is the point of the base class.
struct ColumnOwner : milvus::segcore::ProtoStorageOwner {
    explicit ColumnOwner(DataArray data) : column(std::move(data)) {
    }
    DataArray column;
};

// TrackedColumnOwner additionally reports when it is destroyed, so the lifetime
// test observes the keep-alive rather than assuming it.
struct TrackedColumnOwner : milvus::segcore::ProtoStorageOwner {
    TrackedColumnOwner(DataArray data, bool* destroyed)
        : column(std::move(data)), destroyed_(destroyed) {
    }
    ~TrackedColumnOwner() override {
        *destroyed_ = true;
    }
    DataArray column;
    bool* destroyed_;
};

// valuesBuffer returns the array's value buffer bytes, which is buffer 1 for
// every layout exercised here (buffer 0 is validity).
const uint8_t*
ValuesPtr(const std::shared_ptr<arrow::Array>& array) {
    return array->data()->buffers[1]->data();
}

DataArray
MakeInt64Column(const std::vector<int64_t>& values) {
    DataArray data;
    data.set_type(milvus::proto::schema::DataType::Int64);
    auto* dst = data.mutable_scalars()->mutable_long_data()->mutable_data();
    dst->Add(values.begin(), values.end());
    return data;
}

DataArray
MakeFloatVectorColumn(const std::vector<float>& values, int64_t dim) {
    DataArray data;
    data.set_type(milvus::proto::schema::DataType::FloatVector);
    auto* vectors = data.mutable_vectors();
    vectors->set_dim(dim);
    auto* dst = vectors->mutable_float_vector()->mutable_data();
    dst->Add(values.begin(), values.end());
    return data;
}

}  // namespace

TEST(ArrowAlias, Int64AliasesTheProtobufPayload) {
    auto owner = std::make_shared<ColumnOwner>(MakeInt64Column({7, 8, 9}));
    const auto& col = owner->column;
    const auto* payload = reinterpret_cast<const uint8_t*>(
        col.scalars().long_data().data().data());

    auto aliased = FieldDataToArrow("pk", col, 3, false, owner);
    ASSERT_TRUE(aliased.ok()) << aliased.status().ToString();
    EXPECT_EQ(ValuesPtr(aliased->second), payload)
        << "aliasing path copied instead of pointing at the payload";

    // Same input without an owner must copy, which is what every caller that
    // cannot guarantee the lifetime relies on.
    auto copied = FieldDataToArrow("pk", col, 3, false, nullptr);
    ASSERT_TRUE(copied.ok()) << copied.status().ToString();
    EXPECT_NE(ValuesPtr(copied->second), payload);

    // And both must agree on the values.
    EXPECT_TRUE(aliased->second->Equals(*copied->second));
    EXPECT_EQ(aliased->second->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(aliased->second->null_count(), 0);
}

TEST(ArrowAlias, FloatVectorAliasesTheProtobufPayload) {
    // 2 rows x dim 4.
    auto owner = std::make_shared<ColumnOwner>(
        MakeFloatVectorColumn({1, 2, 3, 4, 5, 6, 7, 8}, 4));
    const auto& col = owner->column;
    const auto* payload = reinterpret_cast<const uint8_t*>(
        col.vectors().float_vector().data().data());

    auto aliased = FieldDataToArrow("vec", col, 2, false, owner);
    ASSERT_TRUE(aliased.ok()) << aliased.status().ToString();
    EXPECT_EQ(ValuesPtr(aliased->second), payload);
    EXPECT_EQ(aliased->second->length(), 2);
    EXPECT_EQ(aliased->second->type()->ToString(),
              arrow::fixed_size_binary(16)->ToString());

    auto copied = FieldDataToArrow("vec", col, 2, false, nullptr);
    ASSERT_TRUE(copied.ok()) << copied.status().ToString();
    EXPECT_NE(ValuesPtr(copied->second), payload);
    EXPECT_TRUE(aliased->second->Equals(*copied->second));
}

TEST(ArrowAlias, OwnerOutlivesEveryOtherHandle) {
    bool destroyed = false;
    std::shared_ptr<arrow::Array> array;
    const uint8_t* payload = nullptr;
    {
        // The owner OWNS the column, mirroring how segment_c.cpp hands over a
        // holder that the user columns were moved into rather than pairing a
        // loose DataArray with an unrelated keep-alive.
        auto owner = std::make_shared<TrackedColumnOwner>(
            MakeInt64Column({11, 22, 33}), &destroyed);
        payload = reinterpret_cast<const uint8_t*>(
            owner->column.scalars().long_data().data().data());
        auto result = FieldDataToArrow("pk", owner->column, 3, false, owner);
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        array = result->second;
        ASSERT_EQ(ValuesPtr(array), payload);
    }
    // Every local handle is gone; only the Arrow array remains.
    ASSERT_FALSE(destroyed)
        << "owner was freed while the array still aliased it";
    ASSERT_EQ(ValuesPtr(array), payload);
    {
        // static_pointer_cast uses the ALIASING constructor: `typed` shares
        // `array`'s control block and co-owns it. It has to go out of scope
        // before the reset below, or array.reset() drops use_count 2->1, the
        // Int64Array -> ArrayData -> ProtoBackedBuffer -> owner chain is never
        // unwound, and this test fails for a reason unrelated to the property
        // it is checking.
        auto typed = std::static_pointer_cast<arrow::Int64Array>(array);
        EXPECT_EQ(typed->Value(0), 11);
        EXPECT_EQ(typed->Value(1), 22);
        EXPECT_EQ(typed->Value(2), 33);
    }

    ASSERT_EQ(array.use_count(), 1)
        << "another handle still co-owns the array, so the reset below would "
           "not exercise the keep-alive at all";
    array.reset();
    EXPECT_TRUE(destroyed) << "owner leaked after the last array was released";
}

TEST(ArrowAlias, NullableColumnFallsBackToCopy) {
    // A validity bitmap means the column is nullable, so aliasing is declined:
    // Arrow's representation puts a zero under the false bit while the source
    // keeps whatever segcore stored, and for VECTORS the payload is compacted
    // outright. Either way the layout is not the one an alias assumes.
    // Three PHYSICAL entries, not two: a nullable SCALAR keeps its full-length
    // payload (MergeDataArray's scalar branch walks src_offset per row), so
    // BuildFixedWidthArray indexes it logically. Only nullable VECTORS are
    // compacted, which is what BuildDenseVectorArray's physical indexing is
    // for. Passing two values here tripped the length assert before any
    // expectation ran.
    auto owner = std::make_shared<ColumnOwner>(MakeInt64Column({7, 0, 9}));
    auto& col = owner->column;
    col.mutable_valid_data()->Add(true);
    col.mutable_valid_data()->Add(false);
    col.mutable_valid_data()->Add(true);
    const auto* payload = reinterpret_cast<const uint8_t*>(
        col.scalars().long_data().data().data());

    auto result = FieldDataToArrow("pk", col, 3, false, owner);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_NE(ValuesPtr(result->second), payload)
        << "a nullable column must not be aliased";
    ASSERT_EQ(result->second->length(), 3);
    EXPECT_EQ(result->second->null_count(), 1);
    auto typed = std::static_pointer_cast<arrow::Int64Array>(result->second);
    EXPECT_EQ(typed->Value(0), 7);
    EXPECT_TRUE(result->second->IsNull(1));
    EXPECT_EQ(typed->Value(2), 9);
}

TEST(ArrowAlias, BoolAndNarrowedIntsFallBackToCopy) {
    {
        // protobuf stores one byte per bool; Arrow bit-packs.
        DataArray boolCol;
        boolCol.set_type(milvus::proto::schema::DataType::Bool);
        auto* dst =
            boolCol.mutable_scalars()->mutable_bool_data()->mutable_data();
        dst->Add(true);
        dst->Add(false);
        auto owner = std::make_shared<ColumnOwner>(std::move(boolCol));
        const auto& col = owner->column;
        const auto* payload = reinterpret_cast<const uint8_t*>(
            col.scalars().bool_data().data().data());
        auto result = FieldDataToArrow("b", col, 2, false, owner);
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        EXPECT_NE(ValuesPtr(result->second), payload);
        EXPECT_EQ(result->second->type()->id(), arrow::Type::BOOL);
    }
    {
        // INT8 is widened to int32 in protobuf, so int8() cannot alias it.
        DataArray i8Col;
        i8Col.set_type(milvus::proto::schema::DataType::Int8);
        auto* dst = i8Col.mutable_scalars()->mutable_int_data()->mutable_data();
        dst->Add(1);
        dst->Add(2);
        auto owner = std::make_shared<ColumnOwner>(std::move(i8Col));
        const auto& col = owner->column;
        const auto* payload = reinterpret_cast<const uint8_t*>(
            col.scalars().int_data().data().data());
        auto preserved = FieldDataToArrow(
            "i8", col, 2, /*preserve_integer_width=*/true, owner);
        ASSERT_TRUE(preserved.ok()) << preserved.status().ToString();
        EXPECT_NE(ValuesPtr(preserved->second), payload);
        EXPECT_EQ(preserved->second->type()->id(), arrow::Type::INT8);

        // Without preserve_integer_width the same column is exported as int32,
        // which IS the protobuf layout, so it aliases.
        auto widened = FieldDataToArrow("i8", col, 2, false, owner);
        ASSERT_TRUE(widened.ok()) << widened.status().ToString();
        EXPECT_EQ(ValuesPtr(widened->second), payload);
        EXPECT_EQ(widened->second->type()->id(), arrow::Type::INT32);
    }
}
