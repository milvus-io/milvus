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

#include "common/Types.h"
#include "common/Utils.h"
#include "common/Span.h"
#include "common/ValidityView.h"
#include "common/VectorTrait.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentGrowing.h"
#include "test_utils/DataGen.h"

const int64_t ROW_COUNT = 100 * 1000;

TEST(Common, Span) {
    using namespace milvus;
    using namespace milvus::segcore;

    Span<float> s1(nullptr, nullptr, 100);
    Span<milvus::FloatVector> s2(nullptr, 10, 16 * sizeof(float));
    SpanBase b1 = s1;
    SpanBase b2 = s2;
    auto r1 = static_cast<Span<float>>(b1);
    auto r2 = static_cast<Span<milvus::FloatVector>>(b2);
    ASSERT_EQ(r1.row_count(), 100);
    ASSERT_EQ(r2.row_count(), 10);
    ASSERT_EQ(r2.element_sizeof(), 16 * sizeof(float));
}

TEST(Span, ValidityViewSupportsExpandedAndPackedData) {
    using namespace milvus;

    const bool expanded[] = {true, false, true, false, true};
    const uint8_t packed[] = {0x15};
    const auto expanded_view = ValidityView::FromExpanded(expanded);
    const auto packed_view = ValidityView::FromPacked(packed);

    EXPECT_TRUE(expanded_view);
    EXPECT_FALSE(expanded_view.is_packed());
    EXPECT_EQ(expanded_view.expanded_data(), expanded);
    EXPECT_TRUE(packed_view);
    EXPECT_TRUE(packed_view.is_packed());
    EXPECT_EQ(packed_view.expanded_data(), nullptr);
    for (int64_t i = 0; i < 5; ++i) {
        EXPECT_EQ(expanded_view[i], expanded[i]);
        EXPECT_EQ(packed_view[i], expanded[i]);
    }

    const auto subview = packed_view.Subview(1);
    EXPECT_FALSE(subview[0]);
    EXPECT_TRUE(subview[1]);
    EXPECT_FALSE(subview[2]);
    EXPECT_EQ(expanded_view.Subview(1).expanded_data(), expanded + 1);
}

TEST(Span, PackedValidityMasksBitmapsWithoutLosingOffsets) {
    using namespace milvus;
    using namespace milvus::exec;

    constexpr int64_t validity_offset = 3;
    constexpr int64_t result_offset = 5;
    constexpr int64_t size = 70;
    const uint8_t packed[] = {
        0b10110101,
        0b01011010,
        0b11110000,
        0b00111100,
        0b11000011,
        0b10011001,
        0b01100110,
        0b11111111,
        0b00010101,
        0b00011110,
    };
    const auto validity =
        ValidityView::FromPacked(packed).Subview(validity_offset);

    EXPECT_EQ(validity.packed_data(), packed);
    EXPECT_EQ(validity.bit_offset(), validity_offset);

    TargetBitmap result(size + result_offset + 3, false);
    TargetBitmap valid_result(size + result_offset + 3, true);
    for (int64_t i = 0; i < size; ++i) {
        result[result_offset + i] = (i % 3) != 0;
    }
    ApplyValidMask(validity,
                   TargetBitmapView(result).view(result_offset),
                   TargetBitmapView(valid_result).view(result_offset),
                   size);

    for (int64_t i = 0; i < size; ++i) {
        const bool expected_valid = validity[i];
        EXPECT_EQ(result[result_offset + i], ((i % 3) != 0) && expected_valid)
            << "row " << i;
        EXPECT_EQ(valid_result[result_offset + i], expected_valid)
            << "row " << i;
    }
    for (int64_t i = 0; i < result_offset; ++i) {
        EXPECT_FALSE(result[i]);
        EXPECT_TRUE(valid_result[i]);
    }
}

TEST(Span, Naive) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    int64_t N = ROW_COUNT;
    constexpr int64_t size_per_chunk = 32 * 1024;
    auto schema = std::make_shared<Schema>();
    auto bin_vec_fid = schema->AddDebugField(
        "binaryvec", DataType::VECTOR_BINARY, 512, knowhere::metric::JACCARD);
    auto float_fid = schema->AddDebugField("age", DataType::FLOAT);
    auto float_vec_fid = schema->AddDebugField(
        "floatvec", DataType::VECTOR_FLOAT, 32, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto dataset = DataGen(schema, N, 42, 0, 1, 10, false, true, true);
    auto segment = CreateGrowingSegment(schema, empty_index_meta, -1);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto vec_ptr = dataset.get_col<uint8_t>(bin_vec_fid);
    auto age_ptr = dataset.get_col<float>(float_fid);
    auto float_ptr = dataset.get_col<float>(float_vec_fid);
    auto nullable_data_ptr = dataset.get_col<int64_t>(nullable_fid);
    auto nullable_valid_data_ptr = dataset.get_col_valid(nullable_fid);
    auto num_chunk = segment->num_chunk(FieldId(0));
    ASSERT_EQ(num_chunk, upper_div(N, size_per_chunk));
    auto row_count = segment->get_row_count();
    ASSERT_EQ(N, row_count);
    for (auto chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
        auto vec_span = segment->chunk_data<milvus::BinaryVector>(
            nullptr, bin_vec_fid, chunk_id);
        auto age_span =
            segment->chunk_data<float>(nullptr, float_fid, chunk_id);
        auto float_span = segment->chunk_data<milvus::FloatVector>(
            nullptr, float_vec_fid, chunk_id);
        auto null_field_span =
            segment->chunk_data<int64_t>(nullptr, nullable_fid, chunk_id);
        auto begin = chunk_id * size_per_chunk;
        auto end = std::min((chunk_id + 1) * size_per_chunk, N);
        auto size_of_chunk = end - begin;
        ASSERT_FALSE(age_span.get().validity());
        for (int i = 0; i < size_of_chunk * 512 / 8; ++i) {
            ASSERT_EQ(vec_span.get().data()[i], vec_ptr[i + begin * 512 / 8]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(age_span.get().data()[i], age_ptr[i + begin]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(float_span.get().data()[i], float_ptr[i + begin * 32]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(null_field_span.get().data()[i],
                      nullable_data_ptr[i + begin]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(null_field_span.get().is_valid(i),
                      nullable_valid_data_ptr[i + begin]);
        }
    }
}
