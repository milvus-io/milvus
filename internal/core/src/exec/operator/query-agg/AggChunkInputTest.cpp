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

#include "exec/operator/query-agg/AggChunkInput.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "cachinglayer/Manager.h"
#include "common/Chunk.h"
#include "common/FieldMeta.h"
#include "mmap/ChunkedColumn.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus::exec {
namespace {

constexpr int64_t kFieldId = 100;
constexpr int32_t kElementSize = sizeof(int32_t);

std::vector<int32_t>
CollectOffsets(const AggSelectedChunk& chunk) {
    std::vector<int32_t> offsets;
    chunk.ForEachSelectedOffset(
        [&](int32_t offset) { offsets.push_back(offset); });
    return offsets;
}

std::vector<char>
BuildFixedWidthChunkBuffer(const std::vector<int32_t>& values,
                           const std::vector<bool>& valid) {
    const auto rows = static_cast<int64_t>(values.size());
    const int64_t bitmap_bytes = (rows + 7) / 8;
    std::vector<char> buffer(bitmap_bytes + rows * kElementSize, 0);
    for (int64_t i = 0; i < rows; ++i) {
        if (valid[i]) {
            buffer[i >> 3] |= (1 << (i & 0x07));
        }
    }
    std::memcpy(buffer.data() + bitmap_bytes,
                values.data(),
                values.size() * sizeof(int32_t));
    return buffer;
}

std::shared_ptr<ChunkedColumnInterface>
MakeFixedWidthColumn(std::vector<std::vector<char>>* buffers) {
    // Existing test helper types are local to other translation units, so this
    // fixture only reuses the shared TestChunkTranslator and ChunkedColumn path.
    std::vector<std::unique_ptr<Chunk>> chunks;
    chunks.emplace_back(std::make_unique<FixedWidthChunk>(
        4,
        1,
        (*buffers)[0].data(),
        (*buffers)[0].size(),
        kElementSize,
        true,
        std::make_shared<ChunkMmapGuard>(nullptr, 0, "")));
    chunks.emplace_back(std::make_unique<FixedWidthChunk>(
        3,
        1,
        (*buffers)[1].data(),
        (*buffers)[1].size(),
        kElementSize,
        true,
        std::make_shared<ChunkMmapGuard>(nullptr, 0, "")));

    auto translator = std::make_unique<TestChunkTranslator>(
        std::vector<int64_t>{4, 3}, "agg_chunk_input", std::move(chunks));
    auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
        std::move(translator), nullptr);
    FieldMeta field_meta(FieldName("value"),
                         FieldId(kFieldId),
                         DataType::INT32,
                         true,
                         std::nullopt);
    return std::static_pointer_cast<ChunkedColumnInterface>(
        std::make_shared<ChunkedColumn>(std::move(slot), field_meta));
}

}  // namespace

TEST(AggChunkInputTest, BuildsChunkLocalSelectionFromFilterMask) {
    TargetBitmap filter_mask(12, false);
    filter_mask.set(5, true);
    filter_mask.set(7, true);
    filter_mask.set(8, true);
    filter_mask.set(9, true);
    filter_mask.set(10, true);
    filter_mask.set(11, true);

    auto input =
        AggChunkInput::FromChunkBounds({0, 4, 8, 12}, filter_mask.view());

    ASSERT_EQ(input.selected_count(), 6);
    ASSERT_EQ(input.chunks().size(), 2);

    const auto& first = input.chunks()[0];
    EXPECT_EQ(first.chunk_id(), 0);
    EXPECT_EQ(first.segment_offset(), 0);
    EXPECT_EQ(first.row_count(), 4);
    EXPECT_TRUE(first.all_selected());
    EXPECT_EQ(first.selected_count(), 4);
    EXPECT_EQ(CollectOffsets(first), (std::vector<int32_t>{0, 1, 2, 3}));

    const auto& second = input.chunks()[1];
    EXPECT_EQ(second.chunk_id(), 1);
    EXPECT_EQ(second.segment_offset(), 4);
    EXPECT_EQ(second.row_count(), 4);
    EXPECT_FALSE(second.all_selected());
    EXPECT_EQ(second.selected_count(), 2);
    EXPECT_EQ(CollectOffsets(second), (std::vector<int32_t>{0, 2}));
}

TEST(AggChunkInputTest, RequiresChunkBoundsToMatchFilterMaskSize) {
    TargetBitmap filter_mask(10, false);

    EXPECT_THROW(
        AggChunkInput::FromChunkBounds({0, 4, 8, 16}, filter_mask.view()),
        SegcoreError);
}

TEST(AggChunkInputTest, PinsChunkedColumnSpanThroughExistingAccess) {
    std::vector<std::vector<char>> buffers;
    buffers.emplace_back(BuildFixedWidthChunkBuffer({10, 11, 12, 13},
                                                    {true, true, false, true}));
    buffers.emplace_back(
        BuildFixedWidthChunkBuffer({20, 21, 22}, {true, false, true}));
    auto column = MakeFixedWidthColumn(&buffers);

    TargetBitmap filter_mask(7, false);
    filter_mask.set(1, true);
    auto input = AggChunkInput::FromChunkedColumn(*column, filter_mask.view());

    ASSERT_EQ(input.selected_count(), 6);
    ASSERT_EQ(input.chunks().size(), 2);
    ASSERT_FALSE(input.chunks()[0].all_selected());
    ASSERT_TRUE(input.chunks()[1].all_selected());

    AggChunkedColumnView column_view(nullptr, *column);
    auto first_pin = column_view.PinSpan(input.chunks()[0]);
    const auto first_span = first_pin.get();
    ASSERT_EQ(first_span.row_count(), 4);
    ASSERT_NE(first_span.valid_data(), nullptr);
    const auto* first_values = static_cast<const int32_t*>(first_span.data());
    EXPECT_EQ(first_values[0], 10);
    EXPECT_EQ(first_values[2], 12);
    EXPECT_FALSE(first_span.valid_data()[2]);
    EXPECT_EQ(CollectOffsets(input.chunks()[0]),
              (std::vector<int32_t>{0, 2, 3}));

    auto second_pin = column_view.PinSpan(input.chunks()[1]);
    const auto second_span = second_pin.get();
    ASSERT_EQ(second_span.row_count(), 3);
    const auto* second_values = static_cast<const int32_t*>(second_span.data());
    EXPECT_EQ(second_values[0], 20);
    EXPECT_EQ(second_values[2], 22);
    EXPECT_EQ(CollectOffsets(input.chunks()[1]),
              (std::vector<int32_t>{0, 1, 2}));
}

}  // namespace milvus::exec
