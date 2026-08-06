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

#include <cstdint>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentGrowingImpl.h"

namespace milvus::segcore {

TEST(SegmentChunkReader, NumericVariantMismatchIsSystemError) {
    const data_access_type value = int64_t{7};

    try {
        (void)get_from_variant<int32_t>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

TEST(SegmentChunkReader, StringVariantMismatchIsSystemError) {
    const data_access_type value = true;

    try {
        (void)get_from_variant<std::string>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

// MoveCursorForSingleChunk used to lack a break: once processed_rows reached
// batch_size the condition stayed true for every remaining row, so the loops ran
// to the end of the segment and left the cursor on the last row instead of on
// batch_size. Compare MoveCursorForMultipleChunk (same class) and
// MoveCursorForDataSingleChunk (exec/expression/Expr.h), which both stop.
TEST(SegmentChunkReader, MoveCursorForSingleChunkStopsAtBatchBoundary) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    const int64_t size_per_chunk = segment->size_per_chunk();
    ASSERT_GT(size_per_chunk, 0);
    const int64_t active_count = 2 * size_per_chunk + 1;
    const SegmentChunkReader reader(nullptr, segment.get(), active_count);

    const int64_t num_chunk =
        (active_count + size_per_chunk - 1) / size_per_chunk;
    const int64_t batch_size = size_per_chunk;

    int64_t chunk_id = 0;
    int64_t chunk_pos = 0;
    reader.MoveCursorForSingleChunk(chunk_id, chunk_pos, num_chunk, batch_size);

    const int64_t absolute_offset = chunk_id * size_per_chunk + chunk_pos;
    EXPECT_EQ(absolute_offset, batch_size)
        << "cursor must advance by exactly batch_size";
    EXPECT_LT(absolute_offset, active_count)
        << "cursor was pushed to the segment end";

    // A second move continues from where the first one stopped.
    reader.MoveCursorForSingleChunk(chunk_id, chunk_pos, num_chunk, batch_size);
    EXPECT_EQ(chunk_id * size_per_chunk + chunk_pos, 2 * batch_size);
}

}  // namespace milvus::segcore
