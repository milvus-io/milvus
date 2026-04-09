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

#include <cachinglayer/Translator.h>
#include "common/Chunk.h"
#include "gtest/gtest.h"
#include "mmap/ChunkedColumn.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus {

TEST(test_chunked_column, test_get_chunkid) {
    std::vector<int64_t> num_rows_per_chunk = {0, 10, 20, 30};
    auto num_chunks = num_rows_per_chunk.size();
    std::vector<std::unique_ptr<Chunk>> chunks;
    for (auto i = 0; i < num_chunks; ++i) {
        auto row_num = num_rows_per_chunk[i];
        auto chunk_mmap_guard =
            std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
        auto chunk = std::make_unique<FixedWidthChunk<int64_t>>(
            row_num, nullptr, 0, sizeof(int64_t), false, chunk_mmap_guard);
        chunks.push_back(std::move(chunk));
    }
    auto translator = std::make_unique<TestChunkTranslator>(
        num_rows_per_chunk, "test", std::move(chunks));
    FieldMeta field_meta(
        FieldName("test"), FieldId(1), DataType::INT64, false, std::nullopt);
    auto slot =
        cachinglayer::Manager::GetInstance().CreateCacheSlot<milvus::Chunk>(
            std::move(translator), nullptr);
    ChunkedColumn column(std::move(slot), field_meta);

    int offset = 0;
    for (int i = 0; i < num_chunks; ++i) {
        for (int j = 0; j < num_rows_per_chunk[i]; ++j) {
            auto [chunk_id, offset_in_chunk] =
                column.GetChunkIDByOffset(offset);
            ASSERT_EQ(chunk_id, i);
            ASSERT_EQ(offset_in_chunk, j);
            offset++;
        }
    }
}
TEST(test_chunked_column, test_nullable_build_valid_row_ids) {
    // Create 3 nullable chunks with varying valid/null patterns
    // Chunk 0: 5 rows, valid pattern: [T, F, T, T, F] -> 3 valid
    // Chunk 1: 3 rows, valid pattern: [F, F, F] -> 0 valid
    // Chunk 2: 4 rows, valid pattern: [T, T, T, T] -> 4 valid

    std::vector<int64_t> num_rows_per_chunk = {5, 3, 4};
    auto num_chunks = num_rows_per_chunk.size();

    std::vector<std::vector<bool>> valid_patterns = {
        {true, false, true, true, false},
        {false, false, false},
        {true, true, true, true}};

    // Pre-allocate buffers so chunk data pointers remain stable
    int32_t element_size = 4;
    std::vector<std::vector<char>> buffers(num_chunks);
    for (size_t i = 0; i < num_chunks; i++) {
        auto row_num = num_rows_per_chunk[i];
        int32_t null_bitmap_bytes = (row_num + 7) / 8;
        int32_t total_bytes = null_bitmap_bytes + row_num * element_size;
        buffers[i].resize(total_bytes, 0);
        // Set null bitmap: bit j = 1 means row j is valid
        for (int j = 0; j < row_num; j++) {
            if (valid_patterns[i][j]) {
                buffers[i][j >> 3] |= (1 << (j & 0x07));
            }
        }
    }

    // Create chunks pointing to stable buffer addresses
    std::vector<std::unique_ptr<Chunk>> chunks;
    for (size_t i = 0; i < num_chunks; i++) {
        auto row_num = num_rows_per_chunk[i];
        auto chunk_mmap_guard =
            std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
        auto chunk =
            std::make_unique<FixedWidthChunk<int32_t>>(row_num,
                                                       buffers[i].data(),
                                                       buffers[i].size(),
                                                       element_size,
                                                       true,
                                                       chunk_mmap_guard);
        chunks.push_back(std::move(chunk));
    }

    auto translator = std::make_unique<TestChunkTranslator>(
        num_rows_per_chunk, "test_nullable", std::move(chunks));
    FieldMeta field_meta(
        FieldName("test"), FieldId(1), DataType::INT32, true, std::nullopt);
    auto slot =
        cachinglayer::Manager::GetInstance().CreateCacheSlot<milvus::Chunk>(
            std::move(translator), nullptr);
    ChunkedColumn column(std::move(slot), field_meta);

    // Call BuildValidRowIds to populate valid row tracking
    column.BuildValidRowIds(nullptr);

    // Verify GetNumValidRowsUntilChunk()
    // Expected cumulative valid counts: [0, 3, 3, 7]
    auto& num_valid_rows = column.GetNumValidRowsUntilChunk();
    ASSERT_EQ(num_valid_rows.size(), num_chunks + 1);
    EXPECT_EQ(num_valid_rows[0], 0);
    EXPECT_EQ(num_valid_rows[1], 3);  // chunk 0 has 3 valid
    EXPECT_EQ(num_valid_rows[2], 3);  // chunk 1 has 0 valid
    EXPECT_EQ(num_valid_rows[3], 7);  // chunk 2 has 4 valid

    // Verify GetValidCountPerChunk()
    auto& valid_counts = column.GetValidCountPerChunk();
    ASSERT_EQ(valid_counts.size(), num_chunks);
    EXPECT_EQ(valid_counts[0], 3);
    EXPECT_EQ(valid_counts[1], 0);
    EXPECT_EQ(valid_counts[2], 4);

    // Verify GetValidData() matches the patterns
    auto& valid_data = column.GetValidData();
    int total_rows = 5 + 3 + 4;
    ASSERT_EQ(valid_data.size(), total_rows);
    int idx = 0;
    for (size_t i = 0; i < num_chunks; i++) {
        for (size_t j = 0; j < valid_patterns[i].size(); j++) {
            EXPECT_EQ(valid_data[idx], valid_patterns[i][j])
                << "Mismatch at global row " << idx << " (chunk " << i
                << ", row " << j << ")";
            idx++;
        }
    }
}

TEST(test_chunked_column, test_nullable_get_num_valid_rows_non_nullable) {
    // For non-nullable columns, GetNumValidRowsUntilChunk() should
    // fall back to GetNumRowsUntilChunk()
    std::vector<int64_t> num_rows_per_chunk = {10, 20, 30};
    auto num_chunks = num_rows_per_chunk.size();
    std::vector<std::unique_ptr<Chunk>> chunks;
    for (size_t i = 0; i < num_chunks; ++i) {
        auto row_num = num_rows_per_chunk[i];
        auto chunk_mmap_guard =
            std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
        auto chunk = std::make_unique<FixedWidthChunk<int64_t>>(
            row_num, nullptr, 0, sizeof(int64_t), false, chunk_mmap_guard);
        chunks.push_back(std::move(chunk));
    }
    auto translator = std::make_unique<TestChunkTranslator>(
        num_rows_per_chunk, "test_non_nullable", std::move(chunks));
    FieldMeta field_meta(
        FieldName("test"), FieldId(1), DataType::INT64, false, std::nullopt);
    auto slot =
        cachinglayer::Manager::GetInstance().CreateCacheSlot<milvus::Chunk>(
            std::move(translator), nullptr);
    ChunkedColumn column(std::move(slot), field_meta);

    // For non-nullable, GetNumValidRowsUntilChunk should equal
    // GetNumRowsUntilChunk
    auto& valid_rows = column.GetNumValidRowsUntilChunk();
    auto& total_rows = column.GetNumRowsUntilChunk();
    ASSERT_EQ(valid_rows.size(), total_rows.size());
    for (size_t i = 0; i < valid_rows.size(); i++) {
        EXPECT_EQ(valid_rows[i], total_rows[i]);
    }
}

TEST(test_chunked_column, test_nullable_all_valid) {
    // All rows valid: physical offsets should equal logical offsets
    std::vector<int64_t> num_rows_per_chunk = {4, 6};
    auto num_chunks = num_rows_per_chunk.size();

    std::vector<std::vector<bool>> valid_patterns = {
        {true, true, true, true}, {true, true, true, true, true, true}};

    int32_t element_size = 4;
    std::vector<std::vector<char>> buffers(num_chunks);
    for (size_t i = 0; i < num_chunks; i++) {
        auto row_num = num_rows_per_chunk[i];
        int32_t null_bitmap_bytes = (row_num + 7) / 8;
        int32_t total_bytes = null_bitmap_bytes + row_num * element_size;
        buffers[i].resize(total_bytes, 0);
        for (int j = 0; j < row_num; j++) {
            buffers[i][j >> 3] |= (1 << (j & 0x07));  // all valid
        }
    }

    std::vector<std::unique_ptr<Chunk>> chunks;
    for (size_t i = 0; i < num_chunks; i++) {
        auto row_num = num_rows_per_chunk[i];
        auto chunk_mmap_guard =
            std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
        auto chunk =
            std::make_unique<FixedWidthChunk<int32_t>>(row_num,
                                                       buffers[i].data(),
                                                       buffers[i].size(),
                                                       element_size,
                                                       true,
                                                       chunk_mmap_guard);
        chunks.push_back(std::move(chunk));
    }

    auto translator = std::make_unique<TestChunkTranslator>(
        num_rows_per_chunk, "test_all_valid", std::move(chunks));
    FieldMeta field_meta(
        FieldName("test"), FieldId(1), DataType::INT32, true, std::nullopt);
    auto slot =
        cachinglayer::Manager::GetInstance().CreateCacheSlot<milvus::Chunk>(
            std::move(translator), nullptr);
    ChunkedColumn column(std::move(slot), field_meta);

    column.BuildValidRowIds(nullptr);

    // When all rows are valid, num_valid_rows_until_chunk should equal
    // num_rows_until_chunk
    auto& num_valid_rows = column.GetNumValidRowsUntilChunk();
    auto& num_rows = column.GetNumRowsUntilChunk();
    ASSERT_EQ(num_valid_rows.size(), num_rows.size());
    for (size_t i = 0; i < num_valid_rows.size(); i++) {
        EXPECT_EQ(num_valid_rows[i], num_rows[i]);
    }

    // All valid counts should equal row counts
    auto& valid_counts = column.GetValidCountPerChunk();
    EXPECT_EQ(valid_counts[0], 4);
    EXPECT_EQ(valid_counts[1], 6);
}

TEST(test_chunked_column, test_nullable_all_null) {
    // All rows null: valid counts should all be 0
    std::vector<int64_t> num_rows_per_chunk = {3, 5};
    auto num_chunks = num_rows_per_chunk.size();

    int32_t element_size = 4;
    std::vector<std::vector<char>> buffers(num_chunks);
    for (size_t i = 0; i < num_chunks; i++) {
        auto row_num = num_rows_per_chunk[i];
        int32_t null_bitmap_bytes = (row_num + 7) / 8;
        int32_t total_bytes = null_bitmap_bytes + row_num * element_size;
        buffers[i].resize(total_bytes, 0);  // all bits zero = all null
    }

    std::vector<std::unique_ptr<Chunk>> chunks;
    for (size_t i = 0; i < num_chunks; i++) {
        auto row_num = num_rows_per_chunk[i];
        auto chunk_mmap_guard =
            std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
        auto chunk =
            std::make_unique<FixedWidthChunk<int32_t>>(row_num,
                                                       buffers[i].data(),
                                                       buffers[i].size(),
                                                       element_size,
                                                       true,
                                                       chunk_mmap_guard);
        chunks.push_back(std::move(chunk));
    }

    auto translator = std::make_unique<TestChunkTranslator>(
        num_rows_per_chunk, "test_all_null", std::move(chunks));
    FieldMeta field_meta(
        FieldName("test"), FieldId(1), DataType::INT32, true, std::nullopt);
    auto slot =
        cachinglayer::Manager::GetInstance().CreateCacheSlot<milvus::Chunk>(
            std::move(translator), nullptr);
    ChunkedColumn column(std::move(slot), field_meta);

    column.BuildValidRowIds(nullptr);

    auto& num_valid_rows = column.GetNumValidRowsUntilChunk();
    ASSERT_EQ(num_valid_rows.size(), num_chunks + 1);
    // All cumulative valid counts should be 0
    for (size_t i = 0; i <= num_chunks; i++) {
        EXPECT_EQ(num_valid_rows[i], 0);
    }

    auto& valid_counts = column.GetValidCountPerChunk();
    for (size_t i = 0; i < num_chunks; i++) {
        EXPECT_EQ(valid_counts[i], 0);
    }
}

}  // namespace milvus
