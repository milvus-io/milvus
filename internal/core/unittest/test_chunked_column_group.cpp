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

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <cstring>

#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "mmap/ChunkedColumnGroup.h"
#include "test_cachinglayer/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "storage/Event.h"
#include "storage/Util.h"
#include "common/ChunkWriter.h"
#include "common/FieldData.h"

using namespace milvus;
using namespace milvus::storage;

std::shared_ptr<Chunk>
create_chunk(const FixedVector<int64_t>& data) {
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT64);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::INT64,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    return create_chunk(field_meta, 1, array_vec);
}

// Helper function to create chunks for string data
std::shared_ptr<Chunk>
create_chunk(const FixedVector<std::string>& data) {
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::VARCHAR);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("b"),
                         milvus::FieldId(2),
                         DataType::STRING,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    return create_chunk(field_meta, 1, array_vec);
}

// Test fixture for chunk tests
class ChunkedColumnGroupTest : public ::testing::Test {
 protected:
    ChunkedColumnGroupTest()
        : int64_field_meta(
              FieldName(""), FieldId(0), DataType::INT64, false, std::nullopt),
          string_field_meta(FieldName(""),
                            FieldId(0),
                            DataType::STRING,
                            false,
                            std::nullopt) {
    }

    void
    SetUp() override {
        // Create test data
        int64_data = {1, 2, 3, 4, 5};
        string_data = {"a", "b", "c", "d", "e"};

        // Create field metadata
        int64_field_meta = FieldMeta(FieldName("int64_field"),
                                     milvus::FieldId(1),
                                     DataType::INT64,
                                     false,
                                     std::nullopt);
        string_field_meta = FieldMeta(FieldName("string_field"),
                                      milvus::FieldId(2),
                                      DataType::STRING,
                                      false,
                                      std::nullopt);

        // Create chunks
        int64_chunk = std::move(create_chunk(int64_data));
        string_chunk = std::move(create_chunk(string_data));
    }

    FixedVector<int64_t> int64_data;
    FixedVector<std::string> string_data;
    FieldMeta int64_field_meta;
    FieldMeta string_field_meta;
    std::shared_ptr<Chunk> int64_chunk;
    std::shared_ptr<Chunk> string_chunk;
};

TEST_F(ChunkedColumnGroupTest, GroupChunk) {
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    chunks[FieldId(1)] = int64_chunk;
    chunks[FieldId(2)] = string_chunk;

    auto group_chunk = std::make_unique<GroupChunk>(chunks);

    // Has chunk
    EXPECT_EQ(group_chunk->RowNums(), 5);
    EXPECT_TRUE(group_chunk->HasChunk(FieldId(1)));
    EXPECT_TRUE(group_chunk->HasChunk(FieldId(2)));
    EXPECT_FALSE(group_chunk->HasChunk(FieldId(3)));

    // Get chunk
    auto retrieved_int64_chunk = group_chunk->GetChunk(FieldId(1));
    auto retrieved_string_chunk = group_chunk->GetChunk(FieldId(2));
    EXPECT_EQ(retrieved_int64_chunk->RowNums(), 5);
    EXPECT_EQ(retrieved_string_chunk->RowNums(), 5);

    // Get all chunks
    auto all_chunks = group_chunk->GetChunks();
    EXPECT_EQ(all_chunks.size(), 2);
    EXPECT_EQ(all_chunks[FieldId(1)], int64_chunk);
    EXPECT_EQ(all_chunks[FieldId(2)], string_chunk);

    // Add chunk
    auto new_int64_chunk = create_chunk(FixedVector<int64_t>{6, 7, 8, 9, 10});
    EXPECT_NO_THROW(group_chunk->AddChunk(FieldId(3), new_int64_chunk));
    EXPECT_TRUE(group_chunk->HasChunk(FieldId(3)));
    EXPECT_EQ(group_chunk->GetChunk(FieldId(3))->RowNums(), 5);
    auto another_int64_chunk =
        create_chunk(FixedVector<int64_t>{11, 12, 13, 14, 15});
    EXPECT_THROW(group_chunk->AddChunk(FieldId(3), another_int64_chunk),
                 std::exception);

    // Size
    uint64_t expected_size =
        int64_chunk->Size() + string_chunk->Size() + new_int64_chunk->Size();
    EXPECT_EQ(group_chunk->Size(), expected_size);

    // Cell byte size
    uint64_t expected_cell_size = int64_chunk->CellByteSize() +
                                  string_chunk->CellByteSize() +
                                  new_int64_chunk->CellByteSize();
    EXPECT_EQ(group_chunk->CellByteSize(), expected_cell_size);

    // Test empty group chunk
    auto empty_group_chunk = std::make_unique<GroupChunk>();
    EXPECT_EQ(empty_group_chunk->RowNums(), 0);
    EXPECT_EQ(empty_group_chunk->Size(), 0);
    EXPECT_EQ(empty_group_chunk->CellByteSize(), 0);
}

TEST_F(ChunkedColumnGroupTest, ChunkedColumnGroup) {
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    chunks[FieldId(1)] = int64_chunk;
    chunks[FieldId(2)] = string_chunk;
    auto group_chunk = std::make_unique<GroupChunk>(chunks);

    std::vector<std::unique_ptr<GroupChunk>> group_chunks;
    group_chunks.push_back(std::move(group_chunk));
    auto translator = std::make_unique<TestGroupChunkTranslator>(
        std::vector<int64_t>{5}, "test_key", std::move(group_chunks));
    auto column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));

    // basic properties
    EXPECT_EQ(column_group->num_chunks(), 1);
    EXPECT_EQ(column_group->NumRows(), 5);
    EXPECT_EQ(column_group->NumRows(), 5);

    // Get group chunk
    auto retrieved_group_chunk = column_group->GetGroupChunk(0);
    EXPECT_NE(retrieved_group_chunk.get(), nullptr);
    EXPECT_EQ(retrieved_group_chunk.get()->RowNums(), 5);

    // GetNumRowsUntilChunk
    EXPECT_EQ(column_group->GetNumRowsUntilChunk(0), 0);
    EXPECT_EQ(column_group->GetNumRowsUntilChunk(1), 5);

    // GetNumRowsUntilChunk vector
    const auto& rows_until_chunk = column_group->GetNumRowsUntilChunk();
    EXPECT_EQ(rows_until_chunk.size(), 2);
    EXPECT_EQ(rows_until_chunk[0], 0);
    EXPECT_EQ(rows_until_chunk[1], 5);

    // boundary conditions
    EXPECT_EQ(column_group->GetNumRowsUntilChunk(100),
              0);  // Out of range
}

TEST_F(ChunkedColumnGroupTest, ProxyChunkColumn) {
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    chunks[FieldId(1)] = int64_chunk;
    chunks[FieldId(2)] = string_chunk;
    auto group_chunk = std::make_unique<GroupChunk>(chunks);

    std::vector<std::unique_ptr<GroupChunk>> group_chunks;
    group_chunks.push_back(std::move(group_chunk));
    auto translator = std::make_unique<TestGroupChunkTranslator>(
        std::vector<int64_t>{5}, "test_key", std::move(group_chunks));
    auto column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));

    // Test int64 proxy
    auto proxy_int64 = std::make_shared<ProxyChunkColumn>(
        column_group, FieldId(1), int64_field_meta);
    EXPECT_EQ(proxy_int64->NumRows(), 5);
    EXPECT_EQ(proxy_int64->num_chunks(), 1);
    EXPECT_FALSE(proxy_int64->IsNullable());
    EXPECT_NE(proxy_int64->DataOfChunk(0).get(), nullptr);
    EXPECT_NE(proxy_int64->ValueAt(0), nullptr);
    EXPECT_TRUE(proxy_int64->IsValid(0));
    EXPECT_TRUE(proxy_int64->IsValid(0, 0));

    // Test string proxy
    auto proxy_string = std::make_shared<ProxyChunkColumn>(
        column_group, FieldId(2), string_field_meta);
    EXPECT_EQ(proxy_string->NumRows(), 5);
    EXPECT_EQ(proxy_string->num_chunks(), 1);
    EXPECT_FALSE(proxy_string->IsNullable());
}