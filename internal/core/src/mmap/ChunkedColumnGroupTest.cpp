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
#include <atomic>
#include <chrono>
#include <future>
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
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "storage/Event.h"
#include "storage/Util.h"
#include "common/ChunkWriter.h"
#include "common/FieldData.h"

using namespace milvus;
using namespace milvus::storage;

std::shared_ptr<Chunk>
create_chunk_int64(const FixedVector<int64_t>& data) {
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::INT64,
                                                       DataType::NONE);
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
    return create_chunk(field_meta, array_vec);
}

// Helper function to create chunks for string data
std::shared_ptr<Chunk>
create_chunk_string(const FixedVector<std::string>& data) {
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE);
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
    return create_chunk(field_meta, array_vec);
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
        int64_chunk = create_chunk_int64(int64_data);
        string_chunk = create_chunk_string(string_data);
    }

    std::unique_ptr<Translator<GroupChunk>>
    MakeGroupTranslator(const std::string& key) {
        std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
        chunks[FieldId(1)] = int64_chunk;
        chunks[FieldId(2)] = string_chunk;
        std::vector<std::unique_ptr<GroupChunk>> group_chunks;
        group_chunks.push_back(std::make_unique<GroupChunk>(chunks));
        return std::make_unique<TestGroupChunkTranslator>(
            2, std::vector<int64_t>{5}, key, std::move(group_chunks));
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

    EXPECT_EQ(group_chunk->RowNums(), 5);

    // Get chunk
    auto retrieved_int64_chunk = group_chunk->GetChunk(FieldId(1));
    auto retrieved_string_chunk = group_chunk->GetChunk(FieldId(2));
    EXPECT_EQ(retrieved_int64_chunk->RowNums(), 5);
    EXPECT_EQ(retrieved_string_chunk->RowNums(), 5);

    // Size
    uint64_t expected_size = int64_chunk->Size() + string_chunk->Size();
    EXPECT_EQ(group_chunk->Size(), expected_size);

    // Cell byte size
    uint64_t expected_cell_size = int64_chunk->CellByteSize().memory_bytes +
                                  string_chunk->CellByteSize().memory_bytes;
    EXPECT_EQ(group_chunk->CellByteSize().memory_bytes, expected_cell_size);

    // Test empty group chunk
    auto empty_group_chunk = std::make_unique<GroupChunk>();
    EXPECT_EQ(empty_group_chunk->RowNums(), 0);
    EXPECT_EQ(empty_group_chunk->Size(), 0);
    EXPECT_EQ(empty_group_chunk->CellByteSize().memory_bytes, 0);
}

TEST_F(ChunkedColumnGroupTest, ChunkedColumnGroup) {
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    chunks[FieldId(1)] = int64_chunk;
    chunks[FieldId(2)] = string_chunk;
    auto group_chunk = std::make_unique<GroupChunk>(chunks);

    std::vector<std::unique_ptr<GroupChunk>> group_chunks;
    group_chunks.push_back(std::move(group_chunk));
    auto translator = std::make_unique<TestGroupChunkTranslator>(
        2, std::vector<int64_t>{5}, "test_key", std::move(group_chunks));
    auto meta = static_cast<segcore::storagev2translator::GroupCTMeta*>(
        translator->meta());
    meta->chunk_memory_size_ = {128};
    auto column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));

    // basic properties
    EXPECT_EQ(column_group->num_chunks(), 1);
    EXPECT_EQ(column_group->NumRows(), 5);
    EXPECT_EQ(column_group->memory_size(), 128);

    // Get group chunk
    auto retrieved_group_chunk = column_group->GetGroupChunk(nullptr, 0);
    EXPECT_NE(retrieved_group_chunk.get(), nullptr);
    EXPECT_EQ(retrieved_group_chunk.get()->RowNums(), 5);
    EXPECT_EQ(column_group->memory_size(), 128);

    // GetNumRowsUntilChunk
    EXPECT_EQ(column_group->GetNumRowsUntilChunk(0), 0);
    EXPECT_EQ(column_group->GetNumRowsUntilChunk(1), 5);

    // GetNumRowsUntilChunk vector
    const auto& rows_until_chunk = column_group->GetNumRowsUntilChunk();
    EXPECT_EQ(rows_until_chunk.size(), 2);
    EXPECT_EQ(rows_until_chunk[0], 0);
    EXPECT_EQ(rows_until_chunk[1], 5);

    // boundary conditions
    EXPECT_THROW(column_group->GetNumRowsUntilChunk(100),
                 std::exception);  // Out of range
}

TEST_F(ChunkedColumnGroupTest, DeferredGroupKeepsStateProbesCold) {
    int factory_calls = 0;
    auto params = std::make_shared<int>(128);
    std::weak_ptr<int> captured_params = params;
    auto factory = [&, params = std::move(params)](OpContext*) {
        ++factory_calls;
        auto translator = MakeGroupTranslator("deferred-state-probes");
        auto meta = static_cast<segcore::storagev2translator::GroupCTMeta*>(
            translator->meta());
        meta->chunk_memory_size_ = {static_cast<size_t>(*params)};
        return translator;
    };
    {
        auto unused = std::make_shared<ChunkedColumnGroup>(5, 2, factory);
    }
    EXPECT_EQ(factory_calls, 0);

    auto group =
        std::make_shared<ChunkedColumnGroup>(5, 2, std::move(factory));
    auto column = std::make_shared<ProxyChunkColumn>(
        group, FieldId(1), int64_field_meta);
    int64_t offset = 0;
    EXPECT_TRUE(column->IsLazy());
    EXPECT_FALSE(column->IsMaterialized());
    EXPECT_EQ(column->NumRows(), 5);
    EXPECT_TRUE(column->IsInMultiFieldColumnGroup());
    EXPECT_FALSE(column->CellsLoaded(&offset, 1));
    EXPECT_TRUE(column->CellsLoaded(nullptr, 0));
    EXPECT_FALSE(group->CellsLoaded({0}));
    EXPECT_TRUE(group->CellsLoaded({}));
    group->CancelWarmup();
    group->ManualEvictCache();
    EXPECT_EQ(factory_calls, 0);

    EXPECT_FALSE(captured_params.expired());
    EXPECT_EQ(column->DataByteSize(), 128);
    EXPECT_TRUE(captured_params.expired());
    EXPECT_EQ(column->GetNumRowsUntilChunk(),
              (std::vector<int64_t>{0, 5}));
    EXPECT_TRUE(column->IsMaterialized());
    EXPECT_TRUE(column->IsLazy());
    EXPECT_EQ(column->num_chunks(), 1);
    EXPECT_FALSE(column->CellsLoaded(&offset, 1));
    EXPECT_EQ(factory_calls, 1);

    EXPECT_NE(column->DataOfChunk(nullptr, 0).get(), nullptr);
    EXPECT_TRUE(column->CellsLoaded(&offset, 1));
    EXPECT_EQ(column->DataByteSize(), 128);
    EXPECT_EQ(column->num_chunks(), 1);
    EXPECT_EQ(factory_calls, 1);
}

TEST_F(ChunkedColumnGroupTest, DeferredFieldsShareConcurrentInitialization) {
    std::atomic<int> factory_calls{0};
    std::promise<void> started;
    auto started_future = started.get_future();
    std::promise<void> release;
    auto release_future = release.get_future().share();
    auto group = std::make_shared<ChunkedColumnGroup>(
        5, 2, [&](OpContext*) {
            if (++factory_calls == 1) {
                started.set_value();
            }
            release_future.wait();
            return MakeGroupTranslator("deferred-concurrent");
        });
    auto first = std::make_shared<ProxyChunkColumn>(
        group, FieldId(1), int64_field_meta);
    auto second = std::make_shared<ProxyChunkColumn>(
        group, FieldId(2), string_field_meta);

    std::vector<std::future<std::pair<int64_t, int64_t>>> reads;
    for (int i = 0; i < 8; ++i) {
        reads.push_back(std::async(
            std::launch::async, [column = i % 2 == 0 ? first : second] {
                auto num_chunks = column->num_chunks();
                auto rows = column->GetChunk(nullptr, 0).get()->RowNums();
                return std::make_pair(num_chunks, rows);
            }));
    }
    auto status = started_future.wait_for(std::chrono::seconds(5));
    // Always release the builder before asserting or joining readers.
    release.set_value();
    EXPECT_EQ(status, std::future_status::ready);
    for (auto& read : reads) {
        auto [num_chunks, rows] = read.get();
        EXPECT_EQ(num_chunks, 1);
        EXPECT_EQ(rows, 5);
    }
    EXPECT_EQ(factory_calls.load(), 1);
    EXPECT_TRUE(first->IsMaterialized());
    EXPECT_TRUE(second->IsMaterialized());
}

TEST_F(ChunkedColumnGroupTest, DeferredInitializationFailureAllowsRetry) {
    int factory_calls = 0;
    auto params = std::make_shared<int>(5);
    std::weak_ptr<int> captured_params = params;
    auto group = std::make_shared<ChunkedColumnGroup>(
        5, 2, [&, params = std::move(params)](OpContext*) {
            EXPECT_EQ(*params, 5);
            if (++factory_calls == 1) {
                ThrowInfo(ErrorCode::Unsupported, "injected factory failure");
            }
            return MakeGroupTranslator("deferred-failure-retry");
        });
    try {
        (void)group->num_chunks();
        FAIL() << "expected factory failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::Unsupported);
    }
    EXPECT_FALSE(group->IsMaterialized());
    EXPECT_FALSE(captured_params.expired());
    EXPECT_EQ(factory_calls, 1);
    EXPECT_EQ(group->num_chunks(), 1);
    EXPECT_TRUE(group->IsMaterialized());
    EXPECT_TRUE(captured_params.expired());
    EXPECT_EQ(factory_calls, 2);
}

TEST_F(ChunkedColumnGroupTest, DeferredBulkReadChecksCancellationAfterLayout) {
    int factory_calls = 0;
    OpContext* observed_ctx = nullptr;
    auto group = std::make_shared<ChunkedColumnGroup>(
        5, 2, [&](OpContext* op_ctx) {
            observed_ctx = op_ctx;
            ++factory_calls;
            return MakeGroupTranslator("deferred-cancellation-retry");
        });
    auto column = std::make_shared<ProxyChunkColumn>(
        group, FieldId(1), int64_field_meta);
    int64_t offset = 0;
    int64_t value = -1;
    auto read = [&](OpContext* op_ctx) {
        column->BulkValueAt(
            op_ctx,
            [&](const char* data, size_t) {
                std::memcpy(&value, data, sizeof(value));
            },
            &offset,
            1);
    };
    folly::CancellationSource pre_cancelled;
    pre_cancelled.requestCancellation();
    OpContext pre_cancelled_ctx(pre_cancelled.getToken());
    try {
        read(&pre_cancelled_ctx);
        FAIL() << "expected cancelled data access";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    // Layout resolution has no request context. It can initialize the slot,
    // but cache loading must still honor cancellation before reading data.
    EXPECT_TRUE(column->IsMaterialized());
    EXPECT_EQ(observed_ctx, nullptr);
    EXPECT_FALSE(column->CellsLoaded(&offset, 1));
    EXPECT_EQ(value, -1);
    EXPECT_EQ(factory_calls, 1);

    OpContext fresh_ctx;
    read(&fresh_ctx);
    EXPECT_EQ(observed_ctx, nullptr);
    EXPECT_TRUE(column->IsMaterialized());
    EXPECT_TRUE(column->IsLazy());
    EXPECT_EQ(value, 1);
    EXPECT_EQ(factory_calls, 1);
}

TEST_F(ChunkedColumnGroupTest, ProxyChunkColumn) {
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    chunks[FieldId(1)] = int64_chunk;
    chunks[FieldId(2)] = string_chunk;
    auto group_chunk = std::make_unique<GroupChunk>(chunks);

    std::vector<std::unique_ptr<GroupChunk>> group_chunks;
    group_chunks.push_back(std::move(group_chunk));
    auto translator = std::make_unique<TestGroupChunkTranslator>(
        2, std::vector<int64_t>{5}, "test_key", std::move(group_chunks));
    auto column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));

    // Test int64 proxy
    auto proxy_int64 = std::make_shared<ProxyChunkColumn>(
        column_group, FieldId(1), int64_field_meta);
    EXPECT_EQ(proxy_int64->NumRows(), 5);
    EXPECT_EQ(proxy_int64->num_chunks(), 1);
    EXPECT_FALSE(proxy_int64->IsNullable());
    EXPECT_NE(proxy_int64->DataOfChunk(nullptr, 0).get(), nullptr);
    int64_t offset = 0;
    proxy_int64->BulkValueAt(
        nullptr,
        [&](const char* value, size_t size) { EXPECT_NE(value, nullptr); },
        &offset,
        1);
    proxy_int64->BulkIsValid(
        nullptr,
        [&](bool is_valid, size_t offset) { EXPECT_TRUE(is_valid); },
        &offset,
        1);

    // Regression: a non-nullable column must invoke the callback EXACTLY once
    // per row. Before the missing-return fix, the !nullable_ branch fell
    // through into the nullable branch and invoked the callback a second time
    // per row.
    {
        std::vector<int64_t> all_offsets = {0, 1, 2, 3, 4};
        std::unordered_map<size_t, int> call_count;
        proxy_int64->BulkIsValid(
            nullptr,
            [&](bool is_valid, size_t i) {
                EXPECT_TRUE(is_valid);
                call_count[i]++;
            },
            all_offsets.data(),
            static_cast<int64_t>(all_offsets.size()));
        ASSERT_EQ(call_count.size(), all_offsets.size());
        for (const auto& kv : call_count) {
            EXPECT_EQ(kv.second, 1)
                << "row " << kv.first << " callback invoked " << kv.second
                << " times";
        }
    }

    // Test string proxy
    auto proxy_string = std::make_shared<ProxyChunkColumn>(
        column_group, FieldId(2), string_field_meta);
    EXPECT_EQ(proxy_string->NumRows(), 5);
    EXPECT_EQ(proxy_string->num_chunks(), 1);
    EXPECT_FALSE(proxy_string->IsNullable());
}
