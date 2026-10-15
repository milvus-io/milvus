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

#include <arrow/api.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <vector>

#include "storage/ChunkRangeReader.h"

namespace milvus::storage {

namespace {

class TestChunkReader : public milvus_storage::api::ChunkReader {
 public:
    std::vector<milvus_storage::api::ChunkInfo> chunk_infos;
    std::function<arrow::Status(const std::vector<int64_t>&)> read;

    size_t
    total_number_of_chunks() const override {
        return chunk_infos.size();
    }
    arrow::Result<std::vector<int64_t>>
    get_chunk_indices(const std::vector<int64_t>&) override {
        return arrow::Status::NotImplemented("Unused by range reads");
    }
    arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    get_chunk(int64_t) override {
        return arrow::Status::NotImplemented("Unused by range reads");
    }
    arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
    get_chunks(const std::vector<int64_t>& indices, size_t) override {
        ARROW_RETURN_NOT_OK(read(indices));
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (auto index : indices) {
            arrow::Int64Builder builder;
            ARROW_RETURN_NOT_OK(builder.Append(index));
            ARROW_ASSIGN_OR_RAISE(auto values, builder.Finish());
            batches.push_back(arrow::RecordBatch::Make(
                arrow::schema({arrow::field("row", arrow::int64())}),
                1,
                {values}));
        }
        return batches;
    }
    arrow::Result<std::vector<uint64_t>>
    get_chunk_estimated_size() override {
        return arrow::Status::NotImplemented(
            "Stored bytes, not decoded estimates, determine grouping");
    }
    arrow::Result<std::vector<std::vector<uint64_t>>>
    get_chunk_column_estimated_size() override {
        return arrow::Status::NotImplemented("Unused by range reads");
    }
    arrow::Result<std::vector<uint64_t>>
    get_chunk_rows() override {
        std::vector<uint64_t> rows;
        for (const auto& range : chunk_infos) {
            rows.push_back(range.number_of_rows);
        }
        return rows;
    }
    arrow::Result<std::vector<milvus_storage::api::ChunkInfo>>
    get_chunk_infos() override {
        return chunk_infos;
    }
    arrow::Status
    get_chunks_into(const std::vector<int64_t>& indices,
                    std::span<uint8_t>) override {
        return read(indices);
    }
};

}  // namespace

TEST(ChunkRangeReaderTest,
     GroupsStoredBytesWithoutSplittingChunksOrCrossingFiles) {
    TestChunkReader input;
    input.chunk_infos = {{.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 1,
                          .file_range = arrow::io::ReadRange{0, 3}},
                         {.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 2,
                          .file_range = arrow::io::ReadRange{3, 6}},
                         {.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 3,
                          .file_range = arrow::io::ReadRange{9, 20}},
                         {.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 4,
                          .file_range = arrow::io::ReadRange{29, 1}},
                         {.file_index = 1,
                          .number_of_rows = 1,
                          .global_row_end = 5,
                          .file_range = arrow::io::ReadRange{0, 3}},
                         {.file_index = 1,
                          .number_of_rows = 1,
                          .global_row_end = 6,
                          .file_range = arrow::io::ReadRange{3, 3}}};
    std::vector<std::vector<int64_t>> calls;
    input.read = [&](const auto& indices) {
        calls.push_back(indices);
        return arrow::Status::OK();
    };
    ChunkRangeReadOptions options;
    options.storage.concurrency = 1;
    options.storage.request_size = 8;
    options.destination.emplace();
    auto result = ChunkRangeReader::Create(input, options);
    ASSERT_TRUE(result.ok()) << result.status();
    auto reader = std::move(result).ValueOrDie();
    uint64_t rows = 0;
    for (;;) {
        auto range = reader->Next();
        ASSERT_TRUE(range.ok()) << range.status();
        if (!*range)
            break;
        EXPECT_EQ((*range)->row_offset, rows);
        rows += (*range)->row_count;
    }
    reader.reset();
    EXPECT_EQ(rows, 6);
    EXPECT_EQ(calls,
              (std::vector<std::vector<int64_t>>{{0}, {1}, {2}, {3}, {4, 5}}));
}

TEST(ChunkRangeReaderTest,
     ReplenishesBeforeSlowReadCompletesAndPreservesFailure) {
    TestChunkReader input;
    input.chunk_infos = {{.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 1,
                          .file_range = arrow::io::ReadRange{0, 8}},
                         {.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 2,
                          .file_range = arrow::io::ReadRange{8, 8}},
                         {.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 3,
                          .file_range = arrow::io::ReadRange{16, 8}}};
    std::promise<void> third_started;
    auto ready = third_started.get_future();
    std::atomic<int> active{0}, peak{0};
    input.read = [&](const auto& indices) {
        const int count = ++active;
        int observed = peak.load();
        while (count > observed &&
               !peak.compare_exchange_weak(observed, count)) {
        }
        arrow::Status status;
        if (indices.front() == 0) {
            if (ready.wait_for(std::chrono::seconds(5)) !=
                std::future_status::ready) {
                status = arrow::Status::Invalid(
                    "Read scheduling waited for a batch barrier");
            }
        } else if (indices.front() == 2) {
            third_started.set_value();
            status = arrow::Status::IOError("injected read failure");
        }
        --active;
        return status;
    };
    ChunkRangeReadOptions options;
    options.storage.concurrency = 2;
    options.storage.request_size = 8;
    options.destination.emplace();
    auto result = ChunkRangeReader::Create(input, options);
    ASSERT_TRUE(result.ok()) << result.status();
    auto reader = std::move(result).ValueOrDie();
    arrow::Status failure;
    for (;;) {
        auto range = reader->Next();
        if (!range.ok()) {
            failure = range.status();
            break;
        }
        if (!*range)
            break;
    }
    reader.reset();
    EXPECT_TRUE(failure.IsIOError()) << failure;
    EXPECT_EQ(failure.message(), "injected read failure");
    EXPECT_LE(peak.load(), 2);
    EXPECT_EQ(active.load(), 0);
}

TEST(ChunkRangeReaderTest, BatchConsumptionPreservesRowOrder) {
    TestChunkReader input;
    input.chunk_infos = {{.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 1,
                          .file_range = arrow::io::ReadRange{0, 8}},
                         {.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 2,
                          .file_range = arrow::io::ReadRange{8, 8}},
                         {.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 3,
                          .file_range = arrow::io::ReadRange{16, 8}}};
    std::promise<void> second_started;
    auto ready = second_started.get_future();
    input.read = [&](const auto& indices) {
        if (indices.front() == 0 && ready.wait_for(std::chrono::seconds(5)) !=
                                        std::future_status::ready) {
            return arrow::Status::Invalid("concurrent read did not start");
        }
        if (indices.front() == 1) {
            second_started.set_value();
        }
        return arrow::Status::OK();
    };
    ChunkRangeReadOptions options;
    options.storage.concurrency = 2;
    options.storage.request_size = 8;
    auto result = ChunkRangeReader::Create(input, options);
    ASSERT_TRUE(result.ok()) << result.status();
    auto reader = std::move(result).ValueOrDie();
    for (int64_t row = 0; row < 3; ++row) {
        auto batch = reader->NextBatch();
        ASSERT_TRUE(batch.ok()) << batch.status();
        ASSERT_NE(*batch, nullptr);
        EXPECT_EQ(
            std::static_pointer_cast<arrow::Int64Array>((*batch)->column(0))
                ->Value(0),
            row);
    }
    auto end = reader->NextBatch();
    ASSERT_TRUE(end.ok()) << end.status();
    EXPECT_EQ(*end, nullptr);
}

TEST(ChunkRangeReaderTest, DestructionJoinsUnconsumedReads) {
    TestChunkReader input;
    input.chunk_infos = {{.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 1,
                          .file_range = arrow::io::ReadRange{0, 8}},
                         {.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 2,
                          .file_range = arrow::io::ReadRange{8, 8}},
                         {.file_index = 0,
                          .number_of_rows = 1,
                          .global_row_end = 3,
                          .file_range = arrow::io::ReadRange{16, 8}}};
    std::promise<void> started;
    auto ready = started.get_future();
    std::atomic<size_t> calls{0};
    input.read = [&](const auto&) {
        if (calls.fetch_add(1) == 0) {
            started.set_value();
        }
        return arrow::Status::OK();
    };
    ChunkRangeReadOptions options;
    options.storage.concurrency = 1;
    options.storage.request_size = 8;
    auto result = ChunkRangeReader::Create(input, options);
    ASSERT_TRUE(result.ok()) << result.status();
    auto reader = std::move(result).ValueOrDie();
    ASSERT_EQ(ready.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    reader.reset();
    // The ordered window cannot advance without consumption. Destruction wakes
    // its waiting worker instead of requiring the caller to drain the input.
    EXPECT_EQ(calls.load(), 1);
}

}  // namespace milvus::storage
