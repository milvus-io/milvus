// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/storagev2translator/AsyncChunkReader.h"

#include <cstddef>
#include <string_view>
#include <utility>

#include <fmt/format.h>

#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "folly/coro/Collect.h"
#include "folly/coro/WithCancellation.h"
#include "milvus-storage/common/extend_status.h"
#include "segcore/storagev2translator/AsyncLoadExecutor.h"

namespace milvus::segcore::storagev2translator {
namespace {

using ChunkReaderOpenResult =
    arrow::Result<std::unique_ptr<milvus_storage::api::ChunkReader>>;

// Throws FollyCancel when reader preparation observes cancellation.
void
CheckOpenCancellation(const folly::CancellationToken& cancellation_token,
                      const int64_t segment_id,
                      const std::string_view operation) {
    if (cancellation_token.isCancellationRequested()) {
        throw SegcoreError(
            ErrorCode::FollyCancel,
            fmt::format("{} cancelled for segment {}", operation, segment_id));
    }
}

// Rejects cancellation before invoking the storage factory. Once issued, the
// storage future is drained because its public API cannot interrupt the open.
[[nodiscard]] folly::coro::Task<ChunkReaderOpenResult>
OpenOneChunkReaderAsync(
    const int64_t segment_id,
    const std::shared_ptr<milvus_storage::api::Reader>& reader,
    ChunkReaderOpenSpec spec) {
    const auto& cancellation_token =
        co_await folly::coro::co_current_cancellation_token;
    CheckOpenCancellation(
        cancellation_token, segment_id, "AsyncChunkReader::open");
    co_return co_await reader->get_chunk_reader_async(spec.column_group_index,
                                                      spec.needed_columns);
}

// Propagates the merged cancellation token to each open, drains all issued
// opens on partial failure or cancellation, and restores request order.
[[nodiscard]] folly::coro::Task<std::vector<ChunkReaderPtr>>
OpenChunkReadersAsyncImpl(const int64_t segment_id,
                          std::shared_ptr<milvus_storage::api::Reader> reader,
                          std::vector<ChunkReaderOpenSpec> specs,
                          folly::CancellationToken context_cancellation_token) {
    const auto caller_cancellation_token =
        co_await folly::coro::co_current_cancellation_token;
    const auto cancellation_token = folly::cancellation_token_merge(
        std::move(context_cancellation_token), caller_cancellation_token);

    CheckOpenCancellation(
        cancellation_token, segment_id, "AsyncChunkReader::open");
    AssertInfo(reader != nullptr,
               "[StorageV3] async chunk reader open requires a reader for "
               "segment {}",
               segment_id);
    if (specs.empty()) {
        co_return std::vector<ChunkReaderPtr>{};
    }

    std::vector<folly::coro::Task<ChunkReaderOpenResult>> open_tasks;
    open_tasks.reserve(specs.size());
    std::vector<int64_t> column_group_indices;
    column_group_indices.reserve(specs.size());
    for (auto& spec : specs) {
        column_group_indices.push_back(spec.column_group_index);
        open_tasks.push_back(
            OpenOneChunkReaderAsync(segment_id, reader, std::move(spec)));
    }

    auto open_results = co_await folly::coro::co_withCancellation(
        cancellation_token,
        folly::coro::collectAllTryRange(std::move(open_tasks)));
    CheckOpenCancellation(
        cancellation_token, segment_id, "AsyncChunkReader::open");

    std::vector<ChunkReaderPtr> chunk_readers;
    chunk_readers.reserve(open_results.size());
    for (size_t i = 0; i < open_results.size(); ++i) {
        open_results[i].throwUnlessValue();
        auto chunk_reader_result = std::move(open_results[i]).value();
        if (!chunk_reader_result.ok()) {
            const auto error =
                milvus_storage::ToSegcoreError(chunk_reader_result.status());
            ThrowInfo(error.get_error_code(),
                      "async chunk reader open failed, segment {}, column "
                      "group index {}, request index {}, status: {}",
                      segment_id,
                      column_group_indices[i],
                      i,
                      error.what());
        }
        auto chunk_reader = std::move(chunk_reader_result).ValueOrDie();
        AssertInfo(chunk_reader != nullptr,
                   "[StorageV3] async chunk reader open returned null for "
                   "segment {}, column group index {}, request index {}",
                   segment_id,
                   column_group_indices[i],
                   i);
        chunk_readers.emplace_back(std::move(chunk_reader));
    }
    co_return chunk_readers;
}

// Adapts TaskWithExecutor back to the public Task return type while retaining
// the executor keep-alive for the complete reader-open operation.
[[nodiscard]] folly::coro::Task<std::vector<ChunkReaderPtr>>
OpenChunkReadersOnExecutor(
    folly::Executor::KeepAlive<> executor,
    folly::coro::Task<std::vector<ChunkReaderPtr>> task) {
    co_return co_await folly::coro::co_withExecutor(std::move(executor),
                                                    std::move(task));
}

}  // namespace

folly::coro::Task<std::vector<ChunkReaderPtr>>
OpenChunkReadersAsync(const milvus::OpContext* ctx,
                      const int64_t segment_id,
                      std::shared_ptr<milvus_storage::api::Reader> reader,
                      std::vector<ChunkReaderOpenSpec> specs,
                      AsyncChunkReaderOpenOptions options) {
    auto executor = ResolveAsyncLoadExecutor(std::move(options.executor),
                                             options.load_priority);
    const auto context_cancellation_token =
        ctx ? ctx->cancellation_token : folly::CancellationToken{};
    return OpenChunkReadersOnExecutor(
        std::move(executor),
        OpenChunkReadersAsyncImpl(segment_id,
                                  std::move(reader),
                                  std::move(specs),
                                  context_cancellation_token));
}

}  // namespace milvus::segcore::storagev2translator
