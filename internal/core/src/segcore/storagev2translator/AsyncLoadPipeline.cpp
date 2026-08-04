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

#include "segcore/storagev2translator/AsyncLoadPipeline.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <string_view>
#include <utility>

#include "arrow/api.h"
#include "common/EasyAssert.h"
#include "folly/ScopeGuard.h"
#include "folly/coro/Collect.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/ExecutorWithPriority.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "milvus-storage/common/extend_status.h"
#include "segcore/Utils.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/EntryStreamUtils.h"
#include "storage/ThreadPool.h"

namespace milvus::segcore::storagev2translator {
namespace {

struct IndexedCell {
    CellSpec cell;
    size_t request_index;
};

struct WindowCellResult {
    size_t request_index;
    AsyncCellResult cell;
};

using WindowLoadResult = std::vector<WindowCellResult>;
using ChunkReadResult =
    arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>;

class WindowFailureState {
 public:
    folly::CancellationToken
    GetToken() const {
        return cancellation_source_.getToken();
    }

    void
    RecordAndCancel(std::exception_ptr failure) {
        {
            std::lock_guard lock(mutex_);
            if (!first_failure_) {
                first_failure_ = std::move(failure);
            }
        }
        cancellation_source_.requestCancellation();
    }

    void
    RequestCancellation() {
        cancellation_source_.requestCancellation();
    }

    std::exception_ptr
    FirstFailure() const {
        std::lock_guard lock(mutex_);
        return first_failure_;
    }

 private:
    mutable std::mutex mutex_;
    std::exception_ptr first_failure_;
    folly::CancellationSource cancellation_source_;
};

class PriorityThreadPoolExecutor final : public folly::CPUThreadPoolExecutor {
 public:
    PriorityThreadPoolExecutor()
        : folly::CPUThreadPoolExecutor(
              std::max(1, milvus::CPU_NUM),
              folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(2),
              std::make_shared<folly::NamedThreadFactory>(
                  "MILVUS_ASYNC_LOAD_")) {
    }
};

size_t
SaturatingAdd(size_t left, size_t right) {
    if (right > std::numeric_limits<size_t>::max() - left) {
        return std::numeric_limits<size_t>::max();
    }
    return left + right;
}

int8_t
ExecutorPriority(milvus::proto::common::LoadPriority priority) {
    return priority == milvus::proto::common::LoadPriority::LOW
               ? folly::Executor::LO_PRI
               : folly::Executor::HI_PRI;
}

folly::Executor::KeepAlive<>
WithExecutorPriority(folly::Executor::KeepAlive<> executor, int8_t priority) {
    if (executor->getNumPriorities() <= 1) {
        return executor;
    }
    return folly::ExecutorWithPriority::create(std::move(executor), priority);
}

storage::TransientBudgetPriority
BudgetPriority(milvus::proto::common::LoadPriority priority) {
    return priority == milvus::proto::common::LoadPriority::LOW
               ? storage::TransientBudgetPriority::Low
               : storage::TransientBudgetPriority::High;
}

void
CheckCancellationToken(const folly::CancellationToken& cancellation_token,
                       int64_t segment_id,
                       std::string_view operation) {
    if (cancellation_token.isCancellationRequested()) {
        throw SegcoreError(
            ErrorCode::FollyCancel,
            fmt::format("{} cancelled for segment {}", operation, segment_id));
    }
}

WindowLoadResult
FinalizeWindow(int64_t segment_id,
               const folly::CancellationToken& cancellation_token,
               AsyncReadWindow window,
               CellFinalizeFunc& finalize_cell,
               ChunkReadResult batches_result) {
    CheckCancellationToken(
        cancellation_token, segment_id, "AsyncLoadPipeline::read");
    if (!batches_result.ok()) {
        throw milvus_storage::ToSegcoreError(batches_result.status());
    }

    auto batches = std::move(batches_result).ValueOrDie();
    if (batches.size() != window.chunk_indices.size()) {
        throw milvus_storage::ToSegcoreError(arrow::Status::Invalid(
            "async chunk reader returned an unexpected batch count"));
    }

    WindowLoadResult results;
    results.reserve(window.cells.size());
    size_t batch_offset = 0;
    for (size_t i = 0; i < window.cells.size(); ++i) {
        CheckCancellationToken(
            cancellation_token, segment_id, "AsyncLoadPipeline::finalize");
        const auto& cell = window.cells[i];
        auto rg_count = static_cast<size_t>(cell.rg_count);
        if (rg_count > batches.size() - batch_offset) {
            throw milvus_storage::ToSegcoreError(arrow::Status::Invalid(
                "async chunk reader returned fewer batches than requested"));
        }

        std::vector<std::shared_ptr<arrow::Table>> tables;
        tables.reserve(rg_count);
        for (size_t j = 0; j < rg_count; ++j) {
            auto table_result = arrow::Table::FromRecordBatches(
                {std::move(batches[batch_offset + j])});
            if (!table_result.ok()) {
                throw milvus_storage::ToSegcoreError(table_result.status());
            }
            tables.push_back(std::move(table_result).ValueOrDie());
        }
        batch_offset += rg_count;
        auto chunk = finalize_cell(tables, cell.cid);
        AssertInfo(chunk != nullptr,
                   "[StorageV2] async finalizer returned null for cell {}",
                   cell.cid);
        results.push_back({window.request_indices[i],
                           {static_cast<milvus::cachinglayer::cid_t>(cell.cid),
                            std::move(chunk)}});
    }
    return results;
}

folly::coro::Task<WindowLoadResult>
FinalizeWindowAsync(int64_t segment_id,
                    folly::CancellationToken cancellation_token,
                    AsyncReadWindow window,
                    std::shared_ptr<CellFinalizeFunc> finalize_cell,
                    storage::TransientBudgetLease lease,
                    ChunkReadResult batches_result,
                    std::shared_ptr<WindowFailureState> failure_state) {
    (void)lease;
    try {
        co_return FinalizeWindow(segment_id,
                                 cancellation_token,
                                 std::move(window),
                                 *finalize_cell,
                                 std::move(batches_result));
    } catch (...) {
        failure_state->RecordAndCancel(std::current_exception());
        throw;
    }
}

folly::coro::Task<WindowLoadResult>
LoadWindowAsync(int64_t segment_id,
                std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
                AsyncReadWindow window,
                std::shared_ptr<CellFinalizeFunc> finalize_cell,
                folly::coro::Future<storage::TransientBudgetLease> admission,
                std::function<folly::Executor::KeepAlive<>()>
                    finalization_executor_provider,
                int8_t executor_priority,
                folly::CancellationToken cancellation_token,
                std::shared_ptr<WindowFailureState> failure_state) {
    storage::TransientBudgetLease lease;
    try {
        lease = co_await std::move(admission);
        CheckCancellationToken(
            cancellation_token, segment_id, "AsyncLoadPipeline::admission");

        auto batches_result = co_await chunk_reader->get_chunks_async(
            window.chunk_indices, /*parallelism=*/1);
        auto finalization_executor = finalization_executor_provider
                                         ? finalization_executor_provider()
                                         : folly::Executor::KeepAlive<>{};
        if (!finalization_executor) {
            (void)lease;
            co_return FinalizeWindow(segment_id,
                                     cancellation_token,
                                     std::move(window),
                                     *finalize_cell,
                                     std::move(batches_result));
        }

        co_return co_await folly::coro::co_withExecutor(
            WithExecutorPriority(std::move(finalization_executor),
                                 executor_priority),
            FinalizeWindowAsync(segment_id,
                                cancellation_token,
                                std::move(window),
                                std::move(finalize_cell),
                                std::move(lease),
                                std::move(batches_result),
                                failure_state));
    } catch (...) {
        failure_state->RecordAndCancel(std::current_exception());
        throw;
    }
}

folly::coro::Task<std::vector<AsyncCellResult>>
LoadCellsAsyncImpl(
    std::vector<CellSpec> cells,
    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
    CellFinalizeFunc finalize_cell,
    int64_t segment_id,
    int64_t read_window_bytes,
    folly::Executor::KeepAlive<> executor_keep_alive,
    std::function<folly::Executor::KeepAlive<>()>
        finalization_executor_provider,
    int8_t executor_priority,
    storage::TransientBudgetPriority budget_priority,
    folly::CancellationToken context_cancellation_token) {
    auto caller_cancellation_token =
        co_await folly::coro::co_current_cancellation_token;
    auto cancellation_token = folly::cancellation_token_merge(
        std::move(context_cancellation_token), caller_cancellation_token);

    CheckCancellationToken(
        cancellation_token, segment_id, "AsyncLoadPipeline::admission");
    AssertInfo(chunk_reader != nullptr,
               "[StorageV2] async load requires a chunk reader");
    AssertInfo(static_cast<bool>(finalize_cell),
               "[StorageV2] async load requires a cell finalizer");

    if (cells.empty()) {
        co_return std::vector<AsyncCellResult>{};
    }
    for (const auto& cell : cells) {
        AssertInfo(cell.file_idx == 0,
                   "[StorageV2] manifest async load expects one logical chunk "
                   "reader, cell {} has file index {}",
                   cell.cid,
                   cell.file_idx);
    }

    if (read_window_bytes < 0) {
        read_window_bytes = StorageV2AsyncLoadReadWindowSizeBytes();
    }
    auto windows = BuildAsyncReadWindows(cells, read_window_bytes);
    auto shared_finalizer =
        std::make_shared<CellFinalizeFunc>(std::move(finalize_cell));
    auto work_executor =
        WithExecutorPriority(std::move(executor_keep_alive), executor_priority);
    auto& budget = storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto window_failure_state = std::make_shared<WindowFailureState>();
    auto window_cancellation_token = folly::cancellation_token_merge(
        cancellation_token, window_failure_state->GetToken());
    auto cancel_pending_admissions = folly::makeGuard([window_failure_state]() {
        window_failure_state->RequestCancellation();
    });

    std::vector<folly::coro::TaskWithExecutor<WindowLoadResult>> tasks;
    tasks.reserve(windows.size());
    for (auto& window : windows) {
        auto admission = budget.AcquireAsync(
            window.budget_bytes, budget_priority, window_cancellation_token);
        tasks.push_back(folly::coro::co_withExecutor(
            work_executor.copy(),
            LoadWindowAsync(segment_id,
                            chunk_reader,
                            std::move(window),
                            shared_finalizer,
                            std::move(admission),
                            finalization_executor_provider,
                            executor_priority,
                            window_cancellation_token,
                            window_failure_state)));
    }

    auto request_count = cells.size();
    auto tries = co_await folly::coro::collectAllTryRange(std::move(tasks));
    CheckCancellationToken(
        cancellation_token, segment_id, "AsyncLoadPipeline::complete");
    if (auto failure = window_failure_state->FirstFailure()) {
        std::rethrow_exception(failure);
    }
    cancel_pending_admissions.dismiss();
    std::vector<std::optional<AsyncCellResult>> ordered(request_count);
    for (auto& result : tries) {
        result.throwUnlessValue();
        for (auto& cell : result.value()) {
            AssertInfo(cell.request_index < ordered.size(),
                       "[StorageV2] async result index {} is out of range {}",
                       cell.request_index,
                       ordered.size());
            ordered[cell.request_index] = std::move(cell.cell);
        }
    }

    std::vector<AsyncCellResult> results;
    results.reserve(request_count);
    for (size_t i = 0; i < ordered.size(); ++i) {
        AssertInfo(ordered[i].has_value(),
                   "[StorageV2] async load result {} is missing",
                   i);
        results.push_back(std::move(*ordered[i]));
    }
    co_return results;
}

}  // namespace

folly::Executor*
GetAsyncLoadExecutor() {
    static PriorityThreadPoolExecutor executor;
    return &executor;
}

std::vector<AsyncReadWindow>
BuildAsyncReadWindows(const std::vector<CellSpec>& cells,
                      int64_t read_window_bytes) {
    AssertInfo(read_window_bytes >= 0,
               "[StorageV2] async read window must be non-negative, got {}",
               read_window_bytes);
    if (cells.empty()) {
        return {};
    }

    std::vector<IndexedCell> indexed_cells;
    indexed_cells.reserve(cells.size());
    for (size_t i = 0; i < cells.size(); ++i) {
        AssertInfo(cells[i].memory_size > 0,
                   "[StorageV2] async cell {} has invalid memory size {}",
                   cells[i].cid,
                   cells[i].memory_size);
        AssertInfo(cells[i].rg_count > 0,
                   "[StorageV2] async cell {} has invalid row group count {}",
                   cells[i].cid,
                   cells[i].rg_count);
        indexed_cells.push_back({cells[i], i});
    }
    std::stable_sort(indexed_cells.begin(),
                     indexed_cells.end(),
                     [](const IndexedCell& left, const IndexedCell& right) {
                         if (left.cell.file_idx != right.cell.file_idx) {
                             return left.cell.file_idx < right.cell.file_idx;
                         }
                         return left.cell.local_rg_offset <
                                right.cell.local_rg_offset;
                     });

    std::vector<AsyncReadWindow> windows;
    AsyncReadWindow current;
    int64_t current_memory_bytes = 0;
    int64_t current_end = 0;
    size_t current_file = 0;

    auto append_window = [&]() {
        if (!current.cells.empty()) {
            windows.push_back(std::move(current));
            current = {};
            current_memory_bytes = 0;
        }
    };

    for (const auto& indexed : indexed_cells) {
        const auto& cell = indexed.cell;
        bool split = false;
        if (!current.cells.empty()) {
            auto would_exceed =
                read_window_bytes > 0 &&
                cell.memory_size >
                    read_window_bytes -
                        std::min(current_memory_bytes, read_window_bytes);
            split = cell.file_idx != current_file ||
                    cell.local_rg_offset != current_end || would_exceed;
        }
        if (split) {
            append_window();
        }
        if (current.cells.empty()) {
            current_file = cell.file_idx;
        }

        current.cells.push_back(cell);
        current.request_indices.push_back(indexed.request_index);
        auto overhead_bytes = cell.loading_overhead_size > 0
                                  ? cell.loading_overhead_size
                                  : cell.memory_size;
        current.budget_bytes = SaturatingAdd(
            current.budget_bytes, static_cast<size_t>(overhead_bytes));
        for (int64_t i = 0; i < cell.rg_count; ++i) {
            current.chunk_indices.push_back(cell.local_rg_offset + i);
        }
        current_memory_bytes =
            cell.memory_size >
                    std::numeric_limits<int64_t>::max() - current_memory_bytes
                ? std::numeric_limits<int64_t>::max()
                : current_memory_bytes + cell.memory_size;
        current_end = cell.local_rg_offset + cell.rg_count;
    }
    append_window();
    return windows;
}

folly::coro::Task<std::vector<AsyncCellResult>>
LoadCellsAsync(milvus::OpContext* ctx,
               std::vector<CellSpec> cells,
               std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
               CellFinalizeFunc finalize_cell,
               AsyncLoadPipelineOptions options) {
    auto executor =
        options.executor ? options.executor : GetAsyncLoadExecutor();
    auto executor_keep_alive = folly::getKeepAliveToken(executor);
    auto finalization_executor_provider =
        std::move(options.finalization_executor_provider);
    auto executor_priority = ExecutorPriority(options.load_priority);
    auto budget_priority = BudgetPriority(options.load_priority);
    auto context_cancellation_token =
        ctx ? ctx->cancellation_token : folly::CancellationToken{};

    return LoadCellsAsyncImpl(std::move(cells),
                              std::move(chunk_reader),
                              std::move(finalize_cell),
                              options.segment_id,
                              options.read_window_bytes,
                              std::move(executor_keep_alive),
                              std::move(finalization_executor_provider),
                              executor_priority,
                              budget_priority,
                              std::move(context_cancellation_token));
}

}  // namespace milvus::segcore::storagev2translator
