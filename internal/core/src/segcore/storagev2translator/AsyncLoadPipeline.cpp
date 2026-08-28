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
#include <chrono>
#include <exception>
#include <mutex>
#include <optional>
#include <string_view>
#include <utility>

#include "arrow/api.h"
#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "folly/coro/AsyncScope.h"
#include "folly/coro/WithCancellation.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/ExecutorWithPriority.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "log/Log.h"
#include "milvus-storage/common/extend_status.h"
#include "segcore/Utils.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/ThreadPool.h"
#include "storage/TransientMemoryBudget.h"

namespace milvus::segcore::storagev2translator {
namespace {

struct IndexedCell {
    CellSpec cell;
    size_t request_index;
};

struct AsyncReadWindow {
    std::vector<CellSpec> cells;
    std::vector<size_t> request_indices;
    std::vector<int64_t> chunk_indices;
    size_t budget_bytes{0};
};

struct WindowCellResult {
    size_t request_index;
    AsyncCellResult cell;
};

using WindowLoadResult = std::vector<WindowCellResult>;
using RecordBatches = std::vector<std::shared_ptr<arrow::RecordBatch>>;

// Groups contiguous cells into bounded remote-read windows while retaining
// each cell's original request index.
std::vector<AsyncReadWindow>
BuildAsyncReadWindows(const std::vector<CellSpec>& cells,
                      size_t read_window_bytes);

// Stores the first window failure and propagates cancellation to peer windows.
class WindowFailureState {
 public:
    [[nodiscard]] folly::CancellationToken
    GetCancellationToken() const {
        return cancellation_source_.getToken();
    }

    // Publishes the first exception before cancelling outstanding work.
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

    // Returns the first recorded exception, if any.
    [[nodiscard]] std::exception_ptr
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

// Maps load priority to the executor's two priority queues.
[[nodiscard]] constexpr int8_t
ExecutorPriority(milvus::proto::common::LoadPriority priority) noexcept {
    return priority == milvus::proto::common::LoadPriority::LOW
               ? folly::Executor::LO_PRI
               : folly::Executor::HI_PRI;
}

// Applies a priority wrapper only when the executor exposes multiple queues.
[[nodiscard]] folly::Executor::KeepAlive<>
WithExecutorPriority(folly::Executor::KeepAlive<> executor, int8_t priority) {
    if (executor->getNumPriorities() <= 1) {
        return executor;
    }
    return folly::ExecutorWithPriority::create(std::move(executor), priority);
}

// Maps load priority to the transient-memory admission class.
[[nodiscard]] constexpr storage::TransientBudgetPriority
BudgetPriority(milvus::proto::common::LoadPriority priority) noexcept {
    return priority == milvus::proto::common::LoadPriority::LOW
               ? storage::TransientBudgetPriority::Low
               : storage::TransientBudgetPriority::High;
}

// Throws FollyCancel when a pipeline phase observes cancellation.
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

// Converts a window's Arrow batches into cache cells in request order.
WindowLoadResult
FinalizeWindow(int64_t segment_id,
               const folly::CancellationToken& cancellation_token,
               const AsyncReadWindow& window,
               const CellFinalizeFunc& finalize_cell,
               RecordBatches batches) {
    CheckCancellationToken(
        cancellation_token, segment_id, "AsyncLoadPipeline::read");

    WindowLoadResult results;
    results.reserve(window.cells.size());
    size_t batch_offset = 0;
    for (size_t i = 0; i < window.cells.size(); ++i) {
        CheckCancellationToken(
            cancellation_token, segment_id, "AsyncLoadPipeline::finalize");
        const auto& cell = window.cells[i];
        const auto rg_count = static_cast<size_t>(cell.rg_count);
        if (rg_count > batches.size() - batch_offset) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "async chunk reader returned fewer batches than "
                      "requested");
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
                   "[StorageV3] async finalizer returned null for cell {}",
                   cell.cid);
        results.push_back({window.request_indices[i],
                           {static_cast<milvus::cachinglayer::cid_t>(cell.cid),
                            std::move(chunk)}});
    }
    return results;
}

// Runs finalization on an optional dedicated executor while retaining the
// transient-memory lease until all Arrow-backed buffers are released.
folly::coro::Task<WindowLoadResult>
FinalizeWindowAsync(int64_t segment_id,
                    folly::CancellationToken cancellation_token,
                    AsyncReadWindow window,
                    std::shared_ptr<const CellFinalizeFunc> finalize_cell,
                    storage::TransientBudgetLease lease,
                    RecordBatches batches,
                    std::shared_ptr<WindowFailureState> failure_state) {
    try {
        (void)lease;
        co_return FinalizeWindow(segment_id,
                                 cancellation_token,
                                 window,
                                 *finalize_cell,
                                 std::move(batches));
    } catch (...) {
        // This frame owns the moved lease during finalization. Publish the
        // failure before unwinding releases it.
        failure_state->RecordAndCancel(std::current_exception());
        throw;
    }
}

// Reads one window and writes its completed cells into its exclusive slot.
folly::coro::Task<void>
LoadWindowAsync(int64_t segment_id,
                std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
                AsyncReadWindow window,
                std::shared_ptr<const CellFinalizeFunc> finalize_cell,
                storage::TransientBudgetLease lease,
                std::function<folly::Executor::KeepAlive<>()>
                    finalization_executor_provider,
                int8_t executor_priority,
                folly::CancellationToken cancellation_token,
                std::shared_ptr<WindowFailureState> failure_state,
                std::optional<WindowLoadResult>& result_slot) {
    try {
        CheckCancellationToken(
            cancellation_token, segment_id, "AsyncLoadPipeline::admission");

        auto batches_result = co_await chunk_reader->get_chunks_async(
            window.chunk_indices, /*parallelism=*/1);
        CheckCancellationToken(
            cancellation_token, segment_id, "AsyncLoadPipeline::read");
        if (!batches_result.ok()) {
            throw milvus_storage::ToSegcoreError(batches_result.status());
        }
        auto batches = std::move(batches_result).ValueOrDie();
        if (batches.size() != window.chunk_indices.size()) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "async chunk reader returned an unexpected batch "
                      "count");
        }
        auto finalization_executor = finalization_executor_provider
                                         ? finalization_executor_provider()
                                         : folly::Executor::KeepAlive<>{};
        if (!finalization_executor) {
            (void)lease;
            result_slot = FinalizeWindow(segment_id,
                                         cancellation_token,
                                         window,
                                         *finalize_cell,
                                         std::move(batches));
            co_return;
        }

        result_slot = co_await folly::coro::co_withExecutor(
            WithExecutorPriority(std::move(finalization_executor),
                                 executor_priority),
            FinalizeWindowAsync(segment_id,
                                cancellation_token,
                                std::move(window),
                                std::move(finalize_cell),
                                std::move(lease),
                                std::move(batches),
                                failure_state));
    } catch (...) {
        // AsyncScope tasks must consume exceptions. Record the failure first
        // so any lease still owned by this frame is released after cancellation
        // becomes visible.
        failure_state->RecordAndCancel(std::current_exception());
    }
    co_return;
}

// Orchestrates admission and parallel window work, then restores request order.
folly::coro::Task<std::vector<AsyncCellResult>>
LoadCellsAsyncImpl(
    std::vector<CellSpec> cells,
    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
    CellFinalizeFunc finalize_cell,
    int64_t segment_id,
    std::optional<size_t> read_window_bytes,
    folly::Executor::KeepAlive<> executor_keep_alive,
    std::function<folly::Executor::KeepAlive<>()>
        finalization_executor_provider,
    int8_t executor_priority,
    storage::TransientBudgetPriority budget_priority,
    folly::CancellationToken context_cancellation_token) {
    const auto caller_cancellation_token =
        co_await folly::coro::co_current_cancellation_token;
    const auto cancellation_token = folly::cancellation_token_merge(
        std::move(context_cancellation_token), caller_cancellation_token);

    CheckCancellationToken(
        cancellation_token, segment_id, "AsyncLoadPipeline::admission");
    AssertInfo(chunk_reader != nullptr,
               "[StorageV3] async load requires a chunk reader");
    AssertInfo(static_cast<bool>(finalize_cell),
               "[StorageV3] async load requires a cell finalizer");

    if (cells.empty()) {
        co_return std::vector<AsyncCellResult>{};
    }
    for (const auto& cell : cells) {
        AssertInfo(cell.file_idx == 0,
                   "[StorageV3] manifest async load expects one logical chunk "
                   "reader, cell {} has file index {}",
                   cell.cid,
                   cell.file_idx);
    }

    const auto effective_read_window_bytes = read_window_bytes.value_or(
        static_cast<size_t>(StorageV2AsyncLoadReadWindowSizeBytes()));
    auto windows = BuildAsyncReadWindows(cells, effective_read_window_bytes);
    auto& budget = storage::TransientMemoryBudget::GetLoadTransientBudget();
    const auto* const priority_name =
        budget_priority == storage::TransientBudgetPriority::High ? "high"
                                                                  : "low";
    LOG_INFO(
        "[StorageV3] async load pipeline: segment {} loads {} cells in {} "
        "windows (read_window={}MB, budget_capacity={}MB, priority={})",
        segment_id,
        cells.size(),
        windows.size(),
        effective_read_window_bytes >> 20,
        budget.CapacityBytes() >> 20,
        priority_name);
    const std::shared_ptr<const CellFinalizeFunc> shared_finalizer =
        std::make_shared<CellFinalizeFunc>(std::move(finalize_cell));
    const auto work_executor =
        WithExecutorPriority(std::move(executor_keep_alive), executor_priority);
    const auto window_failure_state = std::make_shared<WindowFailureState>();
    const auto window_cancellation_token = folly::cancellation_token_merge(
        cancellation_token, window_failure_state->GetCancellationToken());
    const auto request_count = cells.size();
    // Slots have stable addresses and outlive every AsyncScope task. Each task
    // exclusively writes one slot.
    std::vector<std::optional<WindowLoadResult>> window_results(windows.size());
    folly::coro::AsyncScope scope;
    const bool debug_logging_enabled = VLOG_IS_ON(GLOG_DEBUG);
    try {
        for (size_t i = 0; i < windows.size(); ++i) {
            std::chrono::steady_clock::time_point admission_start;
            if (debug_logging_enabled) {
                admission_start = std::chrono::steady_clock::now();
                LOG_DEBUG(
                    "[StorageV3] async load segment {} waits for window {} "
                    "budget (budget_bytes={}, priority={})",
                    segment_id,
                    i,
                    windows[i].budget_bytes,
                    priority_name);
            }
            auto lease =
                co_await budget.AcquireAsync(windows[i].budget_bytes,
                                             budget_priority,
                                             window_cancellation_token);
            if (debug_logging_enabled) {
                const auto admission_wait_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - admission_start)
                        .count();
                LOG_DEBUG(
                    "[StorageV3] async load segment {} admits window {} "
                    "(budget_bytes={}, wait_us={})",
                    segment_id,
                    i,
                    windows[i].budget_bytes,
                    admission_wait_us);
            }
            CheckCancellationToken(window_cancellation_token,
                                   segment_id,
                                   "AsyncLoadPipeline::admission");
            scope.add(folly::coro::co_withCancellation(
                window_cancellation_token,
                folly::coro::co_withExecutor(
                    work_executor.copy(),
                    LoadWindowAsync(segment_id,
                                    chunk_reader,
                                    std::move(windows[i]),
                                    shared_finalizer,
                                    std::move(lease),
                                    finalization_executor_provider,
                                    executor_priority,
                                    window_cancellation_token,
                                    window_failure_state,
                                    window_results[i]))));
        }
    } catch (...) {
        window_failure_state->RecordAndCancel(std::current_exception());
    }

    co_await scope.joinAsync();
    CheckCancellationToken(
        cancellation_token, segment_id, "AsyncLoadPipeline::complete");
    if (const auto failure = window_failure_state->FirstFailure()) {
        std::rethrow_exception(failure);
    }
    std::vector<std::optional<AsyncCellResult>> ordered(request_count);
    for (auto& result : window_results) {
        AssertInfo(result.has_value(),
                   "[StorageV3] async window result is missing");
        for (auto& cell : *result) {
            AssertInfo(cell.request_index < ordered.size(),
                       "[StorageV3] async result index {} is out of range {}",
                       cell.request_index,
                       ordered.size());
            ordered[cell.request_index] = std::move(cell.cell);
        }
    }

    std::vector<AsyncCellResult> results;
    results.reserve(request_count);
    for (size_t i = 0; i < ordered.size(); ++i) {
        AssertInfo(ordered[i].has_value(),
                   "[StorageV3] async load result {} is missing",
                   i);
        results.push_back(std::move(*ordered[i]));
    }
    co_return results;
}

// Returns the process-lifetime executor used when no caller executor is set.
[[nodiscard]] folly::Executor&
GetAsyncLoadExecutor() {
    static PriorityThreadPoolExecutor executor;
    return executor;
}

std::vector<AsyncReadWindow>
BuildAsyncReadWindows(const std::vector<CellSpec>& cells,
                      size_t read_window_bytes) {
    AssertInfo(read_window_bytes > 0,
               "[StorageV3] async read window must be positive, got {}",
               read_window_bytes);
    if (cells.empty()) {
        return {};
    }

    std::vector<IndexedCell> indexed_cells;
    indexed_cells.reserve(cells.size());
    for (size_t i = 0; i < cells.size(); ++i) {
        AssertInfo(cells[i].memory_size > 0,
                   "[StorageV3] async cell {} has invalid memory size {}",
                   cells[i].cid,
                   cells[i].memory_size);
        AssertInfo(cells[i].rg_count > 0,
                   "[StorageV3] async cell {} has invalid row group count {}",
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
    size_t current_memory_bytes = 0;
    int64_t current_end = 0;
    size_t current_file = 0;

    const auto append_window = [&]() {
        if (!current.cells.empty()) {
            windows.push_back(std::move(current));
            current = {};
            current_memory_bytes = 0;
        }
    };

    for (const auto& indexed : indexed_cells) {
        const auto& cell = indexed.cell;
        const auto cell_memory_bytes = static_cast<size_t>(cell.memory_size);
        const bool would_exceed =
            !current.cells.empty() &&
            cell_memory_bytes >
                read_window_bytes -
                    std::min(current_memory_bytes, read_window_bytes);
        const bool should_split =
            !current.cells.empty() &&
            (cell.file_idx != current_file ||
             cell.local_rg_offset != current_end || would_exceed);
        if (should_split) {
            append_window();
        }
        if (current.cells.empty()) {
            current_file = cell.file_idx;
        }

        current.cells.push_back(cell);
        current.request_indices.push_back(indexed.request_index);
        const auto overhead_bytes = cell.loading_overhead_size > 0
                                        ? cell.loading_overhead_size
                                        : cell.memory_size;
        current.budget_bytes = SaturatingAdd(
            current.budget_bytes, static_cast<size_t>(overhead_bytes));
        for (int64_t i = 0; i < cell.rg_count; ++i) {
            current.chunk_indices.push_back(cell.local_rg_offset + i);
        }
        current_memory_bytes =
            SaturatingAdd(current_memory_bytes, cell_memory_bytes);
        current_end = cell.local_rg_offset + cell.rg_count;
    }
    append_window();
    return windows;
}

}  // namespace

folly::coro::Task<std::vector<AsyncCellResult>>
LoadCellsAsync(const milvus::OpContext* ctx,
               int64_t segment_id,
               std::vector<CellSpec> cells,
               std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
               CellFinalizeFunc finalize_cell,
               AsyncLoadPipelineOptions options) {
    auto executor_keep_alive = std::move(options.executor);
    if (!executor_keep_alive) {
        executor_keep_alive = folly::getKeepAliveToken(GetAsyncLoadExecutor());
    }
    auto finalization_executor_provider =
        std::move(options.finalization_executor_provider);
    const auto executor_priority = ExecutorPriority(options.load_priority);
    const auto budget_priority = BudgetPriority(options.load_priority);
    const auto context_cancellation_token =
        ctx ? ctx->cancellation_token : folly::CancellationToken{};

    return LoadCellsAsyncImpl(std::move(cells),
                              std::move(chunk_reader),
                              std::move(finalize_cell),
                              segment_id,
                              options.read_window_bytes,
                              std::move(executor_keep_alive),
                              std::move(finalization_executor_provider),
                              executor_priority,
                              budget_priority,
                              context_cancellation_token);
}

}  // namespace milvus::segcore::storagev2translator
