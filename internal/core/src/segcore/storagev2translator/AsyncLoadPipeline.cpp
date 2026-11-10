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
#include "log/Log.h"
#include "milvus-storage/common/extend_status.h"
#include "segcore/Utils.h"
#include "segcore/storagev2translator/AsyncLoadException.h"
#include "storage/AsyncLoadExecutor.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/ThreadPool.h"
#include "storage/LoadAdmissionController.h"

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

std::vector<AsyncReadWindow>
BuildAsyncReadWindows(const std::vector<CellSpec>& cells,
                      size_t read_window_bytes);

using RecordBatches = std::vector<std::shared_ptr<arrow::RecordBatch>>;

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

// Maps load priority to the load admission class.
[[nodiscard]] constexpr storage::LoadAdmissionPriority
AdmissionPriority(const milvus::proto::common::LoadPriority priority) noexcept {
    return priority == milvus::proto::common::LoadPriority::LOW
               ? storage::LoadAdmissionPriority::Low
               : storage::LoadAdmissionPriority::High;
}

// Throws FollyCancel when a pipeline phase observes cancellation.
void
CheckCancellationToken(const folly::CancellationToken& cancellation_token,
                       const int64_t segment_id,
                       const std::string_view operation) {
    if (cancellation_token.isCancellationRequested()) {
        throw SegcoreError(
            ErrorCode::FollyCancel,
            fmt::format("{} cancelled for segment {}", operation, segment_id));
    }
}

// Consumes each cell under the window lease, releasing its Arrow batches.
void
FinalizeWindow(const int64_t segment_id,
               const folly::CancellationToken& cancellation_token,
               const AsyncReadWindow& window,
               const CellReadCallback& consume_cell,
               RecordBatches batches) {
    CheckCancellationToken(
        cancellation_token, segment_id, "AsyncLoadPipeline::read");

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

        auto cell_batches = std::span(batches).subspan(batch_offset, rg_count);
        consume_cell(cell_batches, cell, window.request_indices[i]);
        for (auto& batch : cell_batches) {
            batch.reset();
        }
        batch_offset += rg_count;
    }
}

// Runs finalization on an optional dedicated executor while retaining the
// transient-memory lease until all Arrow-backed buffers are released.
folly::coro::Task<void>
FinalizeWindowAsync(const int64_t segment_id,
                    folly::CancellationToken cancellation_token,
                    AsyncReadWindow window,
                    std::shared_ptr<const CellReadCallback> consume_cell,
                    storage::LoadAdmissionLease lease,
                    RecordBatches batches,
                    std::shared_ptr<WindowFailureState> failure_state) {
    try {
        (void)lease;
        FinalizeWindow(segment_id,
                       cancellation_token,
                       window,
                       *consume_cell,
                       std::move(batches));
        co_return;
    } catch (...) {
        // This frame owns the moved lease during finalization. Publish the
        // failure before unwinding releases it.
        failure_state->RecordAndCancel(std::current_exception());
        throw;
    }
}

// Reads one window and invokes its consumer while retaining admission.
folly::coro::Task<void>
LoadWindowAsync(const int64_t segment_id,
                std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
                AsyncReadWindow window,
                std::shared_ptr<const CellReadCallback> consume_cell,
                storage::LoadAdmissionLease lease,
                std::function<folly::Executor::KeepAlive<>()>
                    finalization_executor_provider,
                const milvus::proto::common::LoadPriority load_priority,
                folly::CancellationToken cancellation_token,
                std::shared_ptr<WindowFailureState> failure_state) {
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
            FinalizeWindow(segment_id,
                           cancellation_token,
                           window,
                           *consume_cell,
                           std::move(batches));
            co_return;
        }

        co_await folly::coro::co_withExecutor(
            storage::ResolveAsyncLoadExecutor(std::move(finalization_executor),
                                              load_priority),
            FinalizeWindowAsync(segment_id,
                                cancellation_token,
                                std::move(window),
                                std::move(consume_cell),
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

// Orchestrates admission and parallel window work, then drains all tasks.
folly::coro::Task<void>
ReadCellsAsyncImpl(
    std::vector<CellSpec> cells,
    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
    CellReadCallback consume_cell,
    const int64_t segment_id,
    const std::optional<size_t> read_window_bytes,
    folly::Executor::KeepAlive<> work_executor,
    std::function<folly::Executor::KeepAlive<>()>
        finalization_executor_provider,
    const milvus::proto::common::LoadPriority load_priority,
    const storage::LoadAdmissionPriority admission_priority,
    folly::CancellationToken context_cancellation_token) {
    const auto caller_cancellation_token =
        co_await folly::coro::co_current_cancellation_token;
    const auto cancellation_token = folly::cancellation_token_merge(
        std::move(context_cancellation_token), caller_cancellation_token);

    CheckCancellationToken(
        cancellation_token, segment_id, "AsyncLoadPipeline::admission");
    AssertInfo(chunk_reader != nullptr,
               "[StorageV3] async load requires a chunk reader");
    AssertInfo(static_cast<bool>(consume_cell),
               "[StorageV3] async load requires a cell finalizer");

    if (cells.empty()) {
        co_return;
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
    auto& admission = storage::LoadAdmissionController::GetInstance();
    const auto* const priority_name =
        admission_priority == storage::LoadAdmissionPriority::High ? "high"
                                                                   : "low";
    LOG_INFO(
        "[StorageV3] async load pipeline: segment {} loads {} cells in {} "
        "windows (read_window={}MB, budget_capacity={}MB, slot_capacity={}, "
        "priority={})",
        segment_id,
        cells.size(),
        windows.size(),
        effective_read_window_bytes >> 20,
        admission.CapacityBytes() >> 20,
        admission.CapacitySlots(),
        priority_name);
    const auto shared_consumer =
        std::make_shared<CellReadCallback>(std::move(consume_cell));
    const auto window_failure_state = std::make_shared<WindowFailureState>();
    const auto window_cancellation_token = folly::cancellation_token_merge(
        cancellation_token, window_failure_state->GetCancellationToken());
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
                co_await admission.AcquireAsync({windows[i].budget_bytes, 1},
                                                admission_priority,
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
                                    shared_consumer,
                                    std::move(lease),
                                    finalization_executor_provider,
                                    load_priority,
                                    window_cancellation_token,
                                    window_failure_state))));
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
    co_return;
}

std::vector<AsyncReadWindow>
BuildAsyncReadWindows(const std::vector<CellSpec>& cells,
                      const size_t read_window_bytes) {
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

folly::coro::Task<void>
ReadCellsAsync(const milvus::OpContext* ctx,
               const int64_t segment_id,
               std::vector<CellSpec> cells,
               std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
               CellReadCallback consume_cell,
               AsyncLoadPipelineOptions options) {
    try {
        auto executor_keep_alive = storage::ResolveAsyncLoadExecutor(
            std::move(options.executor), options.load_priority);
        auto finalization_executor_provider =
            std::move(options.finalization_executor_provider);
        const auto admission_priority =
            AdmissionPriority(options.load_priority);
        const auto context_cancellation_token =
            ctx ? ctx->cancellation_token : folly::CancellationToken{};

        return detail::ClassifyAsyncLoadExceptions(
            ReadCellsAsyncImpl(std::move(cells),
                               std::move(chunk_reader),
                               std::move(consume_cell),
                               segment_id,
                               options.read_window_bytes,
                               std::move(executor_keep_alive),
                               std::move(finalization_executor_provider),
                               options.load_priority,
                               admission_priority,
                               context_cancellation_token));
    } catch (...) {
        detail::RethrowAsyncLoadException();
    }
}

namespace {
folly::coro::Task<std::vector<AsyncCellResult>>
CollectCellsAsync(folly::coro::Task<void> task,
                  std::shared_ptr<std::vector<AsyncCellResult>> results) {
    co_await std::move(task);
    co_return std::move(*results);
}
}  // namespace

folly::coro::Task<std::vector<AsyncCellResult>>
LoadCellsAsync(const milvus::OpContext* ctx,
               const int64_t segment_id,
               std::vector<CellSpec> cells,
               std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
               CellFinalizeFunc finalize_cell,
               AsyncLoadPipelineOptions options) {
    try {
        AssertInfo(static_cast<bool>(finalize_cell),
                   "[StorageV3] async load requires a cell finalizer");
        auto results =
            std::make_shared<std::vector<AsyncCellResult>>(cells.size());
        auto consume =
            [results, finalize_cell = std::move(finalize_cell)](
                std::span<std::shared_ptr<arrow::RecordBatch>> batches,
                const CellSpec& cell,
                size_t request_index) {
                std::vector<std::shared_ptr<arrow::Table>> tables;
                tables.reserve(batches.size());
                for (auto& batch : batches) {
                    auto table =
                        arrow::Table::FromRecordBatches({std::move(batch)});
                    if (!table.ok()) {
                        throw milvus_storage::ToSegcoreError(table.status());
                    }
                    tables.push_back(std::move(table).ValueOrDie());
                }
                auto chunk = finalize_cell(tables, cell.cid);
                AssertInfo(
                    chunk != nullptr,
                    "[StorageV3] async finalizer returned null for cell {}",
                    cell.cid);
                (*results)[request_index] = {
                    static_cast<milvus::cachinglayer::cid_t>(cell.cid),
                    std::move(chunk)};
            };
        auto task = ReadCellsAsync(ctx,
                                   segment_id,
                                   std::move(cells),
                                   std::move(chunk_reader),
                                   std::move(consume),
                                   std::move(options));
        return CollectCellsAsync(std::move(task), std::move(results));
    } catch (...) {
        detail::RethrowAsyncLoadException();
    }
}

}  // namespace milvus::segcore::storagev2translator
