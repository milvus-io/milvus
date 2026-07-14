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
#include <limits>
#include <optional>

#include "arrow/api.h"
#include "common/EasyAssert.h"
#include "futures/Executor.h"
#include "milvus-storage/common/extend_status.h"
#include "segcore/Utils.h"
#include "storage/EntryStreamUtils.h"
#include "storage/ThreadPools.h"

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

class PriorityThreadPoolExecutor : public folly::Executor {
 public:
    void
    add(folly::Func func) override {
        Submit(milvus::ThreadPoolPriority::MIDDLE, std::move(func));
    }

    void
    addWithPriority(folly::Func func, int8_t priority) override {
        auto pool_priority = priority >= futures::ExecutePriority::LOW
                                 ? milvus::ThreadPoolPriority::LOW
                                 : milvus::ThreadPoolPriority::HIGH;
        Submit(pool_priority, std::move(func));
    }

    uint8_t
    getNumPriorities() const override {
        return 3;
    }

 private:
    static void
    Submit(milvus::ThreadPoolPriority priority, folly::Func func) {
        auto task = std::make_shared<folly::Func>(std::move(func));
        ThreadPools::GetThreadPool(priority).Submit(
            [task = std::move(task)]() mutable { (*task)(); });
    }
};

folly::Executor*
DefaultAsyncLoadExecutor() {
    static PriorityThreadPoolExecutor executor;
    return &executor;
}

size_t
LoadingBudgetBytes(const CellSpec& cell) {
    auto bytes = cell.loading_overhead_size > 0 ? cell.loading_overhead_size
                                                : cell.memory_size;
    AssertInfo(bytes > 0,
               "[StorageV2] async cell {} has invalid loading budget {}",
               cell.cid,
               bytes);
    return static_cast<size_t>(bytes);
}

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
               ? futures::ExecutePriority::LOW
               : futures::ExecutePriority::HIGH;
}

storage::TransientBudgetPriority
BudgetPriority(milvus::proto::common::LoadPriority priority) {
    return priority == milvus::proto::common::LoadPriority::LOW
               ? storage::TransientBudgetPriority::Low
               : storage::TransientBudgetPriority::High;
}

folly::CancellationToken
CancellationToken(milvus::OpContext* ctx) {
    return ctx ? ctx->cancellation_token : folly::CancellationToken{};
}

WindowLoadResult
FinalizeWindow(milvus::OpContext* ctx,
               int64_t segment_id,
               AsyncReadWindow window,
               CellFinalizeFunc& finalize_cell,
               arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
                   batches_result) {
    CheckCancellation(ctx, segment_id, "AsyncLoadPipeline::read");
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
        CheckCancellation(ctx, segment_id, "AsyncLoadPipeline::finalize");
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

}  // namespace

std::vector<AsyncReadWindow>
BuildAsyncReadWindows(const std::vector<CellSpec>& cells,
                      int64_t read_window_bytes) {
    AssertInfo(read_window_bytes > 0,
               "[StorageV2] async read window must be positive, got {}",
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

    auto flush = [&]() {
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
                cell.memory_size >
                read_window_bytes -
                    std::min(current_memory_bytes, read_window_bytes);
            split = cell.file_idx != current_file ||
                    cell.local_rg_offset != current_end || would_exceed;
        }
        if (split) {
            flush();
        }
        if (current.cells.empty()) {
            current_file = cell.file_idx;
        }

        current.cells.push_back(cell);
        current.request_indices.push_back(indexed.request_index);
        current.budget_bytes =
            SaturatingAdd(current.budget_bytes, LoadingBudgetBytes(cell));
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
    flush();
    return windows;
}

folly::SemiFuture<std::vector<AsyncCellResult>>
LoadCellsAsync(milvus::OpContext* ctx,
               std::vector<CellSpec> cells,
               std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
               CellFinalizeFunc finalize_cell,
               AsyncLoadPipelineOptions options) {
    CheckCancellation(ctx, options.segment_id, "AsyncLoadPipeline::admission");
    AssertInfo(chunk_reader != nullptr,
               "[StorageV2] async load requires a chunk reader");
    AssertInfo(static_cast<bool>(finalize_cell),
               "[StorageV2] async load requires a cell finalizer");

    if (cells.empty()) {
        return folly::makeSemiFuture(std::vector<AsyncCellResult>{});
    }
    for (const auto& cell : cells) {
        AssertInfo(cell.file_idx == 0,
                   "[StorageV2] manifest async load expects one logical chunk "
                   "reader, cell {} has file index {}",
                   cell.cid,
                   cell.file_idx);
    }

    auto read_window_bytes = options.read_window_bytes > 0
                                 ? options.read_window_bytes
                                 : FieldDataReadWindowBytes();
    auto windows = BuildAsyncReadWindows(cells, read_window_bytes);
    auto executor =
        options.executor ? options.executor : DefaultAsyncLoadExecutor();
    auto executor_keep_alive = folly::getKeepAliveToken(executor);
    auto executor_priority = ExecutorPriority(options.load_priority);
    auto budget_priority = BudgetPriority(options.load_priority);
    auto cancellation_token = CancellationToken(ctx);
    auto shared_finalizer =
        std::make_shared<CellFinalizeFunc>(std::move(finalize_cell));

    std::vector<folly::Future<WindowLoadResult>> futures;
    futures.reserve(windows.size());
    auto& budget = storage::TransientMemoryBudget::GetLoadTransientBudget();
    for (auto& window : windows) {
        auto acquire = budget.AcquireAsync(
            window.budget_bytes, budget_priority, cancellation_token);
        futures.push_back(
            std::move(acquire)
                .via(executor_keep_alive.copy(), executor_priority)
                .thenValue([ctx,
                            segment_id = options.segment_id,
                            chunk_reader,
                            window = std::move(window),
                            shared_finalizer,
                            executor_keep_alive = executor_keep_alive.copy(),
                            executor_priority](
                               storage::TransientBudgetLease lease) mutable {
                    CheckCancellation(
                        ctx, segment_id, "AsyncLoadPipeline::admission");
                    auto read_future = chunk_reader->get_chunks_async(
                        window.chunk_indices, /*parallelism=*/1);
                    return std::move(read_future)
                        .via(std::move(executor_keep_alive), executor_priority)
                        .thenValue(
                            [ctx,
                             segment_id,
                             window = std::move(window),
                             shared_finalizer,
                             lease = std::move(lease)](auto result) mutable {
                                (void)lease;
                                return FinalizeWindow(ctx,
                                                      segment_id,
                                                      std::move(window),
                                                      *shared_finalizer,
                                                      std::move(result));
                            });
                }));
    }

    auto request_count = cells.size();
    return folly::collectAll(std::move(futures))
        .via(std::move(executor_keep_alive), executor_priority)
        .thenValue([ctx, segment_id = options.segment_id, request_count](
                       std::vector<folly::Try<WindowLoadResult>> tries) {
            CheckCancellation(ctx, segment_id, "AsyncLoadPipeline::complete");
            std::vector<std::optional<AsyncCellResult>> ordered(request_count);
            for (auto& result : tries) {
                result.throwUnlessValue();
                for (auto& cell : result.value()) {
                    AssertInfo(cell.request_index < ordered.size(),
                               "[StorageV2] async result index {} is out of "
                               "range {}",
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
            return results;
        })
        .semi();
}

}  // namespace milvus::segcore::storagev2translator
