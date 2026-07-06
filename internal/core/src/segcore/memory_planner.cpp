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
#include <cxxabi.h>
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <exception>
#include <future>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "cachinglayer/LoadingOverheadTracker.h"
#include "common/Channel.h"
#include "common/Common.h"
#include "common/EasyAssert.h"
#include "common/protobuf_utils.h"
#include "folly/ScopeGuard.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "storage/KeyRetriever.h"
#include "storage/StatusToErrorCode.h"
#include "storage/EntryStreamUtils.h"
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"

namespace milvus::segcore {

namespace {

std::atomic<int64_t> FIELD_DATA_LOAD_BATCH_TARGET_BYTES(
    kDefaultFieldDataLoadBatchTargetBytes);
std::atomic<int64_t> FIELD_DATA_READ_WINDOW_BYTES(
    kDefaultFieldDataReadWindowBytes);
std::atomic<int64_t> FIELD_DATA_MAX_READ_PARALLELISM(
    kDefaultFieldDataMaxReadParallelism);

int64_t
PositiveBytes(int64_t bytes, int64_t fallback) {
    return bytes > 0 ? bytes : fallback;
}

}  // namespace

int64_t
FieldDataLoadBatchTargetBytes() {
    return PositiveBytes(FIELD_DATA_LOAD_BATCH_TARGET_BYTES.load(),
                         kDefaultFieldDataLoadBatchTargetBytes);
}

int64_t
FieldDataLoadBatchSplitTargetBytes() {
    auto target = FieldDataLoadBatchTargetBytes();
    auto budget_capacity =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget()
            .CapacityBytes();
    if (budget_capacity == 0) {
        return target;
    }
    return std::max<int64_t>(
        1, std::min<int64_t>(target, static_cast<int64_t>(budget_capacity)));
}

int64_t
FieldDataReadWindowBytes() {
    return PositiveBytes(FIELD_DATA_READ_WINDOW_BYTES.load(),
                         kDefaultFieldDataReadWindowBytes);
}

int64_t
FieldDataMaxReadParallelism() {
    return std::max<int64_t>(FIELD_DATA_MAX_READ_PARALLELISM.load(), 1);
}

void
SetFieldDataLoadBatchTargetBytes(int64_t bytes) {
    FIELD_DATA_LOAD_BATCH_TARGET_BYTES.store(
        PositiveBytes(bytes, kDefaultFieldDataLoadBatchTargetBytes));
    LOG_INFO("set field data load batch target bytes: {}",
             FIELD_DATA_LOAD_BATCH_TARGET_BYTES.load());
}

void
SetFieldDataReadWindowBytes(int64_t bytes) {
    FIELD_DATA_READ_WINDOW_BYTES.store(
        PositiveBytes(bytes, kDefaultFieldDataReadWindowBytes));
    LOG_INFO("set field data read window bytes: {}",
             FIELD_DATA_READ_WINDOW_BYTES.load());
}

void
SetFieldDataMaxReadParallelism(int64_t parallelism) {
    FIELD_DATA_MAX_READ_PARALLELISM.store(std::max<int64_t>(parallelism, 1));
    LOG_INFO("set field data max read parallelism: {}",
             FIELD_DATA_MAX_READ_PARALLELISM.load());
}

milvus::cachinglayer::ResourceUsage
FieldDataLoadingOverheadUpperBound(int64_t max_memory_overhead,
                                   std::optional<int64_t> max_file_overhead) {
    auto budget_capacity = static_cast<int64_t>(
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget()
            .CapacityBytes());
    if (budget_capacity == 0) {
        return milvus::cachinglayer::LoadingOverheadTracker::kUnlimited;
    }

    auto memory_ub = std::max<int64_t>(budget_capacity, max_memory_overhead);
    auto file_ub =
        max_file_overhead.has_value()
            ? std::max<int64_t>(budget_capacity, max_file_overhead.value())
            : int64_t{0};
    return {memory_ub, file_ub};
}

namespace {

size_t
CellLoadingBudgetBytes(const CellSpec& cell) {
    auto overhead_size = cell.loading_overhead_size > 0
                             ? cell.loading_overhead_size
                             : cell.memory_size;
    AssertInfo(overhead_size > 0,
               "[StorageV2] Cell loading overhead size must be positive, "
               "cid={}, got {}, memory_size={}",
               cell.cid,
               overhead_size,
               cell.memory_size);
    return static_cast<size_t>(overhead_size);
}

size_t
BatchLoadingBudgetBytes(const std::vector<CellSpec>& cells) {
    size_t total = 0;
    for (const auto& cell : cells) {
        auto bytes = CellLoadingBudgetBytes(cell);
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            return std::numeric_limits<size_t>::max();
        }
        total += bytes;
    }
    return total;
}

int64_t
BatchReaderMemoryLimit(int64_t batch_memory, int64_t memory_limit) {
    auto capped = std::min(batch_memory, memory_limit);
    return std::max<int64_t>(capped, FieldDataReadWindowBytes());
}

uint64_t
BatchReadParallelism(size_t batch_budget_bytes, int64_t total_rg_count) {
    if (total_rg_count <= 1) {
        return 1;
    }

    auto read_window = std::max<int64_t>(FieldDataReadWindowBytes(), 1);
    auto budget_capacity =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget()
            .CapacityBytes();
    auto bounded_budget = budget_capacity == 0
                              ? batch_budget_bytes
                              : std::min(batch_budget_bytes, budget_capacity);
    auto parallelism_by_budget =
        std::max<size_t>(1, bounded_budget / static_cast<size_t>(read_window));
    auto max_parallelism = FieldDataMaxReadParallelism();
    auto parallelism = std::min<size_t>(parallelism_by_budget,
                                        static_cast<size_t>(max_parallelism));
    return std::min<uint64_t>(static_cast<uint64_t>(parallelism),
                              static_cast<uint64_t>(total_rg_count));
}

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>>
ReadFileRowGroupBlock(const milvus_storage::ArrowFileSystemPtr& fs,
                      const std::string& file,
                      int64_t rg_offset,
                      int64_t rg_count,
                      int64_t reader_memory_limit) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          milvus_storage::FileRowGroupReader::Make(
                              fs,
                              file,
                              nullptr,
                              reader_memory_limit,
                              milvus::storage::GetReaderProperties(),
                              milvus::storage::GetArrowReaderProperties()));
    auto close_guard = folly::makeGuard([&reader]() { (void)reader->Close(); });
    ARROW_RETURN_NOT_OK(reader->SetRowGroupOffsetAndCount(rg_offset, rg_count));
    std::vector<std::shared_ptr<arrow::Table>> tables;
    tables.reserve(rg_count);
    for (int64_t i = 0; i < rg_count; ++i) {
        std::shared_ptr<arrow::Table> table;
        ARROW_RETURN_NOT_OK(reader->ReadNextRowGroup(&table));
        tables.push_back(std::move(table));
    }
    return tables;
}

}  // namespace

MemoryBasedSplitStrategy::MemoryBasedSplitStrategy(
    const milvus_storage::RowGroupMetadataVector& row_group_metadatas)
    : row_group_metadatas_(row_group_metadatas) {
}

ParallelDegreeSplitStrategy::ParallelDegreeSplitStrategy(
    uint64_t parallel_degree)
    : parallel_degree_(parallel_degree) {
}

std::vector<RowGroupBlock>
MemoryBasedSplitStrategy::split(const std::vector<int64_t>& input_row_groups) {
    std::vector<RowGroupBlock> blocks;
    if (input_row_groups.empty()) {
        return blocks;
    }

    std::vector<int64_t> sorted_row_groups = input_row_groups;
    std::sort(sorted_row_groups.begin(), sorted_row_groups.end());

    int64_t current_start = sorted_row_groups[0];
    int64_t current_count = 1;
    int64_t current_memory =
        row_group_metadatas_.Get(current_start).memory_size();

    for (size_t i = 1; i < sorted_row_groups.size(); ++i) {
        int64_t next_row_group = sorted_row_groups[i];
        int64_t next_memory =
            row_group_metadatas_.Get(next_row_group).memory_size();

        if (next_row_group == current_start + current_count &&
            current_memory + next_memory <= MAX_ROW_GROUP_BLOCK_MEMORY) {
            current_count++;
            current_memory += next_memory;
            continue;
        }

        blocks.push_back({current_start, current_count});
        current_start = next_row_group;
        current_count = 1;
        current_memory = next_memory;
    }

    if (current_count > 0) {
        blocks.push_back({current_start, current_count});
    }

    return blocks;
}

std::vector<RowGroupBlock>
ParallelDegreeSplitStrategy::split(
    const std::vector<int64_t>& input_row_groups) {
    std::vector<RowGroupBlock> blocks;
    if (input_row_groups.empty()) {
        return blocks;
    }

    std::vector<int64_t> sorted_row_groups = input_row_groups;
    std::sort(sorted_row_groups.begin(), sorted_row_groups.end());

    uint64_t actual_parallel_degree = std::min(
        parallel_degree_, static_cast<uint64_t>(sorted_row_groups.size()));
    if (actual_parallel_degree == 0) {
        return blocks;
    }

    // Helper function to create continuous blocks
    auto create_continuous_blocks = [&](size_t max_block_size = SIZE_MAX) {
        std::vector<RowGroupBlock> continuous_blocks;
        int64_t current_start = sorted_row_groups[0];
        int64_t current_count = 1;

        for (size_t i = 1; i < sorted_row_groups.size(); ++i) {
            int64_t next_row_group = sorted_row_groups[i];

            if (next_row_group == current_start + current_count &&
                current_count < max_block_size) {
                current_count++;
                continue;
            }
            continuous_blocks.push_back({current_start, current_count});
            current_start = next_row_group;
            current_count = 1;
        }

        if (current_count > 0) {
            continuous_blocks.push_back({current_start, current_count});
        }
        return continuous_blocks;
    };

    // If row group size is less than parallel degree, split non-continuous groups
    if (sorted_row_groups.size() <= actual_parallel_degree) {
        return create_continuous_blocks();
    }

    // Otherwise, split based on parallel degree
    size_t avg_block_size =
        (sorted_row_groups.size() + actual_parallel_degree - 1) /
        actual_parallel_degree;

    return create_continuous_blocks(avg_block_size);
}

void
LoadWithStrategy(const std::vector<std::string>& remote_files,
                 std::shared_ptr<ArrowReaderChannel> channel,
                 int64_t memory_limit,
                 std::unique_ptr<RowGroupSplitStrategy> strategy,
                 const std::vector<std::vector<int64_t>>& row_group_lists,
                 const milvus_storage::ArrowFileSystemPtr& fs,
                 const std::shared_ptr<arrow::Schema> schema,
                 milvus::proto::common::LoadPriority priority) {
    try {
        AssertInfo(remote_files.size() == row_group_lists.size(),
                   "[StorageV2] Number of remote files must match number of "
                   "row group lists");
        auto& pool =
            ThreadPools::GetThreadPool(milvus::PriorityForLoad(priority));

        for (size_t file_idx = 0; file_idx < remote_files.size(); ++file_idx) {
            const auto& file = remote_files[file_idx];
            const auto& row_groups = row_group_lists[file_idx];

            if (row_groups.empty()) {
                continue;
            }

            // Use provided strategy to split row groups
            auto blocks = strategy->split(row_groups);

            LOG_INFO("[StorageV2] split row groups into blocks: {} for file {}",
                     blocks.size(),
                     file);

            // Create and submit tasks for each block
            std::vector<std::future<std::shared_ptr<milvus::ArrowDataWrapper>>>
                futures;
            futures.reserve(blocks.size());

            auto reader_memory_limit = std::max<int64_t>(
                memory_limit / blocks.size(), FILE_SLICE_SIZE.load());

            for (const auto& block : blocks) {
                futures.emplace_back(pool.Submit(
                    [block, fs, file, file_idx, schema, reader_memory_limit]() {
                        AssertInfo(fs != nullptr,
                                   "[StorageV2] file system is nullptr");
                        auto result = milvus_storage::FileRowGroupReader::Make(
                            fs,
                            file,
                            schema,
                            reader_memory_limit,
                            milvus::storage::GetReaderProperties(),
                            milvus::storage::GetArrowReaderProperties());
                        if (!result.ok()) {
                            ThrowInfo(milvus::storage::ArrowStatusToErrorCode(
                                          result.status()),
                                      "[StorageV2] Failed to create row group "
                                      "reader: {}",
                                      result.status().ToString());
                        }
                        auto row_group_reader = result.ValueOrDie();
                        auto close_guard =
                            folly::makeGuard([&row_group_reader]() {
                                (void)row_group_reader->Close();
                            });
                        auto status =
                            row_group_reader->SetRowGroupOffsetAndCount(
                                block.offset, block.count);
                        if (!status.ok()) {
                            ThrowInfo(
                                milvus::storage::ArrowStatusToErrorCode(status),
                                "[StorageV2] Failed to set row group offset "
                                "and count {} and {} with error {}",
                                block.offset,
                                block.count,
                                status.ToString());
                        }
                        auto ret = std::make_shared<ArrowDataWrapper>();
                        for (int64_t i = 0; i < block.count; ++i) {
                            std::shared_ptr<arrow::Table> table;
                            auto status =
                                row_group_reader->ReadNextRowGroup(&table);
                            if (!status.ok()) {
                                ThrowInfo(
                                    milvus::storage::ArrowStatusToErrorCode(
                                        status),
                                    "[StorageV2] Failed to read row group "
                                    "{} from file {} with error {}",
                                    block.offset + i,
                                    file,
                                    status.ToString());
                            }
                            ret->arrow_tables.push_back(
                                {file_idx,
                                 static_cast<size_t>(block.offset + i),
                                 table});
                        }
                        return ret;
                    }));
            }

            for (auto& future : futures) {
                auto field_data = future.get();
                channel->push(field_data);
            }
        }

        channel->close();
    } catch (std::exception& e) {
        LOG_INFO("[StorageV2] failed to load data from remote: {}", e.what());
        channel->close();
        throw e;
    }
}

std::vector<std::future<void>>
LoadCellBatchAsync(milvus::OpContext* op_ctx,
                   std::vector<CellSpec> cell_specs,
                   BatchReaderFactory reader_factory,
                   std::shared_ptr<CellReaderChannel>& channel,
                   int64_t memory_limit,
                   milvus::proto::common::LoadPriority priority,
                   CellFinalizeFunc finalize_cell) {
    if (cell_specs.empty()) {
        channel->close();
        return {};
    }

    // Sort by (file_idx, local_rg_offset) for IO merging
    std::sort(cell_specs.begin(),
              cell_specs.end(),
              [](const CellSpec& a, const CellSpec& b) {
                  if (a.file_idx != b.file_idx) {
                      return a.file_idx < b.file_idx;
                  }
                  return a.local_rg_offset < b.local_rg_offset;
              });

    for (const auto& spec : cell_specs) {
        AssertInfo(spec.memory_size > 0,
                   "[StorageV2] CellSpec cid={} has memory_size={}, "
                   "callers must provide actual memory size",
                   spec.cid,
                   spec.memory_size);
    }

    // Group consecutive, same-key cells into batches for IO merging
    struct CellBatch {
        size_t file_idx;
        int64_t rg_offset;
        int64_t rg_count;
        int64_t batch_memory = 0;
        std::vector<CellSpec> cells;
    };

    std::vector<CellBatch> batches;
    CellBatch current{};

    for (const auto& spec : cell_specs) {
        bool should_split = false;
        if (!current.cells.empty()) {
            bool batch_full =
                (current.batch_memory + spec.memory_size > memory_limit);
            if (spec.file_idx != current.file_idx ||
                spec.local_rg_offset != current.rg_offset + current.rg_count ||
                batch_full) {
                should_split = true;
            }
        }
        if (should_split) {
            batches.push_back(std::move(current));
            current = {};
        }
        if (current.cells.empty()) {
            current.file_idx = spec.file_idx;
            current.rg_offset = spec.local_rg_offset;
            current.rg_count = 0;
            current.batch_memory = 0;
        }
        current.rg_count += spec.rg_count;
        current.batch_memory += spec.memory_size;
        current.cells.push_back(spec);
    }
    if (!current.cells.empty()) {
        batches.push_back(std::move(current));
    }

    if (batches.empty()) {
        channel->close();
        return {};
    }

    uint64_t max_read_parallelism = 1;
    for (const auto& batch : batches) {
        max_read_parallelism =
            std::max(max_read_parallelism,
                     BatchReadParallelism(BatchLoadingBudgetBytes(batch.cells),
                                          batch.rg_count));
    }

    LOG_INFO(
        "[StorageV2] LoadCellBatchAsync: {} cells -> {} batches "
        "(memory_limit={}MB, budget_capacity={}MB, read_window={}MB, "
        "max_read_parallelism={})",
        cell_specs.size(),
        batches.size(),
        memory_limit >> 20,
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget()
                .CapacityBytes() >>
            20,
        FieldDataReadWindowBytes() >> 20,
        max_read_parallelism);

    auto& pool = ThreadPools::GetThreadPool(milvus::PriorityForLoad(priority));
    auto remaining = std::make_shared<std::atomic<size_t>>(batches.size());
    auto shared_factory =
        std::make_shared<BatchReaderFactory>(std::move(reader_factory));
    auto shared_finalizer =
        std::make_shared<CellFinalizeFunc>(std::move(finalize_cell));

    std::vector<std::future<void>> futures;
    futures.reserve(batches.size());

    for (auto& batch : batches) {
        auto batch_budget_bytes = BatchLoadingBudgetBytes(batch.cells);
        auto read_parallelism =
            BatchReadParallelism(batch_budget_bytes, batch.rg_count);
        auto reader_memory_limit =
            BatchReaderMemoryLimit(batch.batch_memory, memory_limit);
        futures.emplace_back(pool.Submit([batch = std::move(batch),
                                          shared_factory,
                                          batch_budget_bytes,
                                          reader_memory_limit,
                                          read_parallelism,
                                          channel,
                                          remaining,
                                          shared_finalizer,
                                          op_ctx]() {
            auto task_guard = folly::makeGuard([&channel, &remaining]() {
                if (remaining->fetch_sub(1) == 1) {
                    channel->close();
                }
            });
            CheckCancellation(op_ctx, -1, "LoadCellBatchAsync");

            auto& budget = milvus::storage::TransientMemoryBudget::
                GetLoadTransientBudget();
            auto acquired = budget.AcquireUntil(batch_budget_bytes, [op_ctx]() {
                return op_ctx &&
                       op_ctx->cancellation_token.isCancellationRequested();
            });
            if (!acquired) {
                CheckCancellation(op_ctx, -1, "LoadCellBatchAsync");
                return;
            }
            size_t transferred_budget_bytes = 0;
            auto release_guard = folly::makeGuard(
                [&budget, batch_budget_bytes, &transferred_budget_bytes]() {
                    if (transferred_budget_bytes < batch_budget_bytes) {
                        budget.Release(batch_budget_bytes -
                                       transferred_budget_bytes);
                    }
                });
            CheckCancellation(op_ctx, -1, "LoadCellBatchAsync");

            auto tables_result = (*shared_factory)(batch.file_idx,
                                                   batch.rg_offset,
                                                   batch.rg_count,
                                                   reader_memory_limit,
                                                   read_parallelism);
            if (!tables_result.ok()) {
                ThrowInfo(milvus::storage::ArrowStatusToErrorCode(
                              tables_result.status()),
                          "[StorageV2] Failed to read batch: " +
                              tables_result.status().ToString());
            }
            auto all_tables = std::move(tables_result).ValueOrDie();
            AssertInfo(all_tables.size() == static_cast<size_t>(batch.rg_count),
                       "reader returns less tables than expected, batch rg "
                       "count: {}, result size: {}",
                       batch.rg_count,
                       all_tables.size());
            CheckCancellation(op_ctx, -1, "LoadCellBatchAsync");

            int64_t table_offset = 0;
            for (const auto& cell : batch.cells) {
                CheckCancellation(op_ctx, -1, "LoadCellBatchAsync");
                auto cell_result = std::make_shared<CellLoadResult>();
                cell_result->cid = cell.cid;
                auto cell_budget_bytes = CellLoadingBudgetBytes(cell);
                cell_result->budget_bytes = cell_budget_bytes;
                cell_result->tables.reserve(cell.rg_count);
                for (int64_t i = 0; i < cell.rg_count; ++i) {
                    cell_result->tables.push_back(
                        std::move(all_tables[table_offset + i]));
                }
                table_offset += cell.rg_count;
                if (*shared_finalizer) {
                    cell_result->chunk =
                        (*shared_finalizer)(cell_result->tables, cell.cid);
                    ReleaseCellLoadResultBudget(cell_result);
                    transferred_budget_bytes += cell_budget_bytes;
                    CheckCancellation(op_ctx, -1, "LoadCellBatchAsync");
                    channel->push(std::move(cell_result));
                    continue;
                }
                channel->push(std::move(cell_result));
                transferred_budget_bytes += cell_budget_bytes;
            }
        }));
    }

    return futures;
}

void
ReleaseCellLoadResultBudget(
    const std::shared_ptr<CellLoadResult>& cell_load_result) {
    if (cell_load_result == nullptr) {
        return;
    }
    cell_load_result->tables.clear();
    if (cell_load_result->budget_bytes == 0) {
        return;
    }
    milvus::storage::TransientMemoryBudget::GetLoadTransientBudget().Release(
        cell_load_result->budget_bytes);
    cell_load_result->budget_bytes = 0;
}

BatchReaderFactory
MakeFileReaderFactory(std::vector<std::string> remote_files,
                      milvus_storage::ArrowFileSystemPtr fs) {
    auto files =
        std::make_shared<std::vector<std::string>>(std::move(remote_files));
    return [files, fs](size_t batch_key,
                       int64_t rg_offset,
                       int64_t total_rg_count,
                       int64_t reader_memory_limit,
                       uint64_t /*read_parallelism*/)
               -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
        const auto& file = (*files)[batch_key];
        return ReadFileRowGroupBlock(
            fs, file, rg_offset, total_rg_count, reader_memory_limit);
    };
}

BatchReaderFactory
MakeChunkReaderFactory(
    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader) {
    return [chunk_reader](size_t /*batch_key*/,
                          int64_t rg_offset,
                          int64_t total_rg_count,
                          int64_t /*reader_memory_limit*/,
                          uint64_t /*read_parallelism*/)
               -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
        std::vector<int64_t> rg_indices(total_rg_count);
        std::iota(rg_indices.begin(), rg_indices.end(), rg_offset);
        ARROW_ASSIGN_OR_RAISE(
            auto batches,
            chunk_reader->get_chunks(rg_indices, /*parallelism=*/1));
        std::vector<std::shared_ptr<arrow::Table>> tables;
        tables.reserve(batches.size());
        for (auto& batch : batches) {
            ARROW_ASSIGN_OR_RAISE(
                auto table,
                arrow::Table::FromRecordBatches({std::move(batch)}));
            tables.push_back(std::move(table));
        }
        return tables;
    };
}

}  // namespace milvus::segcore
