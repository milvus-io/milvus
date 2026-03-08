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
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
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
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"

namespace milvus::segcore {

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
                            milvus::storage::GetReaderProperties());
                        AssertInfo(
                            result.ok(),
                            "[StorageV2] Failed to create row group reader: " +
                                result.status().ToString());
                        auto row_group_reader = result.ValueOrDie();
                        auto close_guard =
                            folly::makeGuard([&row_group_reader]() {
                                (void)row_group_reader->Close();
                            });
                        auto status =
                            row_group_reader->SetRowGroupOffsetAndCount(
                                block.offset, block.count);
                        AssertInfo(status.ok(),
                                   "[StorageV2] Failed to set row group offset "
                                   "and count " +
                                       std::to_string(block.offset) + " and " +
                                       std::to_string(block.count) +
                                       " with error " + status.ToString());
                        auto ret = std::make_shared<ArrowDataWrapper>();
                        for (int64_t i = 0; i < block.count; ++i) {
                            std::shared_ptr<arrow::Table> table;
                            auto status =
                                row_group_reader->ReadNextRowGroup(&table);
                            AssertInfo(status.ok(),
                                       "[StorageV2] Failed to read row group " +
                                           std::to_string(block.offset + i) +
                                           " from file " + file +
                                           " with error " + status.ToString());
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
                   milvus::proto::common::LoadPriority priority) {
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

    // Determine batch size based on parallel degree
    auto parallel_degree =
        static_cast<size_t>(memory_limit / FILE_SLICE_SIZE.load());
    size_t cells_per_batch = std::max<size_t>(
        1, (cell_specs.size() + parallel_degree - 1) / parallel_degree);

    // Group consecutive, same-key cells into batches for IO merging
    struct CellBatch {
        size_t file_idx;
        int64_t rg_offset;
        int64_t rg_count;
        std::vector<CellSpec> cells;
    };

    std::vector<CellBatch> batches;
    CellBatch current{};

    for (const auto& spec : cell_specs) {
        bool should_split = false;
        if (!current.cells.empty()) {
            if (spec.file_idx != current.file_idx ||
                spec.local_rg_offset != current.rg_offset + current.rg_count ||
                current.cells.size() >= cells_per_batch) {
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
        }
        current.rg_count += spec.rg_count;
        current.cells.push_back(spec);
    }
    if (!current.cells.empty()) {
        batches.push_back(std::move(current));
    }

    if (batches.empty()) {
        channel->close();
        return {};
    }

    auto& pool = ThreadPools::GetThreadPool(milvus::PriorityForLoad(priority));
    auto remaining = std::make_shared<std::atomic<size_t>>(batches.size());
    auto reader_memory_limit =
        std::max<int64_t>(memory_limit / static_cast<int64_t>(batches.size()),
                          FILE_SLICE_SIZE.load());
    auto shared_factory =
        std::make_shared<BatchReaderFactory>(std::move(reader_factory));

    std::vector<std::future<void>> futures;
    futures.reserve(batches.size());

    for (auto& batch : batches) {
        futures.emplace_back(pool.Submit([batch = std::move(batch),
                                          shared_factory,
                                          reader_memory_limit,
                                          channel,
                                          remaining,
                                          op_ctx]() {
            auto task_guard = folly::makeGuard([&channel, &remaining]() {
                if (remaining->fetch_sub(1) == 1) {
                    channel->close();
                }
            });
            CheckCancellation(op_ctx, -1, "LoadCellBatchAsync");

            auto tables_result = (*shared_factory)(batch.file_idx,
                                                   batch.rg_offset,
                                                   batch.rg_count,
                                                   reader_memory_limit);
            AssertInfo(tables_result.ok(),
                       "[StorageV2] Failed to read batch: " +
                           tables_result.status().ToString());
            auto all_tables = std::move(tables_result).ValueOrDie();
            AssertInfo(all_tables.size() == static_cast<size_t>(batch.rg_count),
                       "reader returns less tables than expected, batch rg "
                       "count: {}, result size: {}",
                       batch.rg_count,
                       all_tables.size());

            int64_t table_offset = 0;
            for (const auto& cell : batch.cells) {
                auto cell_result = std::make_shared<CellLoadResult>();
                cell_result->cid = cell.cid;
                cell_result->tables.reserve(cell.rg_count);
                for (int64_t i = 0; i < cell.rg_count; ++i) {
                    cell_result->tables.push_back(
                        std::move(all_tables[table_offset + i]));
                }
                table_offset += cell.rg_count;
                channel->push(std::move(cell_result));
            }
        }));
    }

    return futures;
}

BatchReaderFactory
MakeFileReaderFactory(std::vector<std::string> remote_files,
                      milvus_storage::ArrowFileSystemPtr fs) {
    auto files =
        std::make_shared<std::vector<std::string>>(std::move(remote_files));
    return [files, fs](size_t batch_key,
                       int64_t rg_offset,
                       int64_t total_rg_count,
                       int64_t reader_memory_limit)
               -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
        ARROW_ASSIGN_OR_RAISE(auto reader,
                              milvus_storage::FileRowGroupReader::Make(
                                  fs,
                                  (*files)[batch_key],
                                  nullptr,
                                  reader_memory_limit,
                                  milvus::storage::GetReaderProperties()));
        auto close_guard =
            folly::makeGuard([&reader]() { (void)reader->Close(); });
        ARROW_RETURN_NOT_OK(
            reader->SetRowGroupOffsetAndCount(rg_offset, total_rg_count));
        std::vector<std::shared_ptr<arrow::Table>> tables;
        tables.reserve(total_rg_count);
        for (int64_t i = 0; i < total_rg_count; ++i) {
            std::shared_ptr<arrow::Table> table;
            ARROW_RETURN_NOT_OK(reader->ReadNextRowGroup(&table));
            tables.push_back(std::move(table));
        }
        return tables;
    };
}

BatchReaderFactory
MakeChunkReaderFactory(
    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader) {
    return [chunk_reader](size_t /*batch_key*/,
                          int64_t rg_offset,
                          int64_t total_rg_count,
                          int64_t /*reader_memory_limit*/)
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
