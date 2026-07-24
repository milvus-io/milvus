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
#pragma once

#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cachinglayer/Utils.h"
#include "common/Channel.h"
#include "common/FieldData.h"
#include "common/GroupChunk.h"
#include "common/OpContext.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/reader.h"

namespace milvus::segcore {

struct RowGroupBlock {
    int64_t offset;  // Start offset of the row group block
    int64_t count;   // Number of row groups in this block

    bool
    operator==(const RowGroupBlock& other) const {
        return offset == other.offset && count == other.count;
    }
};

const std::size_t MAX_ROW_GROUP_BLOCK_MEMORY = 16 << 20;

// Strategy interface for row group splitting
class RowGroupSplitStrategy {
 public:
    virtual ~RowGroupSplitStrategy() = default;
    virtual std::vector<RowGroupBlock>
    split(const std::vector<int64_t>& input_row_groups) = 0;
};

// Memory-based splitting strategy
class MemoryBasedSplitStrategy : public RowGroupSplitStrategy {
 public:
    explicit MemoryBasedSplitStrategy(
        const milvus_storage::RowGroupMetadataVector& row_group_metadatas);
    std::vector<RowGroupBlock>
    split(const std::vector<int64_t>& input_row_groups) override;

 private:
    const milvus_storage::RowGroupMetadataVector& row_group_metadatas_;
};

// Parallel degree based splitting strategy
class ParallelDegreeSplitStrategy : public RowGroupSplitStrategy {
 public:
    explicit ParallelDegreeSplitStrategy(uint64_t parallel_degree);
    std::vector<RowGroupBlock>
    split(const std::vector<int64_t>& input_row_groups) override;

 private:
    uint64_t parallel_degree_;
};

/**
 * Load storage v2 files with specified strategy. The number of row group readers is determined by the strategy.
 *
 * @param remote_files: list of remote files
 * @param channel: channel to store the loaded data
 * @param memory_limit: memory limit for each chunk
 * @param strategy: strategy to split row groups
 * @param row_group_lists: list of row group lists
 * @param schema: schema of the data, if not provided, storage v2 will read all columns of the files.
 */
void
LoadWithStrategy(const std::vector<std::string>& remote_files,
                 std::shared_ptr<ArrowReaderChannel> channel,
                 int64_t memory_limit,
                 std::unique_ptr<RowGroupSplitStrategy> strategy,
                 const std::vector<std::vector<int64_t>>& row_group_lists,
                 const milvus_storage::ArrowFileSystemPtr& fs,
                 const std::shared_ptr<arrow::Schema> schema = nullptr,
                 milvus::proto::common::LoadPriority priority =
                     milvus::proto::common::LoadPriority::HIGH);

// ---- Cell-batch loading ----

// Load-time readers use one process-wide transient memory budget, so all
// translators backed by that budget must share one MCL overhead group.
constexpr const char* kLoadTransientOverheadGroup = "LoadTransientOverhead";

constexpr int64_t kDefaultFieldDataLoadBatchTargetBytes =
    DEFAULT_FIELD_MAX_MEMORY_LIMIT / 4;

constexpr int64_t kDefaultFieldDataReadWindowBytes =
    DEFAULT_INDEX_FILE_SLICE_SIZE;

int64_t
FieldDataLoadBatchTargetBytes();

int64_t
FieldDataLoadBatchSplitTargetBytes();

int64_t
FieldDataReadWindowBytes();

void
SetFieldDataLoadBatchTargetBytes(int64_t bytes);

void
SetFieldDataReadWindowBytes(int64_t bytes);

int64_t
LoadTransientPoolUpperBound(size_t max_task_overhead_bytes);

// Process-wide upper bound enforced by the configured load transient budget.
// A single oversized task can exceed the budget and therefore supplies the
// lower bound. Callers must not register a shared cap when the budget is zero.
int64_t
LoadTransientSharedOverheadUpperBound(size_t max_task_overhead_bytes);

milvus::cachinglayer::ResourceUsage
FieldDataLoadingOverheadUpperBound(
    int64_t max_memory_overhead,
    std::optional<int64_t> max_file_overhead = std::nullopt);

// A cell specification: identifies a cell's location within a specific file.
struct CellSpec {
    int64_t cid;              // cell id
    size_t file_idx;          // index into the remote_files list
    int64_t local_rg_offset;  // file-local row group start offset
    int64_t rg_count;         // number of row groups in this cell
    int64_t memory_size = 0;  // estimated final loaded memory in bytes
    int64_t loading_overhead_size =
        0;  // extra transient bytes during loading; 0 = memory_size
};

struct LoadedCell {
    int64_t cid;
    std::unique_ptr<milvus::GroupChunk> chunk;
};

using LoadedCellBatch = std::vector<LoadedCell>;
using CellLoadFuture = std::future<LoadedCellBatch>;

// Creates a batch reader for a range of contiguous row groups.
// Returns all row groups as a vector of tables in one call.
// batch_key: grouping key (file_idx for files, 0 for single reader)
// rg_offset: start row group index for this batch
// total_rg_count: total row groups across all cells in this batch
// reader_memory_limit: per-reader window size for this batch
using BatchReaderFactory =
    std::function<arrow::Result<std::vector<std::shared_ptr<arrow::Table>>>(
        size_t batch_key,
        int64_t rg_offset,
        int64_t total_rg_count,
        int64_t reader_memory_limit)>;

using CellFinalizeFunc = std::function<std::unique_ptr<milvus::GroupChunk>(
    const std::vector<std::shared_ptr<arrow::Table>>& tables, int64_t cid)>;

/**
 * Load cells in batches using a pluggable reader factory. Cells are sorted by
 * (file_idx, local_rg_offset) and grouped into IO-merged batches.
 * Each completed cell is finalized and returned in its batch future.
 * Batch admission waits on the global transient memory budget before
 * submitting work to the load pool. After finalize_cell returns, the batch task
 * releases transient Arrow tables before storing the result in the future.
 *
 * @param op_ctx operation context for cancellation
 * @param cell_specs cell specifications (sorted internally)
 * @param reader_factory factory that reads all row groups for a batch
 * @param memory_limit split target for per-batch loading overhead and
 * per-reader upper bound for final loaded memory. A global transient memory
 * budget gates concurrent batch loading across calls.
 * @param priority load priority
 * @param finalize_cell converts loaded Arrow tables into the final GroupChunk
 * before the cell is stored in the batch result
 * @return futures containing finalized cells for each batch
 */
std::vector<CellLoadFuture>
LoadCellBatchAsync(milvus::OpContext* op_ctx,
                   std::vector<CellSpec> cell_specs,
                   BatchReaderFactory reader_factory,
                   int64_t memory_limit,
                   milvus::proto::common::LoadPriority priority,
                   CellFinalizeFunc finalize_cell);

/**
 * Creates a BatchReaderFactory that reads from Parquet files via
 * FileRowGroupReader. This factory uses one reader for the requested contiguous
 * row group range and leaves row group internals to the storage/Arrow reader.
 * The returned factory owns a copy of remote_files, so the caller's vector
 * need not outlive the factory.
 */
BatchReaderFactory
MakeFileReaderFactory(std::vector<std::string> remote_files,
                      milvus_storage::ArrowFileSystemPtr fs);

/**
 * Creates a BatchReaderFactory that reads from a ChunkReader via batch
 * get_chunks(). Row groups are loaded in a single IO call for IO merging
 * and returned as a vector of tables. The factory captures the shared_ptr
 * by value, extending the ChunkReader's lifetime automatically.
 */
BatchReaderFactory
MakeChunkReaderFactory(
    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader);

}  // namespace milvus::segcore
