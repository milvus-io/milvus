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
#include <string>
#include <vector>

#include "common/Channel.h"
#include "common/FieldData.h"
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

// A cell specification: identifies a cell's location within a specific file.
struct CellSpec {
    int64_t cid;              // cell id
    size_t file_idx;          // index into the remote_files list
    int64_t local_rg_offset;  // file-local row group start offset
    int64_t rg_count;         // number of row groups in this cell
};

// Result of loading a single cell: cid + the arrow tables read.
struct CellLoadResult {
    int64_t cid;
    std::vector<std::shared_ptr<arrow::Table>> tables;
};

using CellReaderChannel = milvus::Channel<std::shared_ptr<CellLoadResult>>;

// Creates a batch reader for a range of contiguous row groups.
// Returns all row groups as a vector of tables in one call.
// batch_key: grouping key (file_idx for files, 0 for single reader)
// rg_offset: start row group index for this batch
// total_rg_count: total row groups across all cells in this batch
// reader_memory_limit: memory budget for this batch's reader
using BatchReaderFactory =
    std::function<arrow::Result<std::vector<std::shared_ptr<arrow::Table>>>(
        size_t batch_key,
        int64_t rg_offset,
        int64_t total_rg_count,
        int64_t reader_memory_limit)>;

/**
 * Load cells in batches using a pluggable reader factory. Cells are sorted by
 * (file_idx, local_rg_offset) and grouped into IO-merged batches.
 * Each completed cell is pushed to the channel immediately, enabling
 * streaming consumption without accumulating all ArrowTables.
 *
 * @param op_ctx operation context for cancellation
 * @param cell_specs cell specifications (sorted internally)
 * @param reader_factory factory that reads all row groups for a batch
 * @param channel channel to receive loaded cell data; closed when all done
 * @param memory_limit total memory limit for readers
 * @param priority load priority
 * @return vector of futures for the batch loading tasks
 */
std::vector<std::future<void>>
LoadCellBatchAsync(milvus::OpContext* op_ctx,
                   std::vector<CellSpec> cell_specs,
                   BatchReaderFactory reader_factory,
                   std::shared_ptr<CellReaderChannel>& channel,
                   int64_t memory_limit,
                   milvus::proto::common::LoadPriority priority =
                       milvus::proto::common::LoadPriority::HIGH);

/**
 * Creates a BatchReaderFactory that reads from Parquet files via FileRowGroupReader.
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
