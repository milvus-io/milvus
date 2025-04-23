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

#include <vector>
#include <cstdint>
#include <cstddef>
#include "milvus-storage/common/metadata.h"
#include <arrow/record_batch.h>
#include <vector>
#include "common/FieldData.h"

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

/*
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
                 const std::shared_ptr<arrow::Schema> schema = nullptr);

}  // namespace milvus::segcore