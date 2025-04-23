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
#include <cstddef>
#include <algorithm>
#include "milvus-storage/common/metadata.h"
#include "segcore/memory_planner.h"
#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <arrow/record_batch.h>

#include <future>
#include <memory>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "milvus-storage/filesystem/fs.h"
#include "log/Log.h"
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

    // If row group size is less than parallel degree, split non-continuous groups
    if (sorted_row_groups.size() <= actual_parallel_degree) {
        int64_t current_start = sorted_row_groups[0];
        int64_t current_count = 1;

        for (size_t i = 1; i < sorted_row_groups.size(); ++i) {
            int64_t next_row_group = sorted_row_groups[i];

            if (next_row_group == current_start + current_count) {
                current_count++;
                continue;
            }
            blocks.push_back({current_start, current_count});
            current_start = next_row_group;
            current_count = 1;
        }

        if (current_count > 0) {
            blocks.push_back({current_start, current_count});
        }
        return blocks;
    }

    // Otherwise, split based on parallel degree
    size_t avg_block_size =
        (sorted_row_groups.size() + actual_parallel_degree - 1) /
        actual_parallel_degree;
    int64_t current_start = sorted_row_groups[0];
    int64_t current_count = 1;

    for (size_t i = 1; i < sorted_row_groups.size(); ++i) {
        int64_t next_row_group = sorted_row_groups[i];

        if (next_row_group == current_start + current_count &&
            current_count < avg_block_size) {
            current_count++;
        } else {
            blocks.push_back({current_start, current_count});
            current_start = next_row_group;
            current_count = 1;
        }
    }

    if (current_count > 0) {
        blocks.push_back({current_start, current_count});
    }

    return blocks;
}

void
LoadWithStrategy(const std::vector<std::string>& remote_files,
                 std::shared_ptr<ArrowReaderChannel> channel,
                 int64_t memory_limit,
                 std::unique_ptr<RowGroupSplitStrategy> strategy,
                 const std::vector<std::vector<int64_t>>& row_group_lists,
                 const std::shared_ptr<arrow::Schema> schema) {
    try {
        AssertInfo(
            remote_files.size() == row_group_lists.size(),
            "Number of remote files must match number of row group lists");
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();
        auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::HIGH);

        for (size_t file_idx = 0; file_idx < remote_files.size(); ++file_idx) {
            const auto& file = remote_files[file_idx];
            const auto& row_groups = row_group_lists[file_idx];

            if (row_groups.empty()) {
                continue;
            }

            // Use provided strategy to split row groups
            auto blocks = strategy->split(row_groups);

            // Create and submit tasks for each block
            std::vector<std::future<std::shared_ptr<milvus::ArrowDataWrapper>>>
                futures;
            futures.reserve(blocks.size());

            // split memory limit for each block, check if it's greater than 0
            auto reader_memory_limit = memory_limit / blocks.size();
            if (reader_memory_limit < FILE_SLICE_SIZE) {
                reader_memory_limit = FILE_SLICE_SIZE;
            }

            for (const auto& block : blocks) {
                futures.emplace_back(pool.Submit([block,
                                                  fs,
                                                  file,
                                                  schema,
                                                  memory_limit]() {
                    AssertInfo(fs != nullptr, "file system is nullptr");
                    auto row_group_reader =
                        std::make_shared<milvus_storage::FileRowGroupReader>(
                            fs, file, schema, memory_limit);
                    AssertInfo(row_group_reader != nullptr,
                               "row group reader is nullptr");
                    row_group_reader->SetRowGroupOffsetAndCount(block.offset,
                                                                block.count);
                    auto ret = std::make_shared<ArrowDataWrapper>();
                    for (int64_t i = 0; i < block.count; ++i) {
                        std::shared_ptr<arrow::Table> table;
                        auto status =
                            row_group_reader->ReadNextRowGroup(&table);
                        AssertInfo(status.ok(),
                                   "Failed to read row group " +
                                       std::to_string(block.offset + i) +
                                       " from file " + file + " with error " +
                                       status.ToString());
                        ret->arrow_tables.push_back(table);
                    }
                    auto close_status = row_group_reader->Close();
                    AssertInfo(close_status.ok(),
                               "Failed to close row group reader for file " +
                                   file + " with error " +
                                   close_status.ToString());
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
        LOG_INFO("failed to load data from remote: {}", e.what());
        channel->close();
    }
}

}  // namespace milvus::segcore
