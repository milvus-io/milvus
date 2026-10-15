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

#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <vector>

#include "milvus-storage/reader.h"
#include "milvus-storage/thread_pool.h"
#include "storage/Types.h"

namespace milvus::storage {

struct ChunkRangeReadOptions {
    IndexBuildStorageConfig storage;
    std::optional<std::span<uint8_t>> destination;
    bool preserve_order{true};
};

struct ChunkRange {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    uint64_t row_offset{0};
    uint64_t row_count{0};
};

// Task-local read scheduling. The caller keeps the reader and destination alive;
// destruction joins all writers before returning ownership of the destination.
class ChunkRangeReader {
 public:
    static arrow::Result<std::unique_ptr<ChunkRangeReader>>
    Create(milvus_storage::api::ChunkReader& reader,
           ChunkRangeReadOptions options);

    ~ChunkRangeReader();

    arrow::Result<std::optional<ChunkRange>>
    Next();

    // Ordered batch consumers use the same request queue without retaining ranges.
    arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    NextBatch();

 private:
    struct Range {
        std::vector<int64_t> chunks;
        uint64_t row_offset{0};
        uint64_t row_count{0};
    };

    ChunkRangeReader(milvus_storage::api::ChunkReader& reader,
                     ChunkRangeReadOptions options);

    arrow::Status
    Plan();

    void
    Read();

    milvus_storage::api::ChunkReader& reader_;
    ChunkRangeReadOptions options_;
    std::shared_ptr<folly::ThreadPoolExecutor> pool_;
    std::vector<Range> ranges_;
    std::vector<std::optional<ChunkRange>> results_;
    std::vector<std::future<void>> workers_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::vector<size_t> completed_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    size_t batch_index_{0};
    size_t next_{0};
    size_t consumed_{0};
    bool stopped_{false};
    arrow::Status error_;
};

}  // namespace milvus::storage
