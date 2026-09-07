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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "folly/Executor.h"
#include "folly/coro/Task.h"
#include "milvus-storage/reader.h"
#include "pb/common.pb.h"

namespace milvus {
struct OpContext;
}

namespace milvus::segcore::storagev2translator {

// Describes one projected chunk reader requested from a storage Reader.
struct ChunkReaderOpenSpec {
    // Zero-based index in the Reader's column-group collection.
    int64_t column_group_index;
    // Per-request projection. Shared ownership matches the storage API and
    // keeps the projection alive until the asynchronous factory completes.
    std::shared_ptr<std::vector<std::string>> needed_columns;
};

// Configures scheduling for a batch of asynchronous reader opens.
struct AsyncChunkReaderOpenOptions {
    // Selects the priority queue used for reader factory work.
    milvus::proto::common::LoadPriority load_priority{
        milvus::proto::common::LoadPriority::HIGH};
    // Empty selects the process-wide async-load executor. The keep-alive is
    // captured when the lazy task is created.
    folly::Executor::KeepAlive<> executor{};
};

// Shared ownership lets a preopened reader move across worker boundaries.
using ChunkReaderPtr = std::shared_ptr<milvus_storage::api::ChunkReader>;

// Opens chunk readers with bounded concurrency per call on the async-load
// executor and returns them in request order. The first observed open failure
// cancels pending dispatches; issued storage opens are drained before returning.
// Caller/context cancellation takes precedence over that failure and is checked
// before each dispatch and after draining. The context token is captured at call
// time, and Arrow statuses retain their Segcore error classification.
// Exceptions from setup or awaited work preserve SegcoreError codes; allocation
// and Folly failures are classified, and untyped failures become UnexpectedError.
[[nodiscard]] folly::coro::Task<std::vector<ChunkReaderPtr>>
OpenChunkReadersAsync(const milvus::OpContext* ctx,
                      int64_t segment_id,
                      std::shared_ptr<milvus_storage::api::Reader> reader,
                      std::vector<ChunkReaderOpenSpec> specs,
                      AsyncChunkReaderOpenOptions options = {});

}  // namespace milvus::segcore::storagev2translator
