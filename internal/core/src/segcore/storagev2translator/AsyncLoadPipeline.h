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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/GroupChunk.h"
#include "common/OpContext.h"
#include "folly/Executor.h"
#include "folly/coro/Task.h"
#include "milvus-storage/reader.h"
#include "pb/common.pb.h"
#include "segcore/memory_planner.h"

namespace milvus::segcore::storagev2translator {

struct AsyncLoadPipelineOptions {
    // Empty uses the process default. Explicit values must be positive bytes.
    std::optional<size_t> read_window_bytes;
    milvus::proto::common::LoadPriority load_priority{
        milvus::proto::common::LoadPriority::HIGH};
    // Empty uses the process default. A single-priority custom executor is
    // scheduled through add(); executors reporting multiple priorities must
    // implement addWithPriority(). Every custom executor must defer submitted
    // work because Folly Task does not support inline-like executors. A dummy
    // keep-alive token does not extend lifetime, so its executor must otherwise
    // outlive the returned task and all work started by it.
    folly::Executor::KeepAlive<> executor{};
    // Empty means finalization runs on executor. Mmap callers may provide a
    // dedicated non-inline local-file executor so Arrow-to-local
    // materialization and the blocking file operations run as one scheduled
    // task. The provider is called after remote reads complete so they do not
    // keep that executor alive while waiting on storage.
    std::function<folly::Executor::KeepAlive<>()>
        finalization_executor_provider{};
};

using AsyncCellResult =
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::GroupChunk>>;

// Lazy coroutine: admission, storage read, and finalization start when the task
// is awaited. segment_id is required for diagnostics. The executor keep-alive
// and ctx cancellation token are captured when this function is called.
[[nodiscard]] folly::coro::Task<std::vector<AsyncCellResult>>
LoadCellsAsync(const milvus::OpContext* ctx,
               int64_t segment_id,
               std::vector<CellSpec> cells,
               std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
               CellFinalizeFunc finalize_cell,
               AsyncLoadPipelineOptions options = {});

}  // namespace milvus::segcore::storagev2translator
