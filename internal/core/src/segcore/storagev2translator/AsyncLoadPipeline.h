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
#include <memory>
#include <utility>
#include <vector>

#include "common/GroupChunk.h"
#include "common/OpContext.h"
#include "folly/Executor.h"
#include "folly/futures/Future.h"
#include "milvus-storage/reader.h"
#include "pb/common.pb.h"
#include "segcore/memory_planner.h"

namespace milvus::segcore::storagev2translator {

struct AsyncReadWindow {
    std::vector<CellSpec> cells;
    std::vector<size_t> request_indices;
    std::vector<int64_t> chunk_indices;
    size_t budget_bytes{0};
};

std::vector<AsyncReadWindow>
BuildAsyncReadWindows(const std::vector<CellSpec>& cells,
                      int64_t read_window_bytes);

struct AsyncLoadPipelineOptions {
    int64_t segment_id{-1};
    int64_t read_window_bytes{0};
    milvus::proto::common::LoadPriority load_priority{
        milvus::proto::common::LoadPriority::HIGH};
    folly::Executor* executor{nullptr};
};

using AsyncCellResult =
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::GroupChunk>>;

folly::SemiFuture<std::vector<AsyncCellResult>>
LoadCellsAsync(milvus::OpContext* ctx,
               std::vector<CellSpec> cells,
               std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
               CellFinalizeFunc finalize_cell,
               AsyncLoadPipelineOptions options = {});

}  // namespace milvus::segcore::storagev2translator
