// Copyright (C) 2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <folly/ScopeGuard.h>

#include <future>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "common/OpContext.h"
#include "common/Types.h"
#include "futures/Executor.h"
#include "futures/Future.h"
#include "segcore/Utils.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"

namespace milvus::segcore {

// Run one segment on the caller and multiple segments on SEARCH.
// Finish all field reads before returning to the caller for merging.
template <typename SegmentFields, typename MaterializeSegment>
void
MaterializeOutputSegments(std::map<int32_t, SegmentFields>& segment_fields,
                          const folly::CancellationToken& cancel_token,
                          MaterializeSegment materialize_segment) {
    if (segment_fields.size() == 1) {
        milvus::futures::throwIfCancelled(cancel_token);
        materialize_segment(segment_fields.begin()->second);
        return;
    }
    std::vector<std::future<void>> futures;
    futures.reserve(segment_fields.size());
    auto futures_guard = folly::makeGuard([&] {
        // Finish accepted tasks before their offsets, callbacks or snapshots
        // can be destroyed, including errors and cancellation.
        storage::DrainFutures(futures);
    });
    auto* executor = milvus::futures::getSearchCPUExecutor();
    for (auto& entry : segment_fields) {
        milvus::futures::throwIfCancelled(cancel_token);
        auto* materialized = &entry.second;
        auto task =
            std::make_shared<std::packaged_task<void()>>([&, materialized] {
                milvus::futures::throwIfCancelled(cancel_token);
                materialize_segment(*materialized);
            });
        // Track the future before submission so an accepted task remains
        // covered by the guard even if submission throws.
        futures.emplace_back(task->get_future());
        executor->addWithPriority([task] { (*task)(); },
                                  milvus::futures::ExecutePriority::HIGH);
    }
    for (auto& future : futures) {
        future.get();
    }
}

struct FetchedOutputField {
    FieldId field_id;
    std::unique_ptr<DataArray> field_data;
    int64_t scanned_remote_bytes;
    int64_t scanned_total_bytes;
};

// Read one field on the caller and multiple fields on SEARCH_FANOUT.
// Each read returns its own data and IO counters.
template <typename FetchField>
std::vector<FetchedOutputField>
FetchOutputFields(const std::vector<FieldId>& field_ids,
                  int64_t segment_id,
                  milvus::OpContext* op_ctx,
                  FetchField fetch_field) {
    std::vector<FetchedOutputField> fetched_fields;
    fetched_fields.reserve(field_ids.size());
    if (field_ids.empty()) {
        return fetched_fields;
    }

    auto fetch_one =
        [segment_id, op_ctx, fetch_field](FieldId field_id) mutable {
            milvus::OpContext field_ctx;
            if (op_ctx != nullptr) {
                field_ctx.cancellation_token = op_ctx->cancellation_token;
                field_ctx.runtime_load_priority = op_ctx->runtime_load_priority;
                field_ctx.coload_fields = op_ctx->coload_fields;
                field_ctx.pinned_segment_state = op_ctx->pinned_segment_state;
                field_ctx.pinned_state_owner = op_ctx->pinned_state_owner;
                field_ctx.trace_context = op_ctx->trace_context;
                field_ctx.trace_span = op_ctx->trace_span;
            }
            CheckCancellation(
                &field_ctx, segment_id, field_id.get(), "FillTargetEntry");
            auto field_data = fetch_field(field_id, &field_ctx);
            return FetchedOutputField{
                field_id,
                std::move(field_data),
                field_ctx.storage_usage.scanned_cold_bytes.load(),
                field_ctx.storage_usage.scanned_total_bytes.load()};
        };
    if (field_ids.size() == 1) {
        fetched_fields.emplace_back(fetch_one(field_ids.front()));
        return fetched_fields;
    }

    std::vector<std::future<FetchedOutputField>> futures;
    futures.reserve(field_ids.size());
    auto futures_guard =
        folly::makeGuard([&] { storage::DrainFutures(futures); });
    auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::SEARCH_FANOUT);
    for (auto field_id : field_ids) {
        CheckCancellation(
            op_ctx, segment_id, field_id.get(), "FillTargetEntry");
        futures.emplace_back(pool.Submit(fetch_one, field_id));
    }

    for (auto& future : futures) {
        fetched_fields.emplace_back(future.get());
    }
    return fetched_fields;
}

}  // namespace milvus::segcore
