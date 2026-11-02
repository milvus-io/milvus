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

#include "folly/coro/Task.h"
#include "index/contracts/IIndex.h"

namespace milvus {
class OpContext;
}  // namespace milvus

namespace milvus::index {

struct IndexCacheConfig;

/**
 * @brief Family-internal contract for prepared state that can create index owners.
 *
 * A concrete family's Create entry owns family validation and metadata parsing.
 * The returned loader owns that family's reusable state; this interface
 * exposes no storage-format properties or partially completed source-opening
 * steps.
 * External loading consumers use LoaderEntry::Load instead of this interface.
 * @note Resident Load calls on one instance must be sequential. Keep the instance alive
 * until its task finishes. A returned index owns its resources independently.
 */
class IndexLoader {
 public:
    virtual ~IndexLoader() = default;

    /**
     * @brief Materialize a new index from the already-opened index.
     * @param context Borrowed for this call only; nullptr uses default priority
     * and inherited coroutine cancellation. Keep it alive through completion.
     * @param cache Optional storage cache policy; cached construction consumes
     * the prepared loader source, including on failure. Do not reuse it afterward.
     * @return An index owning resident storage or its cache and reload source.
     * @note On failure, issued I/O drains and temporary targets are cleaned up
     * before the exception propagates. Resident Load calls may retry.
     * @note A Task does not imply a thread hop. Synchronous transport executes
     * on the calling thread; asynchronous implementations preserve I/O
     * scheduling.
     */
    virtual folly::coro::Task<IndexPtr>
    Load(milvus::OpContext* context = nullptr,
         const IndexCacheConfig* cache = nullptr) = 0;
};

}  // namespace milvus::index
