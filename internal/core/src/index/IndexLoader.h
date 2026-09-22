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
#include "index/contracts/query/IIndexReaderBase.h"

namespace milvus {
class OpContext;
}  // namespace milvus

namespace milvus::index {

/**
 * @brief Common contract for an opened index that can create query readers.
 *
 * A concrete family's Open factory owns input validation and metadata parsing.
 * The returned loader owns that family's reusable state; this interface exposes
 * no storage-format properties or partially completed opening steps.
 * @note Load calls on one instance must be sequential. Keep the instance alive
 * until its task finishes. A returned reader owns its resources independently.
 */
class IndexLoader {
 public:
    virtual ~IndexLoader() = default;

    /**
     * @brief Materialize a new reader from the already-opened index.
     * @param context Borrowed for this call only; nullptr uses default priority
     * and inherited coroutine cancellation. Keep it alive through completion.
     * @return A fully initialized reader owning its resources.
     * @note On failure, issued I/O drains and temporary targets are cleaned up
     * before the exception propagates. A later call may retry.
     * @note A Task does not imply a thread hop. Synchronous transport executes
     * on the calling thread; asynchronous implementations preserve I/O
     * scheduling.
     */
    virtual folly::coro::Task<IIndexReaderBasePtr>
    Load(milvus::OpContext* context = nullptr) = 0;
};

}  // namespace milvus::index
