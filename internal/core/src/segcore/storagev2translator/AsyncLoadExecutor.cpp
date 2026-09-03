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

#include "segcore/storagev2translator/AsyncLoadExecutor.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/ExecutorWithPriority.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "storage/ThreadPool.h"

namespace milvus::segcore::storagev2translator {
namespace {

// Owns the process-wide priority queues used by async-load CPU work.
class PriorityThreadPoolExecutor final : public folly::CPUThreadPoolExecutor {
 public:
    PriorityThreadPoolExecutor()
        : folly::CPUThreadPoolExecutor(
              std::max(1, milvus::CPU_NUM),
              folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(2),
              std::make_shared<folly::NamedThreadFactory>(
                  "MILVUS_ASYNC_LOAD_")) {
    }
};

// Maps load priority to the executor's two priority queues.
[[nodiscard]] constexpr int8_t
ExecutorPriority(const milvus::proto::common::LoadPriority priority) noexcept {
    return priority == milvus::proto::common::LoadPriority::LOW
               ? folly::Executor::LO_PRI
               : folly::Executor::HI_PRI;
}

// Returns the process-wide executor shared by async-load operations.
[[nodiscard]] folly::Executor&
GetDefaultAsyncLoadExecutor() {
    static PriorityThreadPoolExecutor executor;
    return executor;
}

}  // namespace

folly::Executor::KeepAlive<>
ResolveAsyncLoadExecutor(
    folly::Executor::KeepAlive<> executor,
    const milvus::proto::common::LoadPriority load_priority) {
    if (!executor) {
        executor = folly::getKeepAliveToken(GetDefaultAsyncLoadExecutor());
    }
    if (executor->getNumPriorities() <= 1) {
        return executor;
    }
    return folly::ExecutorWithPriority::create(std::move(executor),
                                               ExecutorPriority(load_priority));
}

}  // namespace milvus::segcore::storagev2translator
