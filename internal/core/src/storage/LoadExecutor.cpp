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

#include "storage/LoadExecutor.h"

#include <algorithm>
#include <memory>

#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/ExecutorWithPriority.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "storage/ThreadPool.h"

namespace milvus::storage {
namespace {

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

PriorityThreadPoolExecutor&
LoadExecutorInstance() {
    static PriorityThreadPoolExecutor executor;
    return executor;
}

}  // namespace

folly::Executor*
GetLoadExecutor() {
    return &LoadExecutorInstance();
}

int8_t
LoadExecutorPriority(proto::common::LoadPriority priority) {
    return priority == proto::common::LoadPriority::LOW
               ? folly::Executor::LO_PRI
               : folly::Executor::HI_PRI;
}

folly::Executor::KeepAlive<>
GetLoadExecutorForPriority(proto::common::LoadPriority priority) {
    return folly::ExecutorWithPriority::create(
        folly::getKeepAliveToken(GetLoadExecutor()),
        LoadExecutorPriority(priority));
}

int64_t
GetLoadExecutorWorkerCount() {
    return static_cast<int64_t>(LoadExecutorInstance().numThreads());
}

}  // namespace milvus::storage
