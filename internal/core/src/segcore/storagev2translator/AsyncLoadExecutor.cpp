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
#include <exception>
#include <memory>
#include <mutex>
#include <utility>

#include "common/EasyAssert.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/ExecutorWithPriority.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "storage/ThreadPool.h"

namespace milvus::segcore::storagev2translator {
namespace {

// Owns the process-wide priority queues used by async-load CPU work.
class PriorityThreadPoolExecutor final : public folly::CPUThreadPoolExecutor {
 public:
    explicit PriorityThreadPoolExecutor(const int threads)
        : folly::CPUThreadPoolExecutor(
              threads,
              folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(2),
              std::make_shared<folly::NamedThreadFactory>(
                  "MILVUS_ASYNC_LOAD_")) {
    }
};

// Keeps configuration lazy and serializes resizes separately from executor
// acquisition, so a finishing worker can acquire a keep-alive during shrink.
struct DefaultExecutorState {
    std::mutex configure_mutex;
    std::mutex executor_mutex;
    int initial_threads = std::max(
        1, std::min(milvus::CPU_NUM, DEFAULT_THREAD_POOL_MAX_THREADS_SIZE));
    std::unique_ptr<PriorityThreadPoolExecutor> executor;
};

DefaultExecutorState&
GetDefaultExecutorState() {
    static DefaultExecutorState state;
    return state;
}

// Maps load priority to the executor's two priority queues.
[[nodiscard]] constexpr int8_t
ExecutorPriority(const milvus::proto::common::LoadPriority priority) noexcept {
    return priority == milvus::proto::common::LoadPriority::LOW
               ? folly::Executor::LO_PRI
               : folly::Executor::HI_PRI;
}

// Returns the process-wide executor shared by async-load operations.
[[nodiscard]] folly::Executor::KeepAlive<>
GetDefaultAsyncLoadExecutor() {
    // The pool is never replaced. Static initialization synchronizes its first
    // publication without taking the configuration lock on every load window.
    static auto& executor = []() -> PriorityThreadPoolExecutor& {
        auto& state = GetDefaultExecutorState();
        std::lock_guard lock(state.executor_mutex);
        state.executor =
            std::make_unique<PriorityThreadPoolExecutor>(state.initial_threads);
        return *state.executor;
    }();
    return folly::getKeepAliveToken(&executor);
}

}  // namespace

void
SetAsyncLoadThreadPoolSize(const int threads) {
    if (threads <= 0) {
        ThrowInfo(ErrorCode::ConfigInvalid,
                  "Async load thread pool size must be positive, got {}",
                  threads);
    }
    auto& state = GetDefaultExecutorState();
    std::lock_guard configure_lock(state.configure_mutex);
    PriorityThreadPoolExecutor* executor;
    {
        std::lock_guard executor_lock(state.executor_mutex);
        if (!state.executor) {
            state.initial_threads = threads;
            return;
        }
        executor = state.executor.get();
    }
    const auto previous = executor->numThreads();
    if (previous == static_cast<size_t>(threads)) {
        return;
    }
    try {
        executor->setNumThreads(threads);
    } catch (...) {
        const auto resize_error = std::current_exception();
        try {
            executor->setNumThreads(previous);
        } catch (const std::exception& rollback_error) {
            LOG_ERROR("Failed to restore async load worker count to {}: {}",
                      previous,
                      rollback_error.what());
        }
        std::rethrow_exception(resize_error);
    }
}

int
GetAsyncLoadThreadPoolSize() {
    auto& state = GetDefaultExecutorState();
    std::lock_guard lock(state.executor_mutex);
    return state.executor ? static_cast<int>(state.executor->numThreads())
                          : state.initial_threads;
}

folly::Executor::KeepAlive<>
ResolveAsyncLoadExecutor(
    folly::Executor::KeepAlive<> executor,
    const milvus::proto::common::LoadPriority load_priority) {
    if (!executor) {
        executor = GetDefaultAsyncLoadExecutor();
    }
    if (executor->getNumPriorities() <= 1) {
        return executor;
    }
    return folly::ExecutorWithPriority::create(std::move(executor),
                                               ExecutorPriority(load_priority));
}

}  // namespace milvus::segcore::storagev2translator
