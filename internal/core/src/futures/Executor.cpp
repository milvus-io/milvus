// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>

#include "Executor.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/FunctionScheduler.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "monitor/Monitor.h"
#include "storage/ThreadPools.h"

namespace milvus::futures {

const int kNumPriority = 3;
const int kJsonStatsBuildNumPriority = 1;
const size_t kJsonStatsBuildMinThreads = 1;

namespace {

class SearchFanoutTaskMetrics final
    : public folly::ThreadPoolExecutor::TaskObserver {
 public:
    void
    taskEnqueued(const folly::ThreadPoolExecutor::TaskInfo&) noexcept override {
        monitor::internal_storage_pool_task_submitted_total_search_fanout
            .Increment();
    }

    void
    taskDequeued(const folly::ThreadPoolExecutor::DequeuedTaskInfo&
                     task) noexcept override {
        monitor::internal_storage_pool_queue_duration_seconds_search_fanout
            .Observe(std::chrono::duration<double>(task.waitTime).count());
    }

    void
    taskProcessed(const folly::ThreadPoolExecutor::ProcessedTaskInfo&
                      task) noexcept override {
        monitor::internal_storage_pool_execute_duration_seconds_search_fanout
            .Observe(std::chrono::duration<double>(task.runTime).count());
        monitor::internal_storage_pool_task_completed_total_search_fanout
            .Increment();
    }
};

}  // namespace

folly::CPUThreadPoolExecutor*
getSearchFanoutCPUExecutor() {
    static auto executor = [] {
        auto drain_and_delete = [](folly::CPUThreadPoolExecutor* executor) {
            executor->join();
            delete executor;
        };
        const auto threads = static_cast<size_t>(
            std::max(1, static_cast<int>(std::round(milvus::CPU_NUM * 2.0f))));
        std::unique_ptr<folly::CPUThreadPoolExecutor,
                        decltype(drain_and_delete)>
            executor(new folly::CPUThreadPoolExecutor(
                         std::pair<size_t, size_t>{threads, 1},
                         std::make_shared<folly::NamedThreadFactory>(
                             "SRCH_FAN_POOL")),
                     drain_and_delete);
        executor->setThreadDeathTimeout(std::chrono::seconds(2));
        executor->addTaskObserver(std::make_unique<SearchFanoutTaskMetrics>());
        monitor::internal_storage_pool_capacity_search_fanout.Set(threads);
        return executor;
    }();
    // Stop sampling before draining and destroying the executor.
    static auto metrics_sampler = [] {
        auto sampler = std::make_unique<folly::FunctionScheduler>();
        sampler->addFunction(
            [pool = executor.get()] {
                const auto stats = pool->getPoolStats();
                monitor::internal_storage_pool_active_threads_search_fanout.Set(
                    stats.activeThreadCount);
                monitor::internal_storage_pool_idle_threads_search_fanout.Set(
                    stats.idleThreadCount);
                monitor::internal_storage_pool_queue_depth_search_fanout.Set(
                    stats.pendingTaskCount);
            },
            std::chrono::seconds(2),
            "search_fanout_metrics",
            std::chrono::seconds(2));
        sampler->start();
        return sampler;
    }();
    return executor.get();
}

void
setSearchFanoutThreadCoreCoefficient(float coefficient) {
    if (coefficient <= 0) {
        LOG_ERROR("Failed to resize search fanout pool, coefficient:{}",
                  coefficient);
        return;
    }
    // Serialize resizes without locking pool acquisition.
    static std::mutex resize_mutex;
    std::lock_guard lock(resize_mutex);
    const auto threads = std::max(
        1, static_cast<int>(std::round(milvus::CPU_NUM * coefficient)));
    getSearchFanoutCPUExecutor()->setNumThreads(threads);
    monitor::internal_storage_pool_capacity_search_fanout.Set(threads);
    LOG_INFO("set search fanout thread pool core coefficient: {}", coefficient);
}

folly::CPUThreadPoolExecutor*
getSearchCPUExecutor() {
    auto thread_num = std::max(1, milvus::CPU_NUM);
    static folly::CPUThreadPoolExecutor executor(
        thread_num,
        folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(kNumPriority),
        std::make_shared<folly::NamedThreadFactory>("MILVUS_SEARCH_"));
    return &executor;
}

folly::CPUThreadPoolExecutor*
getLoadCPUExecutor() {
    auto thread_num = std::max(1, milvus::CPU_NUM);
    static folly::CPUThreadPoolExecutor executor(
        thread_num,
        folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(kNumPriority),
        std::make_shared<folly::NamedThreadFactory>("MILVUS_LOAD_"));
    return &executor;
}

folly::CPUThreadPoolExecutor*
getJsonStatsBuildExecutor() {
    auto max_thread_num = static_cast<size_t>(std::max(1, milvus::CPU_NUM));
    static folly::CPUThreadPoolExecutor executor(
        std::pair<size_t, size_t>{max_thread_num, kJsonStatsBuildMinThreads},
        folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(
            kJsonStatsBuildNumPriority),
        std::make_shared<folly::NamedThreadFactory>(
            "MILVUS_JSON_STATS_BUILD_"));
    return &executor;
}

folly::CPUThreadPoolExecutor*
getGlobalCPUExecutor() {
    return getSearchCPUExecutor();
}

};  // namespace milvus::futures
