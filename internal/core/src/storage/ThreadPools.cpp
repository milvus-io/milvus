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

//
// Created by zilliz on 2023/7/31.
//

#include "ThreadPools.h"

#include <math.h>
#include <atomic>
#include <chrono>

#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <mutex>
#include <utility>

#include "glog/logging.h"
#include "log/Log.h"
#include "monitor/Monitor.h"
#include "storage/LoadOverheadController.h"
#include "storage/LoadConfig.h"

namespace milvus {

int CPU_NUM = DEFAULT_CPU_NUM;
std::atomic<int> THREAD_POOL_MAX_THREADS_SIZE(
    DEFAULT_THREAD_POOL_MAX_THREADS_SIZE);

std::atomic<float> HIGH_PRIORITY_THREAD_CORE_COEFFICIENT(
    DEFAULT_HIGH_PRIORITY_THREAD_CORE_COEFFICIENT);
std::atomic<float> MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT(
    DEFAULT_MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT);
std::atomic<float> LOW_PRIORITY_THREAD_CORE_COEFFICIENT(
    DEFAULT_LOW_PRIORITY_THREAD_CORE_COEFFICIENT);

void
SetHighPriorityThreadCoreCoefficient(const float coefficient) {
    HIGH_PRIORITY_THREAD_CORE_COEFFICIENT.store(coefficient);
    LOG_INFO("set high priority thread pool core coefficient: {}",
             HIGH_PRIORITY_THREAD_CORE_COEFFICIENT.load());
}

void
SetMiddlePriorityThreadCoreCoefficient(const float coefficient) {
    MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.store(coefficient);
    LOG_INFO("set middle priority thread pool core coefficient: {}",
             MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.load());
}

void
SetLowPriorityThreadCoreCoefficient(const float coefficient) {
    LOW_PRIORITY_THREAD_CORE_COEFFICIENT.store(coefficient);
    LOG_INFO("set low priority thread pool core coefficient: {}",
             LOW_PRIORITY_THREAD_CORE_COEFFICIENT.load());
}

void
InitCpuNum(const int num) {
    CPU_NUM = num;
}

void
SetThreadPoolMaxThreadsSize(const int size) {
    THREAD_POOL_MAX_THREADS_SIZE.store(size);
    LOG_INFO("set thread pool max threads size: {}", size);
}

namespace {

struct PoolMetrics {
    prometheus::Gauge* capacity{};
    prometheus::Gauge* active{};
    prometheus::Gauge* idle{};
    prometheus::Gauge* queue_depth{};
    prometheus::Counter* submitted{};
    prometheus::Counter* completed{};
    prometheus::Histogram* queue_duration{};
    prometheus::Histogram* execute_duration{};
};

PoolMetrics
MetricsFor(ThreadPoolPriority priority) {
    switch (priority) {
        case HIGH:
            return {
                &monitor::internal_storage_pool_capacity_high,
                &monitor::internal_storage_pool_active_threads_high,
                &monitor::internal_storage_pool_idle_threads_high,
                &monitor::internal_storage_pool_queue_depth_high,
                &monitor::internal_storage_pool_task_submitted_total_high,
                &monitor::internal_storage_pool_task_completed_total_high,
                &monitor::internal_storage_pool_queue_duration_seconds_high,
                &monitor::internal_storage_pool_execute_duration_seconds_high};
        case MIDDLE:
            return {
                &monitor::internal_storage_pool_capacity_middle,
                &monitor::internal_storage_pool_active_threads_middle,
                &monitor::internal_storage_pool_idle_threads_middle,
                &monitor::internal_storage_pool_queue_depth_middle,
                &monitor::internal_storage_pool_task_submitted_total_middle,
                &monitor::internal_storage_pool_task_completed_total_middle,
                &monitor::internal_storage_pool_queue_duration_seconds_middle,
                &monitor::
                    internal_storage_pool_execute_duration_seconds_middle};
        case LOW:
            return {
                &monitor::internal_storage_pool_capacity_low,
                &monitor::internal_storage_pool_active_threads_low,
                &monitor::internal_storage_pool_idle_threads_low,
                &monitor::internal_storage_pool_queue_depth_low,
                &monitor::internal_storage_pool_task_submitted_total_low,
                &monitor::internal_storage_pool_task_completed_total_low,
                &monitor::internal_storage_pool_queue_duration_seconds_low,
                &monitor::internal_storage_pool_execute_duration_seconds_low};
    }
    return {};
}

class LoadPoolTaskMetrics final
    : public folly::ThreadPoolExecutor::TaskObserver {
 public:
    explicit LoadPoolTaskMetrics(PoolMetrics metrics) : metrics_(metrics) {
    }

    void
    taskEnqueued(const folly::ThreadPoolExecutor::TaskInfo&) noexcept override {
        if (metrics_.submitted) {
            metrics_.submitted->Increment();
        }
    }

    void
    taskDequeued(const folly::ThreadPoolExecutor::DequeuedTaskInfo&
                     task) noexcept override {
        if (metrics_.queue_duration) {
            metrics_.queue_duration->Observe(
                std::chrono::duration<double>(task.waitTime).count());
        }
    }

    void
    taskProcessed(const folly::ThreadPoolExecutor::ProcessedTaskInfo&
                      task) noexcept override {
        if (metrics_.execute_duration) {
            metrics_.execute_duration->Observe(
                std::chrono::duration<double>(task.runTime).count());
        }
        if (metrics_.completed) {
            metrics_.completed->Increment();
        }
    }

 private:
    const PoolMetrics metrics_;
};

bool
UpdateLoadOverheadControllers(int64_t executor_workers) {
    if (milvus::storage::LoadAsyncReadEnabled()) {
        return true;
    }
    // All current Group bindings provide max_runtime_unit, so both policy
    // updates should succeed. If that invariant is violated and only one
    // update succeeds, ResizeThreadPool's ordering keeps admission
    // fail-conservative: expansion stops before resizing, while shrinking
    // updates the policies after resizing.
    auto memory_updated = storage::LoadMemoryOverheadController::GetInstance()
                              .UpdateConcurrencyLimit(executor_workers);
    auto file_updated = storage::LoadFileOverheadController::GetInstance()
                            .UpdateConcurrencyLimit(executor_workers);
    if (memory_updated != file_updated) {
        LOG_ERROR(
            "Load overhead controllers were updated partially, "
            "memory_updated:{}, file_updated:{}, executor_workers:{}",
            memory_updated,
            file_updated,
            executor_workers);
    }
    return memory_updated && file_updated;
}

}  // namespace

std::map<ThreadPoolPriority, ThreadPools::ExecutorPtr>
    ThreadPools::thread_pool_map;
std::shared_mutex ThreadPools::mutex_;
std::mutex ThreadPools::resize_mutex_;
std::atomic<int64_t> ThreadPools::load_executor_workers_{-1};

void
ThreadPools::ExecutorDeleter::operator()(
    folly::CPUThreadPoolExecutor* executor) const {
    if (metrics_sampler) {
        metrics_sampler->shutdown();
    }
    executor->join();
    delete executor;
}

void
ThreadPools::ShutDown() {
    thread_pool_map.clear();
}

folly::CPUThreadPoolExecutor&
ThreadPools::GetExecutor(milvus::ThreadPoolPriority priority) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto iter = thread_pool_map.find(priority);
    if (iter != thread_pool_map.end()) {
        return *(iter->second);
    }
    float coefficient = 1.0;
    switch (priority) {
        case milvus::ThreadPoolPriority::HIGH:
            coefficient = HIGH_PRIORITY_THREAD_CORE_COEFFICIENT.load();
            break;
        case milvus::ThreadPoolPriority::MIDDLE:
            coefficient = MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.load();
            break;
        default:
            coefficient = LOW_PRIORITY_THREAD_CORE_COEFFICIENT.load();
            break;
    }
    const auto name = name_map()[priority];
    const auto max_threads = ComputeThreadPoolMaxThreads(coefficient);
    LOG_INFO("Init thread pool:{}", name)
        << " with min worker num:" << 1
        << " and max worker num:" << max_threads;
    ExecutorPtr pool(
        new folly::CPUThreadPoolExecutor(
            std::pair<size_t, size_t>{static_cast<size_t>(max_threads), 1},
            std::make_shared<folly::NamedThreadFactory>(name)),
        ExecutorDeleter{});
    pool->setThreadDeathTimeout(std::chrono::seconds(2));
    const auto metrics = MetricsFor(priority);
    pool->addTaskObserver(std::make_unique<LoadPoolTaskMetrics>(metrics));
    if (metrics.capacity) {
        metrics.capacity->Set(max_threads);
    }
    auto sampler = std::make_unique<folly::FunctionScheduler>();
    sampler->addFunction(
        [executor = pool.get(), metrics] {
            const auto stats = executor->getPoolStats();
            if (metrics.active) {
                metrics.active->Set(stats.activeThreadCount);
            }
            if (metrics.idle) {
                metrics.idle->Set(stats.idleThreadCount);
            }
            if (metrics.queue_depth) {
                metrics.queue_depth->Set(stats.pendingTaskCount);
            }
        },
        std::chrono::seconds(2),
        name + "_metrics",
        std::chrono::seconds(2));
    sampler->start();
    pool.get_deleter().metrics_sampler = std::move(sampler);
    return *thread_pool_map.emplace(priority, std::move(pool)).first->second;
}

void
ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority priority,
                              float ratio) {
    std::lock_guard<std::mutex> resize_lock(resize_mutex_);
    int size = static_cast<int>(std::round(milvus::CPU_NUM * ratio));
    if (size < 1) {
        LOG_ERROR("Failed to resize threadPool, size:{}", size);
        return;
    }
    size = ClampThreadPoolMaxThreads(size);
    auto is_load_pool = priority == ThreadPoolPriority::HIGH ||
                        priority == ThreadPoolPriority::LOW;
    folly::CPUThreadPoolExecutor* pool = nullptr;
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        // Publish with creation excluded so lazy pools see the latest ratio.
        if (priority == MIDDLE) {
            MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.store(ratio);
        }
        auto iter = thread_pool_map.find(priority);
        if (iter == thread_pool_map.end()) {
            if (priority == MIDDLE) {
                return;
            }
            LOG_ERROR("Failed to find threadPool, priority:{}", priority);
            return;
        }
        pool = iter->second.get();
    }
    auto old_size = pool->numThreads();
    auto old_load_workers =
        is_load_pool ? GetLoadExecutorWorkers() : int64_t{0};
    auto new_load_workers =
        is_load_pool ? old_load_workers - static_cast<int64_t>(old_size) + size
                     : old_load_workers;
    if (new_load_workers > old_load_workers &&
        !UpdateLoadOverheadControllers(new_load_workers)) {
        LOG_ERROR(
            "Failed to expand threadPool because the load overhead group "
            "update failed, priority:{}, size:{}",
            priority,
            size);
        return;
    }

    pool->setNumThreads(static_cast<size_t>(size));
    if (auto* capacity = MetricsFor(priority).capacity) {
        capacity->Set(size);
    }
    if (is_load_pool) {
        load_executor_workers_.store(new_load_workers);
    }

    if (is_load_pool && new_load_workers <= old_load_workers &&
        !UpdateLoadOverheadControllers(new_load_workers)) {
        LOG_ERROR(
            "Failed to update load overhead groups after resizing "
            "threadPool, priority:{}, size:{}",
            priority,
            size);
    }
    LOG_INFO("Resized threadPool priority:{}, size:{}", priority, size);
}

int64_t
ThreadPools::GetLoadExecutorWorkers() {
    auto cached_workers = load_executor_workers_.load();
    if (cached_workers >= 0) {
        return cached_workers;
    }

    auto& high = GetExecutor(ThreadPoolPriority::HIGH);
    auto& low = GetExecutor(ThreadPoolPriority::LOW);
    auto initial_workers = static_cast<int64_t>(high.numThreads()) +
                           static_cast<int64_t>(low.numThreads());
    if (load_executor_workers_.compare_exchange_strong(cached_workers,
                                                       initial_workers)) {
        return initial_workers;
    }
    return cached_workers;
}

}  // namespace milvus
