// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ThreadPool.h"

#include <gflags/gflags.h>

#include <chrono>
#include <mutex>

#include "log/Log.h"

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

// The hand-rolled pool used to shrink idle workers back to the minimum after
// WAIT_SECONDS(2). folly's CPUThreadPoolExecutor keeps idle threads alive for
// its global "threadtimeout_ms" (default 60s). Restore the old 2s idle timeout
// so idle workers are reclaimed promptly like before.
void
ConfigureFollyThreadTimeout() {
    static std::once_flag flag;
    std::call_once(flag, []() {
        gflags::SetCommandLineOption("threadtimeout_ms", "2000");
    });
}

}  // namespace

ThreadPool::ThreadPool(const float thread_core_coefficient, std::string name)
    : name_(std::move(name)) {
    int max_threads = ComputeThreadPoolMaxThreads(thread_core_coefficient);
    max_threads_size_.store(max_threads);

    LOG_INFO("Init thread pool:{}", name_)
        << " with min worker num:" << 1
        << " and max worker num:" << max_threads;

    ConfigureFollyThreadTimeout();

    // folly::CPUThreadPoolExecutor(std::pair{a, b}) uses `a` as maxThreads_
    // (via setNumThreads(a)) and `b` as minThreads_. Therefore an elastic pool
    // that scales between 1 and max_threads workers is {max_threads, 1}, NOT
    // {1, max_threads} which would pin the pool to a single worker.
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
        std::pair<size_t, size_t>{static_cast<size_t>(max_threads), 1},
        std::make_shared<folly::NamedThreadFactory>(name_));
}

ThreadPool::~ThreadPool() {
    ShutDown();
}

size_t
ThreadPool::GetThreadNum() {
    // folly's numActiveThreads() tracks alive (spawned) workers, which matches
    // the previous custom pool's current_threads_size_ semantics rather than a
    // busy-thread count.
    return executor_ ? executor_->numActiveThreads() : 0;
}

size_t
ThreadPool::GetMaxThreadNum() {
    return max_threads_size_.load();
}

void
ThreadPool::Resize(int new_size) {
    new_size = ClampThreadPoolMaxThreads(new_size);
    max_threads_size_.store(new_size);
    if (executor_) {
        executor_->setNumThreads(static_cast<size_t>(new_size));
    }
    if (metric_capacity_) {
        metric_capacity_->Set(new_size);
    }
}

void
ThreadPool::ShutDown() {
    if (!executor_) {
        return;
    }
    LOG_INFO("Start shutting down {}", name_);
    metrics_sampler_stop_.store(true);
    if (metrics_sampler_thread_.joinable()) {
        metrics_sampler_thread_.join();
    }
    executor_->join();
    executor_.reset();
    LOG_INFO("Finish shutting down {}", name_);
}

void
ThreadPool::SetMetrics(prometheus::Gauge* capacity,
                       prometheus::Gauge* active,
                       prometheus::Gauge* idle,
                       prometheus::Gauge* queue_depth,
                       prometheus::Counter* submitted,
                       prometheus::Counter* completed,
                       prometheus::Histogram* queue_duration,
                       prometheus::Histogram* execute_duration) {
    metric_capacity_ = capacity;
    metric_active_ = active;
    metric_idle_ = idle;
    metric_queue_depth_ = queue_depth;
    metric_submitted_ = submitted;
    metric_completed_ = completed;
    metric_queue_duration_ = queue_duration;
    metric_execute_duration_ = execute_duration;
    if (metric_capacity_) {
        metric_capacity_->Set(max_threads_size_.load());
    }
    if (!metrics_sampler_thread_.joinable()) {
        metrics_sampler_stop_.store(false);
        metrics_sampler_thread_ =
            std::thread(&ThreadPool::MetricsSamplerLoop, this);
    }
}

void
ThreadPool::MetricsSamplerLoop() {
    while (!metrics_sampler_stop_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        if (metrics_sampler_stop_.load(std::memory_order_relaxed) ||
            !executor_) {
            break;
        }
        auto stats = executor_->getPoolStats();
        if (metric_active_) {
            metric_active_->Set(stats.activeThreadCount);
        }
        if (metric_idle_) {
            metric_idle_->Set(stats.idleThreadCount);
        }
        if (metric_queue_depth_) {
            metric_queue_depth_->Set(stats.pendingTaskCount);
        }
    }
}

}  // namespace milvus
