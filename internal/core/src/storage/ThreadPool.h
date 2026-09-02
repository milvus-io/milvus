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

#pragma once

#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>

#include "log/Log.h"

namespace milvus {

const int DEFAULT_CPU_NUM = 1;

const int64_t DEFAULT_HIGH_PRIORITY_THREAD_CORE_COEFFICIENT = 10;
const int64_t DEFAULT_MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT = 5;
const int64_t DEFAULT_LOW_PRIORITY_THREAD_CORE_COEFFICIENT = 1;

const int DEFAULT_THREAD_POOL_MAX_THREADS_SIZE = 16;

extern std::atomic<float> HIGH_PRIORITY_THREAD_CORE_COEFFICIENT;
extern std::atomic<float> MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT;
extern std::atomic<float> LOW_PRIORITY_THREAD_CORE_COEFFICIENT;

extern int CPU_NUM;
extern std::atomic<int> THREAD_POOL_MAX_THREADS_SIZE;

void
SetHighPriorityThreadCoreCoefficient(const float coefficient);

void
SetMiddlePriorityThreadCoreCoefficient(const float coefficient);

void
SetLowPriorityThreadCoreCoefficient(const float coefficient);

void
InitCpuNum(const int core);

void
SetThreadPoolMaxThreadsSize(const int size);

inline int
ClampThreadPoolMaxThreads(int size) {
    size = std::max(1, size);
    auto max_limit = THREAD_POOL_MAX_THREADS_SIZE.load();
    if (max_limit > 0 && size > max_limit) {
        size = max_limit;
    }
    return size;
}

inline int
ComputeThreadPoolMaxThreads(float thread_core_coefficient) {
    return ClampThreadPoolMaxThreads(
        static_cast<int>(std::round(CPU_NUM * thread_core_coefficient)));
}

class ThreadPool {
 public:
    explicit ThreadPool(const float thread_core_coefficient, std::string name);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool&
    operator=(const ThreadPool&) = delete;
    ThreadPool&
    operator=(ThreadPool&&) = delete;

    template <typename F, typename... Args>
    auto
    Submit(F&& f, Args&&... args) -> std::future<decltype(f(args...))> {
        using ReturnType = decltype(f(args...));
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));
        auto future = task->get_future();

        auto enqueue_time = std::chrono::steady_clock::now();
        auto* queue_metric = metric_queue_duration_;
        auto* execute_metric = metric_execute_duration_;
        auto* completed_metric = metric_completed_;
        std::function<void()> wrap_func = [task,
                                           enqueue_time,
                                           queue_metric,
                                           execute_metric,
                                           completed_metric]() {
            auto execute_start = std::chrono::steady_clock::now();
            if (queue_metric) {
                queue_metric->Observe(
                    std::chrono::duration<double>(execute_start - enqueue_time)
                        .count());
            }
            auto observe_execute = [&]() {
                if (execute_metric) {
                    execute_metric->Observe(
                        std::chrono::duration<double>(
                            std::chrono::steady_clock::now() - execute_start)
                            .count());
                }
                if (completed_metric) {
                    completed_metric->Increment();
                }
            };
            try {
                (*task)();
            } catch (...) {
                observe_execute();
                throw;
            }
            observe_execute();
        };

        if (metric_submitted_) {
            metric_submitted_->Increment();
        }
        // Deterministic test seam: simulates a worker-spawn failure. A failure
        // here must not fail the queued task -- the task is still handed to the
        // underlying executor and will run once a worker is available.
        try {
            if (worker_spawn_hook_for_test_) {
                worker_spawn_hook_for_test_();
            }
        } catch (const std::exception& e) {
            LOG_WARN("Worker spawn hook failed for thread pool {}: {}",
                     name_,
                     e.what());
        } catch (...) {
            LOG_WARN("Worker spawn hook failed for thread pool {}", name_);
        }
        try {
            executor_->add(std::move(wrap_func));
        } catch (const std::exception& e) {
            LOG_WARN("Failed to submit task to thread pool {}: {}", name_, e.what());
        } catch (...) {
            LOG_WARN("Failed to submit task to thread pool {}", name_);
        }

        return future;
    }

    // folly::ThreadPoolExecutor::numActiveThreads() reports the number of
    // currently alive worker threads (including idle ones), which matches the
    // previous hand-rolled pool's current_threads_size_ semantics rather than a
    // busy-thread count.
    size_t
    GetThreadNum();

    size_t
    GetMaxThreadNum();

    void
    Resize(int new_size);

    void
    ShutDown();

    void
    SetMetrics(prometheus::Gauge* capacity,
               prometheus::Gauge* active,
               prometheus::Gauge* idle,
               prometheus::Gauge* queue_depth,
               prometheus::Counter* submitted,
               prometheus::Counter* completed,
               prometheus::Histogram* queue_duration,
               prometheus::Histogram* execute_duration);

 private:
    friend class ThreadPoolTest_WorkerSpawnFailureDoesNotFailQueuedTask_Test;

    void
    MetricsSamplerLoop();

    std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::string name_;
    std::atomic<int> max_threads_size_;

    std::thread metrics_sampler_thread_;
    std::atomic<bool> metrics_sampler_stop_{false};

    // Prometheus metrics (set via SetMetrics, nullptr if not wired)
    prometheus::Gauge* metric_capacity_{nullptr};
    prometheus::Gauge* metric_active_{nullptr};
    prometheus::Gauge* metric_idle_{nullptr};
    prometheus::Gauge* metric_queue_depth_{nullptr};
    prometheus::Counter* metric_submitted_{nullptr};
    prometheus::Counter* metric_completed_{nullptr};
    prometheus::Histogram* metric_queue_duration_{nullptr};
    prometheus::Histogram* metric_execute_duration_{nullptr};

    // Deterministic test seam for worker-spawn failure coverage.
    std::function<void()> worker_spawn_hook_for_test_;
};

}  // namespace milvus
