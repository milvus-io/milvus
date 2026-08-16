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
#include <mutex>
#include <utility>

#include "glog/logging.h"
#include "log/Log.h"
#include "monitor/Monitor.h"
#include "storage/LoadOverheadController.h"
#include "storage/ThreadPool.h"

namespace milvus {

namespace {

bool
UpdateLoadOverheadControllers(int64_t executor_workers) {
    // All current Group bindings provide max_runtime_unit, so both policy
    // updates should succeed. If that invariant is violated and only one
    // update succeeds, ResizeThreadPool's ordering keeps admission
    // fail-conservative: expansion stops before resizing, while shrinking
    // updates the policies after resizing.
    auto memory_updated = storage::LoadMemoryOverheadController::GetInstance()
                              .UpdateExecutorWorkers(executor_workers);
    auto file_updated = storage::LoadFileOverheadController::GetInstance()
                            .UpdateExecutorWorkers(executor_workers);
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

std::map<ThreadPoolPriority, std::unique_ptr<ThreadPool>>
    ThreadPools::thread_pool_map;
std::shared_mutex ThreadPools::mutex_;
std::mutex ThreadPools::resize_mutex_;
std::atomic<int64_t> ThreadPools::load_executor_workers_{-1};

void
ThreadPools::ShutDown() {
    for (auto& itr : thread_pool_map) {
        LOG_INFO("Start shutting down threadPool with priority:", itr.first);
        itr.second->ShutDown();
        LOG_INFO("Finish shutting down threadPool with priority:", itr.first);
    }
}

ThreadPool&
ThreadPools::GetThreadPool(milvus::ThreadPoolPriority priority) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto iter = thread_pool_map.find(priority);
    if (iter != thread_pool_map.end()) {
        return *(iter->second);
    } else {
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
        std::string name = name_map()[priority];
        auto result = thread_pool_map.emplace(
            priority, std::make_unique<ThreadPool>(coefficient, name));
        auto& pool = *(result.first->second);
        switch (priority) {
            case HIGH:
                pool.SetMetrics(
                    &monitor::internal_storage_pool_capacity_high,
                    &monitor::internal_storage_pool_active_threads_high,
                    &monitor::internal_storage_pool_idle_threads_high,
                    &monitor::internal_storage_pool_queue_depth_high,
                    &monitor::internal_storage_pool_task_submitted_total_high,
                    &monitor::internal_storage_pool_task_completed_total_high,
                    &monitor::internal_storage_pool_queue_duration_seconds_high,
                    &monitor::
                        internal_storage_pool_execute_duration_seconds_high);
                break;
            case MIDDLE:
                pool.SetMetrics(
                    &monitor::internal_storage_pool_capacity_middle,
                    &monitor::internal_storage_pool_active_threads_middle,
                    &monitor::internal_storage_pool_idle_threads_middle,
                    &monitor::internal_storage_pool_queue_depth_middle,
                    &monitor::internal_storage_pool_task_submitted_total_middle,
                    &monitor::internal_storage_pool_task_completed_total_middle,
                    &monitor::
                        internal_storage_pool_queue_duration_seconds_middle,
                    &monitor::
                        internal_storage_pool_execute_duration_seconds_middle);
                break;
            case LOW:
                pool.SetMetrics(
                    &monitor::internal_storage_pool_capacity_low,
                    &monitor::internal_storage_pool_active_threads_low,
                    &monitor::internal_storage_pool_idle_threads_low,
                    &monitor::internal_storage_pool_queue_depth_low,
                    &monitor::internal_storage_pool_task_submitted_total_low,
                    &monitor::internal_storage_pool_task_completed_total_low,
                    &monitor::internal_storage_pool_queue_duration_seconds_low,
                    &monitor::
                        internal_storage_pool_execute_duration_seconds_low);
                break;
        }
        return pool;
    }
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
    auto is_load_pool = priority == ThreadPoolPriority::HIGH ||
                        priority == ThreadPoolPriority::LOW;
    ThreadPool* pool = nullptr;
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto iter = thread_pool_map.find(priority);
        if (iter == thread_pool_map.end()) {
            LOG_ERROR("Failed to find threadPool, priority:{}", priority);
            return;
        }
        pool = iter->second.get();
    }
    size = ClampThreadPoolMaxThreads(size);
    auto old_size = pool->GetMaxThreadNum();
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

    pool->Resize(size);
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

    auto& high = GetThreadPool(ThreadPoolPriority::HIGH);
    auto& low = GetThreadPool(ThreadPoolPriority::LOW);
    auto initial_workers = static_cast<int64_t>(high.GetMaxThreadNum()) +
                           static_cast<int64_t>(low.GetMaxThreadNum());
    if (load_executor_workers_.compare_exchange_strong(cached_workers,
                                                       initial_workers)) {
        return initial_workers;
    }
    return cached_workers;
}

}  // namespace milvus
