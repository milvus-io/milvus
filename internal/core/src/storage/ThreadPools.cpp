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
#include <mutex>

namespace milvus {

std::map<ThreadPoolPriority, std::unique_ptr<ThreadPool>>
    ThreadPools::thread_pool_map;
std::shared_mutex ThreadPools::mutex_;

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
                coefficient = HIGH_PRIORITY_THREAD_CORE_COEFFICIENT;
                break;
            case milvus::ThreadPoolPriority::MIDDLE:
                coefficient = MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT;
                break;
            default:
                coefficient = LOW_PRIORITY_THREAD_CORE_COEFFICIENT;
                break;
        }
        std::string name = name_map()[priority];
        auto result = thread_pool_map.emplace(
            priority, std::make_unique<ThreadPool>(coefficient, name));
        return *(result.first->second);
    }
}

void
ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority priority,
                              float ratio) {
    int size = static_cast<int>(std::round(milvus::CPU_NUM * ratio));
    if (size < 1) {
        LOG_ERROR("Failed to resize threadPool, size:{}", size);
        return;
    }
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto iter = thread_pool_map.find(priority);
    if (iter == thread_pool_map.end()) {
        LOG_ERROR("Failed to find threadPool, priority:{}", priority);
        return;
    }
    iter->second->Resize(size);
    LOG_INFO("Resized threadPool priority:{}, size:{}", priority, size);
}

}  // namespace milvus
