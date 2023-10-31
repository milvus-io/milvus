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

namespace milvus {

std::map<ThreadPoolPriority, std::unique_ptr<ThreadPool>>
    ThreadPools::thread_pool_map;
std::map<ThreadPoolPriority, int64_t> ThreadPools::coefficient_map;
std::map<ThreadPoolPriority, std::string> ThreadPools::name_map;
std::shared_mutex ThreadPools::mutex_;
ThreadPools ThreadPools::threadPools;
bool ThreadPools::has_setup_coefficients = false;

void
ThreadPools::ShutDown() {
    for (auto& itr : thread_pool_map) {
        LOG_SEGCORE_INFO_ << "Start shutting down threadPool with priority:"
                          << itr.first;
        itr.second->ShutDown();
        LOG_SEGCORE_INFO_ << "Finish shutting down threadPool with priority:"
                          << itr.first;
    }
}

ThreadPool&
ThreadPools::GetThreadPool(milvus::ThreadPoolPriority priority) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto iter = thread_pool_map.find(priority);
    if (!ThreadPools::has_setup_coefficients) {
        ThreadPools::SetUpCoefficients();
        ThreadPools::has_setup_coefficients = true;
    }
    if (iter != thread_pool_map.end()) {
        return *(iter->second);
    } else {
        int64_t coefficient = coefficient_map[priority];
        std::string name = name_map[priority];
        auto result = thread_pool_map.emplace(
            priority, std::make_unique<ThreadPool>(coefficient, name));
        return *(result.first->second);
    }
}

}  // namespace milvus
