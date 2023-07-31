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
    for (auto itr = thread_pool_map.begin(); itr != thread_pool_map.end();
         ++itr) {
        LOG_SEGCORE_INFO_ << "Start shutting down threadPool with priority:"
                          << itr->first;
        itr->second->ShutDown();
        LOG_SEGCORE_INFO_ << "Finish shutting down threadPool with priority:"
                          << itr->first;
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
