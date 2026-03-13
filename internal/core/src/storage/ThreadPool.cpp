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

#include "log/Log.h"

namespace milvus {

int CPU_NUM = DEFAULT_CPU_NUM;

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

ThreadPool::ThreadPool(const float thread_core_coefficient, std::string name)
    : name_(std::move(name)) {
    int max_threads = std::max(
        1, static_cast<int>(std::round(CPU_NUM * thread_core_coefficient)));

    // only IO pool will set large limit, but the CPU helps nothing to IO operations,
    // we need to limit the max thread num, each thread will download 16~64 MiB data,
    // according to our benchmark, 16 threads is enough to saturate the network bandwidth.
    if (max_threads > 16) {
        max_threads = 16;
    }
    max_threads_size_.store(max_threads);

    LOG_INFO("Init thread pool:{}", name_)
        << " with min worker num:" << 1
        << " and max worker num:" << max_threads;

    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
        std::pair<size_t, size_t>{1, static_cast<size_t>(max_threads)},
        std::make_shared<folly::NamedThreadFactory>(name_));
}

ThreadPool::~ThreadPool() {
    ShutDown();
}

size_t
ThreadPool::GetThreadNum() {
    return executor_ ? executor_->numActiveThreads() : 0;
}

size_t
ThreadPool::GetMaxThreadNum() {
    return max_threads_size_.load();
}

void
ThreadPool::Resize(int new_size) {
    new_size = std::max(1, new_size);
    if (new_size > 16) {
        new_size = 16;
    }
    max_threads_size_.store(new_size);
    if (executor_) {
        executor_->setNumThreads(new_size);
    }
}

void
ThreadPool::ShutDown() {
    if (executor_) {
        LOG_INFO("Start shutting down {}", name_);
        executor_->join();
        executor_.reset();
        LOG_INFO("Finish shutting down {}", name_);
    }
}

}  // namespace milvus
