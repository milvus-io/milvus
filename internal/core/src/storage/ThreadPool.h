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
#include <cmath>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <utility>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>

#include "log/Log.h"

namespace milvus {

const int DEFAULT_CPU_NUM = 1;

const int64_t DEFAULT_HIGH_PRIORITY_THREAD_CORE_COEFFICIENT = 10;
const int64_t DEFAULT_MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT = 5;
const int64_t DEFAULT_LOW_PRIORITY_THREAD_CORE_COEFFICIENT = 1;

extern std::atomic<float> HIGH_PRIORITY_THREAD_CORE_COEFFICIENT;
extern std::atomic<float> MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT;
extern std::atomic<float> LOW_PRIORITY_THREAD_CORE_COEFFICIENT;

extern int CPU_NUM;

void
SetHighPriorityThreadCoreCoefficient(const float coefficient);

void
SetMiddlePriorityThreadCoreCoefficient(const float coefficient);

void
SetLowPriorityThreadCoreCoefficient(const float coefficient);

void
InitCpuNum(const int core);

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
        executor_->add([task]() { (*task)(); });
        return future;
    }

    size_t
    GetThreadNum();

    size_t
    GetMaxThreadNum();

    void
    Resize(int new_size);

    void
    ShutDown();

 private:
    std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::string name_;
    std::atomic<int> max_threads_size_;
};

}  // namespace milvus
