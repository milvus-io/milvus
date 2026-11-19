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

#ifndef MILVUS_THREADPOOLS_H
#define MILVUS_THREADPOOLS_H

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/FunctionScheduler.h>

#include "common/Common.h"
#include "common/OpContext.h"
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

constexpr const char* LOAD_PRIORITY = "load_priority";

enum ThreadPoolPriority {
    HIGH = 0,
    MIDDLE = 1,
    LOW = 2,
};

inline ThreadPoolPriority
PriorityForLoad(milvus::proto::common::LoadPriority priority) {
    return priority == milvus::proto::common::LoadPriority::HIGH
               ? ThreadPoolPriority::HIGH
               : ThreadPoolPriority::LOW;
}

inline milvus::proto::common::LoadPriority
PriorityForLoad(const std::string& priority_str) {
    if (priority_str == "LOW") {
        return milvus::proto::common::LoadPriority::LOW;
    }
    return milvus::proto::common::LoadPriority::HIGH;
}

// A query override applies only to this load; warmup retains its configured priority.
inline milvus::proto::common::LoadPriority
ResolveLoadPriority(const milvus::OpContext* ctx,
                    milvus::proto::common::LoadPriority configured_priority) {
    return ctx == nullptr
               ? configured_priority
               : static_cast<milvus::proto::common::LoadPriority>(
                     ctx->runtime_load_priority.value_or(configured_priority));
}

class ThreadPools {
 public:
    // Process-wide executor; submit work with folly::via(...).semi().
    static folly::CPUThreadPoolExecutor&
    GetExecutor(ThreadPoolPriority priority);

    // Each priority keeps its own coefficient, including before first use.
    static void
    ResizeThreadPool(ThreadPoolPriority priority, float ratio);

    static int64_t
    GetLoadExecutorWorkers();

    ~ThreadPools() {
        ShutDown();
    }

 private:
    friend struct ThreadPoolsTestAccess;

    ThreadPools() {
    }
    void
    ShutDown();

    static std::map<ThreadPoolPriority, std::string>
    name_map() {
        static std::map<ThreadPoolPriority, std::string> name_map = {
            {HIGH, "HIGH_SEGC_POOL"},
            {MIDDLE, "MIDD_SEGC_POOL"},
            {LOW, "LOW_SEGC_POOL"}};
        return name_map;
    }

    // Native destruction stops queued tasks; explicitly drain after sampling stops.
    struct ExecutorDeleter {
        std::unique_ptr<folly::FunctionScheduler> metrics_sampler;
        void
        operator()(folly::CPUThreadPoolExecutor* executor) const;
    };
    using ExecutorPtr =
        std::unique_ptr<folly::CPUThreadPoolExecutor, ExecutorDeleter>;
    static std::map<ThreadPoolPriority, ExecutorPtr> thread_pool_map;
    static std::shared_mutex mutex_;
    static std::mutex resize_mutex_;
    static std::atomic<int64_t> load_executor_workers_;
};

}  // namespace milvus

template <>
struct fmt::formatter<milvus::ThreadPoolPriority> : formatter<std::string> {
    auto
    format(milvus::ThreadPoolPriority priority, format_context& ctx) const {
        std::string name;
        switch (priority) {
            case milvus::HIGH:
                name = "HIGH";
                break;
            case milvus::MIDDLE:
                name = "MIDDLE";
                break;
            case milvus::LOW:
                name = "LOW";
                break;
            default:
                name = "UNKNOWN";
        }
        return formatter<std::string>::format(name, ctx);
    }
};

#endif  //MILVUS_THREADPOOLS_H
