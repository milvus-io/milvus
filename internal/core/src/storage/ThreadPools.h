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

#include "ThreadPool.h"
#include "common/Common.h"

namespace milvus {

constexpr const char* THREAD_POOL_PRIORITY = "priority";

enum ThreadPoolPriority {
    HIGH = 0,
    LOW = 1,
};

class ThreadPools {
 public:
    static ThreadPool&
    GetThreadPool(ThreadPoolPriority priority);

    ~ThreadPools() {
        ShutDown();
    }

 private:
    ThreadPools() {
        name_map[HIGH] = "high_priority_thread_pool";
        name_map[LOW] = "low_priority_thread_pool";
    }
    static void
    SetUpCoefficients() {
        coefficient_map[HIGH] = HIGH_PRIORITY_THREAD_CORE_COEFFICIENT;
        coefficient_map[LOW] = LOW_PRIORITY_THREAD_CORE_COEFFICIENT;
        LOG_INFO("Init ThreadPools, high_priority_co={}, low={}",
                 HIGH_PRIORITY_THREAD_CORE_COEFFICIENT,
                 LOW_PRIORITY_THREAD_CORE_COEFFICIENT);
    }
    void
    ShutDown();
    static std::map<ThreadPoolPriority, std::unique_ptr<ThreadPool>>
        thread_pool_map;
    static std::map<ThreadPoolPriority, int64_t> coefficient_map;
    static std::map<ThreadPoolPriority, std::string> name_map;
    static std::shared_mutex mutex_;
    static ThreadPools threadPools;
    static bool has_setup_coefficients;
};

}  // namespace milvus

#endif  //MILVUS_THREADPOOLS_H
