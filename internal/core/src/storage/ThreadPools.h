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

constexpr const char* LOAD_PRIORITY = "load_priority";

enum ThreadPoolPriority {
    HIGH = 0,
    LOW = 1,
};

inline ThreadPoolPriority
PriorityForLoad(milvus::proto::common::LoadPriority priority) {
    return priority == milvus::proto::common::LoadPriority::HIGH
               ? ThreadPoolPriority::HIGH
               : ThreadPoolPriority::LOW;
}

class ThreadPools {
 public:
    static ThreadPool&
    GetThreadPool(ThreadPoolPriority priority);

    static void
    ResizeThreadPool(ThreadPoolPriority priority, float ratio);

    ~ThreadPools() {
        ShutDown();
    }

 private:
    ThreadPools() {
        name_map[HIGH] = "high_priority_thread_pool";
        name_map[LOW] = "low_priority_thread_pool";
    }

    void
    ShutDown();
    static std::map<ThreadPoolPriority, std::unique_ptr<ThreadPool>>
        thread_pool_map;
    static std::map<ThreadPoolPriority, std::string> name_map;
    static std::shared_mutex mutex_;
};

}  // namespace milvus

#endif  //MILVUS_THREADPOOLS_H
