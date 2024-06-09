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

#include <chrono>
#include "Executor.h"
#include "common/Common.h"

namespace milvus::futures {

const int kNumPriority = 3;
const int kMaxQueueSizeFactor = 16;

folly::Executor::KeepAlive<>
getGlobalCPUExecutor() {
    static ExecutorSingleton singleton;
    return singleton.GetCPUExecutor();
}

folly::Executor::KeepAlive<>
ExecutorSingleton::GetCPUExecutor() {
    // TODO: fix the executor with a non-block way.
    std::call_once(cpu_executor_once_, [this]() {
        int num_threads = milvus::CPU_NUM;
        auto num_priority = kNumPriority;
        auto max_queue_size = num_threads * kMaxQueueSizeFactor;
        cpu_executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
            num_threads,
            std::make_unique<folly::PriorityLifoSemMPMCQueue<
                folly::CPUThreadPoolExecutor::CPUTask,
                folly::QueueBehaviorIfFull::BLOCK>>(num_priority,
                                                    max_queue_size),
            std::make_shared<folly::NamedThreadFactory>("MILVUS_CPU_"));
    });
    return folly::getKeepAliveToken(cpu_executor_.get());
}

};  // namespace milvus::futures