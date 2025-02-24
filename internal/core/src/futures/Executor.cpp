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
#include "monitor/prometheus_client.h"

namespace milvus::futures {

const int kNumPriority = 3;

folly::CPUThreadPoolExecutor*
getGlobalCPUExecutor() {
    auto thread_num = std::thread::hardware_concurrency();
    static folly::CPUThreadPoolExecutor executor(
        thread_num,
        folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(kNumPriority),
        std::make_shared<folly::NamedThreadFactory>("MILVUS_FUTURE_CPU_"));
    return &executor;
}

};  // namespace milvus::futures