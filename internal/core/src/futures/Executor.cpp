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

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "Executor.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "storage/ThreadPool.h"

namespace milvus::futures {

const int kNumPriority = 3;
const int kJsonStatsBuildNumPriority = 1;
const size_t kJsonStatsBuildMinThreads = 1;

folly::CPUThreadPoolExecutor*
getSearchCPUExecutor() {
    auto thread_num = std::max(1, milvus::CPU_NUM);
    static folly::CPUThreadPoolExecutor executor(
        thread_num,
        folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(kNumPriority),
        std::make_shared<folly::NamedThreadFactory>("MILVUS_SEARCH_"));
    return &executor;
}

folly::CPUThreadPoolExecutor*
getLoadCPUExecutor() {
    auto thread_num = std::max(1, milvus::CPU_NUM);
    static folly::CPUThreadPoolExecutor executor(
        thread_num,
        folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(kNumPriority),
        std::make_shared<folly::NamedThreadFactory>("MILVUS_LOAD_"));
    return &executor;
}

folly::CPUThreadPoolExecutor*
getJsonStatsBuildExecutor() {
    auto max_thread_num = static_cast<size_t>(std::max(1, milvus::CPU_NUM));
    static folly::CPUThreadPoolExecutor executor(
        std::pair<size_t, size_t>{max_thread_num, kJsonStatsBuildMinThreads},
        folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(
            kJsonStatsBuildNumPriority),
        std::make_shared<folly::NamedThreadFactory>(
            "MILVUS_JSON_STATS_BUILD_"));
    return &executor;
}

folly::CPUThreadPoolExecutor*
getGlobalCPUExecutor() {
    return getSearchCPUExecutor();
}

};  // namespace milvus::futures
