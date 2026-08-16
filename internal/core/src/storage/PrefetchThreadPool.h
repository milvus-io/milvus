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

#pragma once
#include <algorithm>
#include <cstdint>
#include <memory>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>

namespace milvus {
namespace detail {

inline std::shared_ptr<folly::CPUThreadPoolExecutor>&
PrefetchThreadPool() {
    static std::shared_ptr<folly::CPUThreadPoolExecutor> pool =
        std::make_shared<folly::CPUThreadPoolExecutor>(
            1,
            folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(1),
            std::make_shared<folly::NamedThreadFactory>("MILVUS_PREFETCH_"));
    return pool;
}

}  // namespace detail

inline std::shared_ptr<folly::CPUThreadPoolExecutor>
GetPrefetchThreadPool() {
    return detail::PrefetchThreadPool();
}

inline void
SetPrefetchThreadPoolSize(uint32_t num_threads) {
    detail::PrefetchThreadPool()->setNumThreads(
        std::max<uint32_t>(1, num_threads));
}
}  // namespace milvus
