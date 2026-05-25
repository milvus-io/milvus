#pragma once
#include <folly/executors/CPUThreadPoolExecutor.h>
namespace milvus {

inline std::shared_ptr<folly::CPUThreadPoolExecutor>
GetPrefetchThreadPool() {
    static std::shared_ptr<folly::CPUThreadPoolExecutor> pool =
        std::make_shared<folly::CPUThreadPoolExecutor>(
            64,
            folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(1),
            std::make_shared<folly::NamedThreadFactory>("MILVUS_PREFETCH_"));
    return pool;
}
}  // namespace milvus