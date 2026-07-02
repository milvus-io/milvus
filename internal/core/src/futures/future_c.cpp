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

#include "Executor.h"
#include "Future.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "future_c.h"
#include "common/CGoCatch.h"
#include "futures/future_c_types.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "monitor/Monitor.h"
#include "prometheus/gauge.h"

// Ring-3 note: future_cancel / future_is_ready / future_register_ready_callback
// / future_leak_and_get delegate to IFuture methods that are declared noexcept
// (futures/Future.h) — an exception inside them terminates at the noexcept
// boundary itself, so a catch here would be dead code. The executor_set_*
// entry points below call non-noexcept code and get catch tails.
extern "C" void
future_cancel(CFuture* future) {
    static_cast<milvus::futures::IFuture*>(static_cast<void*>(future))
        ->cancel();
}

extern "C" bool
future_is_ready(CFuture* future) {
    return static_cast<milvus::futures::IFuture*>(static_cast<void*>(future))
        ->isReady();
}

extern "C" void
future_register_ready_callback(CFuture* future,
                               CUnlockGoMutexFn unlockFn,
                               CLockedGoMutex* mutex) {
    static_cast<milvus::futures::IFuture*>(static_cast<void*>(future))
        ->registerReadyCallback(unlockFn, mutex);
}

extern "C" CStatus
future_leak_and_get(CFuture* future, void** result) {
    auto [r, s] =
        static_cast<milvus::futures::IFuture*>(static_cast<void*>(future))
            ->leakyGet();
    *result = r;
    return s;
}

extern "C" void
future_destroy(CFuture* future) {
    milvus::futures::IFuture::releaseLeakedFuture(
        static_cast<milvus::futures::IFuture*>(static_cast<void*>(future)));
}

extern "C" void
executor_set_thread_num(int thread_num) {
    executor_set_search_thread_num(thread_num);
}

extern "C" void
executor_set_search_thread_num(int thread_num) {
    try {
        milvus::futures::getSearchCPUExecutor()->setNumThreads(thread_num);
        milvus::monitor::internal_cgo_pool_size_search.Set(thread_num);
        LOG_INFO(
            "future executor setup search cpu executor with thread num: {}",
            thread_num);
    }
    CGO_CATCH_AND_LOG("executor_set_search_thread_num")
}

extern "C" void
executor_set_load_thread_num(int thread_num) {
    try {
        milvus::futures::getLoadCPUExecutor()->setNumThreads(thread_num);
        milvus::monitor::internal_cgo_pool_size_load.Set(thread_num);
        LOG_INFO("future executor setup load cpu executor with thread num: {}",
                 thread_num);
    }
    CGO_CATCH_AND_LOG("executor_set_load_thread_num")
}
