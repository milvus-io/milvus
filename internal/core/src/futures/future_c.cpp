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

#include <memory>

#include "future_c.h"
#include "folly/init/Init.h"
#include "Future.h"
#include "Executor.h"
#include "log/Log.h"
#include "monitor/prometheus_client.h"

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
    milvus::futures::getGlobalCPUExecutor()->setNumThreads(thread_num);
    milvus::monitor::internal_cgo_pool_size_all.Set(thread_num);
    LOG_INFO("future executor setup cpu executor with thread num: {}",
             thread_num);
}
