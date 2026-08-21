// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include "common/EasyAssert.h"
#include "folly/Executor.h"
#include "folly/futures/Future.h"
#include "folly/futures/Promise.h"
#include "pb/common.pb.h"

namespace milvus::storage {

// Returns the process-wide executor shared by asynchronous field-data and
// index loading. HIGH and LOW loads use priority views of this one physical
// thread pool.
folly::Executor*
GetLoadExecutor();

int8_t
LoadExecutorPriority(proto::common::LoadPriority priority);

folly::Executor::KeepAlive<>
GetLoadExecutorForPriority(proto::common::LoadPriority priority);

int64_t
GetLoadExecutorWorkerCount();

template <typename Func>
auto
SubmitLoadTask(proto::common::LoadPriority priority, Func&& func)
    -> folly::SemiFuture<std::invoke_result_t<std::decay_t<Func>&>> {
    using TaskFunc = std::decay_t<Func>;
    using Result = std::invoke_result_t<TaskFunc&>;

    folly::Promise<Result> promise;
    auto future = promise.getSemiFuture();
    auto shared_promise =
        std::make_shared<folly::Promise<Result>>(std::move(promise));
    auto shared_func = std::make_shared<TaskFunc>(std::forward<Func>(func));

    try {
        auto* executor = GetLoadExecutor();
        AssertInfo(executor != nullptr, "Shared LoadExecutor is unavailable");
        executor->addWithPriority(
            [shared_promise, shared_func]() mutable {
                try {
                    if constexpr (std::is_void_v<Result>) {
                        std::invoke(*shared_func);
                        shared_promise->setValue();
                    } else {
                        shared_promise->setValue(std::invoke(*shared_func));
                    }
                } catch (...) {
                    shared_promise->setException(
                        folly::exception_wrapper(std::current_exception()));
                }
            },
            LoadExecutorPriority(priority));
    } catch (...) {
        shared_promise->setException(
            folly::exception_wrapper(std::current_exception()));
    }

    return future;
}

}  // namespace milvus::storage
