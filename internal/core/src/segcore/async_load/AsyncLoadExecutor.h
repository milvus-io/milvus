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

#pragma once

#include <exception>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include "arrow/status.h"
#include "arrow/util/thread_pool.h"
#include "folly/futures/Future.h"
#include "folly/futures/Promise.h"
#include "milvus-storage/common/extend_status.h"
#include "pb/common.pb.h"
#include "storage/ThreadPools.h"

namespace arrow::internal {
class Executor;
}  // namespace arrow::internal

namespace milvus::segcore::async_load {

arrow::internal::Executor*
AsyncLoadDiskExecutor();

arrow::internal::Executor*
AsyncLoadMaterializeExecutor();

namespace detail {

template <typename Result>
void
SetExecutorFailure(folly::Promise<Result>& promise, arrow::Status status) {
    if constexpr (std::is_constructible_v<Result, arrow::Status>) {
        promise.setValue(Result(std::move(status)));
    } else {
        promise.setException(milvus_storage::ToSegcoreError(status));
    }
}

}  // namespace detail

template <typename Result, typename Func>
folly::SemiFuture<Result>
SubmitAsyncLoadExecutorTask(arrow::internal::Executor* executor, Func&& func) {
    if (executor == nullptr) {
        folly::Promise<Result> promise;
        auto future = promise.getSemiFuture();
        detail::SetExecutorFailure(
            promise, arrow::Status::Invalid("AsyncLoad executor is null"));
        return future;
    }

    using TaskFunc = std::decay_t<Func>;
    auto task = std::make_shared<TaskFunc>(std::forward<Func>(func));

    if (executor->OwnsThisThread()) {
        try {
            if constexpr (std::is_void_v<Result>) {
                std::invoke(*task);
                return folly::makeSemiFuture();
            } else {
                return folly::makeSemiFuture(std::invoke(*task));
            }
        } catch (...) {
            return folly::makeSemiFuture<Result>(
                folly::exception_wrapper(std::current_exception()));
        }
    }

    folly::Promise<Result> promise;
    auto future = promise.getSemiFuture();
    auto shared_promise =
        std::make_shared<folly::Promise<Result>>(std::move(promise));
    auto status = executor->Spawn([shared_promise, task]() mutable {
        try {
            if constexpr (std::is_void_v<Result>) {
                std::invoke(*task);
                shared_promise->setValue();
            } else {
                shared_promise->setValue(std::invoke(*task));
            }
        } catch (...) {
            shared_promise->setException(
                folly::exception_wrapper(std::current_exception()));
        }
    });
    if (!status.ok()) {
        detail::SetExecutorFailure(*shared_promise, std::move(status));
    }
    return future;
}

template <typename Func>
auto
SubmitPriorityLoadTask(milvus::proto::common::LoadPriority priority,
                       Func&& func)
    -> folly::SemiFuture<std::invoke_result_t<std::decay_t<Func>&>> {
    using TaskFunc = std::decay_t<Func>;
    using Result = std::invoke_result_t<TaskFunc&>;

    folly::Promise<Result> promise;
    auto future = promise.getSemiFuture();
    auto shared_promise =
        std::make_shared<folly::Promise<Result>>(std::move(promise));
    auto shared_func = std::make_shared<TaskFunc>(std::forward<Func>(func));

    try {
        auto& pool = milvus::ThreadPools::GetThreadPool(
            milvus::PriorityForLoad(priority));
        pool.Submit([shared_promise, shared_func]() mutable {
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
        });
    } catch (...) {
        shared_promise->setException(
            folly::exception_wrapper(std::current_exception()));
    }

    return future;
}

}  // namespace milvus::segcore::async_load
