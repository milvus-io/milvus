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

#include "segcore/async_load/AsyncLoadExecutor.h"

#include <algorithm>
#include <exception>
#include <memory>

#include "arrow/status.h"
#include "arrow/util/thread_pool.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "storage/ThreadPools.h"

namespace milvus::segcore::async_load {
namespace {

class MilvusThreadPoolArrowExecutor final : public arrow::internal::Executor {
 public:
    explicit MilvusThreadPoolArrowExecutor(milvus::ThreadPoolPriority priority)
        : priority_(priority) {
    }

    int
    GetCapacity() override {
        return static_cast<int>(
            milvus::ThreadPools::GetThreadPool(priority_).GetMaxThreadNum());
    }

    bool
    OwnsThisThread() override {
        return current_executor_ == this;
    }

 protected:
    arrow::Status
    SpawnReal(arrow::internal::TaskHints,
              arrow::internal::FnOnce<void()> task,
              arrow::StopToken stop_token,
              StopCallback&& stop_callback) override {
        auto task_ptr =
            std::make_shared<arrow::internal::FnOnce<void()>>(std::move(task));
        auto stop_callback_ptr =
            std::make_shared<StopCallback>(std::move(stop_callback));
        try {
            milvus::ThreadPools::GetThreadPool(priority_).Submit(
                [this,
                 task_ptr,
                 stop_token = std::move(stop_token),
                 stop_callback_ptr]() mutable {
                    if (stop_token.IsStopRequested()) {
                        if (*stop_callback_ptr) {
                            std::move (*stop_callback_ptr)(stop_token.Poll());
                        }
                        return;
                    }
                    auto* previous_executor = current_executor_;
                    current_executor_ = this;
                    try {
                        std::move (*task_ptr)();
                    } catch (...) {
                        current_executor_ = previous_executor;
                        throw;
                    }
                    current_executor_ = previous_executor;
                });
        } catch (const std::exception& e) {
            return arrow::Status::IOError(e.what());
        } catch (...) {
            return arrow::Status::IOError(
                "failed to submit task to Milvus thread pool");
        }
        return arrow::Status::OK();
    }

 private:
    milvus::ThreadPoolPriority priority_;
    static thread_local MilvusThreadPoolArrowExecutor* current_executor_;
};

thread_local MilvusThreadPoolArrowExecutor*
    MilvusThreadPoolArrowExecutor::current_executor_ = nullptr;

class AsyncLoadDiskArrowExecutor final : public arrow::internal::Executor {
 public:
    AsyncLoadDiskArrowExecutor()
        : thread_count_(std::max(1, milvus::CPU_NUM)),
          executor_(
              thread_count_,
              std::make_shared<folly::NamedThreadFactory>("ASYNC_LOAD_DISK_")) {
    }

    int
    GetCapacity() override {
        return static_cast<int>(thread_count_);
    }

    bool
    OwnsThisThread() override {
        return current_executor_ == this;
    }

 protected:
    arrow::Status
    SpawnReal(arrow::internal::TaskHints,
              arrow::internal::FnOnce<void()> task,
              arrow::StopToken stop_token,
              StopCallback&& stop_callback) override {
        auto task_ptr =
            std::make_shared<arrow::internal::FnOnce<void()>>(std::move(task));
        auto stop_callback_ptr =
            std::make_shared<StopCallback>(std::move(stop_callback));
        try {
            executor_.add([this,
                           task_ptr,
                           stop_token = std::move(stop_token),
                           stop_callback_ptr]() mutable {
                if (stop_token.IsStopRequested()) {
                    if (*stop_callback_ptr) {
                        std::move (*stop_callback_ptr)(stop_token.Poll());
                    }
                    return;
                }
                auto* previous_executor = current_executor_;
                current_executor_ = this;
                try {
                    std::move (*task_ptr)();
                } catch (...) {
                    current_executor_ = previous_executor;
                    throw;
                }
                current_executor_ = previous_executor;
            });
        } catch (const std::exception& e) {
            return arrow::Status::IOError(e.what());
        } catch (...) {
            return arrow::Status::IOError(
                "failed to submit task to AsyncLoad disk executor");
        }
        return arrow::Status::OK();
    }

 private:
    size_t thread_count_;
    folly::CPUThreadPoolExecutor executor_;
    static thread_local AsyncLoadDiskArrowExecutor* current_executor_;
};

thread_local AsyncLoadDiskArrowExecutor*
    AsyncLoadDiskArrowExecutor::current_executor_ = nullptr;

}  // namespace

arrow::internal::Executor*
AsyncLoadDiskExecutor() {
    static AsyncLoadDiskArrowExecutor executor;
    return &executor;
}

arrow::internal::Executor*
AsyncLoadMaterializeExecutor() {
    static MilvusThreadPoolArrowExecutor executor(milvus::HIGH);
    return &executor;
}

}  // namespace milvus::segcore::async_load
