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

#include "storage/LocalFileIOPool.h"

#include <algorithm>
#include <exception>
#include <thread>
#include <utility>

#include "folly/executors/thread_factory/NamedThreadFactory.h"
#include "log/Log.h"

namespace milvus::storage {
namespace {

int
ClampWorkerCount(int worker_count) {
    if (worker_count < 0) {
        LOG_WARN("Invalid local file I/O worker count {}, using 0",
                 worker_count);
        return 0;
    }

    auto max_workers = std::max(1u, std::thread::hardware_concurrency());
    if (static_cast<unsigned int>(worker_count) > max_workers) {
        LOG_WARN("Local file I/O worker count {} exceeds {}, using {}",
                 worker_count,
                 max_workers,
                 max_workers);
        return static_cast<int>(max_workers);
    }
    return worker_count;
}

}  // namespace

LocalFileIOPool::WritePermit::WritePermit(WritePermit&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)) {
}

LocalFileIOPool::WritePermit&
LocalFileIOPool::WritePermit::operator=(WritePermit&& other) noexcept {
    if (this != &other) {
        Release();
        owner_ = std::exchange(other.owner_, nullptr);
    }
    return *this;
}

LocalFileIOPool::WritePermit::~WritePermit() {
    Release();
}

void
LocalFileIOPool::WritePermit::Release() noexcept {
    if (owner_ != nullptr) {
        owner_->ReleaseWritePermit();
        owner_ = nullptr;
    }
}

LocalFileIOPool&
LocalFileIOPool::GetInstance() {
    static LocalFileIOPool instance;
    return instance;
}

void
LocalFileIOPool::Configure(int worker_count) {
    worker_count = ClampWorkerCount(worker_count);
    std::lock_guard configure_lock(configure_mutex_);

    std::shared_ptr<Executor> executor_to_resize;
    std::shared_ptr<Executor> retired_executor;
    {
        std::lock_guard executor_lock(executor_mutex_);
        if (worker_count == 0) {
            retired_executor = std::move(executor_);
        } else if (executor_ == nullptr) {
            auto new_executor = std::make_shared<Executor>(
                worker_count,
                Executor::makeDefaultPriorityQueue(2),
                std::make_shared<folly::NamedThreadFactory>("MILVUS_LF_IO_"));
            executor_ = std::move(new_executor);
        } else {
            executor_to_resize = executor_;
        }
    }

    if (executor_to_resize != nullptr &&
        executor_to_resize->numThreads() != static_cast<size_t>(worker_count)) {
        auto old_worker_count = executor_to_resize->numThreads();
        try {
            executor_to_resize->setNumThreads(worker_count);
        } catch (...) {
            auto resize_error = std::current_exception();
            try {
                executor_to_resize->setNumThreads(old_worker_count);
            } catch (const std::exception& rollback_error) {
                LOG_ERROR(
                    "Failed to restore local file I/O worker count to {}: {}",
                    old_worker_count,
                    rollback_error.what());
            }
            std::rethrow_exception(resize_error);
        }
    }
    if (retired_executor != nullptr) {
        retired_executor->join();
    }

    {
        std::lock_guard permit_lock(permit_mutex_);
        write_concurrency_limit_.store(static_cast<size_t>(worker_count),
                                       std::memory_order_release);
    }
    permit_cv_.notify_all();

    LOG_INFO("Set local file I/O worker count to {}", worker_count);
}

folly::Executor::KeepAlive<>
LocalFileIOPool::GetExecutor() const {
    std::lock_guard lock(executor_mutex_);
    if (executor_ == nullptr) {
        return {};
    }
    return folly::getKeepAliveToken(executor_.get());
}

LocalFileIOPool::WritePermit
LocalFileIOPool::AcquireWritePermit() {
    // Keep the default (unlimited) path free of global mutex contention.
    if (write_concurrency_limit_.load(std::memory_order_acquire) == 0) {
        return {};
    }

    std::unique_lock lock(permit_mutex_);
    permit_cv_.wait(lock, [this]() {
        auto limit = write_concurrency_limit_.load(std::memory_order_acquire);
        return limit == 0 || active_write_permits_ < limit;
    });
    if (write_concurrency_limit_.load(std::memory_order_acquire) == 0) {
        return {};
    }
    ++active_write_permits_;
    return WritePermit(this);
}

void
LocalFileIOPool::ReleaseWritePermit() noexcept {
    {
        std::lock_guard lock(permit_mutex_);
        --active_write_permits_;
    }
    permit_cv_.notify_one();
}

LocalFileIOPool::~LocalFileIOPool() {
    std::shared_ptr<Executor> executor;
    {
        std::lock_guard lock(executor_mutex_);
        executor = std::move(executor_);
    }
    if (executor != nullptr) {
        executor->join();
    }
}

}  // namespace milvus::storage
