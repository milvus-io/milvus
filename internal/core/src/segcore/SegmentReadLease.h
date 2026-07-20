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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include <folly/CancellationToken.h>

#include "common/EasyAssert.h"
#include "common/OpContext.h"

namespace milvus::segcore {

struct SegmentReadGateState {
    std::mutex mutex;
    std::condition_variable cv;
    uint64_t active_readers = 0;
    uint64_t blocked_readers_total = 0;
    uint64_t published_generation = 0;
    bool writer_pending = false;
    bool writer_active = false;
};

class SegmentReadGate;

class SegmentReadLease {
 public:
    SegmentReadLease(const SegmentReadLease&) = delete;
    SegmentReadLease&
    operator=(const SegmentReadLease&) = delete;

    SegmentReadLease(SegmentReadLease&& other) noexcept
        : state_(std::move(other.state_)) {
    }

    SegmentReadLease&
    operator=(SegmentReadLease&& other) noexcept {
        if (this != &other) {
            Release();
            state_ = std::move(other.state_);
        }
        return *this;
    }

    ~SegmentReadLease() {
        Release();
    }

    bool
    valid() const {
        return state_ != nullptr;
    }

 private:
    friend class SegmentReadGate;

    SegmentReadLease() = default;

    void
    Release() noexcept {
        auto state = std::move(state_);
        if (state == nullptr) {
            return;
        }

        bool notify_writer = false;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->active_readers > 0) {
                --state->active_readers;
                notify_writer = state->active_readers == 0;
            }
        }
        if (notify_writer) {
            state->cv.notify_all();
        }
    }

    std::shared_ptr<SegmentReadGateState> state_;
};

class PublishLease {
 public:
    PublishLease(const PublishLease&) = delete;
    PublishLease&
    operator=(const PublishLease&) = delete;

    PublishLease(PublishLease&& other) noexcept
        : state_(std::move(other.state_)) {
    }

    PublishLease&
    operator=(PublishLease&& other) noexcept {
        if (this != &other) {
            Release();
            state_ = std::move(other.state_);
        }
        return *this;
    }

    ~PublishLease() {
        Release();
    }

    bool
    valid() const {
        return state_ != nullptr;
    }

    void
    MarkPublished() {
        AssertInfo(state_ != nullptr, "publish lease is not active");
        std::lock_guard<std::mutex> lock(state_->mutex);
        ++state_->published_generation;
    }

 private:
    friend class SegmentReadGate;

    explicit PublishLease(std::shared_ptr<SegmentReadGateState> state)
        : state_(std::move(state)) {
    }

    void
    Release() noexcept {
        auto state = std::move(state_);
        if (state == nullptr) {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->writer_active = false;
            state->writer_pending = false;
        }
        state->cv.notify_all();
    }

    std::shared_ptr<SegmentReadGateState> state_;
};

class SegmentReadGate {
 public:
    SegmentReadGate() : state_(std::make_shared<SegmentReadGateState>()) {
    }

    std::shared_ptr<SegmentReadLease>
    AcquireRead(const folly::CancellationToken& cancel_token,
                int64_t segment_id) const {
        auto lease = std::shared_ptr<SegmentReadLease>(new SegmentReadLease());
        std::unique_lock<std::mutex> lock(state_->mutex);
        bool recorded_block = false;
        while (state_->writer_pending || state_->writer_active) {
            if (!recorded_block) {
                ++state_->blocked_readers_total;
                recorded_block = true;
            }
            if (cancel_token.isCancellationRequested()) {
                ThrowInfo(ErrorCode::FollyCancel,
                          "read lease acquisition cancelled for segment {}",
                          segment_id);
            }
            state_->cv.wait_for(lock, std::chrono::milliseconds(10));
        }
        ++state_->active_readers;
        lease->state_ = state_;
        return lease;
    }

    PublishLease
    AcquirePublish(milvus::OpContext* op_ctx, int64_t segment_id) const {
        std::unique_lock<std::mutex> lock(state_->mutex);
        AssertInfo(!state_->writer_pending && !state_->writer_active,
                   "concurrent publisher entered segment {} read gate",
                   segment_id);
        state_->writer_pending = true;

        while (state_->active_readers != 0) {
            if (op_ctx != nullptr &&
                op_ctx->cancellation_token.isCancellationRequested()) {
                state_->writer_pending = false;
                lock.unlock();
                state_->cv.notify_all();
                ThrowInfo(ErrorCode::FollyCancel,
                          "publication drain cancelled for segment {}",
                          segment_id);
            }
            state_->cv.wait_for(lock, std::chrono::milliseconds(10));
        }

        state_->writer_active = true;
        return PublishLease(state_);
    }

    uint64_t
    ActiveReaders() const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        return state_->active_readers;
    }

    uint64_t
    BlockedReadersTotal() const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        return state_->blocked_readers_total;
    }

    uint64_t
    PublishedGeneration() const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        return state_->published_generation;
    }

    bool
    WriterPending() const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        return state_->writer_pending;
    }

 private:
    std::shared_ptr<SegmentReadGateState> state_;
};

}  // namespace milvus::segcore
