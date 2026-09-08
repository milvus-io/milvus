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

#include "local/ManagedSubtree.h"

#include <exception>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::local {

ManagedSubtree::WriteLease::WriteLease(
    std::shared_ptr<ManagedSubtree> directory)
    : directory_(std::move(directory)) {
}

ManagedSubtree::WriteLease&
ManagedSubtree::WriteLease::operator=(WriteLease&& other) noexcept {
    if (this != &other) {
        Release();
        directory_ = std::move(other.directory_);
    }
    return *this;
}

ManagedSubtree::WriteLease::~WriteLease() {
    Release();
}

void
ManagedSubtree::WriteLease::Release() noexcept {
    auto directory = std::exchange(directory_, nullptr);
    if (directory != nullptr) {
        directory->ReleaseWriter();
    }
}

ManagedSubtree::ManagedSubtree(FileSystem parent, Path path)
    : parent_(std::move(parent)),
      path_(std::move(path)),
      subtree_(parent_.Subtree(path_)) {
}

ManagedSubtree::WriteLease
ManagedSubtree::AcquireWriter() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ != State::Open) {
        ThrowInfo(ErrorCode::FileWriteFailed,
                  "cannot acquire a writer after local subtree cleanup begins");
    }
    ++writers_;
    return WriteLease(shared_from_this());
}

bool
ManagedSubtree::BeginRemovalLocked() noexcept {
    if (state_ != State::Closing || writers_ != 0) {
        return false;
    }
    state_ = State::Removing;
    return true;
}

std::shared_ptr<ManagedSubtree::RemovalAttempt>
ManagedSubtree::RequestRemoval(bool retry_failed) {
    std::shared_ptr<RemovalAttempt> attempt;
    bool remove = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ == State::Failed && retry_failed) {
            // Allocate before changing state: failure leaves the old attempt
            // available. Existing waiters keep that attempt's result.
            removal_attempt_ = std::make_shared<RemovalAttempt>();
            state_ = State::Closing;
        } else if (state_ == State::Open) {
            state_ = State::Closing;
        }
        attempt = removal_attempt_;
        remove = BeginRemovalLocked();
    }
    if (remove) {
        PerformRemoval();
    }
    return attempt;
}

void
ManagedSubtree::RemoveWhenIdle() noexcept {
    try {
        static_cast<void>(RequestRemoval(false));
    } catch (...) {
        // Keep owner destructors noexcept even if locking fails. Filesystem
        // failures are retained by PerformRemoval for explicit recovery.
    }
}

void
ManagedSubtree::RemoveAndWait() {
    const auto attempt = RequestRemoval(true);

    std::exception_ptr cleanup_error;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&attempt]() { return attempt->complete; });
        cleanup_error = attempt->error;
    }
    if (cleanup_error) {
        std::rethrow_exception(cleanup_error);
    }
}

void
ManagedSubtree::ReleaseWriter() noexcept {
    bool remove = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (writers_ == 0) {
            return;
        }
        --writers_;
        remove = BeginRemovalLocked();
    }
    if (remove) {
        PerformRemoval();
    }
}

void
ManagedSubtree::PerformRemoval() noexcept {
    std::exception_ptr cleanup_error;
    try {
        parent_.RemoveAll(path_);
    } catch (...) {
        cleanup_error = std::current_exception();
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        removal_attempt_->error = cleanup_error;
        removal_attempt_->complete = true;
        state_ = cleanup_error ? State::Failed : State::Removed;
    }
    cv_.notify_all();
}

}  // namespace milvus::local
