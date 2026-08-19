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
    if (state_ != State::Closing || writers_ != 0 || removal_in_progress_) {
        return false;
    }
    removal_in_progress_ = true;
    return true;
}

bool
ManagedSubtree::RequestRemoval() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == State::Removed) {
        return false;
    }
    state_ = State::Closing;
    return BeginRemovalLocked();
}

void
ManagedSubtree::RemoveWhenIdle() noexcept {
    try {
        if (RequestRemoval()) {
            PerformRemoval();
        }
    } catch (...) {
        // RequestRemoval only mutates in-memory state. Keep this method
        // noexcept even if a future implementation changes that detail.
    }
}

void
ManagedSubtree::RemoveAndWait() {
    if (RequestRemoval()) {
        PerformRemoval();
    }

    std::optional<ErrorCode> cleanup_error_code;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return state_ == State::Removed; });
        cleanup_error_code = cleanup_error_code_;
    }
    if (cleanup_error_code.has_value()) {
        ThrowInfo(*cleanup_error_code,
                  "failed to remove managed local subtree");
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
    try {
        parent_.RemoveAll(path_);
    } catch (const SegcoreError& error) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cleanup_error_code_ = error.get_error_code();
            removal_in_progress_ = false;
            state_ = State::Removed;
        }
        cv_.notify_all();
        return;
    } catch (const std::exception&) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cleanup_error_code_ = ErrorCode::UnexpectedError;
            removal_in_progress_ = false;
            state_ = State::Removed;
        }
        cv_.notify_all();
        return;
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cleanup_error_code_ = ErrorCode::UnexpectedError;
            removal_in_progress_ = false;
            state_ = State::Removed;
        }
        cv_.notify_all();
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        cleanup_error_code_.reset();
        removal_in_progress_ = false;
        state_ = State::Removed;
    }
    cv_.notify_all();
}

}  // namespace milvus::local
