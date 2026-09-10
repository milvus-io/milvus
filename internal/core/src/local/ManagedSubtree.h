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

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>

#include "local/FileSystem.h"

namespace milvus::local {

// One directory owner shared by every writer and cleanup caller. Coordination
// applies only to this object, not other handles naming the same/overlapping
// directory. The owner must prevent unleased writes, ancestor deletion, and
// path reuse until cleanup succeeds (including any explicit retries).
class ManagedSubtree final
    : public std::enable_shared_from_this<ManagedSubtree> {
 public:
    class WriteLease final {
     public:
        WriteLease() = default;

        WriteLease(const WriteLease&) = delete;
        WriteLease&
        operator=(const WriteLease&) = delete;

        WriteLease(WriteLease&& other) noexcept = default;
        WriteLease&
        operator=(WriteLease&& other) noexcept;

        ~WriteLease();

        explicit operator bool() const noexcept {
            return directory_ != nullptr;
        }

     private:
        friend class ManagedSubtree;

        explicit WriteLease(std::shared_ptr<ManagedSubtree> directory);

        void
        Release() noexcept;

        std::shared_ptr<ManagedSubtree> directory_;
    };

    ManagedSubtree(const ManagedSubtree&) = delete;
    ManagedSubtree&
    operator=(const ManagedSubtree&) = delete;

    // Hold the lease through all writes, including native-library operations.
    // Once removal is requested, writer acquisition is permanently rejected.
    WriteLease
    AcquireWriter();

    // Reject new writers and remove after the final active writer exits.
    // May perform synchronous I/O. Does not retry a failed attempt; an owner
    // that needs failure recovery must keep this object and use RemoveAndWait.
    void
    RemoveWhenIdle() noexcept;

    // Join the current attempt, or retry a previously failed removal once.
    // Waiters rethrow their attempt's original exception even if a later retry
    // has started. Must not be called while holding a writer lease on this owner.
    void
    RemoveAndWait();

    // This handle does not acquire leases or initiate cleanup automatically.
    const FileSystem&
    Files() const noexcept {
        return subtree_;
    }

 private:
    friend class FileSystem;

    enum class State {
        Open,
        Closing,
        Removing,
        Failed,
        Removed,
    };

    struct RemovalAttempt {
        bool complete{false};
        std::exception_ptr error;
    };

    ManagedSubtree(FileSystem parent, Path path);

    // Capture an attempt under the lock; run any ready removal outside it.
    std::shared_ptr<RemovalAttempt>
    RequestRemoval(bool retry_failed);

    // With mutex_ held, claim removal once all writer leases have exited.
    bool
    BeginRemovalLocked() noexcept;

    void
    ReleaseWriter() noexcept;

    // Publish completion and retain the original exception for this attempt.
    void
    PerformRemoval() noexcept;

    FileSystem parent_;
    Path path_;
    FileSystem subtree_;

    std::mutex mutex_;
    std::condition_variable cv_;
    State state_{State::Open};
    size_t writers_{0};
    // Preallocate the first attempt so destructor-driven cleanup needs no new
    // attempt allocation. Access attempt fields only with mutex_ held.
    std::shared_ptr<RemovalAttempt> removal_attempt_{
        std::make_shared<RemovalAttempt>()};
};

}  // namespace milvus::local
