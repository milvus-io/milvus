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
#include <memory>
#include <mutex>
#include <optional>

#include "common/EasyAssert.h"
#include "local/FileSystem.h"

namespace milvus::local {

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

        explicit
        operator bool() const noexcept {
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

    WriteLease
    AcquireWriter();

    // Reject new writers and remove after the final active writer exits.
    void
    RemoveWhenIdle() noexcept;

    // Request removal, wait for it to finish, and rethrow cleanup failures.
    void
    RemoveAndWait();

    const FileSystem&
    Files() const noexcept {
        return subtree_;
    }

 private:
    friend class FileSystem;

    enum class State {
        Open,
        Closing,
        Removed,
    };

    ManagedSubtree(FileSystem parent, Path path);

    bool
    RequestRemoval();

    bool
    BeginRemovalLocked() noexcept;

    void
    ReleaseWriter() noexcept;

    void
    PerformRemoval() noexcept;

    FileSystem parent_;
    Path path_;
    FileSystem subtree_;

    std::mutex mutex_;
    std::condition_variable cv_;
    State state_{State::Open};
    size_t writers_{0};
    bool removal_in_progress_{false};
    std::optional<ErrorCode> cleanup_error_code_;
};

}  // namespace milvus::local
