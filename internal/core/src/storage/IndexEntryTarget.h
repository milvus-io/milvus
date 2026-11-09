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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/FileWriter.h"

namespace milvus::storage {

struct MemoryEntryTarget {
    std::shared_ptr<void> owner;
    uint8_t* data;
    size_t bytes;
};

// Shared by entries writing different regions of the same local file.
// Prepare, Finish and Cleanup run on LocalFileIOPool. All writes must drain
// before Finish, Commit or Cleanup; cleanup precedes releasing directory
// leases.
struct IndexFileTarget {
    const std::string path;
    const size_t file_size;
    const bool retain_on_success;
    // Legacy byte concatenation can require BUFFERED writes when physical
    // payload boundaries are unaligned. Nullopt retains the global policy.
    const std::optional<FileWriter::WriteMode> write_mode;

    // Describes the destination without opening it.
    IndexFileTarget(
        std::string path,
        size_t file_size,
        bool retain_on_success,
        std::optional<FileWriter::WriteMode> write_mode = std::nullopt);
    IndexFileTarget(const IndexFileTarget&) = delete;
    IndexFileTarget&
    operator=(const IndexFileTarget&) = delete;
    ~IndexFileTarget();

    // Opens the writer once; validates the size before touching the file.
    void
    Prepare(io::Priority priority);
    bool
    Prepared() const noexcept {
        return writer_ != nullptr || finished_;
    }
    // Writes a non-overlapping range using the shared write limiter.
    void
    WriteAt(size_t offset, const void* data, size_t bytes);
    // Closes the writer, retaining ownership for failure cleanup.
    void
    Finish();
    // Called after index finalization; preserves only retained targets.
    void
    Commit();
    bool
    Committed() const noexcept {
        return committed_;
    }
    // Removes an uncommitted file, even when other shared owners remain.
    // Safe to repeat; destruction also calls this as a fallback.
    void
    Cleanup() noexcept;

 private:
    std::unique_ptr<PositionedFileWriter> writer_;
    bool finished_{false};
    bool committed_{false};
};

struct FileEntryTarget {
    std::shared_ptr<IndexFileTarget> staging;
    size_t offset;
    // Reserved file region, including any entry-tail alignment padding.
    size_t bytes;
};

using EntryTarget = std::variant<MemoryEntryTarget, FileEntryTarget>;

inline size_t
EntryTargetSize(const EntryTarget& target) {
    return std::visit([](const auto& value) { return value.bytes; }, target);
}

// Index code chooses destinations; AsyncIndexEntryReader derives slices and
// CRCs from the immutable directory.
struct EntryLoadPlan {
    std::string name;
    EntryTarget target;
};

inline std::vector<std::shared_ptr<IndexFileTarget>>
CollectIndexFileTargets(const std::vector<EntryLoadPlan>& entries) {
    std::vector<std::shared_ptr<IndexFileTarget>> targets;
    targets.reserve(entries.size());
    std::unordered_set<const IndexFileTarget*> seen;
    seen.reserve(entries.size());
    for (const auto& entry : entries) {
        const auto* mmap_target = std::get_if<FileEntryTarget>(&entry.target);
        if (mmap_target == nullptr || mmap_target->staging == nullptr) {
            continue;
        }
        if (seen.insert(mmap_target->staging.get()).second) {
            targets.push_back(mmap_target->staging);
        }
    }
    return targets;
}

inline void
CleanupUncommittedFileTargets(
    const std::vector<std::shared_ptr<IndexFileTarget>>& targets) noexcept {
    for (const auto& target : targets) {
        if (target != nullptr) {
            target->Cleanup();
        }
    }
}

}  // namespace milvus::storage
