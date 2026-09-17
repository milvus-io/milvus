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
#include <memory>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <unordered_set>
#include <type_traits>
#include <variant>
#include <vector>

#include "common/EasyAssert.h"
#include "pb/common.pb.h"
#include "storage/IndexEntryCatalog.h"
#include "storage/StagingIndexFile.h"

namespace milvus::storage {

struct MemoryEntryTarget {
    std::shared_ptr<void> owner;
    uint8_t* data;
    size_t bytes;
};

struct IndexFileTarget {
    std::string path;
    size_t file_size;
    bool retain_on_success;
    std::shared_ptr<StagingIndexFile> file;
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

// Index code chooses destinations; AsyncIndexEntryReader derives slices and CRCs
// from the immutable catalog.
struct EntryLoadPlan {
    std::string name;
    EntryTarget target;
};

struct MaterializedEntry {
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
        if (target != nullptr && target->file != nullptr &&
            !target->file->Committed()) {
            target->file.reset();
        }
    }
}

class IndexLoadArtifact {
 public:
    IndexLoadArtifact() = default;

    IndexLoadArtifact(const IndexLoadArtifact&) = delete;
    IndexLoadArtifact&
    operator=(const IndexLoadArtifact&) = delete;

    IndexLoadArtifact(IndexLoadArtifact&& other) noexcept
        : entries_(std::move(other.entries_)),
          cleanup_targets_(std::move(other.cleanup_targets_)) {
        other.cleanup_targets_.clear();
    }

    IndexLoadArtifact&
    operator=(IndexLoadArtifact&& other) noexcept {
        if (this != &other) {
            CleanupUncommittedFileTargets(cleanup_targets_);
            entries_ = std::move(other.entries_);
            cleanup_targets_ = std::move(other.cleanup_targets_);
            other.cleanup_targets_.clear();
        }
        return *this;
    }

    ~IndexLoadArtifact() {
        CleanupUncommittedFileTargets(cleanup_targets_);
    }

    const std::vector<MaterializedEntry>&
    Entries() const noexcept {
        return entries_;
    }

    const MaterializedEntry&
    At(std::string_view name) const {
        auto it = std::find_if(
            entries_.begin(), entries_.end(), [name](const auto& entry) {
                return entry.name == name;
            });
        AssertInfo(
            it != entries_.end(), "Materialized Entry not found: {}", name);
        return *it;
    }

    MaterializedEntry&
    At(std::string_view name) {
        return const_cast<MaterializedEntry&>(std::as_const(*this).At(name));
    }

    void
    CommitTargets() {
        for (auto& entry : entries_) {
            if (auto* mmap_target =
                    std::get_if<FileEntryTarget>(&entry.target)) {
                AssertInfo(mmap_target->staging != nullptr &&
                               mmap_target->staging->file != nullptr,
                           "Cannot commit unprepared mmap target '{}'",
                           mmap_target->staging == nullptr
                               ? std::string("<null>")
                               : mmap_target->staging->path);
                if (mmap_target->staging->retain_on_success) {
                    mmap_target->staging->file->Commit();
                }
            }
        }
    }

 private:
    friend class AsyncIndexEntryReader;

    std::vector<MaterializedEntry> entries_;
    std::vector<std::shared_ptr<IndexFileTarget>> cleanup_targets_;
};

}  // namespace milvus::storage
