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
#include <any>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <type_traits>
#include <variant>
#include <vector>

#include "common/EasyAssert.h"
#include "pb/common.pb.h"
#include "storage/IndexEntryCatalog.h"
#include "storage/WritableMmapFile.h"

namespace milvus::storage {

struct MemoryEntryTarget {
    std::shared_ptr<void> owner;
    uint8_t* data;
    size_t bytes;
};

struct MmapFileTarget {
    std::string path;
    size_t file_size;
    bool retain_on_success;
    std::shared_ptr<WritableMmapFile> file;
};

struct MmapEntryTarget {
    std::shared_ptr<MmapFileTarget> staging;
    size_t offset;
    size_t bytes;
};

using EntryTarget = std::variant<MemoryEntryTarget, MmapEntryTarget>;

inline size_t
EntryTargetSize(const EntryTarget& target) {
    return std::visit([](const auto& value) { return value.bytes; }, target);
}

inline std::span<uint8_t>
EntryTargetRegion(EntryTarget& target, size_t offset, size_t bytes) {
    AssertInfo(offset <= EntryTargetSize(target) &&
                   bytes <= EntryTargetSize(target) - offset,
               "Entry target region exceeds target size {}",
               EntryTargetSize(target));
    return std::visit(
        [offset, bytes](auto& value) -> std::span<uint8_t> {
            using Target = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Target, MemoryEntryTarget>) {
                AssertInfo(value.data != nullptr || bytes == 0,
                           "Memory Entry target is null");
                if (bytes == 0) {
                    return {};
                }
                return {value.data + offset, bytes};
            } else {
                AssertInfo(value.staging != nullptr,
                           "Mmap Entry staging descriptor is null");
                AssertInfo(value.staging->file != nullptr,
                           "Mmap Entry target file '{}' is not prepared",
                           value.staging->path);
                AssertInfo(
                    value.offset <= value.staging->file_size &&
                        offset <= value.staging->file_size - value.offset &&
                        bytes <=
                            value.staging->file_size - value.offset - offset,
                    "Mmap Entry target region exceeds staging file '{}' "
                    "size {}",
                    value.staging->path,
                    value.staging->file_size);
                return value.staging->file->Region(value.offset + offset,
                                                   bytes);
            }
        },
        target);
}

struct SlicePlan {
    size_t seq;
    uint64_t entry_offset;
    size_t remote_bytes;
    size_t target_offset;
    size_t target_bytes;

    // Conservative Milvus admission charge. This is not CRT's actual
    // allocation and does not establish a hard memory bound.
    size_t admission_bytes;
};

struct EntryLoadPlan {
    std::string name;
    size_t entry_size;
    uint32_t expected_crc;
    std::vector<SlicePlan> slices;
    EntryTarget target;
    bool required{true};
};

struct IndexLoadPlan {
    std::vector<EntryLoadPlan> entries;
    proto::common::LoadPriority priority{proto::common::LoadPriority::HIGH};

    // Index-specific metadata needed only by FinalizeLoad(). The common
    // materializer carries it without interpreting it.
    std::any finalize_context;

    // Internal request/callback pressure bound. Zero selects the shared
    // LoadExecutor worker count.
    size_t max_inflight_slices{0};
};

inline EntryLoadPlan
MakePlainEntryLoadPlan(const IndexEntryCatalog& catalog,
                       std::string_view name,
                       EntryTarget target,
                       size_t slice_size,
                       bool required = true) {
    AssertInfo(slice_size > 0, "Plain Entry Slice size must be positive");
    const auto& entry = catalog.At(name);
    AssertInfo(std::holds_alternative<PlainEntrySource>(entry.source),
               "Cannot build plaintext load plan for encrypted Entry '{}'",
               name);
    const auto& source = std::get<PlainEntrySource>(entry.source);
    AssertInfo(source.remote_bytes == entry.plaintext_size,
               "Plain Entry '{}' source size {} differs from plaintext size "
               "{}",
               name,
               source.remote_bytes,
               entry.plaintext_size);
    AssertInfo(EntryTargetSize(target) >= entry.plaintext_size,
               "Entry '{}' target size {} is smaller than Entry size {}",
               name,
               EntryTargetSize(target),
               entry.plaintext_size);

    EntryLoadPlan plan{std::string(name),
                       entry.plaintext_size,
                       entry.expected_crc,
                       {},
                       std::move(target),
                       required};
    for (size_t offset = 0, seq = 0; offset < entry.plaintext_size; ++seq) {
        auto bytes = std::min(slice_size, entry.plaintext_size - offset);
        plan.slices.push_back(
            SlicePlan{seq, offset, bytes, offset, bytes, bytes});
        offset += bytes;
    }
    return plan;
}

struct MaterializedEntry {
    std::string name;
    EntryTarget target;
    bool ready{false};
};

inline std::vector<std::shared_ptr<MmapFileTarget>>
CollectMmapFileTargets(const std::vector<EntryLoadPlan>& entries) {
    std::vector<std::shared_ptr<MmapFileTarget>> targets;
    for (const auto& entry : entries) {
        const auto* mmap_target = std::get_if<MmapEntryTarget>(&entry.target);
        if (mmap_target == nullptr || mmap_target->staging == nullptr) {
            continue;
        }
        if (std::find(targets.begin(), targets.end(), mmap_target->staging) ==
            targets.end()) {
            targets.push_back(mmap_target->staging);
        }
    }
    return targets;
}

inline void
CleanupUncommittedMmapTargets(
    const std::vector<std::shared_ptr<MmapFileTarget>>& targets) noexcept {
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
          finalize_context_(std::move(other.finalize_context_)),
          cleanup_targets_(std::move(other.cleanup_targets_)) {
        other.cleanup_targets_.clear();
    }

    IndexLoadArtifact&
    operator=(IndexLoadArtifact&& other) noexcept {
        if (this != &other) {
            CleanupUncommittedMmapTargets(cleanup_targets_);
            entries_ = std::move(other.entries_);
            finalize_context_ = std::move(other.finalize_context_);
            cleanup_targets_ = std::move(other.cleanup_targets_);
            other.cleanup_targets_.clear();
        }
        return *this;
    }

    ~IndexLoadArtifact() {
        CleanupUncommittedMmapTargets(cleanup_targets_);
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
                    std::get_if<MmapEntryTarget>(&entry.target)) {
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

    template <typename Context>
    Context&
    FinalizeContext() {
        return std::any_cast<Context&>(finalize_context_);
    }

    template <typename Context>
    const Context&
    FinalizeContext() const {
        return std::any_cast<const Context&>(finalize_context_);
    }

 private:
    friend class IndexMaterializerAccess;

    std::vector<MaterializedEntry> entries_;
    std::any finalize_context_;
    std::vector<std::shared_ptr<MmapFileTarget>> cleanup_targets_;
};

}  // namespace milvus::storage
