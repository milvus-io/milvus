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

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <vector>

#include "index/IndexLoadUtils.h"
#include "index/Meta.h"
#include "storage/artifact/FileSourceUtils.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"

namespace milvus::index {

/**
 * @brief Per-load engine directory and null-sidecar targets owned by the plan.
 * @note Mmap readers retain the engine directory after finalization. Null
 * sidecars use bounded memory or separate staging outside that directory.
 */
struct PackedDirectoryTargets {
    std::shared_ptr<storage::LocalDirectory> directory;
    std::vector<std::string> paths;
    std::shared_ptr<storage::LocalDirectory> null_directory;
    std::shared_ptr<storage::IndexFileTarget> null_file;
    std::shared_ptr<std::vector<size_t>> null_offsets;
};

/**
 * @brief Validate the packed inventory and append engine/null destinations.
 * @pre targets.directory exists and plan retains the family state owning
 * targets.
 * @param row_count Known row bound; absent means stage null offsets on disk
 * until the engine supplies a bound. No remote payloads are read during
 * planning.
 * @param retain_files Keep committed engine files for file-backed readers.
 */
inline void
PlanPackedDirectory(const storage::IndexEntryDirectory& directory,
                    const nlohmann::json& metadata,
                    std::span<const std::string_view> reserved_names,
                    bool exact_inventory,
                    bool retain_files,
                    PackedDirectoryTargets& targets,
                    IndexLoadPlan& plan,
                    std::optional<size_t> row_count = std::nullopt) {
    const auto names =
        ReadRequiredIndexMeta<std::vector<std::string>>(metadata, FILE_NAMES);
    const auto has_null = ReadRequiredIndexMeta<bool>(metadata, HAS_NULL);
    if (names.empty() || directory.HasEntry(INDEX_NULL_OFFSET) != has_null) {
        ThrowInfo(
            DataFormatBroken,
            "packed index file_names/has_null metadata disagrees with entries");
    }
    std::set<std::string> expected;
    for (const auto& name : names) {
        storage::ValidateArtifactEntryName(name, "packed index");
        if (name == INDEX_NULL_OFFSET ||
            name == storage::MILVUS_V3_META_ENTRY_NAME ||
            std::find(reserved_names.begin(), reserved_names.end(), name) !=
                reserved_names.end() ||
            !expected.insert(name).second || !directory.HasEntry(name)) {
            ThrowInfo(DataFormatBroken,
                      "invalid, duplicate or missing packed engine entry {}",
                      name);
        }
    }
    if (has_null) {
        expected.emplace(INDEX_NULL_OFFSET);
    }
    if (exact_inventory) {
        size_t observed = 0;
        for (const auto& entry : directory.Entries()) {
            if (entry.name == storage::MILVUS_V3_META_ENTRY_NAME) {
                continue;
            }
            ++observed;
            if (!expected.contains(entry.name)) {
                ThrowInfo(DataFormatBroken,
                          "unexpected packed index entry {}",
                          entry.name);
            }
        }
        if (observed != expected.size()) {
            ThrowInfo(DataFormatBroken,
                      "packed index inventory disagrees with metadata");
        }
    }
    targets.paths.reserve(names.size());
    plan.entries.reserve(plan.entries.size() + names.size() + has_null);
    for (const auto& name : names) {
        const auto bytes = directory.At(name).plaintext_size;
        auto path = targets.directory->Path() + "/" + name;
        auto file = std::make_shared<storage::IndexFileTarget>(
            path, bytes, retain_files);
        plan.entries.push_back(
            {name, storage::FileEntryTarget{std::move(file), 0, bytes}});
        targets.paths.push_back(std::move(path));
    }
    size_t null_bytes = 0;
    if (has_null) {
        null_bytes = directory.At(INDEX_NULL_OFFSET).plaintext_size;
        if (null_bytes == 0 || null_bytes % sizeof(size_t) != 0) {
            ThrowInfo(DataFormatBroken,
                      "invalid packed null-offset byte size {}",
                      null_bytes);
        }
    }
    if (!has_null || row_count.has_value()) {
        if (row_count && null_bytes / sizeof(size_t) > *row_count) {
            ThrowInfo(DataFormatBroken,
                      "packed null-offset count exceeds row count");
        }
        targets.null_offsets =
            std::make_shared<std::vector<size_t>>(null_bytes / sizeof(size_t));
        if (has_null) {
            plan.entries.push_back(
                {INDEX_NULL_OFFSET,
                 storage::MemoryEntryTarget{
                     targets.null_offsets,
                     reinterpret_cast<uint8_t*>(targets.null_offsets->data()),
                     null_bytes}});
        }
    } else {
        // Engine row count is unavailable until open. Stage untrusted sidecar
        // sizes without allocating their entire declared payload on the heap.
        targets.null_directory = storage::LocalDirectory::CreateOwned(
            std::filesystem::path(targets.directory->Path())
                .parent_path()
                .string(),
            "packed-null-XXXXXX",
            "packed null offsets");
        targets.null_file = std::make_shared<storage::IndexFileTarget>(
            targets.null_directory->Path() + "/" + INDEX_NULL_OFFSET,
            null_bytes,
            false);
        plan.entries.push_back(
            {INDEX_NULL_OFFSET,
             storage::FileEntryTarget{targets.null_file, 0, null_bytes}});
    }
}

/**
 * @brief Return planned null offsets or read the staged sidecar after engine
 * open.
 * @param count Engine row bound, used before allocating a staged sidecar in
 * memory.
 * @param nested Preserve inverted indexes' element-vs-row count semantics.
 * @pre Planned reads/writes are complete; local file I/O is allowed on this
 * thread.
 */
inline std::shared_ptr<const std::vector<size_t>>
FinishPackedNullOffsets(PackedDirectoryTargets& targets,
                        size_t count,
                        bool nested = false) {
    if (targets.null_offsets) {
        return targets.null_offsets;
    }
    AssertInfo(targets.null_file != nullptr,
               "missing packed null-offset target");
    const auto bytes = targets.null_file->file_size;
    if (!nested && bytes / sizeof(size_t) > count) {
        ThrowInfo(DataFormatBroken,
                  "packed null-offset count exceeds row count");
    }
    auto offsets =
        std::make_shared<std::vector<size_t>>(bytes / sizeof(size_t));
    const auto& path = targets.null_file->path;
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        ThrowInfo(FileOpenFailed,
                  "failed to open packed null-offset file {}: {}",
                  path,
                  std::strerror(errno));
    }
    storage::FileDescriptorGuard guard(fd);
    storage::ReadAll(fd, offsets->data(), bytes, path, "packed null offsets");
    guard.CloseChecked(path, "packed null offsets");
    return offsets;
}

}  // namespace milvus::index
