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
#include <filesystem>
#include <optional>
#include <unordered_set>

#include "storage/DiskFileManagerImpl.h"
#include "storage/IndexLoadPlan.h"

namespace milvus::index {

template <typename T, typename MetadataSource>
T
ReadRequiredIndexMeta(const MetadataSource& source, const char* key) {
    if (!source.HasMeta(key)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "corrupt scalar index: required metadata '{}' is missing",
                  key);
    }
    try {
        return source.template GetMeta<T>(key);
    } catch (const SegcoreError&) {
        throw;
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const std::exception& e) {
        ThrowInfo(
            ErrorCode::DataFormatBroken,
            "corrupt scalar index: metadata '{}' has invalid type/value: {}",
            key,
            e.what());
    }
}

// Owns the directory lease until artifact commit or failure cleanup.
struct IndexDirectoryLoadContext {
    ~IndexDirectoryLoadContext() {
        if (manager && !path.empty() &&
            (files.empty() ||
             !std::all_of(files.begin(), files.end(), [](const auto& file) {
                 return file->file && file->file->Committed();
             }))) {
            manager->RemoveIndexFiles();
        }
    }

    std::shared_ptr<storage::DiskFileManagerImpl> manager;
    std::string path;
    std::vector<std::shared_ptr<storage::MmapFileTarget>> files;
    std::optional<storage::DiskFileManagerImpl::LocalDirWriteLease> lease;
};

inline std::shared_ptr<IndexDirectoryLoadContext>
PlanIndexDirectory(const storage::IndexEntryCatalog& catalog,
                   const std::shared_ptr<storage::DiskFileManagerImpl>& manager,
                   bool retain_on_success,
                   storage::IndexLoadPlan& plan) {
    AssertInfo(manager != nullptr, "Directory load requires DiskFileManager");
    auto context = std::make_shared<IndexDirectoryLoadContext>();
    const auto file_names =
        ReadRequiredIndexMeta<std::vector<std::string>>(catalog, "file_names");
    if (file_names.empty()) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "corrupt scalar index: file_names is empty");
    }
    std::unordered_set<std::string_view> names;
    names.reserve(file_names.size());
    for (const auto& name : file_names) {
        const auto path = std::filesystem::path(name);
        if (name.empty() || path.is_absolute() || path.has_parent_path() ||
            path.filename() != path || name == "." || name == ".." ||
            !names.insert(name).second || !catalog.HasEntry(name)) {
            ThrowInfo(
                ErrorCode::DataFormatBroken,
                "corrupt scalar index: invalid, duplicate or missing file '{}'",
                name);
        }
    }
    context->path = manager->GetLocalIndexObjectPrefix();
    context->lease.emplace(manager->AcquireLocalDirWriteLease(context->path));
    context->manager = manager;
    context->files.reserve(file_names.size());
    plan.entries.reserve(plan.entries.size() + file_names.size());
    for (const auto& name : file_names) {
        const auto size = catalog.At(name).plaintext_size;
        auto file =
            std::make_shared<storage::MmapFileTarget>(storage::MmapFileTarget{
                context->path + "/" + name, size, retain_on_success, nullptr});
        context->files.push_back(file);
        plan.entries.push_back({name, storage::MmapEntryTarget{file, 0, size}});
    }
    return context;
}

}  // namespace milvus::index
