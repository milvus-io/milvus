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

#include "mmap/VortexColumnGroup.h"

#include <memory>
#include <string_view>
#include <utility>

#include <fmt/format.h>

#include "arrow/filesystem/filesystem.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "mmap/SparseVortexFileSystem.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/format/vortex/vortex_footer_reader.h"
#include "milvus-storage/format/vortex/vortex_planner.h"
#include "milvus-storage/format/vortex/vortex_translater.h"

namespace milvus {

namespace {

[[noreturn]] void
ThrowVortexStatus(const arrow::Status& status,
                  ErrorCode fallback_code,
                  std::string_view action) {
    auto code = fallback_code;
    // The Vortex bridge also uses IOError for decode failures, so the caller
    // owns the fallback classification instead of mapping IOError globally.
    if (status.IsOutOfMemory()) {
        code = ErrorCode::MemAllocateFailed;
    } else if (status.IsCancelled()) {
        code = ErrorCode::FollyCancel;
    }
    ThrowInfo(code, "{}: {}", action, status.ToString());
}

}  // namespace

VortexColumnGroup::VortexColumnGroup(
    const std::vector<VortexColumnFileInfo>& files,
    std::shared_ptr<milvus_storage::api::Properties> properties,
    const std::vector<std::string>& field_names,
    CacheWarmupPolicy cache_warmup_policy,
    milvus::OpContext* op_ctx)
    : num_fields_(field_names.size()) {
    AssertInfo(properties != nullptr, "vortex properties is null");
    AssertInfo(!files.empty(), "vortex column group has no files");
    AssertInfo(!field_names.empty(), "vortex column group has no fields");
    files_.reserve(files.size());
    num_rows_until_chunk_.reserve(files.size() + 1);
    num_rows_until_chunk_.push_back(0);
    int64_t row_prefix = 0;

    for (const auto& file : files) {
        auto fs_result = milvus_storage::FilesystemCache::getInstance().get(
            *properties, file.path);
        if (!fs_result.ok()) {
            ThrowVortexStatus(fs_result.status(),
                              ErrorCode::FileOpenFailed,
                              fmt::format("failed to get filesystem for vortex "
                                          "file {}",
                                          file.path));
        }
        auto uri_result = milvus_storage::StorageUri::Parse(file.path);
        if (!uri_result.ok()) {
            ThrowVortexStatus(
                uri_result.status(),
                ErrorCode::PathInvalid,
                fmt::format("failed to parse vortex file uri {}", file.path));
        }

        FileState state;
        state.path = file.path;
        state.resolved_path = uri_result.ValueOrDie().key;
        state.source_fs = fs_result.ValueOrDie();
        state.sparse_path = MakeSparseVortexPath(state.resolved_path);
        state.sparse_fs = MakeSparseVortexFileSystem(state.sparse_path);
        state.footer_reader =
            std::make_shared<milvus_storage::vortex::VortexFooterReader>(
                state.sparse_fs,
                state.sparse_path,
                state.resolved_path,
                file.file_size,
                file.footer_size);
        auto open_status = state.footer_reader->Open(state.source_fs);
        if (!open_status.ok()) {
            ThrowVortexStatus(
                open_status,
                ErrorCode::DataFormatBroken,
                fmt::format("failed to open vortex file {}", file.path));
        }

        auto cell_metas_result =
            milvus_storage::vortex::BuildVortexGroupCellMetas(
                state.footer_reader, field_names);
        if (!cell_metas_result.ok()) {
            ThrowVortexStatus(
                cell_metas_result.status(),
                ErrorCode::DataFormatBroken,
                fmt::format("failed to build vortex group cell metas for file "
                            "{}",
                            file.path));
        }
        auto cell_metas = std::move(cell_metas_result).ValueOrDie();

        auto planner_result = milvus_storage::vortex::VortexPlanner::MakeGroup(
            state.footer_reader, cell_metas);
        if (!planner_result.ok()) {
            ThrowVortexStatus(
                planner_result.status(),
                ErrorCode::DataFormatBroken,
                fmt::format("failed to create vortex group planner for file {}",
                            file.path));
        }
        auto planner = std::move(planner_result).ValueOrDie();

        auto translater_result = milvus_storage::vortex::VortexTranslater::Make(
            std::move(cell_metas),
            state.source_fs,
            state.resolved_path,
            state.sparse_fs,
            state.sparse_path,
            cache_warmup_policy);
        if (!translater_result.ok()) {
            ThrowVortexStatus(
                translater_result.status(),
                ErrorCode::FileOpenFailed,
                fmt::format(
                    "failed to create vortex group translator for file {}",
                    file.path));
        }
        std::unique_ptr<
            cachinglayer::Translator<milvus_storage::vortex::VortexCellGuard>>
            translater = std::move(translater_result).ValueOrDie();
        state.slot = cachinglayer::Manager::GetInstance().CreateCacheSlot(
            std::move(translater), op_ctx);
        state.rows = static_cast<int64_t>(planner->rows());
        state.memory_bytes = planner->memory_bytes();
        state.planner = planner;

        if (file.end_index > file.start_index) {
            AssertInfo(file.end_index - file.start_index == state.rows,
                       "vortex file {} row range [{}, {}) does not match "
                       "reader rows {}",
                       state.path,
                       file.start_index,
                       file.end_index,
                       state.rows);
        }

        row_prefix += state.rows;
        num_rows_until_chunk_.push_back(row_prefix);
        files_.emplace_back(std::move(state));
    }
    num_rows_ = row_prefix;
}

VortexColumnGroup::~VortexColumnGroup() {
    CancelWarmup();
}

void
VortexColumnGroup::ManualEvictCache() const {
    for (const auto& file : files_) {
        file.slot->ManualEvictAll();
    }
}

void
VortexColumnGroup::CancelWarmup() {
    for (const auto& file : files_) {
        file.slot->CancelWarmup();
    }
}

const std::vector<VortexColumnGroup::FileState>&
VortexColumnGroup::files() const {
    return files_;
}

const std::vector<int64_t>&
VortexColumnGroup::num_rows_until_chunk() const {
    return num_rows_until_chunk_;
}

int64_t
VortexColumnGroup::num_rows() const {
    return num_rows_;
}

size_t
VortexColumnGroup::memory_size() const {
    size_t bytes = 0;
    for (const auto& file : files_) {
        bytes += file.memory_bytes;
    }
    return bytes;
}

size_t
VortexColumnGroup::num_fields() const {
    return num_fields_;
}

}  // namespace milvus
