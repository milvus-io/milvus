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

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <memory>
#include <string_view>
#include <utility>

#include <fmt/format.h>

#include "arrow/filesystem/filesystem.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "mmap/SparseVortexFileSystem.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/format/vortex/vortex_footer_reader.h"
#include "milvus-storage/format/vortex/vortex_planner.h"
#include "milvus-storage/format/vortex/vortex_translater.h"

namespace milvus {

namespace {

std::atomic<uint64_t> g_vortex_sparse_path_generation{0};

std::string
MakeFileBackedSparsePath(const VortexColumnGroup::Options& options,
                         size_t file_index) {
    AssertInfo(!options.mmap_dir_path.empty(),
               "vortex file-backed sparse file requires mmap dir path");
    const auto generation =
        g_vortex_sparse_path_generation.fetch_add(1, std::memory_order_relaxed);
    return (std::filesystem::path(options.mmap_dir_path) / "vortex" /
            fmt::format("seg_{}_cg_{}_file_{}_{}.vortex",
                        options.segment_id,
                        options.column_group_index,
                        file_index,
                        generation))
        .string();
}

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
    : VortexColumnGroup(files,
                        std::move(properties),
                        field_names,
                        cache_warmup_policy,
                        op_ctx,
                        Options{}) {
}

VortexColumnGroup::VortexColumnGroup(
    const std::vector<VortexColumnFileInfo>& files,
    std::shared_ptr<milvus_storage::api::Properties> properties,
    const std::vector<std::string>& field_names,
    CacheWarmupPolicy cache_warmup_policy,
    milvus::OpContext* op_ctx,
    Options options)
    : num_fields_(field_names.size()) {
    AssertInfo(properties != nullptr, "vortex properties is null");
    AssertInfo(!files.empty(), "vortex column group has no files");
    AssertInfo(!field_names.empty(), "vortex column group has no fields");
    int64_t expected_start = 0;
    for (const auto& file : files) {
        if (file.start_index != expected_start) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "vortex file {} starts at {}, expected {}",
                      file.path,
                      file.start_index,
                      expected_start);
        }
        if (file.end_index <= file.start_index) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "vortex file {} has invalid row range [{}, {})",
                      file.path,
                      file.start_index,
                      file.end_index);
        }
        expected_start = file.end_index;
    }
    files_.reserve(files.size());
    num_rows_until_chunk_.reserve(files.size() + 1);
    num_rows_until_chunk_.push_back(0);
    int64_t row_prefix = 0;

    for (size_t file_index = 0; file_index < files.size(); ++file_index) {
        const auto& file = files[file_index];
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
        state.start_index = file.start_index;
        state.end_index = file.end_index;
        const auto resolved_path = uri_result.ValueOrDie().key;
        auto source_fs = fs_result.ValueOrDie();
        state.sparse_path = MakeSparseVortexPath(resolved_path);
        SparseVortexFileSystemOptions sparse_options;
        sparse_options.backing = options.sparse_file_backing;
        sparse_options.mmap_populate = options.mmap_populate;
        if (options.sparse_file_backing != SparseVortexFileBacking::Memory) {
            sparse_options.file_path =
                MakeFileBackedSparsePath(options, file_index);
        }
        state.sparse_fs = MakeSparseVortexFileSystem(state.sparse_path,
                                                     std::move(sparse_options));
        state.footer_reader =
            std::make_shared<milvus_storage::vortex::VortexFooterReader>(
                state.sparse_fs,
                state.sparse_path,
                resolved_path,
                file.file_size,
                file.footer_size);
        auto open_status = state.footer_reader->Open(source_fs);
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

        state.field_planners.reserve(field_names.size());
        for (const auto& field_name : field_names) {
            if (state.field_planners.find(field_name) !=
                state.field_planners.end()) {
                continue;
            }
            auto field_cell_metas_result =
                milvus_storage::vortex::BuildVortexCellMetas(
                    state.footer_reader, field_name);
            if (!field_cell_metas_result.ok()) {
                ThrowVortexStatus(field_cell_metas_result.status(),
                                  ErrorCode::DataFormatBroken,
                                  fmt::format("failed to build vortex cell "
                                              "metas for field {} file {}",
                                              field_name,
                                              file.path));
            }
            auto field_planner_result =
                milvus_storage::vortex::VortexPlanner::Make(
                    state.footer_reader,
                    field_name,
                    std::move(field_cell_metas_result).ValueOrDie());
            if (!field_planner_result.ok()) {
                ThrowVortexStatus(
                    field_planner_result.status(),
                    ErrorCode::DataFormatBroken,
                    fmt::format(
                        "failed to create vortex planner for field {} file {}",
                        field_name,
                        file.path));
            }
            auto field_planner = std::move(field_planner_result).ValueOrDie();
            AssertInfo(field_planner->rows() == planner->rows(),
                       "vortex field {} rows {} does not match column group "
                       "rows {} for file {}",
                       field_name,
                       field_planner->rows(),
                       planner->rows(),
                       file.path);
            const auto& field_cells = field_planner->cell_metas();
            const auto& group_cells = planner->cell_metas();
            AssertInfo(field_cells.size() == group_cells.size(),
                       "vortex field {} cells {} does not match column group "
                       "cells {} for file {}",
                       field_name,
                       field_cells.size(),
                       group_cells.size(),
                       file.path);
            for (size_t cell_id = 0; cell_id < field_cells.size(); ++cell_id) {
                AssertInfo(
                    field_cells[cell_id].row_offset ==
                            group_cells[cell_id].row_offset &&
                        field_cells[cell_id].row_count ==
                            group_cells[cell_id].row_count,
                    "vortex field {} cell {} row range [{}, {}) does not "
                    "match column group [{}, {}) for file {}",
                    field_name,
                    cell_id,
                    field_cells[cell_id].row_offset,
                    field_cells[cell_id].row_offset +
                        field_cells[cell_id].row_count,
                    group_cells[cell_id].row_offset,
                    group_cells[cell_id].row_offset +
                        group_cells[cell_id].row_count,
                    file.path);
            }
            state.field_planners.emplace(field_name, std::move(field_planner));
        }

        auto translater_result = milvus_storage::vortex::VortexTranslater::Make(
            std::move(cell_metas),
            source_fs,
            resolved_path,
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
        state.memory_bytes = planner->memory_bytes();
        const auto rows = static_cast<int64_t>(planner->rows());

        if (file.end_index - file.start_index != rows) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "vortex file {} row range [{}, {}) does not match "
                      "reader rows {}",
                      state.path,
                      file.start_index,
                      file.end_index,
                      rows);
        }

        row_prefix = file.end_index;
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

const std::shared_ptr<milvus_storage::vortex::VortexPlanner>&
VortexColumnGroup::FieldPlanner(size_t file_index,
                                std::string_view field_name) const {
    AssertInfo(file_index < files_.size(),
               "vortex file index {} out of range {}",
               file_index,
               files_.size());
    const auto& field_planners = files_[file_index].field_planners;
    auto it = field_planners.find(std::string(field_name));
    AssertInfo(it != field_planners.end(),
               "vortex field {} has no planner in file {}",
               field_name,
               files_[file_index].path);
    return it->second;
}

std::shared_ptr<
    cachinglayer::CellAccessor<milvus_storage::vortex::VortexCellGuard>>
VortexColumnGroup::PinCells(milvus::OpContext* op_ctx,
                            size_t file_index,
                            const std::vector<uint64_t>& cell_ids) const {
    AssertInfo(file_index < files_.size(),
               "vortex file index {} out of range {}",
               file_index,
               files_.size());
    std::vector<cachinglayer::cid_t> cids;
    cids.reserve(cell_ids.size());
    for (auto cell_id : cell_ids) {
        cids.emplace_back(static_cast<cachinglayer::cid_t>(cell_id));
    }
    return cachinglayer::SemiInlineGet(
        files_[file_index].slot->PinCells(op_ctx, cids));
}

bool
VortexColumnGroup::CellsLoaded(size_t file_index,
                               const std::vector<uint64_t>& cell_ids) const {
    AssertInfo(file_index < files_.size(),
               "vortex file index {} out of range {}",
               file_index,
               files_.size());
    const auto& slot = files_[file_index].slot;
    return std::all_of(cell_ids.begin(), cell_ids.end(), [&](uint64_t cell_id) {
        return slot->IsCached(static_cast<cachinglayer::cid_t>(cell_id));
    });
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
