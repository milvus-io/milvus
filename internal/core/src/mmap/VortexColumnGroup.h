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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "mmap/SparseVortexFileSystem.h"
#include "milvus-storage/properties.h"

namespace arrow::fs {
class FileSystem;
}  // namespace arrow::fs

namespace milvus_storage::vortex {
class VortexCellGuard;
class VortexFooterReader;
class VortexPlanner;
}  // namespace milvus_storage::vortex

namespace milvus {

class OpContext;

namespace cachinglayer {
template <typename T>
class CacheSlot;
}  // namespace cachinglayer

struct VortexColumnFileInfo {
    std::string path;
    int64_t start_index = 0;
    int64_t end_index = 0;
    uint64_t file_size = 0;
    uint64_t footer_size = 0;
};

class VortexColumnGroup {
 public:
    using FileInfo = VortexColumnFileInfo;

    struct Options {
        SparseVortexFileBacking sparse_file_backing =
            SparseVortexFileBacking::Memory;
        bool mmap_populate = false;
        std::string mmap_dir_path;
        int64_t segment_id = 0;
        int64_t column_group_index = 0;
    };

    struct FileState {
        std::string path;
        std::string resolved_path;
        std::string sparse_path;
        std::shared_ptr<arrow::fs::FileSystem> source_fs;
        std::shared_ptr<arrow::fs::FileSystem> sparse_fs;
        std::shared_ptr<milvus_storage::vortex::VortexFooterReader>
            footer_reader;
        std::shared_ptr<milvus_storage::vortex::VortexPlanner> planner;
        std::shared_ptr<
            cachinglayer::CacheSlot<milvus_storage::vortex::VortexCellGuard>>
            slot;
        int64_t rows = 0;
        size_t memory_bytes = 0;
    };

    VortexColumnGroup(
        const std::vector<VortexColumnFileInfo>& files,
        std::shared_ptr<milvus_storage::api::Properties> properties,
        const std::vector<std::string>& field_names,
        CacheWarmupPolicy cache_warmup_policy,
        milvus::OpContext* op_ctx);

    VortexColumnGroup(
        const std::vector<VortexColumnFileInfo>& files,
        std::shared_ptr<milvus_storage::api::Properties> properties,
        const std::vector<std::string>& field_names,
        CacheWarmupPolicy cache_warmup_policy,
        milvus::OpContext* op_ctx,
        Options options);

    ~VortexColumnGroup();

    void
    ManualEvictCache() const;

    void
    CancelWarmup();

    const std::vector<FileState>&
    files() const;

    const std::vector<int64_t>&
    num_rows_until_chunk() const;

    int64_t
    num_rows() const;

    size_t
    memory_size() const;

    size_t
    num_fields() const;

 private:
    std::vector<FileState> files_;
    std::vector<int64_t> num_rows_until_chunk_;
    int64_t num_rows_ = 0;
    size_t num_fields_;
};

}  // namespace milvus
