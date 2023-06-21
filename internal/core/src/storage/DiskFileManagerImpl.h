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

#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "storage/IndexData.h"
#include "storage/FileManager.h"
#include "storage/ChunkManager.h"

#include "common/Consts.h"

namespace milvus::storage {

class DiskFileManagerImpl : public FileManagerImpl {
 public:
    explicit DiskFileManagerImpl(const FieldDataMeta& field_mata,
                                 IndexMeta index_meta,
                                 ChunkManagerPtr remote_chunk_manager);

    virtual ~DiskFileManagerImpl();

    virtual bool
    LoadFile(const std::string& filename) noexcept;

    virtual bool
    AddFile(const std::string& filename) noexcept;

    virtual std::optional<bool>
    IsExisted(const std::string& filename) noexcept;

    virtual bool
    RemoveFile(const std::string& filename) noexcept;

 public:
    virtual std::string
    GetName() const {
        return "DiskFileManagerImpl";
    }

    std::string
    GetLocalIndexObjectPrefix();

    std::string
    GetLocalRawDataObjectPrefix();

    std::map<std::string, int64_t>
    GetRemotePathsToFileSize() const {
        return remote_paths_to_size_;
    }

    std::vector<std::string>
    GetLocalFilePaths() const {
        return local_paths_;
    }

    void
    CacheIndexToDisk(const std::vector<std::string>& remote_files);

    uint64_t
    CacheBatchIndexFilesToDisk(const std::vector<std::string>& remote_files,
                               const std::string& local_file_name,
                               uint64_t local_file_init_offfset);

    void
    AddBatchIndexFiles(const std::string& local_file_name,
                       const std::vector<int64_t>& local_file_offsets,
                       const std::vector<std::string>& remote_files,
                       const std::vector<int64_t>& remote_file_sizes);

    std::string
    CacheRawDataToDisk(std::vector<std::string> remote_files);

 private:
    int64_t
    GetIndexBuildId() {
        return index_meta_.build_id;
    }

    std::string
    GetFileName(const std::string& localfile);

    std::string
    GetRemoteIndexPath(const std::string& file_name, int64_t slice_num) const;

 private:
    // local file path (abs path)
    std::vector<std::string> local_paths_;

    // remote file path
    std::map<std::string, int64_t> remote_paths_to_size_;
};

using DiskANNFileManagerImplPtr = std::shared_ptr<DiskFileManagerImpl>;

}  // namespace milvus::storage
