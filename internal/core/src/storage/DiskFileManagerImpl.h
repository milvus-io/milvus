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
#include "storage/LocalChunkManager.h"
#include "common/Consts.h"
#include "storage/Types.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {

class DiskFileManagerImpl : public FileManagerImpl {
 public:
    explicit DiskFileManagerImpl(const FileManagerContext& fileManagerContext);

    ~DiskFileManagerImpl() override;

    bool
    LoadFile(const std::string& filename) noexcept override;

    bool
    AddFile(const std::string& filename) noexcept override;

    std::optional<bool>
    IsExisted(const std::string& filename) noexcept override;

    bool
    RemoveFile(const std::string& filename) noexcept override;

 public:
    bool
    AddTextLog(const std::string& filename) noexcept;

    bool
    AddJsonKeyIndexLog(const std::string& filename) noexcept;

 public:
    std::string
    GetName() const override {
        return "DiskFileManagerImpl";
    }

    std::string
    GetIndexIdentifier();

    std::string
    GetLocalIndexObjectPrefix();

    std::string
    GetLocalTempIndexObjectPrefix();

    // Similar to GetTextIndexIdentifier, segment_id and field_id is also required.
    std::string
    GetLocalTextIndexPrefix();

    std::string
    GetLocalTempTextIndexPrefix();

    // Used for loading index, using this index prefix dir to store index.
    std::string
    GetLocalJsonKeyIndexPrefix();

    std::string
    GetLocalTempJsonKeyIndexPrefix();

    // Used for upload index to remote storage, using this index prefix dir as remote storage directory
    std::string
    GetRemoteJsonKeyLogPrefix();

    // Used for upload index to remote storage, using this index prefix dir as remote storage directory
    std::string
    GetLocalNgramIndexPrefix();

    // Used for loading index, using this index prefix dir to store index.
    std::string
    GetLocalTempNgramIndexPrefix();

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
    CacheIndexToDisk(const std::vector<std::string>& remote_files,
                     milvus::proto::common::LoadPriority priority);

    void
    CacheTextLogToDisk(const std::vector<std::string>& remote_files,
                       milvus::proto::common::LoadPriority priority);

    void
    CacheJsonKeyIndexToDisk(const std::vector<std::string>& remote_files,
                            milvus::proto::common::LoadPriority priority);

    void
    CacheNgramIndexToDisk(const std::vector<std::string>& remote_files,
                          milvus::proto::common::LoadPriority priority);

    void
    RemoveIndexFiles();

    void
    RemoveTextLogFiles();

    void
    RemoveJsonKeyIndexFiles();

    void
    RemoveNgramIndexFiles();

    void
    AddBatchIndexFiles(const std::string& local_file_name,
                       const std::vector<int64_t>& local_file_offsets,
                       const std::vector<std::string>& remote_files,
                       const std::vector<int64_t>& remote_file_sizes);

    template <typename DataType>
    std::string
    CacheRawDataToDisk(const Config& config);

    std::string
    CacheOptFieldToDisk(OptFieldT& fields_map);

    std::string
    GetRemoteIndexPrefix() const {
        return GetRemoteIndexObjectPrefix();
    }

    size_t
    GetAddedTotalFileSize() const {
        return added_total_file_size_;
    }

    std::string
    GetFileName(const std::string& localfile);

 private:
    int64_t
    GetIndexBuildId() {
        return index_meta_.build_id;
    }

    std::string
    GetRemoteIndexPath(const std::string& file_name, int64_t slice_num) const;

    std::string
    GetRemoteTextLogPath(const std::string& file_name, int64_t slice_num) const;

    std::string
    GetRemoteJsonKeyIndexPath(const std::string& file_name, int64_t slice_num);

    bool
    AddFileInternal(const std::string& file_name,
                    const std::function<std::string(const std::string&, int)>&
                        get_remote_path) noexcept;

    void
    CacheIndexToDiskInternal(const std::vector<std::string>& remote_files,
                             const std::string& local_index_prefix,
                             milvus::proto::common::LoadPriority priority =
                                 milvus::proto::common::LoadPriority::HIGH);

    template <typename DataType>
    std::string
    cache_raw_data_to_disk_internal(const Config& config);

    template <typename T>
    std::string
    cache_raw_data_to_disk_storage_v2(const Config& config);

    template <typename DataType>
    void
    cache_raw_data_to_disk_common(
        const FieldDataPtr& field_data,
        const std::shared_ptr<LocalChunkManager>& local_chunk_manager,
        std::string& local_data_path,
        bool& file_created,
        uint32_t& dim,
        int64_t& write_offset);

 private:
    // local file path (abs path)
    std::vector<std::string> local_paths_;

    // remote file path
    std::map<std::string, int64_t> remote_paths_to_size_;

    size_t added_total_file_size_ = 0;
};

using DiskANNFileManagerImplPtr = std::shared_ptr<DiskFileManagerImpl>;

}  // namespace milvus::storage
