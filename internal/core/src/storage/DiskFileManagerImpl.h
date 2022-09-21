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

namespace milvus::storage {

class DiskFileManagerImpl : public FileManagerImpl {
 public:
    explicit DiskFileManagerImpl(const FieldDataMeta& field_mata, const IndexMeta& index_meta);

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
    GetRemoteIndexObjectPrefix();

    std::string
    GetLocalIndexObjectPrefix();

    std::string
    GetLocalRawDataObjectPrefix();

    std::map<std::string, int64_t>
    GetRemotePathsToFileSize() const {
        return remote_paths_to_size_;
    }

    void
    CacheIndexToDisk(std::vector<std::string> remote_files);

    FieldDataMeta
    GetFileDataMeta() const {
        return field_meta_;
    }

    IndexMeta
    GetIndexMeta() const {
        return index_meta_;
    }

 private:
    int64_t
    GetIndexBuildId() {
        return index_meta_.build_id;
    }

    std::string
    GetFileName(const std::string& localfile);

 private:
    // collection meta
    FieldDataMeta field_meta_;

    // index meta
    IndexMeta index_meta_;

    // local file path (abs path)
    std::vector<std::string> local_paths_;

    // remote file path
    std::map<std::string, int64_t> remote_paths_to_size_;
};

using DiskANNFileManagerImplPtr = std::shared_ptr<DiskFileManagerImpl>;

}  // namespace milvus::storage
