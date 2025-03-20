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

#include <cstdint>
#include <map>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

#include "storage/IndexData.h"
#include "storage/FileManager.h"
#include "storage/ChunkManager.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {

class MemFileManagerImpl : public FileManagerImpl {
 public:
    explicit MemFileManagerImpl(const FileManagerContext& fileManagerContext);

    virtual bool
    LoadFile(const std::string& filename) noexcept;

    virtual bool
    AddFile(const std::string& filename /* unused */) noexcept;

    virtual std::optional<bool>
    IsExisted(const std::string& filename) noexcept;

    virtual bool
    RemoveFile(const std::string& filename) noexcept;

 public:
    virtual std::string
    GetName() const {
        return "MemIndexFileManagerImpl";
    }

    std::map<std::string, FieldDataPtr>
    LoadIndexToMemory(const std::vector<std::string>& remote_files,
                      milvus::ThreadPoolPriority priority);

    std::vector<FieldDataPtr>
    CacheRawDataToMemory(std::vector<std::string> remote_files);

    bool
    AddFile(const BinarySet& binary_set);

    bool
    AddTextLog(const BinarySet& binary_set);

    std::map<std::string, int64_t>
    GetRemotePathsToFileSize() const {
        return remote_paths_to_size_;
    }

    size_t
    GetAddedTotalMemSize() const {
        return added_total_mem_size_;
    }

    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>
    CacheOptFieldToMemory(OptFieldT& fields_map);

 private:
    bool
    AddBinarySet(const BinarySet& binary_set, const std::string& prefix);

 private:
    // remote file path
    std::map<std::string, int64_t> remote_paths_to_size_;

    size_t added_total_mem_size_ = 0;
};

using MemFileManagerImplPtr = std::shared_ptr<MemFileManagerImpl>;

}  // namespace milvus::storage
