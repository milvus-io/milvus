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

#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/ChunkManager.h"
#include "storage/Types.h"

namespace milvus::storage {

// Remote ChunkManager backed by the shared milvus-storage filesystem. Paths
// are complete bucket-relative keys; root_path is metadata, never prepended.
// Local IO continues to use LocalChunkManager.
class ArrowFileSystemChunkManager : public ChunkManager {
 public:
    static ChunkManagerPtr
    Make(const std::shared_ptr<const milvus_storage::api::Properties>&
             properties);

    ArrowFileSystemChunkManager(milvus_storage::ArrowFileSystemPtr fs,
                                std::string bucket_name,
                                std::string root_path,
                                bool check_connection = true);

    virtual ~ArrowFileSystemChunkManager() = default;

    bool
    Exist(const std::string& filepath) override;

    uint64_t
    Size(const std::string& filepath) override;

    std::vector<std::string>
    ListWithPrefix(const std::string& filepath = "") override;

    void
    Remove(const std::string& filepath) override;

    virtual uint64_t
    Read(const std::string& filepath, void* buf, uint64_t len);

    virtual void
    Write(const std::string& filepath, void* buf, uint64_t len);

    virtual uint64_t
    Read(const std::string& filepath,
         uint64_t offset,
         void* buf,
         uint64_t len) {
        ThrowInfo(NotImplemented, GetName() + "Read with offset not implement");
    }

    virtual void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len) {
        ThrowInfo(NotImplemented,
                  GetName() + "Write with offset not implement");
    }

    virtual std::string
    GetName() const {
        return "ArrowFileSystemChunkManager";
    }

    virtual std::string
    GetRootPath() const {
        return remote_root_path_;
    }

    virtual std::string
    GetBucketName() const {
        return default_bucket_name_;
    }

 private:
    arrow::Result<int64_t>
    GetObjectSize(const std::string& filepath);

    [[noreturn]] void
    ThrowReadError(const std::string& path, const arrow::Status& status);

    milvus_storage::ArrowFileSystemPtr fs_;
    std::string default_bucket_name_;
    std::string remote_root_path_;
};

}  // namespace milvus::storage
