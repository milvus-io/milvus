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

/**
 * @brief ChunkManager implementation backed by the milvus-storage
 * ArrowFileSystem (the same filesystem layer used by StorageV2 / packed
 * readers), instead of the legacy AWS-SDK-based MinioChunkManager family.
 *
 * The filesystem is obtained from StorageV2FSCache, so it is shared with all
 * other ArrowFileSystem consumers of the same storage config. All producers
 * return a filesystem rooted at the bucket (or at root_path for local
 * storage), so ChunkManager filepaths map 1:1 to filesystem paths.
 *
 * Coexists with the legacy implementations; selected via
 * `common.storage.useArrowFileSystemChunkManager` (see
 * SetUseArrowFileSystemChunkManager / CreateChunkManager).
 *
 * Behavioral parity notes vs MinioChunkManager:
 * - Exist: not-found -> false, other errors throw.
 * - Size: not-found throws ObjectNotExist.
 * - Remove: not-found is swallowed, other errors throw.
 * - Read/Write with offset: NotImplemented (same as remote legacy impls).
 * - Errors are classified via milvus_storage::ToSegcoreError, which keeps
 *   the transient(2045)/permanent(2044)/not-found(2017) split from the
 *   structured ExtendStatusDetail attached by milvus-storage.
 */
class ArrowFileSystemChunkManager : public ChunkManager {
 public:
    explicit ArrowFileSystemChunkManager(const StorageConfig& storage_config);

    virtual ~ArrowFileSystemChunkManager() = default;

    virtual bool
    Exist(const std::string& filepath);

    virtual uint64_t
    Size(const std::string& filepath);

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

    virtual uint64_t
    Read(const std::string& filepath, void* buf, uint64_t len);

    virtual void
    Write(const std::string& filepath, void* buf, uint64_t len);

    virtual std::vector<std::string>
    ListWithPrefix(const std::string& filepath = "");

    virtual void
    Remove(const std::string& filepath);

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

    // Whether the milvus-storage ArrowFileSystem has a producer for this
    // cloud provider. "gcpnative" (and unknown values) must stay on the
    // legacy chunk managers.
    static bool
    SupportsCloudProvider(const std::string& cloud_provider);

 private:
    milvus_storage::ArrowFileSystemPtr fs_;
    std::string default_bucket_name_;
    std::string remote_root_path_;
};

}  // namespace milvus::storage
