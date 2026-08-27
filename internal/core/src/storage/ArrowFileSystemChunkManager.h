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
 * other ArrowFileSystem consumers of the same storage config. All remote
 * producers return a filesystem rooted at the bucket, so ChunkManager
 * filepaths map 1:1 to filesystem paths. For the local backend the
 * filesystem is rooted at "/" instead: LocalChunkManager callers pass OS
 * paths (absolute, root_path-prefixed), and rooting at "/" lets them pass
 * through unchanged.
 *
 * Coexists with the legacy implementations; selected via
 * `common.storage.useArrowFileSystemChunkManager` (see
 * SetUseArrowFileSystemChunkManager / CreateChunkManager).
 *
 * Behavioral parity notes vs MinioChunkManager (and LocalChunkManager for
 * the local backend):
 * - Exist: not-found -> false, other errors throw.
 * - Size: not-found throws ObjectNotExist.
 * - Remove: not-found is swallowed, other errors throw.
 * - Read/Write with offset: NotImplemented (same as the remote legacy
 *   impls; LocalChunkManager does support these, but no CreateChunkManager
 *   consumer uses them — offset IO goes through LocalChunkManagerSingleton,
 *   which is never rerouted).
 * - Errors are classified via milvus_storage::ToSegcoreError, which keeps
 *   the transient(2045)/permanent(2044)/not-found(2017) split from the
 *   structured ExtendStatusDetail attached by milvus-storage.
 */
class ArrowFileSystemChunkManager : public ChunkManager {
 public:
    // Build a chunk manager for this storage config, or nullptr when
    // milvus-storage has no producer for it (unknown / empty / "gcpnative"
    // cloud provider, or any config its filesystem layer rejects). The caller
    // (CreateChunkManager) then falls back to the legacy chunk managers.
    // milvus-storage owns provider-support classification; milvus does not
    // second-guess it with an allow-list. Never throws for an unsupported
    // config — "cannot build a filesystem" is a fall-back signal, not an error.
    static ChunkManagerPtr
    Make(const StorageConfig& storage_config);

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

 private:
    // Private: fs is already built and non-null (see Make). This constructor
    // does no network IO and never throws — all filesystem construction and
    // failure handling lives in Make.
    ArrowFileSystemChunkManager(milvus_storage::ArrowFileSystemPtr fs,
                                bool local_backend,
                                std::string bucket_name,
                                std::string root_path);

    // LocalChunkManager parity for the "/"-rooted local backend: relative
    // filepaths are resolved against the process CWD, exactly like the OS
    // path semantics of the legacy implementation. Remote paths (object
    // keys) are returned unchanged.
    std::string
    ResolvePath(const std::string& filepath) const;

    milvus_storage::ArrowFileSystemPtr fs_;
    std::string default_bucket_name_;
    std::string remote_root_path_;
    bool local_backend_ = false;
};

}  // namespace milvus::storage
