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
 * @brief Remote ChunkManager that routes only the byte-transfer data plane
 * (Read/Write) through the milvus-storage ArrowFileSystem (the same filesystem
 * layer used by StorageV2 / packed readers), while reusing a legacy
 * MinioChunkManager-family object as a delegate for the control plane
 * (Exist/Size/ListWithPrefix/Remove) and its construction-time PreCheck.
 *
 * Rationale: the legacy chunk managers already implement the exact flat-object
 * S3 semantics segcore relies on -- raw object-key prefix ListObjects, a single
 * idempotent DeleteObject, HeadObject exact-object existence -- and each
 * provider subclass (AwsChunkManager / GcpChunkManager / ...) builds a
 * provider-specific client (IAM / STS). We hold one as a delegate rather than
 * inheriting so a single class serves every provider (the delegate is whichever
 * subclass CreateLegacyChunkManager builds), and only the byte transfer moves
 * to the ArrowFileSystem.
 *
 * Remote only: the local backend keeps using LocalChunkManager -- routing local
 * disk IO through an object-store filesystem buys nothing and hits none of the
 * flat-object concerns. Make() returns nullptr for a local config (and for any
 * remote config milvus-storage has no Arrow producer for), so
 * CreateChunkManager falls back to the legacy managers.
 *
 * The Arrow filesystem is rooted at the bucket, so ChunkManager filepaths map
 * 1:1 to filesystem paths -- and to the delegate's raw object keys
 * (bucket + filepath), so the data plane and control plane address the same
 * object.
 *
 * Behavioral notes:
 * - Exist/Size/ListWithPrefix/Remove are the legacy delegate's, byte-for-byte
 *   (including its not-found error codes and idempotent Remove).
 * - Read (Arrow): not-found -> ObjectNotExist, classified via the delegate's
 *   HeadObject exact-object existence; other failures via
 *   milvus_storage::ToSegcoreError, which keeps the transient(2045) /
 *   permanent(2044) split from the ExtendStatusDetail milvus-storage attaches.
 * - Read/Write with offset: NotImplemented (offset IO goes through
 *   LocalChunkManagerSingleton, which is never rerouted).
 * - Known: the Arrow data-plane request timeout inherits milvus-storage's
 *   sub-second truncation; the legacy control plane keeps the full-resolution
 *   timeout. Tracked separately from this change.
 *
 * Coexists with the legacy implementations; selected via
 * `common.storage.useArrowFileSystemChunkManager` (see
 * SetUseArrowFileSystemChunkManager / CreateChunkManager).
 */
class ArrowFileSystemChunkManager : public ChunkManager {
 public:
    // Build a remote chunk manager for this storage config, or nullptr when the
    // caller should fall back to the legacy managers: a local config, or a
    // remote config milvus-storage has no Arrow producer for (unknown / empty /
    // "gcpnative" cloud provider, or any config its filesystem layer rejects).
    // Throws only when a supported provider fails its delegate PreCheck
    // (fail-fast on a misconfigured endpoint, exactly like the legacy managers).
    static ChunkManagerPtr
    Make(const StorageConfig& storage_config);

    virtual ~ArrowFileSystemChunkManager() = default;

    // --- control plane: delegated to the legacy chunk manager ---

    virtual bool
    Exist(const std::string& filepath) {
        return delegate_->Exist(filepath);
    }

    virtual uint64_t
    Size(const std::string& filepath) {
        return delegate_->Size(filepath);
    }

    virtual std::vector<std::string>
    ListWithPrefix(const std::string& filepath = "") {
        return delegate_->ListWithPrefix(filepath);
    }

    virtual void
    Remove(const std::string& filepath) {
        delegate_->Remove(filepath);
    }

    // --- data plane: routed through the ArrowFileSystem ---

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
    // Private: fs and delegate are already built and non-null (see Make). This
    // constructor does no network IO and never throws — all filesystem/delegate
    // construction and failure handling lives in Make.
    ArrowFileSystemChunkManager(milvus_storage::ArrowFileSystemPtr fs,
                                ChunkManagerPtr delegate,
                                std::string bucket_name,
                                std::string root_path);

    milvus_storage::ArrowFileSystemPtr fs_;  // data plane: Read/Write
    ChunkManagerPtr delegate_;               // control plane + PreCheck
    std::string default_bucket_name_;
    std::string remote_root_path_;
};

}  // namespace milvus::storage
