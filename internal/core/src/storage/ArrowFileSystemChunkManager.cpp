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

#include "storage/ArrowFileSystemChunkManager.h"

#include <chrono>
#include <cstdint>
#include <filesystem>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/io_util.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "milvus-storage/common/extend_status.h"
#include "monitor/Monitor.h"
#include "prometheus/counter.h"
#include "prometheus/histogram.h"
#include "storage/StorageV2FSCache.h"
#include "storage/Util.h"

namespace milvus::storage {

namespace {

// Not-found detection without parsing error messages:
// - local / generic arrow filesystems attach an ENOENT errno detail
//   (arrow::fs::internal::PathNotFound),
// - the milvus-storage S3 filesystem attaches an ExtendStatusDetail with
//   AwsErrorNotFound for NoSuchKey / NoSuchBucket / ResourceNotFound.
bool
IsArrowNotFound(const arrow::Status& status) {
    if (arrow::internal::ErrnoFromStatus(status) == ENOENT) {
        return true;
    }
    auto detail = milvus_storage::ExtendStatusDetail::UnwrapStatus(status);
    return detail != nullptr &&
           detail->code() == milvus_storage::ExtendStatusCode::AwsErrorNotFound;
}

// The S3-backed filesystem reports a missing object from OpenInputFile /
// DeleteFile as arrow's bare PathNotFound IOError, which carries neither an
// errno detail nor an ExtendStatusDetail. Re-stat the path on the failure
// path (errors only, no cost on success): GetFileInfo reports not-found as
// FileType::NotFound instead of an error on every backend. If the re-stat
// itself fails (endpoint down, access denied), fall back to the original
// status classification so transient errors stay transient.
bool
PathMissing(const milvus_storage::ArrowFileSystemPtr& fs,
            const std::string& filepath,
            const arrow::Status& failure) {
    if (IsArrowNotFound(failure)) {
        return true;
    }
    auto info = fs->GetFileInfo(filepath);
    return info.ok() && info->type() == arrow::fs::FileType::NotFound;
}

// Classify and throw. The transient-vs-permanent decision is made where the
// arrow::Status was constructed (milvus-storage attaches ExtendStatusDetail
// at the AWS error sites); ToSegcoreError only unwraps it. See
// milvus-storage extend_status.cpp for the full taxonomy.
[[noreturn]] void
ThrowArrowStorageError(const std::string& func,
                       const std::string& path,
                       const arrow::Status& status) {
    auto segcore_error = milvus_storage::ToSegcoreError(status);
    std::string error_message =
        fmt::format("Error in {}[errcode:{}, filepath:{}, errmessage:{}]",
                    func,
                    fmt::underlying(segcore_error.get_error_code()),
                    path,
                    status.ToString());
    LOG_WARN("{}", error_message);
    throw SegcoreError(segcore_error.get_error_code(), error_message);
}

// Classify-and-throw for read-side failures: the S3 backend surfaces a
// missing object lazily (open succeeds, the first Read fails), so every
// read-side error branch needs the not-found recheck, not just open.
[[noreturn]] void
ThrowReadError(const milvus_storage::ArrowFileSystemPtr& fs,
               const std::string& func,
               const std::string& path,
               const arrow::Status& status) {
    if (PathMissing(fs, path, status)) {
        std::string error_message =
            fmt::format("Error in {}[filepath:{}, errmessage:{}]",
                        func,
                        path,
                        status.ToString());
        LOG_WARN("{}", error_message);
        throw SegcoreError(ObjectNotExist, error_message);
    }
    ThrowArrowStorageError(func, path, status);
}

class LatencyObserver {
 public:
    explicit LatencyObserver(prometheus::Histogram& histogram)
        : histogram_(histogram), start_(std::chrono::system_clock::now()) {
    }
    ~LatencyObserver() {
        histogram_.Observe(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - start_)
                .count());
    }

 private:
    prometheus::Histogram& histogram_;
    std::chrono::system_clock::time_point start_;
};

}  // namespace

ChunkManagerPtr
ArrowFileSystemChunkManager::Make(const StorageConfig& storage_config) {
    auto key = ToStorageV2FSCacheKey(storage_config);
    bool local_backend = false;
    if (storage_config.storage_type == "local") {
        // LocalChunkManager parity: callers pass OS paths (absolute,
        // root_path-prefixed), not root-relative keys. Root the filesystem
        // at "/" so those paths pass through unchanged.
        key.root_path = "/";
        local_backend = true;
    }
    milvus_storage::ArrowFileSystemPtr fs;
    try {
        fs = StorageV2FSCache::Instance().Get(key);
    } catch (const std::exception& e) {
        // Producers mostly report an unsupported/failed filesystem via
        // arrow::Status (-> nullptr below), but some paths throw raw
        // exceptions (e.g. endpoint parsing). Either way the config cannot
        // produce a filesystem: fall back to the legacy chunk managers
        // rather than failing the caller.
        LOG_WARN(
            "arrow filesystem unavailable for chunk manager ({}), storage "
            "config: {}",
            e.what(),
            storage_config.ToString());
        return nullptr;
    }
    if (fs == nullptr) {
        // milvus-storage has no producer for this config (unknown / empty /
        // gcpnative cloud provider, or a config its filesystem layer rejects).
        // Fall back to the legacy chunk managers.
        LOG_WARN(
            "arrow filesystem unavailable for chunk manager, storage "
            "config: {}",
            storage_config.ToString());
        return nullptr;
    }
    LOG_INFO(
        "init ArrowFileSystemChunkManager with "
        "parameter[endpoint={}][bucket_name={}][root_path={}][use_secure={}]",
        storage_config.address,
        storage_config.bucket_name,
        storage_config.root_path,
        storage_config.useSSL);
    return std::shared_ptr<ArrowFileSystemChunkManager>(
        new ArrowFileSystemChunkManager(std::move(fs),
                                        local_backend,
                                        storage_config.bucket_name,
                                        storage_config.root_path));
}

ArrowFileSystemChunkManager::ArrowFileSystemChunkManager(
    milvus_storage::ArrowFileSystemPtr fs,
    bool local_backend,
    std::string bucket_name,
    std::string root_path)
    : fs_(std::move(fs)),
      default_bucket_name_(std::move(bucket_name)),
      remote_root_path_(std::move(root_path)),
      local_backend_(local_backend) {
}

std::string
ArrowFileSystemChunkManager::ResolvePath(const std::string& filepath) const {
    if (!local_backend_ || filepath.empty() || filepath.front() == '/') {
        return filepath;
    }
    // The legacy LocalChunkManager resolves relative paths against the
    // process CWD (plain OS path semantics); keep that on the "/"-rooted
    // arrow filesystem.
    return (std::filesystem::current_path() / filepath).string();
}

bool
ArrowFileSystemChunkManager::Exist(const std::string& filepath) {
    const auto path = ResolvePath(filepath);
    arrow::Result<arrow::fs::FileInfo> info;
    {
        LatencyObserver observer(
            milvus::monitor::internal_storage_request_latency_stat);
        info = fs_->GetFileInfo(path);
    }
    if (!info.ok()) {
        milvus::monitor::internal_storage_op_count_stat_fail.Increment();
        ThrowArrowStorageError("Exist", path, info.status());
    }
    milvus::monitor::internal_storage_op_count_stat_suc.Increment();
    // Remote: only real objects count, mirroring HeadObject semantics of the
    // legacy implementations (a bare prefix/directory is not an object).
    // Local: LocalChunkManager::Exist is boost::filesystem::exists, which is
    // true for directories too.
    return info->type() == arrow::fs::FileType::File ||
           (local_backend_ && info->type() == arrow::fs::FileType::Directory);
}

uint64_t
ArrowFileSystemChunkManager::Size(const std::string& filepath) {
    const auto path = ResolvePath(filepath);
    arrow::Result<arrow::fs::FileInfo> info;
    {
        LatencyObserver observer(
            milvus::monitor::internal_storage_request_latency_stat);
        info = fs_->GetFileInfo(path);
    }
    if (!info.ok()) {
        milvus::monitor::internal_storage_op_count_stat_fail.Increment();
        ThrowArrowStorageError("Size", path, info.status());
    }
    if (info->type() != arrow::fs::FileType::File) {
        milvus::monitor::internal_storage_op_count_stat_fail.Increment();
        std::string error_message = fmt::format(
            "Error in Size[filepath:{}, errmessage:object not found]", path);
        LOG_WARN("{}", error_message);
        throw SegcoreError(ObjectNotExist, error_message);
    }
    milvus::monitor::internal_storage_op_count_stat_suc.Increment();
    return static_cast<uint64_t>(info->size());
}

uint64_t
ArrowFileSystemChunkManager::Read(const std::string& filepath,
                                  void* buf,
                                  uint64_t size) {
    LatencyObserver observer(
        milvus::monitor::internal_storage_request_latency_get);
    milvus::monitor::internal_storage_kv_size_get.Observe(size);

    const auto path = ResolvePath(filepath);
    auto file = fs_->OpenInputFile(path);
    if (!file.ok()) {
        milvus::monitor::internal_storage_op_count_get_fail.Increment();
        ThrowReadError(fs_, "Read", path, file.status());
    }
    uint64_t total = 0;
    while (total < size) {
        auto read =
            (*file)->Read(size - total, static_cast<uint8_t*>(buf) + total);
        if (!read.ok()) {
            milvus::monitor::internal_storage_op_count_get_fail.Increment();
            ThrowReadError(fs_, "Read", path, read.status());
        }
        if (*read == 0) {
            break;
        }
        total += static_cast<uint64_t>(*read);
    }
    auto close_status = (*file)->Close();
    if (!close_status.ok()) {
        milvus::monitor::internal_storage_op_count_get_fail.Increment();
        ThrowReadError(fs_, "Read", path, close_status);
    }
    milvus::monitor::internal_storage_op_count_get_suc.Increment();
    return total;
}

void
ArrowFileSystemChunkManager::Write(const std::string& filepath,
                                   void* buf,
                                   uint64_t size) {
    LatencyObserver observer(
        milvus::monitor::internal_storage_request_latency_put);
    milvus::monitor::internal_storage_kv_size_put.Observe(size);

    const auto path = ResolvePath(filepath);
    auto output = fs_->OpenOutputStream(path);
    // IsArrowNotFound already implies !output.ok() (a not-found status is
    // never ok), so no separate !ok() guard is needed.
    if (IsArrowNotFound(output.status())) {
        // Object stores have no directories, but a local-backed filesystem
        // needs the parent directory to exist. Create it and retry once so
        // the "write object at key" contract holds on every backend.
        auto slash_pos = path.find_last_of('/');
        if (slash_pos != std::string::npos) {
            auto mkdir_status = fs_->CreateDir(path.substr(0, slash_pos), true);
            if (!mkdir_status.ok()) {
                // Surface the real cause: falling through would rethrow the
                // original not-found status and hide why the parent is missing.
                milvus::monitor::internal_storage_op_count_put_fail.Increment();
                ThrowArrowStorageError("Write", path, mkdir_status);
            }
            output = fs_->OpenOutputStream(path);
        }
    }
    if (!output.ok()) {
        milvus::monitor::internal_storage_op_count_put_fail.Increment();
        ThrowArrowStorageError("Write", path, output.status());
    }
    auto write_status = (*output)->Write(buf, size);
    if (!write_status.ok()) {
        milvus::monitor::internal_storage_op_count_put_fail.Increment();
        ThrowArrowStorageError("Write", path, write_status);
    }
    // Close() finalizes the (multipart) upload; errors surface here.
    auto close_status = (*output)->Close();
    if (!close_status.ok()) {
        milvus::monitor::internal_storage_op_count_put_fail.Increment();
        ThrowArrowStorageError("Write", path, close_status);
    }
    milvus::monitor::internal_storage_op_count_put_suc.Increment();
}

std::vector<std::string>
ArrowFileSystemChunkManager::ListWithPrefix(const std::string& filepath) {
    LatencyObserver observer(
        milvus::monitor::internal_storage_request_latency_list);

    // Legacy semantics are raw object-key prefix matching (the prefix may end
    // mid-segment), while arrow FileSelector is directory based. Emulate by
    // listing from the parent directory recursively and filtering on the full
    // prefix.
    const auto resolved = ResolvePath(filepath);
    arrow::fs::FileSelector selector;
    auto slash_pos = resolved.find_last_of('/');
    selector.base_dir =
        slash_pos == std::string::npos ? "" : resolved.substr(0, slash_pos);
    selector.recursive = true;
    selector.allow_not_found = true;

    auto infos = fs_->GetFileInfo(selector);
    if (!infos.ok()) {
        milvus::monitor::internal_storage_op_count_list_fail.Increment();
        ThrowArrowStorageError("ListWithPrefix", resolved, infos.status());
    }
    // The subtree filesystem strips its base (including any leading '/')
    // from listed paths, so match on the stripped form of the resolved
    // prefix, then splice the caller's original prefix back on so results
    // come back in the caller's namespace (absolute or CWD-relative).
    const std::string match_prefix =
        !resolved.empty() && resolved.front() == '/' ? resolved.substr(1)
                                                     : resolved;
    std::vector<std::string> result;
    for (const auto& info : *infos) {
        if (info.type() == arrow::fs::FileType::File &&
            info.path().rfind(match_prefix, 0) == 0) {
            result.emplace_back(filepath +
                                info.path().substr(match_prefix.size()));
        }
    }
    milvus::monitor::internal_storage_op_count_list_suc.Increment();
    return result;
}

void
ArrowFileSystemChunkManager::Remove(const std::string& filepath) {
    const auto path = ResolvePath(filepath);
    arrow::Status status;
    {
        LatencyObserver observer(
            milvus::monitor::internal_storage_request_latency_remove);
        status = fs_->DeleteFile(path);
    }
    if (!status.ok()) {
        // Legacy DeleteObject swallows not-found; keep removal idempotent.
        if (PathMissing(fs_, path, status)) {
            milvus::monitor::internal_storage_op_count_remove_suc.Increment();
            return;
        }
        milvus::monitor::internal_storage_op_count_remove_fail.Increment();
        ThrowArrowStorageError("Remove", path, status);
    }
    milvus::monitor::internal_storage_op_count_remove_suc.Increment();
}

}  // namespace milvus::storage
