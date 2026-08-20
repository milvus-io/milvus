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
#include <unordered_set>

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
    std::string error_message = fmt::format(
        "Error in {}[errcode:{}, filepath:{}, errmessage:{}]",
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
        histogram_.Observe(std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now() - start_)
                               .count());
    }

 private:
    prometheus::Histogram& histogram_;
    std::chrono::system_clock::time_point start_;
};

}  // namespace

bool
ArrowFileSystemChunkManager::SupportsCloudProvider(
    const std::string& cloud_provider) {
    static const std::unordered_set<std::string> supported = {
        "aws", "gcp", "aliyun", "azure", "tencent", "huawei"};
    return supported.find(cloud_provider) != supported.end();
}

ArrowFileSystemChunkManager::ArrowFileSystemChunkManager(
    const StorageConfig& storage_config)
    : default_bucket_name_(storage_config.bucket_name),
      remote_root_path_(storage_config.root_path) {
    fs_ = StorageV2FSCache::Instance().Get(
        ToStorageV2FSCacheKey(storage_config));
    if (fs_ == nullptr) {
        // Init-time failure: the storage config cannot produce a filesystem.
        // Permanent (retrying with the same config fails identically).
        ThrowInfo(StorageError,
                  "failed to create arrow filesystem for chunk manager, "
                  "storage config: {}",
                  storage_config.ToString());
    }
    LOG_INFO(
        "init ArrowFileSystemChunkManager with "
        "parameter[endpoint={}][bucket_name={}][root_path={}][use_secure={}]",
        storage_config.address,
        storage_config.bucket_name,
        storage_config.root_path,
        storage_config.useSSL);
}

bool
ArrowFileSystemChunkManager::Exist(const std::string& filepath) {
    arrow::Result<arrow::fs::FileInfo> info;
    {
        LatencyObserver observer(
            milvus::monitor::internal_storage_request_latency_stat);
        info = fs_->GetFileInfo(filepath);
    }
    if (!info.ok()) {
        milvus::monitor::internal_storage_op_count_stat_fail.Increment();
        ThrowArrowStorageError("Exist", filepath, info.status());
    }
    milvus::monitor::internal_storage_op_count_stat_suc.Increment();
    // Only real objects count, mirroring HeadObject semantics of the legacy
    // implementations (a bare prefix/directory is not an object).
    return info->type() == arrow::fs::FileType::File;
}

uint64_t
ArrowFileSystemChunkManager::Size(const std::string& filepath) {
    arrow::Result<arrow::fs::FileInfo> info;
    {
        LatencyObserver observer(
            milvus::monitor::internal_storage_request_latency_stat);
        info = fs_->GetFileInfo(filepath);
    }
    if (!info.ok()) {
        milvus::monitor::internal_storage_op_count_stat_fail.Increment();
        ThrowArrowStorageError("Size", filepath, info.status());
    }
    if (info->type() != arrow::fs::FileType::File) {
        milvus::monitor::internal_storage_op_count_stat_fail.Increment();
        std::string error_message = fmt::format(
            "Error in Size[filepath:{}, errmessage:object not found]",
            filepath);
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

    auto file = fs_->OpenInputFile(filepath);
    if (!file.ok()) {
        milvus::monitor::internal_storage_op_count_get_fail.Increment();
        ThrowReadError(fs_, "Read", filepath, file.status());
    }
    uint64_t total = 0;
    while (total < size) {
        auto read = (*file)->Read(size - total,
                                  static_cast<uint8_t*>(buf) + total);
        if (!read.ok()) {
            milvus::monitor::internal_storage_op_count_get_fail.Increment();
            ThrowReadError(fs_, "Read", filepath, read.status());
        }
        if (*read == 0) {
            break;
        }
        total += static_cast<uint64_t>(*read);
    }
    auto close_status = (*file)->Close();
    if (!close_status.ok()) {
        milvus::monitor::internal_storage_op_count_get_fail.Increment();
        ThrowReadError(fs_, "Read", filepath, close_status);
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

    auto output = fs_->OpenOutputStream(filepath);
    if (!output.ok() && IsArrowNotFound(output.status())) {
        // Object stores have no directories, but a local-backed filesystem
        // needs the parent directory to exist. Create it and retry once so
        // the "write object at key" contract holds on every backend.
        auto slash_pos = filepath.find_last_of('/');
        if (slash_pos != std::string::npos) {
            auto mkdir_status =
                fs_->CreateDir(filepath.substr(0, slash_pos), true);
            if (mkdir_status.ok()) {
                output = fs_->OpenOutputStream(filepath);
            }
        }
    }
    if (!output.ok()) {
        milvus::monitor::internal_storage_op_count_put_fail.Increment();
        ThrowArrowStorageError("Write", filepath, output.status());
    }
    auto write_status = (*output)->Write(buf, size);
    if (!write_status.ok()) {
        milvus::monitor::internal_storage_op_count_put_fail.Increment();
        ThrowArrowStorageError("Write", filepath, write_status);
    }
    // Close() finalizes the (multipart) upload; errors surface here.
    auto close_status = (*output)->Close();
    if (!close_status.ok()) {
        milvus::monitor::internal_storage_op_count_put_fail.Increment();
        ThrowArrowStorageError("Write", filepath, close_status);
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
    arrow::fs::FileSelector selector;
    auto slash_pos = filepath.find_last_of('/');
    selector.base_dir =
        slash_pos == std::string::npos ? "" : filepath.substr(0, slash_pos);
    selector.recursive = true;
    selector.allow_not_found = true;

    auto infos = fs_->GetFileInfo(selector);
    if (!infos.ok()) {
        milvus::monitor::internal_storage_op_count_list_fail.Increment();
        ThrowArrowStorageError("ListWithPrefix", filepath, infos.status());
    }
    std::vector<std::string> result;
    for (const auto& info : *infos) {
        if (info.type() == arrow::fs::FileType::File &&
            info.path().rfind(filepath, 0) == 0) {
            result.emplace_back(info.path());
        }
    }
    milvus::monitor::internal_storage_op_count_list_suc.Increment();
    return result;
}

void
ArrowFileSystemChunkManager::Remove(const std::string& filepath) {
    arrow::Status status;
    {
        LatencyObserver observer(
            milvus::monitor::internal_storage_request_latency_remove);
        status = fs_->DeleteFile(filepath);
    }
    if (!status.ok()) {
        // Legacy DeleteObject swallows not-found; keep removal idempotent.
        if (PathMissing(fs_, filepath, status)) {
            milvus::monitor::internal_storage_op_count_remove_suc.Increment();
            return;
        }
        milvus::monitor::internal_storage_op_count_remove_fail.Increment();
        ThrowArrowStorageError("Remove", filepath, status);
    }
    milvus::monitor::internal_storage_op_count_remove_suc.Increment();
}

}  // namespace milvus::storage
