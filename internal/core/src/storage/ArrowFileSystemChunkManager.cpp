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
#include <new>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/io_util.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "milvus-storage/common/extend_status.h"
#include "monitor/Monitor.h"
#include "prometheus/counter.h"
#include "prometheus/histogram.h"
#include "storage/Util.h"

namespace milvus::storage {

namespace {

// S3 and Azure use the shared AwsErrorNotFound status detail or an ENOENT
// detail (arrow::fs::internal::PathNotFound). Do not parse error messages.
bool
IsArrowNotFound(const arrow::Status& status) {
    if (arrow::internal::ErrnoFromStatus(status) == ENOENT) {
        return true;
    }
    auto detail = milvus_storage::ExtendStatusDetail::UnwrapStatus(status);
    return detail != nullptr &&
           detail->code() == milvus_storage::ExtendStatusCode::AwsErrorNotFound;
}

// Classify and throw. The transient-vs-permanent decision is made where the
// arrow::Status was constructed (milvus-storage attaches ExtendStatusDetail
// at the provider error sites); ToSegcoreError only unwraps it. See
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
ArrowFileSystemChunkManager::Make(
    const std::shared_ptr<const milvus_storage::api::Properties>& properties) {
    try {
        const auto bucket = GetStorageProperty<std::string>(
            properties, PROPERTY_FS_BUCKET_NAME);
        const auto root =
            GetStorageProperty<std::string>(properties, PROPERTY_FS_ROOT_PATH);
        auto fs = GetArrowFileSystem(properties);
        if (!fs.ok()) {
            // Invalid here describes server configuration, not persisted data.
            if (fs.status().IsInvalid()) {
                ThrowInfo(ConfigInvalid,
                          "create chunk manager: {}",
                          fs.status().ToString());
            }
            ThrowArrowStorageError("CreateChunkManager", bucket, fs.status());
        }
        // Azure historically created its client without a startup LIST.
        // Keep the S3 probe without adding a new Azure permission requirement.
        const bool check_connection =
            GetStorageProperty<std::string>(
                properties, PROPERTY_FS_CLOUD_PROVIDER) != "azure";
        return std::make_shared<ArrowFileSystemChunkManager>(
            *fs, bucket, root, check_connection);
    } catch (const SegcoreError&) {
        throw;
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const std::exception& error) {
        // Some provider constructors throw instead of returning arrow::Status.
        // Preserve their message and keep the failure in the storage family.
        ThrowInfo(StorageError, "create chunk manager: {}", error.what());
    }
}

ArrowFileSystemChunkManager::ArrowFileSystemChunkManager(
    milvus_storage::ArrowFileSystemPtr fs,
    std::string bucket_name,
    std::string root_path,
    const bool check_connection)
    : fs_(std::move(fs)),
      default_bucket_name_(std::move(bucket_name)),
      remote_root_path_(std::move(root_path)) {
    if (!check_connection) {
        return;
    }
    // Keep the startup LIST probe, without enumerating the actual data prefix.
    // allow_not_found=true would also swallow a missing bucket. In the S3 FS,
    // an empty prefix produces ENOENT, while a failed LIST retains the backend
    // ExtendStatusDetail (including NoSuchBucket).
    arrow::fs::FileSelector probe;
    probe.base_dir = remote_root_path_.empty()
                         ? "justforconnectioncheck"
                         : remote_root_path_ + "/justforconnectioncheck";
    probe.allow_not_found = false;
    auto result = fs_->GetFileInfo(probe);
    if (!result.ok() &&
        !(arrow::internal::ErrnoFromStatus(result.status()) == ENOENT &&
          !milvus_storage::ExtendStatusDetail::UnwrapStatus(result.status()))) {
        if (result.status().IsInvalid()) {
            ThrowInfo(ConfigInvalid,
                      "chunk manager startup probe: {}",
                      result.status().ToString());
        }
        ThrowArrowStorageError("PreCheck", probe.base_dir, result.status());
    }
}

arrow::Result<int64_t>
ArrowFileSystemChunkManager::GetObjectSize(const std::string& filepath) {
    // GetFileInfo also recognizes implicit directories. OpenInputFile/GetSize
    // instead issues HEAD for this exact object, without a directory LIST.
    ARROW_ASSIGN_OR_RAISE(auto file, fs_->OpenInputFile(filepath));
    auto size = file->GetSize();
    const auto close_status = file->Close();
    // Close the handle on both paths, preserving the original HEAD error.
    if (!size.ok()) {
        return size.status();
    }
    ARROW_RETURN_NOT_OK(close_status);
    return size;
}

uint64_t
ArrowFileSystemChunkManager::Size(const std::string& filepath) {
    LatencyObserver observer(
        milvus::monitor::internal_storage_request_latency_stat);
    auto size = GetObjectSize(filepath);
    if (!size.ok()) {
        milvus::monitor::internal_storage_op_count_stat_fail.Increment();
        ThrowArrowStorageError("Size", filepath, size.status());
    }
    milvus::monitor::internal_storage_op_count_stat_suc.Increment();
    return *size;
}

bool
ArrowFileSystemChunkManager::Exist(const std::string& filepath) {
    LatencyObserver observer(
        milvus::monitor::internal_storage_request_latency_stat);
    auto size = GetObjectSize(filepath);
    if (!size.ok() && !IsArrowNotFound(size.status())) {
        milvus::monitor::internal_storage_op_count_stat_fail.Increment();
        ThrowArrowStorageError("Exist", filepath, size.status());
    }
    // A missing object is a successful existence check, not a failed stat.
    milvus::monitor::internal_storage_op_count_stat_suc.Increment();
    return size.ok();
}

std::vector<std::string>
ArrowFileSystemChunkManager::ListWithPrefix(const std::string& filepath) {
    // FileSelector lists directories, while this interface accepts a raw byte
    // prefix ("a/3" must match both "a/3" and "a/30"). List its parent and
    // filter complete file keys. Production uses the separate bounded probe.
    arrow::fs::FileSelector selector;
    const auto slash = filepath.rfind('/');
    selector.base_dir =
        slash == std::string::npos ? "" : filepath.substr(0, slash);
    selector.recursive = true;
    selector.allow_not_found = false;
    auto entries = fs_->GetFileInfo(selector);
    if (!entries.ok()) {
        if (arrow::internal::ErrnoFromStatus(entries.status()) == ENOENT &&
            !milvus_storage::ExtendStatusDetail::UnwrapStatus(
                entries.status())) {
            return {};
        }
        ThrowArrowStorageError("ListWithPrefix", filepath, entries.status());
    }
    std::vector<std::string> paths;
    for (const auto& entry : *entries) {
        if (entry.IsFile() &&
            entry.path().compare(0, filepath.size(), filepath) == 0) {
            paths.push_back(entry.path());
        }
    }
    return paths;
}

void
ArrowFileSystemChunkManager::Remove(const std::string& filepath) {
    LatencyObserver observer(
        milvus::monitor::internal_storage_request_latency_remove);
    const auto status = fs_->DeleteFile(filepath);
    if (!status.ok() && !IsArrowNotFound(status)) {
        milvus::monitor::internal_storage_op_count_remove_fail.Increment();
        ThrowArrowStorageError("Remove", filepath, status);
    }
    milvus::monitor::internal_storage_op_count_remove_suc.Increment();
}

void
ArrowFileSystemChunkManager::ThrowReadError(const std::string& path,
                                            const arrow::Status& status) {
    if (IsArrowNotFound(status)) {
        ThrowArrowStorageError("Read", path, status);
    }
    // Retain the exact-object recheck for unclassified backend read failures.
    // A failed recheck must not replace the original read error.
    try {
        if (!Exist(path)) {
            throw SegcoreError(ObjectNotExist,
                               fmt::format("Read[filepath:{}, errmessage:{}]",
                                           path,
                                           status.ToString()));
        }
    } catch (const SegcoreError& error) {
        if (error.get_error_code() == ObjectNotExist) {
            throw;
        }
    }
    ThrowArrowStorageError("Read", path, status);
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
        ThrowReadError(filepath, file.status());
    }
    uint64_t total = 0;
    while (total < size) {
        auto read =
            (*file)->Read(size - total, static_cast<uint8_t*>(buf) + total);
        if (!read.ok()) {
            milvus::monitor::internal_storage_op_count_get_fail.Increment();
            ThrowReadError(filepath, read.status());
        }
        if (*read == 0) {
            break;
        }
        total += static_cast<uint64_t>(*read);
    }
    auto close_status = (*file)->Close();
    if (!close_status.ok()) {
        milvus::monitor::internal_storage_op_count_get_fail.Increment();
        ThrowReadError(filepath, close_status);
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

    // Object stores have no directories, so no parent-dir creation is needed --
    // OpenOutputStream writes the object at the key directly.
    auto output = fs_->OpenOutputStream(filepath);
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

}  // namespace milvus::storage
