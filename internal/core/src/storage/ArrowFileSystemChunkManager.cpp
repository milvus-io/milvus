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

// Not-found detection without parsing error messages: the milvus-storage S3
// filesystem attaches an ExtendStatusDetail with AwsErrorNotFound for
// NoSuchKey / NoSuchBucket / ResourceNotFound; a generic arrow filesystem
// attaches an ENOENT errno detail (arrow::fs::internal::PathNotFound).
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
//
// Decide a genuine miss via the legacy delegate's HeadObject exact-object
// existence, not a dir-aware arrow GetFileInfo re-stat (which would report a
// phantom Directory for a `key/` marker or a child object). The delegate is
// always the remote legacy chunk manager (see Make), so its Exist matches the
// object keys the Arrow data plane reads.
[[noreturn]] void
ThrowReadError(const ChunkManagerPtr& delegate,
               const std::string& func,
               const std::string& path,
               const arrow::Status& status) {
    if (IsArrowNotFound(status) || !delegate->Exist(path)) {
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
    if (storage_config.storage_type == "local") {
        // Local disk IO stays on LocalChunkManager: routing it through an
        // object-store filesystem buys nothing and hits none of the flat-object
        // concerns. Fall back (nullptr) to the legacy chunk managers.
        return nullptr;
    }
    auto key = ToStorageV2FSCacheKey(storage_config);
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

    // Reuse a legacy chunk manager for the control plane
    // (Exist/Size/ListWithPrefix/Remove). Its constructor runs the network
    // PreCheck, so a misconfigured but *supported* provider fails fast here --
    // the exception propagates out of Make to CreateChunkManager and aborts
    // node init -- instead of silently falling back. An *unsupported* provider
    // already returned nullptr above (fs == nullptr) and never reaches this
    // point, so it still falls back to the legacy managers.
    auto delegate = CreateLegacyChunkManager(storage_config);
    return std::shared_ptr<ArrowFileSystemChunkManager>(
        new ArrowFileSystemChunkManager(std::move(fs),
                                        std::move(delegate),
                                        storage_config.bucket_name,
                                        storage_config.root_path));
}

ArrowFileSystemChunkManager::ArrowFileSystemChunkManager(
    milvus_storage::ArrowFileSystemPtr fs,
    ChunkManagerPtr delegate,
    std::string bucket_name,
    std::string root_path)
    : fs_(std::move(fs)),
      delegate_(std::move(delegate)),
      default_bucket_name_(std::move(bucket_name)),
      remote_root_path_(std::move(root_path)) {
}

uint64_t
ArrowFileSystemChunkManager::Read(const std::string& filepath,
                                  void* buf,
                                  uint64_t size) {
    LatencyObserver observer(
        milvus::monitor::internal_storage_request_latency_get);
    milvus::monitor::internal_storage_kv_size_get.Observe(size);

    // The Arrow filesystem is rooted at the bucket, so filepath maps 1:1 to the
    // delegate's object key -- the data plane and control plane hit the same
    // object.
    auto file = fs_->OpenInputFile(filepath);
    if (!file.ok()) {
        milvus::monitor::internal_storage_op_count_get_fail.Increment();
        ThrowReadError(delegate_, "Read", filepath, file.status());
    }
    uint64_t total = 0;
    while (total < size) {
        auto read =
            (*file)->Read(size - total, static_cast<uint8_t*>(buf) + total);
        if (!read.ok()) {
            milvus::monitor::internal_storage_op_count_get_fail.Increment();
            ThrowReadError(delegate_, "Read", filepath, read.status());
        }
        if (*read == 0) {
            break;
        }
        total += static_cast<uint64_t>(*read);
    }
    auto close_status = (*file)->Close();
    if (!close_status.ok()) {
        milvus::monitor::internal_storage_op_count_get_fail.Increment();
        ThrowReadError(delegate_, "Read", filepath, close_status);
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
