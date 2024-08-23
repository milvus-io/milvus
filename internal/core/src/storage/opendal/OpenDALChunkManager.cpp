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

#include <algorithm>
#include <fstream>
#include <string>

#include "log/Log.h"
#include "opendal.h"
#include "common/EasyAssert.h"
#include "storage/Util.h"
#include "storage/opendal/OpenDALChunkManager.h"

namespace milvus::storage {

std::string
ToString(opendal_bytes* bs) {
    return {reinterpret_cast<const char*>(bs->data), bs->len};
}

#define THROWOPENDALERROR(err, msg)                                            \
    do {                                                                       \
        auto exception = SegcoreError(                                         \
            S3Error, fmt::format("{}: {}", (msg), ToString(&(err)->message))); \
        opendal_error_free(err);                                               \
        throw exception;                                                       \
    } while (0)

// std::once_flag init_flag_;

OpenDALChunkManager::OpenDALChunkManager(const StorageConfig& storage_config)
    : default_bucket_name_(storage_config.bucket_name) {
    // std::call_once(init_flag_, []() { opendal_init_logger(); });
    remote_root_path_ = storage_config.root_path;
    std::string storageType;
    if (storage_config.cloud_provider == "gcp") {
        storageType = "gcs";
    } else if (storage_config.cloud_provider == "aliyun") {
        storageType = "oss";
    } else if (storage_config.cloud_provider == "azure") {
        storageType = "azblob";
    } else {
        storageType = "s3";
    }

    opendal_operator_options* op_options_ = opendal_operator_options_new();
    if (!storage_config.access_key_id.empty() &&
        !storage_config.access_key_value.empty()) {
        opendal_operator_options_set(
            op_options_, "access_key_id", storage_config.access_key_id.c_str());
        opendal_operator_options_set(op_options_,
                                     "secret_access_key",
                                     storage_config.access_key_value.c_str());
        storageType = "s3";
    }
    opendal_operator_options_set(op_options_, "root", "/");
    opendal_operator_options_set(
        op_options_, "bucket", storage_config.bucket_name.c_str());
    opendal_operator_options_set(op_options_,
                                 "region",
                                 storage_config.region.empty()
                                     ? "us-east-1"
                                     : storage_config.region.c_str());
    opendal_operator_options_set(
        op_options_,
        "endpoint",
        ((storage_config.useSSL ? "https://" : "http://") +
         storage_config.address)
            .c_str());
    opendal_operator_options_set(
        op_options_,
        "enable_virtual_host_style",
        storage_config.useVirtualHost ? "true" : "false");

    auto op = opendal_operator_new(storageType.c_str(), op_options_);
    if (op.error != nullptr) {
        THROWOPENDALERROR(op.error, "Init opendal error");
    }
    op_ptr_ = op.op;
    opendal_operator_options_free(op_options_);
    LOG_INFO(
        "init OpenDALChunkManager with "
        "parameter[endpoint={}][bucket_name={}][root_path={}][use_secure={}]",
        storage_config.address,
        storage_config.bucket_name,
        storage_config.root_path,
        storage_config.useSSL);
}

OpenDALChunkManager::~OpenDALChunkManager() {
    opendal_operator_free(op_ptr_);
}

uint64_t
OpenDALChunkManager::Size(const std::string& filepath) {
    auto ret = opendal_operator_stat(op_ptr_, filepath.c_str());
    if (ret.error != nullptr) {
        THROWOPENDALERROR(ret.error, "GetObjectSize");
    }
    auto size = opendal_metadata_content_length(ret.meta);
    opendal_metadata_free(ret.meta);
    return size;
}

bool
OpenDALChunkManager::Exist(const std::string& filepath) {
    auto ret = opendal_operator_is_exist(op_ptr_, filepath.c_str());
    if (ret.error != nullptr) {
        THROWOPENDALERROR(ret.error, "ObjectExists");
    }
    return ret.is_exist;
}

void
OpenDALChunkManager::Remove(const std::string& filepath) {
    auto ret = opendal_operator_delete(op_ptr_, filepath.c_str());
    if (ret != nullptr) {
        THROWOPENDALERROR(ret, "RemoveObject");
    }
}

std::vector<std::string>
OpenDALChunkManager::ListWithPrefix(const std::string& filepath) {
    auto ret = opendal_operator_list(op_ptr_, filepath.c_str());
    if (ret.error != nullptr) {
        THROWOPENDALERROR(ret.error, "ListObjects");
    }
    auto lister = OpendalLister(ret.lister);
    std::vector<std::string> objects;
    opendal_result_lister_next result = opendal_lister_next(lister.Get());
    if (result.error != nullptr) {
        THROWOPENDALERROR(result.error, "ListObjects");
    }
    auto entry = result.entry;
    while (entry) {
        const char* de_path = opendal_entry_path(entry);
        objects.push_back(std::string(de_path));
        opendal_entry_free(entry);
        result = opendal_lister_next(lister.Get());
        if (result.error != nullptr) {
            THROWOPENDALERROR(result.error, "ListObjects");
        }
        entry = result.entry;
    }
    return objects;
}

uint64_t
OpenDALChunkManager::Read(const std::string& filepath,
                          void* buf,
                          uint64_t size) {
    auto ret = opendal_operator_reader(op_ptr_, filepath.c_str());
    if (ret.error != nullptr) {
        THROWOPENDALERROR(ret.error, "GetObjectBuffer");
    }
    auto reader = OpendalReader(ret.reader);
    uint64_t buf_size = 16 * 1024;
    uint64_t buf_index = 0;
    while (true) {
        auto read_ret =
            opendal_reader_read(reader.Get(),
                                reinterpret_cast<uint8_t*>(buf) + buf_index,
                                buf_size);
        buf_index += read_ret.size;
        if (read_ret.error != nullptr) {
            THROWOPENDALERROR(read_ret.error, "GetObjectBuffer");
        }
        if (read_ret.size == 0) {
            break;
        }
    }
    if (buf_index != size) {
        PanicInfo(
            S3Error,
            fmt::format(
                "Read size mismatch, target size is {}, actual size is {}",
                size,
                buf_index));
    }
    return buf_index;
}

void
OpenDALChunkManager::Write(const std::string& filepath,
                           void* buf,
                           uint64_t size) {
    auto ret = opendal_operator_write(
        op_ptr_, filepath.c_str(), {reinterpret_cast<uint8_t*>(buf), size});
    if (ret != nullptr) {
        THROWOPENDALERROR(ret, "Write");
    }
}

}  // namespace milvus::storage
