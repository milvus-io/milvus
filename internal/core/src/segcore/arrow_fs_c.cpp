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

#include <cstring>
#include <memory>
#include <vector>
#include <chrono>

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/file.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/filesystem/s3fs.h>

#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/filesystem/s3/multi_part_upload_s3_fs.h"

#include "segcore/arrow_fs_c.h"
#include "common/EasyAssert.h"
#include "common/type_c.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

CStatus
InitLocalArrowFileSystemSingleton(const char* c_path) {
    try {
        std::string path(c_path);
        milvus_storage::ArrowFileSystemConfig conf;
        conf.root_path = path;
        conf.storage_type = "local";
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);

        milvus::storage::LoonFFIPropertiesSingleton::GetInstance().Init(c_path);

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
CleanArrowFileSystemSingleton() {
    milvus_storage::ArrowFileSystemSingleton::GetInstance().Release();
}

CStatus
InitRemoteArrowFileSystemSingleton(CStorageConfig c_storage_config) {
    try {
        milvus_storage::ArrowFileSystemConfig conf;
        conf.address = std::string(c_storage_config.address);
        conf.bucket_name = std::string(c_storage_config.bucket_name);
        conf.access_key_id = std::string(c_storage_config.access_key_id);
        conf.access_key_value = std::string(c_storage_config.access_key_value);
        conf.root_path = std::string(c_storage_config.root_path);
        conf.storage_type = std::string(c_storage_config.storage_type);
        conf.cloud_provider = std::string(c_storage_config.cloud_provider);
        conf.iam_endpoint = std::string(c_storage_config.iam_endpoint);
        conf.log_level = std::string(c_storage_config.log_level);
        conf.region = std::string(c_storage_config.region);
        conf.use_ssl = c_storage_config.useSSL;
        conf.ssl_ca_cert = std::string(c_storage_config.sslCACert);
        conf.use_iam = c_storage_config.useIAM;
        conf.use_virtual_host = c_storage_config.useVirtualHost;
        conf.request_timeout_ms = c_storage_config.requestTimeoutMs;
        conf.gcp_credential_json =
            std::string(c_storage_config.gcp_credential_json);
        conf.use_custom_part_upload = c_storage_config.use_custom_part_upload;
        conf.max_connections = c_storage_config.max_connections;
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);

        milvus::storage::LoonFFIPropertiesSingleton::GetInstance().Init(
            c_storage_config);

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

// ============ Reader Functions ============

CStatus
GetFileStats(const char* c_path,
             int64_t* out_size,
             char*** out_keys,
             char*** out_values,
             int* out_count) {
    try {
        AssertInfo(c_path != nullptr, "Path cannot be null");
        AssertInfo(out_size != nullptr, "Output size pointer cannot be null");

        // Initialize outputs
        *out_size = 0;
        if (out_keys) *out_keys = nullptr;
        if (out_values) *out_values = nullptr;
        if (out_count) *out_count = 0;

        // Get file system singleton
        auto& fs_singleton = milvus_storage::ArrowFileSystemSingleton::GetInstance();
        auto fs = fs_singleton.GetArrowFileSystem();
        AssertInfo(fs != nullptr, "File system not initialized");

        std::string path(c_path);

        // Open input file to read both size and metadata
        auto input_result = fs->OpenInputFile(path);
        if (!input_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, input_result.status().message());
        }
        auto input_file = input_result.ValueOrDie();

        // Get file size
        auto size_result = input_file->GetSize();
        if (!size_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, size_result.status().message());
        }
        *out_size = size_result.ValueOrDie();

        // Read metadata if requested
        if (out_keys && out_values && out_count) {
            auto metadata_result = input_file->ReadMetadata();
            if (metadata_result.ok()) {
                auto metadata = metadata_result.ValueOrDie();
                if (metadata && metadata->size() > 0) {
                    // Get keys and values from KeyValueMetadata
                    const auto& keys = metadata->keys();
                    const auto& values = metadata->values();
                    int count = static_cast<int>(keys.size());

                    // Allocate arrays for keys and values
                    *out_keys = (char**)malloc(count * sizeof(char*));
                    *out_values = (char**)malloc(count * sizeof(char*));
                    AssertInfo(*out_keys != nullptr && *out_values != nullptr,
                              "Failed to allocate memory for metadata");

                    // Copy key-value pairs
                    for (int i = 0; i < count; i++) {
                        (*out_keys)[i] = strdup(keys[i].c_str());
                        (*out_values)[i] = strdup(values[i].c_str());

                        AssertInfo((*out_keys)[i] != nullptr && (*out_values)[i] != nullptr,
                                  "Failed to duplicate metadata strings");
                    }

                    *out_count = count;
                }
            }
            // If metadata read fails or is empty, just leave the outputs as initialized (nullptr/0)
        }

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        // Clean up on error
        if (out_size) *out_size = 0;
        if (out_keys && *out_keys) {
            for (int i = 0; i < (out_count && *out_count ? *out_count : 0); i++) {
                free((*out_keys)[i]);
            }
            free(*out_keys);
            *out_keys = nullptr;
        }
        if (out_values && *out_values) {
            for (int i = 0; i < (out_count && *out_count ? *out_count : 0); i++) {
                free((*out_values)[i]);
            }
            free(*out_values);
            *out_values = nullptr;
        }
        if (out_count) *out_count = 0;
        return milvus::FailureCStatus(&e);
    }
}

CStatus
ReadFileData(const char* c_path, uint8_t** out_data, int64_t* out_size) {
    try {
        AssertInfo(c_path != nullptr, "Path cannot be null");
        AssertInfo(out_data != nullptr, "Output data pointer cannot be null");
        AssertInfo(out_size != nullptr, "Output size pointer cannot be null");

        *out_data = nullptr;
        *out_size = 0;

        // Get file system singleton
        auto& fs_singleton = milvus_storage::ArrowFileSystemSingleton::GetInstance();
        auto fs = fs_singleton.GetArrowFileSystem();
        AssertInfo(fs != nullptr, "File system not initialized");

        std::string path(c_path);

        // Open input file (not stream) to get size
        auto input_result = fs->OpenInputFile(path);
        if (!input_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, input_result.status().message());
        }
        auto input_file = input_result.ValueOrDie();

        // Get file size
        auto size_result = input_file->GetSize();
        if (!size_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, size_result.status().message());
        }
        auto file_size = size_result.ValueOrDie();

        // Allocate memory for file content
        *out_size = file_size;
        *out_data = (uint8_t*)malloc(file_size);
        AssertInfo(*out_data != nullptr, "Failed to allocate memory for file data");

        // Read file content
        auto read_result = input_file->Read(file_size);
        if (!read_result.ok()) {
            free(*out_data);
            *out_data = nullptr;
            *out_size = 0;
            return milvus::FailureCStatus(milvus::UnexpectedError, read_result.status().message());
        }
        auto buffer = read_result.ValueOrDie();

        // Copy data to output
        std::memcpy(*out_data, buffer->data(), file_size);

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        if (out_data && *out_data) {
            free(*out_data);
            *out_data = nullptr;
        }
        if (out_size) {
            *out_size = 0;
        }
        return milvus::FailureCStatus(&e);
    }
}

// ============ Writer Functions ============

CStatus
WriteFileData(const char* c_path,
              const uint8_t* data,
              int64_t data_size,
              const char** metadata_keys,
              const char** metadata_values,
              int metadata_count) {
    try {
        AssertInfo(c_path != nullptr, "Path cannot be null");
        AssertInfo(data != nullptr || data_size == 0, "Data cannot be null if size > 0");
        AssertInfo(data_size >= 0, "Data size must be non-negative");
        AssertInfo(metadata_count >= 0, "Metadata count must be non-negative");
        if (metadata_count > 0) {
            AssertInfo(metadata_keys != nullptr, "Metadata keys cannot be null if count > 0");
            AssertInfo(metadata_values != nullptr, "Metadata values cannot be null if count > 0");
        }

        // Get file system singleton
        auto& fs_singleton = milvus_storage::ArrowFileSystemSingleton::GetInstance();
        auto fs = fs_singleton.GetArrowFileSystem();
        AssertInfo(fs != nullptr, "File system not initialized");

        std::string path(c_path);

        std::shared_ptr<arrow::io::OutputStream> output_stream;

        // Open output stream with custom metadata if necessary
        arrow::Result<std::shared_ptr<arrow::io::OutputStream>> stream_result;
        if (metadata_count > 0) {
            if (!milvus_storage::ExtendFileSystem::IsExtendFileSystem(fs)) {
                return milvus::FailureCStatus(milvus::UnexpectedError, "current fs does not support Condition Write");
            }
            auto fs_ext = std::dynamic_pointer_cast<milvus_storage::ExtendFileSystem>(fs);

            // Convert C-style string arrays to std::vector<std::string>
            std::vector<std::string> keys;
            std::vector<std::string> values;
            keys.reserve(metadata_count);
            values.reserve(metadata_count);

            for (int i = 0; i < metadata_count; i++) {
                keys.emplace_back(metadata_keys[i]);
                values.emplace_back(metadata_values[i]);
            }

            // Construct KeyValueMetadata
            auto metadata = std::make_shared<arrow::KeyValueMetadata>(keys, values);
            stream_result = fs_ext->OpenConditionalOutputStream(path, metadata);
        } else {
            stream_result = fs->OpenOutputStream(path);
        }
        if (!stream_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, stream_result.status().message());
        }
        output_stream = stream_result.ValueOrDie();

        // Write data
        if (data_size > 0) {
            auto write_status = output_stream->Write(data, data_size);
            if (!write_status.ok()) {
                // Try to close the stream before returning error
                (void)output_stream->Close();  // Ignore close errors
                return milvus::FailureCStatus(milvus::UnexpectedError, write_status.message());
            }
        }

        // Close the stream
        auto close_status = output_stream->Close();
        if (!close_status.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, close_status.message());
        }

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
DeleteFile(const char* c_path) {
    try {
        AssertInfo(c_path != nullptr, "Path cannot be null");

        // Get file system singleton
        auto& fs_singleton = milvus_storage::ArrowFileSystemSingleton::GetInstance();
        auto fs = fs_singleton.GetArrowFileSystem();
        AssertInfo(fs != nullptr, "File system not initialized");

        std::string path(c_path);

        // Verify file exists before deletion
        auto file_info_result = fs->GetFileInfo(path);
        if (!file_info_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, file_info_result.status().message());
        }
        auto file_info = file_info_result.ValueOrDie();

        if (file_info.type() == arrow::fs::FileType::NotFound) {
            return milvus::FailureCStatus(milvus::UnexpectedError, "File not found");
        }

        if (file_info.type() != arrow::fs::FileType::File) {
            return milvus::FailureCStatus(milvus::UnexpectedError, "Path is not a file");
        }

        // Delete the file
        auto delete_status = fs->DeleteFile(path);
        if (!delete_status.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, delete_status.message());
        }

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

// ============ Directory Management Functions ============

CStatus
GetFileInfo(const char* c_path,
            bool* out_exists,
            bool* out_is_dir,
            int64_t* out_mtime_ns,
            int64_t* out_ctime_ns) {
    try {
        AssertInfo(c_path != nullptr, "Path cannot be null");
        AssertInfo(out_exists != nullptr, "Output exists pointer cannot be null");

        // Initialize outputs
        *out_exists = false;
        if (out_is_dir) *out_is_dir = false;
        if (out_mtime_ns) *out_mtime_ns = 0;
        if (out_ctime_ns) *out_ctime_ns = 0;

        // Get file system singleton
        auto& fs_singleton = milvus_storage::ArrowFileSystemSingleton::GetInstance();
        auto fs = fs_singleton.GetArrowFileSystem();
        AssertInfo(fs != nullptr, "File system not initialized");

        std::string path(c_path);

        // Get file info to check if path exists and its type
        auto file_info_result = fs->GetFileInfo(path);
        if (!file_info_result.ok()) {
            // Path does not exist
            return milvus::SuccessCStatus();
        }

        auto file_info = file_info_result.ValueOrDie();

        // Check if path exists
        if (file_info.type() == arrow::fs::FileType::NotFound) {
            return milvus::SuccessCStatus();
        }

        // Path exists
        *out_exists = true;

        // Check if it's a directory
        if (out_is_dir) {
            *out_is_dir = (file_info.type() == arrow::fs::FileType::Directory);
        }

        // Get modification time
        if (out_mtime_ns) {
            auto mtime = file_info.mtime();
            if (mtime.time_since_epoch().count() > 0) {
                // Convert to nanoseconds
                auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    mtime.time_since_epoch());
                *out_mtime_ns = duration_ns.count();
            }
        }

        // Get creation time (may not be available, use mtime as fallback)
        if (out_ctime_ns) {
            // Arrow FileInfo doesn't have separate creation time, use mtime
            auto mtime = file_info.mtime();
            if (mtime.time_since_epoch().count() > 0) {
                auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    mtime.time_since_epoch());
                *out_ctime_ns = duration_ns.count();
            }
        }

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        if (out_exists) *out_exists = false;
        if (out_is_dir) *out_is_dir = false;
        if (out_mtime_ns) *out_mtime_ns = 0;
        if (out_ctime_ns) *out_ctime_ns = 0;
        return milvus::FailureCStatus(&e);
    }
}

CStatus
CreateDir(const char* c_path, bool recursive) {
    try {
        AssertInfo(c_path != nullptr, "Path cannot be null");

        // Get file system singleton
        auto& fs_singleton = milvus_storage::ArrowFileSystemSingleton::GetInstance();
        auto fs = fs_singleton.GetArrowFileSystem();
        AssertInfo(fs != nullptr, "File system not initialized");

        std::string path(c_path);

        // Create directory (or bucket if path contains only bucket name)
        auto create_status = fs->CreateDir(path, recursive);
        if (!create_status.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, create_status.message());
        }

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
ListDir(const char* c_path,
        bool recursive,
        char*** out_paths,
        bool** out_is_dirs,
        int64_t** out_sizes,
        int64_t** out_mtime_ns,
        int* out_count) {
    try {
        AssertInfo(c_path != nullptr, "Path cannot be null");
        AssertInfo(out_paths != nullptr, "Output paths pointer cannot be null");
        AssertInfo(out_is_dirs != nullptr, "Output is_dirs pointer cannot be null");
        AssertInfo(out_sizes != nullptr, "Output sizes pointer cannot be null");
        AssertInfo(out_mtime_ns != nullptr, "Output mtime_ns pointer cannot be null");
        AssertInfo(out_count != nullptr, "Output count pointer cannot be null");

        // Initialize outputs
        *out_paths = nullptr;
        *out_is_dirs = nullptr;
        *out_sizes = nullptr;
        *out_mtime_ns = nullptr;
        *out_count = 0;

        // Get file system singleton
        auto& fs_singleton = milvus_storage::ArrowFileSystemSingleton::GetInstance();
        auto fs = fs_singleton.GetArrowFileSystem();
        AssertInfo(fs != nullptr, "File system not initialized");

        std::string path(c_path);

        // Create FileSelector to list directory contents
        arrow::fs::FileSelector selector;
        selector.base_dir = path;
        selector.recursive = recursive;
        selector.allow_not_found = false;

        // Get file info list
        auto file_info_result = fs->GetFileInfo(selector);
        if (!file_info_result.ok()) {
            return milvus::FailureCStatus(milvus::UnexpectedError, file_info_result.status().message());
        }

        auto file_infos = file_info_result.ValueOrDie();
        int count = static_cast<int>(file_infos.size());

        if (count == 0) {
            return milvus::SuccessCStatus();
        }

        // Allocate arrays
        *out_paths = (char**)malloc(count * sizeof(char*));
        *out_is_dirs = (bool*)malloc(count * sizeof(bool));
        *out_sizes = (int64_t*)malloc(count * sizeof(int64_t));
        *out_mtime_ns = (int64_t*)malloc(count * sizeof(int64_t));

        AssertInfo(*out_paths != nullptr && *out_is_dirs != nullptr &&
                  *out_sizes != nullptr && *out_mtime_ns != nullptr,
                  "Failed to allocate memory for list directory results");

        // Fill arrays with file info
        for (int i = 0; i < count; i++) {
            const auto& file_info = file_infos[i];

            // Copy path
            (*out_paths)[i] = strdup(file_info.path().c_str());
            AssertInfo((*out_paths)[i] != nullptr, "Failed to duplicate path string");

            // Set directory flag
            (*out_is_dirs)[i] = (file_info.type() == arrow::fs::FileType::Directory);

            // Set size (0 for directories)
            if (file_info.type() == arrow::fs::FileType::Directory) {
                (*out_sizes)[i] = 0;
            } else {
                (*out_sizes)[i] = file_info.size();
            }

            // Set modification time
            auto mtime = file_info.mtime();
            if (mtime.time_since_epoch().count() > 0) {
                auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    mtime.time_since_epoch());
                (*out_mtime_ns)[i] = duration_ns.count();
            } else {
                (*out_mtime_ns)[i] = 0;
            }
        }

        *out_count = count;

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        // Clean up on error
        if (out_paths && *out_paths && out_count && *out_count > 0) {
            for (int i = 0; i < *out_count; i++) {
                free((*out_paths)[i]);
            }
            free(*out_paths);
            *out_paths = nullptr;
        }
        if (out_is_dirs && *out_is_dirs) {
            free(*out_is_dirs);
            *out_is_dirs = nullptr;
        }
        if (out_sizes && *out_sizes) {
            free(*out_sizes);
            *out_sizes = nullptr;
        }
        if (out_mtime_ns && *out_mtime_ns) {
            free(*out_mtime_ns);
            *out_mtime_ns = nullptr;
        }
        if (out_count) *out_count = 0;
        return milvus::FailureCStatus(&e);
    }
}

// ============ Helper Functions for Memory Management ============

void
FreeMemory(void* ptr) {
    if (ptr != nullptr) {
        free(ptr);
    }
}
