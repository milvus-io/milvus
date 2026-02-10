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

#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <boost/filesystem/path.hpp>

#include "common/Consts.h"
#include "common/type_c.h"
#include "filemanager/FileManager.h"
#include "log/Log.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/properties.h"
#include "storage/ChunkManager.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/IndexEntryEncryptedLocalWriter.h"
#include "storage/IndexEntryWriter.h"
#include "storage/PluginLoader.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"
#include "storage/Types.h"

namespace milvus::storage {

// Normalize path to be consistent with Go's path.Join behavior.
// This handles two issues:
// 1. Removes leading "./" when root_path is "."
// 2. Removes trailing "/." that lexically_normal() may produce
inline std::string
NormalizePath(const boost::filesystem::path& path) {
    auto result = path.lexically_normal().string();
    // Remove trailing "/." if present
    if (result.size() >= 2 && result.substr(result.size() - 2) == "/.") {
        result = result.substr(0, result.size() - 1);
    }
    return result;
}

struct FileManagerContext {
    FileManagerContext() : chunkManagerPtr(nullptr) {
    }
    explicit FileManagerContext(const ChunkManagerPtr& chunkManagerPtr)
        : chunkManagerPtr(chunkManagerPtr) {
    }
    FileManagerContext(const FieldDataMeta& fieldDataMeta,
                       const IndexMeta& indexMeta,
                       const ChunkManagerPtr& chunkManagerPtr,
                       milvus_storage::ArrowFileSystemPtr fs)
        : fieldDataMeta(fieldDataMeta),
          indexMeta(indexMeta),
          chunkManagerPtr(chunkManagerPtr),
          fs(std::move(fs)) {
    }

    bool
    Valid() const {
        return chunkManagerPtr != nullptr;
    }

    void
    set_for_loading_index(bool value) {
        for_loading_index = value;
    }

    void
    set_plugin_context(std::shared_ptr<CPluginContext> context) {
        plugin_context = context;
    }

    /**
     * @brief Set the loon FFI properties for storage access
     *
     * Configures the properties used for accessing loon storage through
     * the FFI interface. These properties contain storage configuration
     * such as endpoints, credentials, and connection settings.
     *
     * @param properties Shared pointer to Properties object
     */
    void
    set_loon_ffi_properties(
        std::shared_ptr<milvus_storage::api::Properties> properties) {
        loon_ffi_properties = std::move(properties);
    }

    FieldDataMeta fieldDataMeta;
    IndexMeta indexMeta;
    ChunkManagerPtr chunkManagerPtr;
    milvus_storage::ArrowFileSystemPtr fs;
    bool for_loading_index{false};
    std::shared_ptr<CPluginContext> plugin_context;
    std::shared_ptr<milvus_storage::api::Properties> loon_ffi_properties;
};

#define FILEMANAGER_TRY try {
#define FILEMANAGER_CATCH                                                   \
    }                                                                       \
    catch (SegcoreError & e) {                                              \
        LOG_ERROR("SegcoreError:{} code {}", e.what(), e.get_error_code()); \
        return false;                                                       \
    }                                                                       \
    catch (std::exception & e) {                                            \
        LOG_ERROR("Exception:{}", e.what());                                \
        return false;
#define FILEMANAGER_END }

class FileManagerImpl : public milvus::FileManager {
 public:
    explicit FileManagerImpl(const FieldDataMeta& field_mata,
                             IndexMeta index_meta)
        : field_meta_(field_mata), index_meta_(std::move(index_meta)) {
    }

 public:
    /**
     * @brief Load a file to the local disk, so we can use stl lib to operate it.
     *
     * @param filename
     * @return false if any error, or return true.
     */
    virtual bool
    LoadFile(const std::string& filename) override = 0;

    /**
     * @brief Add file to FileManager to manipulate it.
     *
     * @param filename
     * @return false if any error, or return true.
     */
    virtual bool
    AddFile(const std::string& filename) override = 0;

    /**
     * @brief Check if a file exists.
     *
     * @param filename
     * @return std::nullopt if any error, or return if the file exists.
     */
    virtual std::optional<bool>
    IsExisted(const std::string& filename) override = 0;

    /**
     * @brief Delete a file from FileManager.
     *
     * @param filename
     * @return false if any error, or return true.
     */
    virtual bool
    RemoveFile(const std::string& filename) override = 0;

    virtual bool
    AddFileMeta(const FileMeta& file_meta) override = 0;

    std::shared_ptr<InputStream>
    OpenInputStream(const std::string& filename) override final {
        return OpenInputStream(filename, /*is_index_file=*/true);
    }

    std::shared_ptr<OutputStream>
    OpenOutputStream(const std::string& filename) override final {
        return OpenOutputStream(filename, /*is_index_file=*/true);
    }

    std::shared_ptr<InputStream>
    OpenInputStream(const std::string& filename, bool is_index_file) {
        AssertInfo(fs_, "fs_ is nullptr, cannot open input stream");
        auto local_file_name = GetFileName(filename);
        auto remote_file_path = is_index_file ? GetRemoteIndexObjectPrefixV2()
                                              : GetRemoteTextLogPrefixV2();
        remote_file_path += "/" + local_file_name;
        auto remote_file = fs_->OpenInputFile(remote_file_path);
        AssertInfo(remote_file.ok(),
                   "failed to open remote file, reason: {}",
                   remote_file.status().ToString());
        return std::static_pointer_cast<milvus::InputStream>(
            std::make_shared<milvus::storage::RemoteInputStream>(
                std::move(remote_file.ValueOrDie())));
    }

    std::shared_ptr<OutputStream>
    OpenOutputStream(const std::string& filename, bool is_index_file) {
        AssertInfo(fs_, "fs_ is nullptr, cannot open output stream");
        auto local_file_name = GetFileName(filename);
        auto remote_file_path = is_index_file ? GetRemoteIndexObjectPrefixV2()
                                              : GetRemoteTextLogPrefixV2();
        remote_file_path += "/" + local_file_name;
        // Ensure parent directory exists before opening the output stream.
        // Only needed for local filesystems; object stores don't require
        // explicit directory creation and the call would waste I/O.
        if (milvus_storage::IsLocalFileSystem(fs_)) {
            auto dir_path =
                remote_file_path.substr(0, remote_file_path.find_last_of('/'));
            if (!dir_path.empty()) {
                auto status = fs_->CreateDir(dir_path, /*recursive=*/true);
                AssertInfo(status.ok(),
                           "failed to create directory {}, reason: {}",
                           dir_path,
                           status.ToString());
            }
        }
        auto remote_stream = fs_->OpenOutputStream(remote_file_path);
        AssertInfo(remote_stream.ok(),
                   "failed to open remote stream, reason: {}",
                   remote_stream.status().ToString());
        return std::make_shared<milvus::storage::RemoteOutputStream>(
            std::move(remote_stream.ValueOrDie()));
    }

    std::unique_ptr<IndexEntryWriter>
    CreateIndexEntryWriterV3(const std::string& filename,
                             bool is_index_file = true) {
        if (plugin_context_) {
            auto cipher_plugin = PluginLoader::GetInstance().getCipherPlugin();
            if (cipher_plugin) {
                auto local_file_name = GetFileName(filename);
                auto remote_path = is_index_file
                                       ? GetRemoteIndexObjectPrefixV2()
                                       : GetRemoteTextLogPrefixV2();
                remote_path += "/" + local_file_name;
                return std::make_unique<IndexEntryEncryptedLocalWriter>(
                    remote_path,
                    fs_,
                    cipher_plugin,
                    plugin_context_->ez_id,
                    plugin_context_->collection_id);
            }
        }
        return std::make_unique<IndexEntryDirectStreamWriter>(
            OpenOutputStream(filename, is_index_file));
    }

 public:
    virtual std::string
    GetName() const = 0;

    virtual FieldDataMeta
    GetFieldDataMeta() const {
        return field_meta_;
    }

    virtual IndexMeta
    GetIndexMeta() const {
        return index_meta_;
    }

    virtual ChunkManagerPtr
    GetChunkManager() const {
        return rcm_;
    }

    virtual std::string
    GetRemoteIndexObjectPrefix() const {
        boost::filesystem::path prefix = rcm_->GetRootPath();
        boost::filesystem::path path = std::string(INDEX_ROOT_PATH);
        boost::filesystem::path path1 =
            std::to_string(index_meta_.build_id) + "/" +
            std::to_string(index_meta_.index_version) + "/" +
            std::to_string(field_meta_.partition_id) + "/" +
            std::to_string(field_meta_.segment_id);
        return NormalizePath(prefix / path / path1);
    }

    virtual std::string
    GetRemoteIndexObjectPrefixV2() const {
        return std::string(INDEX_ROOT_PATH) + "/" +
               std::to_string(index_meta_.build_id) + "/" +
               std::to_string(index_meta_.index_version) + "/" +
               std::to_string(field_meta_.partition_id) + "/" +
               std::to_string(field_meta_.segment_id);
    }

    virtual std::string
    GetRemoteTextLogPrefix() const {
        boost::filesystem::path prefix = rcm_->GetRootPath();
        boost::filesystem::path path = std::string(TEXT_LOG_ROOT_PATH);
        boost::filesystem::path path1 =
            std::to_string(index_meta_.build_id) + "/" +
            std::to_string(index_meta_.index_version) + "/" +
            std::to_string(field_meta_.collection_id) + "/" +
            std::to_string(field_meta_.partition_id) + "/" +
            std::to_string(field_meta_.segment_id) + "/" +
            std::to_string(field_meta_.field_id);
        return NormalizePath(prefix / path / path1);
    }

    virtual std::string
    GetRemoteTextLogPrefixV2() const {
        return std::string(TEXT_LOG_ROOT_PATH) + "/" +
               std::to_string(index_meta_.build_id) + "/" +
               std::to_string(index_meta_.index_version) + "/" +
               std::to_string(field_meta_.collection_id) + "/" +
               std::to_string(field_meta_.partition_id) + "/" +
               std::to_string(field_meta_.segment_id) + "/" +
               std::to_string(field_meta_.field_id);
    }

    static std::string
    GetFileName(const std::string& filepath) {
        return boost::filesystem::path(filepath).filename().string();
    }

 protected:
    // collection meta
    FieldDataMeta field_meta_;

    // index meta
    IndexMeta index_meta_;
    ChunkManagerPtr rcm_;
    milvus_storage::ArrowFileSystemPtr fs_;
    std::shared_ptr<milvus_storage::api::Properties> loon_ffi_properties_;
    std::shared_ptr<CPluginContext> plugin_context_;
};

using FileManagerImplPtr = std::shared_ptr<FileManagerImpl>;

}  // namespace milvus::storage
