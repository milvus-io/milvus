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

#include <string>
#include <optional>
#include <memory>

#include "knowhere/file_manager.h"
#include "common/Consts.h"
#include "storage/ChunkManager.h"
#include "storage/Types.h"
#include "log/Log.h"

namespace milvus::storage {

#define FILEMANAGER_TRY try {
#define FILEMANAGER_CATCH                                                     \
    }                                                                         \
    catch (LocalChunkManagerException & e) {                                  \
        LOG_SEGCORE_ERROR_ << "LocalChunkManagerException:" << e.what();      \
        return false;                                                         \
    }                                                                         \
    catch (MinioException & e) {                                              \
        LOG_SEGCORE_ERROR_ << "milvus::storage::MinioException:" << e.what(); \
        return false;                                                         \
    }                                                                         \
    catch (DiskANNFileManagerException & e) {                                 \
        LOG_SEGCORE_ERROR_ << "milvus::storage::DiskANNFileManagerException:" \
                           << e.what();                                       \
        return false;                                                         \
    }                                                                         \
    catch (ArrowException & e) {                                              \
        LOG_SEGCORE_ERROR_ << "milvus::storage::ArrowException:" << e.what(); \
        return false;                                                         \
    }                                                                         \
    catch (std::exception & e) {                                              \
        LOG_SEGCORE_ERROR_ << "Exception:" << e.what();                       \
        return false;
#define FILEMANAGER_END }

class FileManagerImpl : public knowhere::FileManager {
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
    LoadFile(const std::string& filename) noexcept = 0;

    /**
     * @brief Add file to FileManager to manipulate it.
     *
     * @param filename
     * @return false if any error, or return true.
     */
    virtual bool
    AddFile(const std::string& filename) noexcept = 0;

    /**
     * @brief Check if a file exists.
     *
     * @param filename
     * @return std::nullopt if any error, or return if the file exists.
     */
    virtual std::optional<bool>
    IsExisted(const std::string& filename) noexcept = 0;

    /**
     * @brief Delete a file from FileManager.
     *
     * @param filename
     * @return false if any error, or return true.
     */
    virtual bool
    RemoveFile(const std::string& filename) noexcept = 0;

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

    virtual std::string
    GetRemoteIndexObjectPrefix() const {
        return rcm_->GetRootPath() + "/" + std::string(INDEX_ROOT_PATH) + "/" +
               std::to_string(index_meta_.build_id) + "/" +
               std::to_string(index_meta_.index_version) + "/" +
               std::to_string(field_meta_.partition_id) + "/" +
               std::to_string(field_meta_.segment_id);
    }

 protected:
    // collection meta
    FieldDataMeta field_meta_;

    // index meta
    IndexMeta index_meta_;
    ChunkManagerPtr rcm_;
};

using FileManagerImplPtr = std::shared_ptr<FileManagerImpl>;

}  // namespace milvus::storage
