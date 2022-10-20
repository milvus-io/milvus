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
#include <boost/filesystem.hpp>
#include <mutex>

#include "common/Consts.h"
#include "common/Slice.h"
#include "log/Log.h"
#include "config/ConfigKnowhere.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/LocalChunkManager.h"
#include "storage/MinioChunkManager.h"
#include "storage/Exception.h"
#include "storage/FieldData.h"
#include "storage/IndexData.h"
#include "storage/Util.h"

#define FILEMANAGER_TRY try {
#define FILEMANAGER_CATCH                                                                   \
    }                                                                                       \
    catch (LocalChunkManagerException & e) {                                                \
        LOG_SEGCORE_ERROR_C << "LocalChunkManagerException:" << e.what();                   \
        return false;                                                                       \
    }                                                                                       \
    catch (MinioException & e) {                                                            \
        LOG_SEGCORE_ERROR_C << "milvus::storage::MinioException:" << e.what();              \
        return false;                                                                       \
    }                                                                                       \
    catch (DiskANNFileManagerException & e) {                                               \
        LOG_SEGCORE_ERROR_C << "milvus::storage::DiskANNFileManagerException:" << e.what(); \
        return false;                                                                       \
    }                                                                                       \
    catch (ArrowException & e) {                                                            \
        LOG_SEGCORE_ERROR_C << "milvus::storage::ArrowException:" << e.what();              \
        return false;                                                                       \
    }                                                                                       \
    catch (std::exception & e) {                                                            \
        LOG_SEGCORE_ERROR_C << "Exception:" << e.what();                                    \
        return false;
#define FILEMANAGER_END }

using ReadLock = std::shared_lock<std::shared_mutex>;
using WriteLock = std::lock_guard<std::shared_mutex>;

namespace milvus::storage {

DiskFileManagerImpl::DiskFileManagerImpl(const FieldDataMeta& field_mata,
                                         const IndexMeta& index_meta,
                                         const StorageConfig& storage_config)
    : field_meta_(field_mata), index_meta_(index_meta) {
    remote_root_path_ = storage_config.remote_root_path;
    rcm_ = std::make_unique<MinioChunkManager>(storage_config);
}

DiskFileManagerImpl::~DiskFileManagerImpl() {
    auto& local_chunk_manager = LocalChunkManager::GetInstance();
    local_chunk_manager.RemoveDir(GetLocalIndexPathPrefixWithBuildID(index_meta_.build_id));
}

bool
DiskFileManagerImpl::LoadFile(const std::string& file) noexcept {
    return true;
}

bool
DiskFileManagerImpl::AddFile(const std::string& file) noexcept {
    auto& local_chunk_manager = LocalChunkManager::GetInstance();
    FILEMANAGER_TRY
    if (!local_chunk_manager.Exist(file)) {
        LOG_SEGCORE_ERROR_C << "local file: " << file << " does not exist ";
        return false;
    }

    // record local file path
    local_paths_.emplace_back(file);

    auto fileName = GetFileName(file);
    auto fileSize = local_chunk_manager.Size(file);
    auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[fileSize]);
    local_chunk_manager.Read(file, buf.get(), fileSize);

    // Split local data to multi part with specified size
    int slice_num = 0;
    auto remotePrefix = GetRemoteIndexObjectPrefix();
    for (int64_t offset = 0; offset < fileSize; slice_num++) {
        auto batch_size = std::min(index_file_slice_size << 20, int64_t(fileSize) - offset);

        auto fieldData = std::make_shared<FieldData>(buf.get() + offset, batch_size);
        auto indexData = std::make_shared<IndexData>(fieldData);
        indexData->set_index_meta(index_meta_);
        indexData->SetFieldDataMeta(field_meta_);
        auto serialized_index_data = indexData->serialize_to_remote_file();
        auto serialized_index_size = serialized_index_data.size();

        // Put file to remote
        char objectKey[200];
        snprintf(objectKey, sizeof(objectKey), "%s/%s_%d", remotePrefix.c_str(), fileName.c_str(), slice_num);
        rcm_->Write(objectKey, serialized_index_data.data(), serialized_index_size);

        offset += batch_size;
        // record remote file to save etcd
        remote_paths_to_size_[objectKey] = serialized_index_size;
    }
    FILEMANAGER_CATCH
    FILEMANAGER_END

    return true;
}  // namespace knowhere

void
DiskFileManagerImpl::CacheIndexToDisk(std::vector<std::string> remote_files) {
    auto& local_chunk_manager = LocalChunkManager::GetInstance();

    std::map<std::string, std::vector<int>> index_slices;
    for (auto& file_path : remote_files) {
        auto pos = file_path.find_last_of("_");
        index_slices[file_path.substr(0, pos)].emplace_back(std::stoi(file_path.substr(pos + 1)));
    }

    for (auto& slices : index_slices) {
        std::sort(slices.second.begin(), slices.second.end());
    }

    for (auto& slices : index_slices) {
        auto prefix = slices.first;
        auto local_index_file_name = GetLocalIndexObjectPrefix() + prefix.substr(prefix.find_last_of("/") + 1);
        local_chunk_manager.CreateFile(local_index_file_name);
        int64_t offset = 0;
        for (auto iter = slices.second.begin(); iter != slices.second.end(); iter++) {
            auto origin_file = prefix + "_" + std::to_string(*iter);
            auto fileSize = rcm_->Size(origin_file);
            auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[fileSize]);
            rcm_->Read(origin_file, buf.get(), fileSize);

            auto decoded_index_data = DeserializeFileData(buf.get(), fileSize);
            auto index_payload = decoded_index_data->GetPayload();
            auto index_size = index_payload->rows * sizeof(uint8_t);

            local_chunk_manager.Write(local_index_file_name, offset, const_cast<uint8_t*>(index_payload->raw_data),
                                      index_size);
            offset += index_size;
        }
        local_paths_.emplace_back(local_index_file_name);
    }
}

std::string
DiskFileManagerImpl::GetFileName(const std::string& localfile) {
    boost::filesystem::path localPath(localfile);
    return localPath.filename().string();
}

std::string
DiskFileManagerImpl::GetRemoteIndexObjectPrefix() {
    return remote_root_path_ + "/" + std::string(INDEX_ROOT_PATH) + "/" + std::to_string(index_meta_.build_id) + "/" +
           std::to_string(index_meta_.index_version) + "/" + std::to_string(field_meta_.partition_id) + "/" +
           std::to_string(field_meta_.segment_id);
}

std::string
DiskFileManagerImpl::GetLocalIndexObjectPrefix() {
    return GenLocalIndexPathPrefix(index_meta_.build_id, index_meta_.index_version);
}

std::string
DiskFileManagerImpl::GetLocalRawDataObjectPrefix() {
    return GenFieldRawDataPathPrefix(field_meta_.segment_id, field_meta_.field_id);
}

bool
DiskFileManagerImpl::RemoveFile(const std::string& file) noexcept {
    // TODO: implement this interface
    return false;
}

std::optional<bool>
DiskFileManagerImpl::IsExisted(const std::string& file) noexcept {
    bool isExist = false;
    auto& local_chunk_manager = LocalChunkManager::GetInstance();
    try {
        isExist = local_chunk_manager.Exist(file);
    } catch (LocalChunkManagerException& e) {
        // LOG_SEGCORE_DEBUG_ << "LocalChunkManagerException:"
        //                   << e.what();
        return std::nullopt;
    } catch (std::exception& e) {
        // LOG_SEGCORE_DEBUG_ << "Exception:" << e.what();
        return std::nullopt;
    }
    return isExist;
}

}  // namespace milvus::storage
