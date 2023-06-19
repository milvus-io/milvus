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
#include <utility>

#include "common/Common.h"
#include "common/Slice.h"
#include "log/Log.h"
#include "config/ConfigKnowhere.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/LocalChunkManager.h"
#include "storage/MinioChunkManager.h"
#include "storage/Exception.h"
#include "storage/FieldData.h"
#include "storage/IndexData.h"
#include "storage/ThreadPool.h"
#include "storage/Util.h"
#include "storage/FieldDataFactory.h"

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

using ReadLock = std::shared_lock<std::shared_mutex>;
using WriteLock = std::lock_guard<std::shared_mutex>;

namespace milvus::storage {

DiskFileManagerImpl::DiskFileManagerImpl(const FieldDataMeta& field_meta,
                                         IndexMeta index_meta,
                                         const StorageConfig& storage_config)
    : field_meta_(field_meta), index_meta_(std::move(index_meta)) {
    remote_root_path_ = storage_config.remote_root_path;
    rcm_ = std::make_unique<MinioChunkManager>(storage_config);
}

DiskFileManagerImpl::~DiskFileManagerImpl() {
    auto& local_chunk_manager = LocalChunkManager::GetInstance();
    local_chunk_manager.RemoveDir(
        GetLocalIndexPathPrefixWithBuildID(index_meta_.build_id));
}

bool
DiskFileManagerImpl::LoadFile(const std::string& file) noexcept {
    return true;
}

std::pair<std::string, size_t>
EncodeAndUploadIndexSlice(RemoteChunkManager* remote_chunk_manager,
                          const std::string& file,
                          int64_t offset,
                          int64_t batch_size,
                          const IndexMeta& index_meta,
                          FieldDataMeta field_meta,
                          std::string object_key) {
    auto& local_chunk_manager = LocalChunkManager::GetInstance();
    auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[batch_size]);
    local_chunk_manager.Read(file, offset, buf.get(), batch_size);

    auto field_data =
        milvus::storage::FieldDataFactory::GetInstance().CreateFieldData(
            DataType::INT8);
    field_data->FillFieldData(buf.get(), batch_size);
    auto indexData = std::make_shared<IndexData>(field_data);
    indexData->set_index_meta(index_meta);
    indexData->SetFieldDataMeta(field_meta);
    auto serialized_index_data = indexData->serialize_to_remote_file();
    auto serialized_index_size = serialized_index_data.size();
    remote_chunk_manager->Write(
        object_key, serialized_index_data.data(), serialized_index_size);
    return std::make_pair(std::move(object_key), serialized_index_size);
}

bool
DiskFileManagerImpl::AddFile(const std::string& file) noexcept {
    auto& local_chunk_manager = LocalChunkManager::GetInstance();
    auto& pool = ThreadPool::GetInstance();
    FILEMANAGER_TRY
    if (!local_chunk_manager.Exist(file)) {
        LOG_SEGCORE_ERROR_ << "local file: " << file << " does not exist ";
        return false;
    }

    // record local file path
    local_paths_.emplace_back(file);

    auto fileName = GetFileName(file);
    auto fileSize = local_chunk_manager.Size(file);

    std::vector<std::string> batch_remote_files;
    std::vector<int64_t> remote_file_sizes;
    std::vector<int64_t> local_file_offsets;

    int slice_num = 0;
    auto parallel_degree = uint64_t(DEFAULT_DISK_INDEX_MAX_MEMORY_LIMIT /
                                    (index_file_slice_size << 20));
    for (int64_t offset = 0; offset < fileSize; slice_num++) {
        if (batch_remote_files.size() >= parallel_degree) {
            AddBatchIndexFiles(file,
                               local_file_offsets,
                               batch_remote_files,
                               remote_file_sizes);
            batch_remote_files.clear();
            remote_file_sizes.clear();
            local_file_offsets.clear();
        }

        auto batch_size =
            std::min(index_file_slice_size << 20, int64_t(fileSize) - offset);
        batch_remote_files.emplace_back(
            GenerateRemoteIndexFile(fileName, slice_num));
        remote_file_sizes.emplace_back(batch_size);
        local_file_offsets.emplace_back(offset);
        offset += batch_size;
    }
    if (batch_remote_files.size() > 0) {
        AddBatchIndexFiles(
            file, local_file_offsets, batch_remote_files, remote_file_sizes);
    }
    FILEMANAGER_CATCH
    FILEMANAGER_END

    return true;
}  // namespace knowhere

void
DiskFileManagerImpl::AddBatchIndexFiles(
    const std::string& local_file_name,
    const std::vector<int64_t>& local_file_offsets,
    const std::vector<std::string>& remote_files,
    const std::vector<int64_t>& remote_file_sizes) {
    auto& pool = ThreadPool::GetInstance();

    std::vector<std::future<std::pair<std::string, size_t>>> futures;
    AssertInfo(local_file_offsets.size() == remote_files.size(),
               "inconsistent size of offset slices with file slices");
    AssertInfo(remote_files.size() == remote_file_sizes.size(),
               "inconsistent size of file slices with size slices");

    for (int64_t i = 0; i < remote_files.size(); ++i) {
        futures.push_back(pool.Submit(EncodeAndUploadIndexSlice,
                                      rcm_.get(),
                                      local_file_name,
                                      local_file_offsets[i],
                                      remote_file_sizes[i],
                                      index_meta_,
                                      field_meta_,
                                      remote_files[i]));
    }

    for (auto& future : futures) {
        auto res = future.get();
        remote_paths_to_size_[res.first] = res.second;
    }
}

void
DiskFileManagerImpl::CacheIndexToDisk(
    const std::vector<std::string>& remote_files) {
    auto& local_chunk_manager = LocalChunkManager::GetInstance();

    std::map<std::string, std::vector<int>> index_slices;
    for (auto& file_path : remote_files) {
        auto pos = file_path.find_last_of("_");
        index_slices[file_path.substr(0, pos)].emplace_back(
            std::stoi(file_path.substr(pos + 1)));
    }

    for (auto& slices : index_slices) {
        std::sort(slices.second.begin(), slices.second.end());
    }

    auto EstimateParallelDegree = [&](const std::string& file) -> uint64_t {
        auto fileSize = rcm_->Size(file);
        return uint64_t(DEFAULT_DISK_INDEX_MAX_MEMORY_LIMIT / fileSize);
    };

    for (auto& slices : index_slices) {
        auto prefix = slices.first;
        auto local_index_file_name =
            GetLocalIndexObjectPrefix() +
            prefix.substr(prefix.find_last_of('/') + 1);
        local_chunk_manager.CreateFile(local_index_file_name);
        int64_t offset = 0;
        std::vector<std::string> batch_remote_files;
        uint64_t max_parallel_degree = INT_MAX;
        for (int& iter : slices.second) {
            if (batch_remote_files.size() == max_parallel_degree) {
                auto next_offset = CacheBatchIndexFilesToDisk(
                    batch_remote_files, local_index_file_name, offset);
                offset = next_offset;
                batch_remote_files.clear();
            }
            auto origin_file = prefix + "_" + std::to_string(iter);
            if (batch_remote_files.size() == 0) {
                // Use first file size as average size to estimate
                max_parallel_degree = EstimateParallelDegree(origin_file);
            }
            batch_remote_files.push_back(origin_file);
        }
        if (batch_remote_files.size() > 0) {
            auto next_offset = CacheBatchIndexFilesToDisk(
                batch_remote_files, local_index_file_name, offset);
            offset = next_offset;
            batch_remote_files.clear();
        }
        local_paths_.emplace_back(local_index_file_name);
    }
}

std::unique_ptr<DataCodec>
DownloadAndDecodeRemoteIndexfile(RemoteChunkManager* remote_chunk_manager,
                                 const std::string& file) {
    auto fileSize = remote_chunk_manager->Size(file);
    auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[fileSize]);
    remote_chunk_manager->Read(file, buf.get(), fileSize);

    return DeserializeFileData(buf, fileSize);
}

uint64_t
DiskFileManagerImpl::CacheBatchIndexFilesToDisk(
    const std::vector<std::string>& remote_files,
    const std::string& local_file_name,
    uint64_t local_file_init_offfset) {
    auto& local_chunk_manager = LocalChunkManager::GetInstance();
    auto& pool = ThreadPool::GetInstance();
    int batch_size = remote_files.size();

    std::vector<std::future<std::unique_ptr<DataCodec>>> futures;
    for (int i = 0; i < batch_size; ++i) {
        futures.push_back(pool.Submit(
            DownloadAndDecodeRemoteIndexfile, rcm_.get(), remote_files[i]));
    }

    uint64_t offset = local_file_init_offfset;
    for (int i = 0; i < batch_size; ++i) {
        auto res = futures[i].get();
        auto index_data = res->GetFieldData();
        auto index_size = index_data->Size();
        local_chunk_manager.Write(
            local_file_name,
            offset,
            reinterpret_cast<uint8_t*>(const_cast<void*>(index_data->Data())),
            index_size);
        offset += index_size;
    }

    return offset;
}

std::string
DiskFileManagerImpl::GetFileName(const std::string& localfile) {
    boost::filesystem::path localPath(localfile);
    return localPath.filename().string();
}

std::string
DiskFileManagerImpl::GetRemoteIndexObjectPrefix() const {
    return remote_root_path_ + "/" + std::string(INDEX_ROOT_PATH) + "/" +
           std::to_string(index_meta_.build_id) + "/" +
           std::to_string(index_meta_.index_version) + "/" +
           std::to_string(field_meta_.partition_id) + "/" +
           std::to_string(field_meta_.segment_id);
}

std::string
DiskFileManagerImpl::GetLocalIndexObjectPrefix() {
    return GenLocalIndexPathPrefix(index_meta_.build_id,
                                   index_meta_.index_version);
}

std::string
DiskFileManagerImpl::GetLocalRawDataObjectPrefix() {
    return GenFieldRawDataPathPrefix(field_meta_.segment_id,
                                     field_meta_.field_id);
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
