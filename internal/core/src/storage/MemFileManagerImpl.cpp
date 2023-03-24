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

#include "storage/MemFileManagerImpl.h"
#include "storage/MinioChunkManager.h"

#include "storage/Util.h"

namespace milvus::storage {

MemFileManagerImpl::MemFileManagerImpl(const FieldDataMeta& field_mata,
                                       const IndexMeta& index_meta,
                                       const StorageConfig& storage_config)
    : FileManagerImpl(field_mata, index_meta) {
    rcm_ = std::make_unique<MinioChunkManager>(storage_config);
}

MemFileManagerImpl::MemFileManagerImpl(const FieldDataMeta& field_mata,
                                       const IndexMeta& index_meta,
                                       RemoteChunkManagerPtr remote_chunk_manager)
    : FileManagerImpl(field_mata, index_meta) {
    rcm_ = remote_chunk_manager;
}

bool
MemFileManagerImpl::AddFile(const std::string& filename /* unused */) noexcept {
    return false;
}

bool
MemFileManagerImpl::AddFile(const BinarySet& binary_set) noexcept {
    std::vector<const uint8_t*> data_slices;
    std::vector<int64_t> slice_sizes;
    std::vector<std::string> slice_names;

    auto remotePrefix = GetRemoteIndexObjectPrefix();
    for (auto iter = binary_set.binary_map_.begin(); iter != binary_set.binary_map_.end(); iter++) {
        data_slices.emplace_back(iter->second->data.get());
        slice_sizes.emplace_back(iter->second->size);
        slice_names.emplace_back(remotePrefix + "/" + iter->first);
    }

    auto res = PutIndexData(rcm_.get(), data_slices, slice_sizes, slice_names, field_meta_, index_meta_);
    for (auto iter = res.begin(); iter != res.end(); ++iter) {
        remote_paths_to_size_[iter->first] = iter->second;
    }

    return true;
}

bool
MemFileManagerImpl::LoadFile(const std::string& filename) noexcept {
    return true;
}

BinarySet
MemFileManagerImpl::LoadIndexToMemory(std::vector<std::string> remote_files) {
    auto index_datas = GetObjectData(rcm_.get(), remote_files);
    int batch_size = remote_files.size();
    AssertInfo(index_datas.size() == batch_size, "inconsistent file num and index data num!");

    BinarySet binary_set;
    for (int i = 0; i < batch_size; ++i) {
        std::string index_key(remote_files[i].substr(remote_files[i].find_last_of("/") + 1));
        auto index_data = index_datas[i];
        auto index_size = index_data->Size();
        auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[index_size]);
        memcpy(buf.get(), (uint8_t*)index_data->Data(), index_size);
        binary_set.Append(index_key, buf, index_size);
        index_data.reset();
    }
    index_datas.clear();

    return binary_set;
}

std::vector<FieldDataPtr>
MemFileManagerImpl::CacheRawDataToMemory(std::vector<std::string> remote_files) {
    std::sort(remote_files.begin(), remote_files.end(), [](const std::string& a, const std::string& b) {
        return std::stol(a.substr(a.find_last_of("/") + 1)) < std::stol(b.substr(b.find_last_of("/") + 1));
    });

    auto field_datas = GetObjectData(rcm_.get(), remote_files);
    AssertInfo(field_datas.size() == remote_files.size(), "inconsistent file num and raw data num!");

    return field_datas;
}

std::optional<bool>
MemFileManagerImpl::IsExisted(const std::string& filename) noexcept {
    // TODO: implement this interface
    return false;
}

bool
MemFileManagerImpl::RemoveFile(const std::string& filename) noexcept {
    // TODO: implement this interface
    return false;
}

}  // namespace milvus::storage
