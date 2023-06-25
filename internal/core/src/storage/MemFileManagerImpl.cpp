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

#include "storage/Util.h"
#include "common/Common.h"

namespace milvus::storage {

MemFileManagerImpl::MemFileManagerImpl(const FieldDataMeta& field_mata,
                                       IndexMeta index_meta,
                                       ChunkManagerPtr remote_chunk_manager)
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

    auto AddBatchIndexFiles = [&]() {
        auto res = PutIndexData(rcm_.get(),
                                data_slices,
                                slice_sizes,
                                slice_names,
                                field_meta_,
                                index_meta_);
        for (auto& [file, size] : res) {
            remote_paths_to_size_[file] = size;
        }
    };

    auto remotePrefix = GetRemoteIndexObjectPrefix();
    int64_t batch_size = 0;
    for (auto iter = binary_set.binary_map_.begin();
         iter != binary_set.binary_map_.end();
         iter++) {
        if (batch_size >= DEFAULT_FIELD_MAX_MEMORY_LIMIT) {
            AddBatchIndexFiles();
            data_slices.clear();
            slice_sizes.clear();
            slice_names.clear();
            batch_size = 0;
        }

        data_slices.emplace_back(iter->second->data.get());
        slice_sizes.emplace_back(iter->second->size);
        slice_names.emplace_back(remotePrefix + "/" + iter->first);
        batch_size += iter->second->size;
    }

    if (data_slices.size() > 0) {
        AddBatchIndexFiles();
    }

    return true;
}

bool
MemFileManagerImpl::LoadFile(const std::string& filename) noexcept {
    return true;
}

std::map<std::string, storage::FieldDataPtr>
MemFileManagerImpl::LoadIndexToMemory(
    const std::vector<std::string>& remote_files) {
    std::map<std::string, storage::FieldDataPtr> file_to_index_data;
    auto parallel_degree =
        uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::vector<std::string> batch_files;

    auto LoadBatchIndexFiles = [&]() {
        auto index_datas = GetObjectData(rcm_.get(), batch_files);
        for (size_t idx = 0; idx < batch_files.size(); ++idx) {
            auto file_name =
                batch_files[idx].substr(batch_files[idx].find_last_of("/") + 1);
            file_to_index_data[file_name] = index_datas[idx];
        }
    };

    for (auto& file : remote_files) {
        if (batch_files.size() >= parallel_degree) {
            LoadBatchIndexFiles();
            batch_files.clear();
        }
        batch_files.emplace_back(file);
    }

    if (batch_files.size() > 0) {
        LoadBatchIndexFiles();
    }

    AssertInfo(file_to_index_data.size() == remote_files.size(),
               "inconsistent file num and index data num!");
    return file_to_index_data;
}

std::vector<FieldDataPtr>
MemFileManagerImpl::CacheRawDataToMemory(
    std::vector<std::string> remote_files) {
    std::sort(remote_files.begin(),
              remote_files.end(),
              [](const std::string& a, const std::string& b) {
                  return std::stol(a.substr(a.find_last_of("/") + 1)) <
                         std::stol(b.substr(b.find_last_of("/") + 1));
              });

    auto parallel_degree =
        uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::vector<std::string> batch_files;
    std::vector<FieldDataPtr> field_datas;

    auto FetchRawData = [&]() {
        auto raw_datas = GetObjectData(rcm_.get(), batch_files);
        for (auto& data : raw_datas) {
            field_datas.emplace_back(data);
        }
    };

    for (auto& file : remote_files) {
        if (batch_files.size() >= parallel_degree) {
            FetchRawData();
            batch_files.clear();
        }
        batch_files.emplace_back(file);
    }
    if (batch_files.size() > 0) {
        FetchRawData();
    }

    AssertInfo(field_datas.size() == remote_files.size(),
               "inconsistent file num and raw data num!");
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