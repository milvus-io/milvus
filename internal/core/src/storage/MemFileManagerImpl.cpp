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
#include <memory>
#include <unordered_map>

#include "common/Common.h"
#include "common/FieldData.h"
#include "log/Log.h"
#include "storage/Util.h"
#include "storage/FileManager.h"

namespace milvus::storage {

MemFileManagerImpl::MemFileManagerImpl(
    const FileManagerContext& fileManagerContext)
    : FileManagerImpl(fileManagerContext.fieldDataMeta,
                      fileManagerContext.indexMeta) {
    rcm_ = fileManagerContext.chunkManagerPtr;
}

bool
MemFileManagerImpl::AddFile(const std::string& filename /* unused */) noexcept {
    return false;
}

bool
MemFileManagerImpl::AddFile(const BinarySet& binary_set) {
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
        added_total_mem_size_ += iter->second->size;
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

std::map<std::string, FieldDataPtr>
MemFileManagerImpl::LoadIndexToMemory(
    const std::vector<std::string>& remote_files) {
    std::map<std::string, FieldDataPtr> file_to_index_data;
    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::vector<std::string> batch_files;

    auto LoadBatchIndexFiles = [&]() {
        auto index_datas = GetObjectData(rcm_.get(), batch_files);
        for (size_t idx = 0; idx < batch_files.size(); ++idx) {
            auto file_name =
                batch_files[idx].substr(batch_files[idx].find_last_of('/') + 1);
            file_to_index_data[file_name] =
                index_datas[idx].get()->GetFieldData();
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
    SortByPath(remote_files);

    auto parallel_degree =
        uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::vector<std::string> batch_files;
    std::vector<FieldDataPtr> field_datas;

    auto FetchRawData = [&]() {
        auto raw_datas = GetObjectData(rcm_.get(), batch_files);
        for (auto& data : raw_datas) {
            field_datas.emplace_back(data.get()->GetFieldData());
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

template <DataType T>
std::vector<std::vector<uint32_t>>
GetOptFieldIvfDataImpl(const std::vector<FieldDataPtr>& field_datas) {
    using FieldDataT = DataTypeNativeOrVoid<T>;
    std::unordered_map<FieldDataT, std::vector<uint32_t>> mp;
    uint32_t offset = 0;
    for (const auto& field_data : field_datas) {
        for (int64_t i = 0; i < field_data->get_num_rows(); ++i) {
            auto val =
                *reinterpret_cast<const FieldDataT*>(field_data->RawValue(i));
            mp[val].push_back(offset++);
        }
    }

    // opt field data is not used if there is only one value
    if (mp.size() <= 1) {
        return {};
    }
    std::vector<std::vector<uint32_t>> scalar_info;
    scalar_info.reserve(mp.size());
    for (auto& [field_id, tup] : mp) {
        scalar_info.emplace_back(std::move(tup));
    }
    LOG_INFO("Get opt fields with {} categories", scalar_info.size());
    return scalar_info;
}

std::vector<std::vector<uint32_t>>
GetOptFieldIvfData(const DataType& dt,
                   const std::vector<FieldDataPtr>& field_datas) {
    switch (dt) {
        case DataType::BOOL:
            return GetOptFieldIvfDataImpl<DataType::BOOL>(field_datas);
        case DataType::INT8:
            return GetOptFieldIvfDataImpl<DataType::INT8>(field_datas);
        case DataType::INT16:
            return GetOptFieldIvfDataImpl<DataType::INT16>(field_datas);
        case DataType::INT32:
            return GetOptFieldIvfDataImpl<DataType::INT32>(field_datas);
        case DataType::INT64:
            return GetOptFieldIvfDataImpl<DataType::INT64>(field_datas);
        case DataType::FLOAT:
            return GetOptFieldIvfDataImpl<DataType::FLOAT>(field_datas);
        case DataType::DOUBLE:
            return GetOptFieldIvfDataImpl<DataType::DOUBLE>(field_datas);
        case DataType::STRING:
            return GetOptFieldIvfDataImpl<DataType::STRING>(field_datas);
        case DataType::VARCHAR:
            return GetOptFieldIvfDataImpl<DataType::VARCHAR>(field_datas);
        default:
            LOG_WARN("Unsupported data type in optional scalar field: ", dt);
            return {};
    }
    return {};
}

std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>
MemFileManagerImpl::CacheOptFieldToMemory(OptFieldT& fields_map) {
    const uint32_t num_of_fields = fields_map.size();
    if (0 == num_of_fields) {
        return {};
    } else if (num_of_fields > 1) {
        PanicInfo(
            ErrorCode::NotImplemented,
            "vector index build with multiple fields is not supported yet");
    }

    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>> res;
    for (auto& [field_id, tup] : fields_map) {
        const auto& field_type = std::get<1>(tup);
        auto& field_paths = std::get<2>(tup);
        if (0 == field_paths.size()) {
            LOG_WARN("optional field {} has no data", field_id);
            return {};
        }

        SortByPath(field_paths);
        std::vector<FieldDataPtr> field_datas =
            FetchFieldData(rcm_.get(), field_paths);
        res[field_id] = GetOptFieldIvfData(field_type, field_datas);
    }
    return res;
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
