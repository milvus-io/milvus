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

#include <sys/fcntl.h>
#include <algorithm>
#include <boost/filesystem.hpp>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/File.h"
#include "common/Slice.h"
#include "common/Types.h"
#include "log/Log.h"

#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"
#include "storage/IndexData.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"

namespace milvus::storage {
DiskFileManagerImpl::DiskFileManagerImpl(
    const FileManagerContext& fileManagerContext)
    : FileManagerImpl(fileManagerContext.fieldDataMeta,
                      fileManagerContext.indexMeta,
                      fileManagerContext.for_loading_index) {
    rcm_ = fileManagerContext.chunkManagerPtr;
}

DiskFileManagerImpl::~DiskFileManagerImpl() {
    auto local_chunk_manager = GetLocalChunkManager();
    local_chunk_manager->RemoveDir(GetIndexPathPrefixWithBuildID(
        local_chunk_manager, index_meta_.build_id));
}

LocalChunkManagerSPtr
DiskFileManagerImpl::GetLocalChunkManager() const {
    auto role =
        for_loading_index_ ? milvus::Role::QueryNode : milvus::Role::IndexNode;
    return LocalChunkManagerFactory::GetInstance().GetChunkManager(role);
}

bool
DiskFileManagerImpl::LoadFile(const std::string& file) noexcept {
    return true;
}

std::string
DiskFileManagerImpl::GetRemoteIndexPath(const std::string& file_name,
                                        int64_t slice_num) const {
    std::string remote_prefix;
    remote_prefix = GetRemoteIndexObjectPrefix();
    return remote_prefix + "/" + file_name + "_" + std::to_string(slice_num);
}

std::string
DiskFileManagerImpl::GetRemoteTextLogPath(const std::string& file_name,
                                          int64_t slice_num) const {
    auto remote_prefix = GetRemoteTextLogPrefix();
    return remote_prefix + "/" + file_name + "_" + std::to_string(slice_num);
}

bool
DiskFileManagerImpl::AddFile(const std::string& file) noexcept {
    auto local_chunk_manager =
        LocalChunkManagerFactory::GetInstance().GetChunkManager();
    FILEMANAGER_TRY
    if (!local_chunk_manager->Exist(file)) {
        LOG_ERROR("local file {} not exists", file);
        return false;
    }

    // record local file path
    local_paths_.emplace_back(file);

    auto fileName = GetFileName(file);
    auto fileSize = local_chunk_manager->Size(file);

    std::vector<std::string> batch_remote_files;
    std::vector<int64_t> remote_file_sizes;
    std::vector<int64_t> local_file_offsets;

    int slice_num = 0;
    auto parallel_degree =
        uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
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

        auto batch_size = std::min(FILE_SLICE_SIZE, int64_t(fileSize) - offset);
        batch_remote_files.emplace_back(
            GetRemoteIndexPath(fileName, slice_num));
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

bool
DiskFileManagerImpl::AddTextLog(const std::string& file) noexcept {
    auto local_chunk_manager =
        LocalChunkManagerFactory::GetInstance().GetChunkManager();
    FILEMANAGER_TRY
    if (!local_chunk_manager->Exist(file)) {
        LOG_ERROR("local file {} not exists", file);
        return false;
    }

    // record local file path
    local_paths_.emplace_back(file);

    auto fileName = GetFileName(file);
    auto fileSize = local_chunk_manager->Size(file);

    std::vector<std::string> batch_remote_files;
    std::vector<int64_t> remote_file_sizes;
    std::vector<int64_t> local_file_offsets;

    int slice_num = 0;
    auto parallel_degree =
        uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
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

        auto batch_size = std::min(FILE_SLICE_SIZE, int64_t(fileSize) - offset);
        batch_remote_files.emplace_back(
            GetRemoteTextLogPath(fileName, slice_num));
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
    auto local_chunk_manager =
        LocalChunkManagerFactory::GetInstance().GetChunkManager();
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);

    std::vector<std::future<std::shared_ptr<uint8_t[]>>> futures;
    futures.reserve(remote_file_sizes.size());
    AssertInfo(local_file_offsets.size() == remote_files.size(),
               "inconsistent size of offset slices with file slices");
    AssertInfo(remote_files.size() == remote_file_sizes.size(),
               "inconsistent size of file slices with size slices");

    for (int64_t i = 0; i < remote_files.size(); ++i) {
        futures.push_back(pool.Submit(
            [&](const std::string& file,
                const int64_t offset,
                const int64_t data_size) -> std::shared_ptr<uint8_t[]> {
                auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[data_size]);
                local_chunk_manager->Read(file, offset, buf.get(), data_size);
                return buf;
            },
            local_file_name,
            local_file_offsets[i],
            remote_file_sizes[i]));
    }

    // hold index data util upload index file done
    std::vector<std::shared_ptr<uint8_t[]>> index_datas;
    std::vector<const uint8_t*> data_slices;
    for (auto& future : futures) {
        auto res = future.get();
        index_datas.emplace_back(res);
        data_slices.emplace_back(res.get());
    }

    std::map<std::string, int64_t> res;
    res = PutIndexData(rcm_.get(),
                       data_slices,
                       remote_file_sizes,
                       remote_files,
                       field_meta_,
                       index_meta_);
    for (auto& re : res) {
        remote_paths_to_size_[re.first] = re.second;
    }
}

void
DiskFileManagerImpl::CacheIndexToDisk(
    const std::vector<std::string>& remote_files) {
    auto local_chunk_manager =
        LocalChunkManagerFactory::GetInstance().GetChunkManager();

    std::map<std::string, std::vector<int>> index_slices;
    for (auto& file_path : remote_files) {
        auto pos = file_path.find_last_of('_');
        AssertInfo(pos > 0, "invalided index file path:{}", file_path);
        try {
            auto idx = std::stoi(file_path.substr(pos + 1));
            index_slices[file_path.substr(0, pos)].emplace_back(idx);
        } catch (const std::logic_error& e) {
            auto err_message = fmt::format(
                "invalided index file path:{}, error:{}", file_path, e.what());
            LOG_ERROR(err_message);
            throw std::logic_error(err_message);
        }
    }

    for (auto& slices : index_slices) {
        std::sort(slices.second.begin(), slices.second.end());
    }

    for (auto& slices : index_slices) {
        auto prefix = slices.first;
        auto local_index_file_name =
            GetLocalIndexObjectPrefix() +
            prefix.substr(prefix.find_last_of('/') + 1);
        local_chunk_manager->CreateFile(local_index_file_name);
        auto file =
            File::Open(local_index_file_name, O_CREAT | O_RDWR | O_TRUNC);

        // Get the remote files
        std::vector<std::string> batch_remote_files;
        batch_remote_files.reserve(slices.second.size());

        uint64_t max_parallel_degree =
            uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);

        auto appendIndexFiles = [&]() {
            auto index_chunks = GetObjectData(rcm_.get(), batch_remote_files);
            for (auto& chunk : index_chunks) {
                auto index_data = chunk.get()->GetFieldData();
                auto index_size = index_data->DataSize();
                auto chunk_data = reinterpret_cast<uint8_t*>(
                    const_cast<void*>(index_data->Data()));
                file.Write(chunk_data, index_size);
            }
            batch_remote_files.clear();
        };

        for (int& iter : slices.second) {
            auto origin_file = prefix + "_" + std::to_string(iter);
            batch_remote_files.push_back(origin_file);

            if (batch_remote_files.size() == max_parallel_degree) {
                appendIndexFiles();
            }
        }
        if (batch_remote_files.size() > 0) {
            appendIndexFiles();
        }
        local_paths_.emplace_back(local_index_file_name);
    }
}

void
DiskFileManagerImpl::CacheTextLogToDisk(
    const std::vector<std::string>& remote_files) {
    auto local_chunk_manager =
        LocalChunkManagerFactory::GetInstance().GetChunkManager();

    std::map<std::string, std::vector<int>> index_slices;
    for (auto& file_path : remote_files) {
        auto pos = file_path.find_last_of("_");
        AssertInfo(pos > 0, "invalided index file path:{}", file_path);
        try {
            auto idx = std::stoi(file_path.substr(pos + 1));
            index_slices[file_path.substr(0, pos)].emplace_back(idx);
        } catch (const std::logic_error& e) {
            auto err_message = fmt::format(
                "invalided text log path:{}, error:{}", file_path, e.what());
            LOG_ERROR(err_message);
            throw std::logic_error(err_message);
        }
    }

    for (auto& slices : index_slices) {
        std::sort(slices.second.begin(), slices.second.end());
    }

    for (auto& slices : index_slices) {
        auto prefix = slices.first;
        auto local_index_file_name =
            GetLocalTextIndexPrefix() + "/" +
            prefix.substr(prefix.find_last_of('/') + 1);
        local_chunk_manager->CreateFile(local_index_file_name);
        auto file =
            File::Open(local_index_file_name, O_CREAT | O_RDWR | O_TRUNC);

        // Get the remote files
        std::vector<std::string> batch_remote_files;
        batch_remote_files.reserve(slices.second.size());
        for (int& iter : slices.second) {
            auto origin_file = prefix + "_" + std::to_string(iter);
            batch_remote_files.push_back(origin_file);
        }

        auto index_chunks = GetObjectData(rcm_.get(), batch_remote_files);
        for (auto& chunk : index_chunks) {
            auto index_data = chunk.get()->GetFieldData();
            auto index_size = index_data->Size();
            auto chunk_data = reinterpret_cast<uint8_t*>(
                const_cast<void*>(index_data->Data()));
            file.Write(chunk_data, index_size);
        }
        local_paths_.emplace_back(local_index_file_name);
    }
}

void
SortByPath(std::vector<std::string>& paths) {
    std::sort(paths.begin(),
              paths.end(),
              [](const std::string& a, const std::string& b) {
                  return std::stol(a.substr(a.find_last_of("/") + 1)) <
                         std::stol(b.substr(b.find_last_of("/") + 1));
              });
}

template <typename DataType>
std::string
DiskFileManagerImpl::CacheRawDataToDisk(std::vector<std::string> remote_files) {
    SortByPath(remote_files);

    auto segment_id = GetFieldDataMeta().segment_id;
    auto field_id = GetFieldDataMeta().field_id;

    auto local_chunk_manager = GetLocalChunkManager();
    std::string local_data_path;
    bool file_created = false;

    auto init_file_info = [&](milvus::DataType dt) {
        local_data_path = storage::GenFieldRawDataPathPrefix(
                              local_chunk_manager, segment_id, field_id) +
                          "raw_data";
        if (dt == milvus::DataType::VECTOR_SPARSE_FLOAT) {
            local_data_path += ".sparse_u32_f32";
        }
        local_chunk_manager->CreateFile(local_data_path);
    };

    // get batch raw data from s3 and write batch data to disk file
    // TODO: load and write of different batches at the same time
    std::vector<std::string> batch_files;

    // file format
    // num_rows(uint32) | dim(uint32) | index_data ([]uint8_t)
    uint32_t num_rows = 0;
    uint32_t dim = 0;
    int64_t write_offset = sizeof(num_rows) + sizeof(dim);

    auto FetchRawData = [&]() {
        auto field_datas = GetObjectData(rcm_.get(), batch_files);
        int batch_size = batch_files.size();
        for (int i = 0; i < batch_size; ++i) {
            auto field_data = field_datas[i].get()->GetFieldData();
            num_rows += uint32_t(field_data->get_num_rows());
            auto data_type = field_data->get_data_type();
            if (!file_created) {
                init_file_info(data_type);
                file_created = true;
            }
            if (data_type == milvus::DataType::VECTOR_SPARSE_FLOAT) {
                dim = std::max(
                    dim,
                    (uint32_t)(std::dynamic_pointer_cast<
                                   FieldData<SparseFloatVector>>(field_data)
                                   ->Dim()));
                auto sparse_rows =
                    static_cast<const knowhere::sparse::SparseRow<float>*>(
                        field_data->Data());
                for (size_t i = 0; i < field_data->Length(); ++i) {
                    auto row = sparse_rows[i];
                    auto row_byte_size = row.data_byte_size();
                    uint32_t nnz = row.size();
                    local_chunk_manager->Write(local_data_path,
                                               write_offset,
                                               const_cast<uint32_t*>(&nnz),
                                               sizeof(nnz));
                    write_offset += sizeof(nnz);
                    local_chunk_manager->Write(local_data_path,
                                               write_offset,
                                               row.data(),
                                               row_byte_size);
                    write_offset += row_byte_size;
                }
            } else {
                AssertInfo(dim == 0 || dim == field_data->get_dim(),
                           "inconsistent dim value in multi binlogs!");
                dim = field_data->get_dim();

                auto data_size = field_data->get_num_rows() *
                                 milvus::GetVecRowSize<DataType>(dim);
                local_chunk_manager->Write(
                    local_data_path,
                    write_offset,
                    const_cast<void*>(field_data->Data()),
                    data_size);
                write_offset += data_size;
            }
        }
    };

    auto parallel_degree =
        uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
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

    // write num_rows and dim value to file header
    write_offset = 0;
    local_chunk_manager->Write(
        local_data_path, write_offset, &num_rows, sizeof(num_rows));
    write_offset += sizeof(num_rows);
    local_chunk_manager->Write(
        local_data_path, write_offset, &dim, sizeof(dim));

    return local_data_path;
}

template <typename T, typename = void>
struct has_native_type : std::false_type {};
template <typename T>
struct has_native_type<T, std::void_t<typename T::NativeType>>
    : std::true_type {};
template <DataType T>
using DataTypeNativeOrVoid =
    typename std::conditional<has_native_type<TypeTraits<T>>::value,
                              typename TypeTraits<T>::NativeType,
                              void>::type;
template <DataType T>
using DataTypeToOffsetMap =
    std::unordered_map<DataTypeNativeOrVoid<T>, int64_t>;

template <DataType T>
bool
WriteOptFieldIvfDataImpl(
    const int64_t field_id,
    const std::shared_ptr<LocalChunkManager>& local_chunk_manager,
    const std::string& local_data_path,
    const std::vector<FieldDataPtr>& field_datas,
    uint64_t& write_offset) {
    using FieldDataT = DataTypeNativeOrVoid<T>;
    using OffsetT = uint32_t;
    std::unordered_map<FieldDataT, std::vector<OffsetT>> mp;
    OffsetT offset = 0;
    for (const auto& field_data : field_datas) {
        for (int64_t i = 0; i < field_data->get_num_rows(); ++i) {
            auto val =
                *reinterpret_cast<const FieldDataT*>(field_data->RawValue(i));
            mp[val].push_back(offset++);
        }
    }

    // Do not write to disk if there is only one value
    if (mp.size() == 1) {
        return false;
    }

    local_chunk_manager->Write(local_data_path,
                               write_offset,
                               const_cast<int64_t*>(&field_id),
                               sizeof(field_id));
    write_offset += sizeof(field_id);
    const uint32_t num_of_unique_field_data = mp.size();
    local_chunk_manager->Write(local_data_path,
                               write_offset,
                               const_cast<uint32_t*>(&num_of_unique_field_data),
                               sizeof(num_of_unique_field_data));
    write_offset += sizeof(num_of_unique_field_data);
    for (const auto& [val, offsets] : mp) {
        const uint32_t offsets_cnt = offsets.size();
        local_chunk_manager->Write(local_data_path,
                                   write_offset,
                                   const_cast<uint32_t*>(&offsets_cnt),
                                   sizeof(offsets_cnt));
        write_offset += sizeof(offsets_cnt);
        const size_t data_size = offsets_cnt * sizeof(OffsetT);
        local_chunk_manager->Write(local_data_path,
                                   write_offset,
                                   const_cast<OffsetT*>(offsets.data()),
                                   data_size);
        write_offset += data_size;
    }
    return true;
}

#define GENERATE_OPT_FIELD_IVF_IMPL(DT)               \
    WriteOptFieldIvfDataImpl<DT>(field_id,            \
                                 local_chunk_manager, \
                                 local_data_path,     \
                                 field_datas,         \
                                 write_offset)
bool
WriteOptFieldIvfData(
    const DataType& dt,
    const int64_t field_id,
    const std::shared_ptr<LocalChunkManager>& local_chunk_manager,
    const std::string& local_data_path,
    const std::vector<FieldDataPtr>& field_datas,
    uint64_t& write_offset) {
    switch (dt) {
        case DataType::BOOL:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::BOOL);
        case DataType::INT8:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::INT8);
        case DataType::INT16:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::INT16);
        case DataType::INT32:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::INT32);
        case DataType::INT64:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::INT64);
        case DataType::FLOAT:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::FLOAT);
        case DataType::DOUBLE:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::DOUBLE);
        case DataType::STRING:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::STRING);
        case DataType::VARCHAR:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::VARCHAR);
        default:
            LOG_WARN("Unsupported data type in optional scalar field: ", dt);
            return false;
    }
    return true;
}
#undef GENERATE_OPT_FIELD_IVF_IMPL

void
WriteOptFieldsIvfMeta(
    const std::shared_ptr<LocalChunkManager>& local_chunk_manager,
    const std::string& local_data_path,
    const uint32_t num_of_fields,
    uint64_t& write_offset) {
    const uint8_t kVersion = 0;
    local_chunk_manager->Write(local_data_path,
                               write_offset,
                               const_cast<uint8_t*>(&kVersion),
                               sizeof(kVersion));
    write_offset += sizeof(kVersion);
    local_chunk_manager->Write(local_data_path,
                               write_offset,
                               const_cast<uint32_t*>(&num_of_fields),
                               sizeof(num_of_fields));
    write_offset += sizeof(num_of_fields);
}

std::string
DiskFileManagerImpl::CacheOptFieldToDisk(OptFieldT& fields_map) {
    const uint32_t num_of_fields = fields_map.size();
    if (0 == num_of_fields) {
        return "";
    } else if (num_of_fields > 1) {
        PanicInfo(
            ErrorCode::NotImplemented,
            "vector index build with multiple fields is not supported yet");
    }

    auto segment_id = GetFieldDataMeta().segment_id;
    auto vec_field_id = GetFieldDataMeta().field_id;
    auto local_chunk_manager = GetLocalChunkManager();
    auto local_data_path = storage::GenFieldRawDataPathPrefix(
                               local_chunk_manager, segment_id, vec_field_id) +
                           std::string(VEC_OPT_FIELDS);
    local_chunk_manager->CreateFile(local_data_path);

    std::vector<FieldDataPtr> field_datas;
    std::vector<std::string> batch_files;
    uint64_t write_offset = 0;
    WriteOptFieldsIvfMeta(
        local_chunk_manager, local_data_path, num_of_fields, write_offset);

    auto FetchRawData = [&]() {
        auto fds = GetObjectData(rcm_.get(), batch_files);
        for (size_t i = 0; i < batch_files.size(); ++i) {
            auto data = fds[i].get()->GetFieldData();
            field_datas.emplace_back(data);
        }
    };

    auto parallel_degree =
        uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::unordered_set<int64_t> actual_field_ids;
    for (auto& [field_id, tup] : fields_map) {
        const auto& field_type = std::get<1>(tup);
        auto& field_paths = std::get<2>(tup);
        if (0 == field_paths.size()) {
            LOG_WARN("optional field {} has no data", field_id);
            return "";
        }

        std::vector<FieldDataPtr>().swap(field_datas);
        SortByPath(field_paths);

        for (auto& file : field_paths) {
            if (batch_files.size() >= parallel_degree) {
                FetchRawData();
                batch_files.clear();
            }
            batch_files.emplace_back(file);
        }
        if (batch_files.size() > 0) {
            FetchRawData();
        }
        if (WriteOptFieldIvfData(field_type,
                                 field_id,
                                 local_chunk_manager,
                                 local_data_path,
                                 field_datas,
                                 write_offset)) {
            actual_field_ids.insert(field_id);
        }
    }

    if (actual_field_ids.size() != num_of_fields) {
        write_offset = 0;
        WriteOptFieldsIvfMeta(local_chunk_manager,
                              local_data_path,
                              actual_field_ids.size(),
                              write_offset);
        if (actual_field_ids.empty()) {
            return "";
        }
    }

    return local_data_path;
}

std::string
DiskFileManagerImpl::GetFileName(const std::string& localfile) {
    boost::filesystem::path localPath(localfile);
    return localPath.filename().string();
}

std::string
DiskFileManagerImpl::GetLocalIndexObjectPrefix() {
    auto local_chunk_manager = GetLocalChunkManager();
    return GenIndexPathPrefix(
        local_chunk_manager, index_meta_.build_id, index_meta_.index_version);
}

std::string
DiskFileManagerImpl::GetLocalTextIndexPrefix() {
    auto local_chunk_manager = GetLocalChunkManager();
    return GenTextIndexPathPrefix(local_chunk_manager,
                                  index_meta_.build_id,
                                  index_meta_.index_version,
                                  field_meta_.segment_id,
                                  field_meta_.field_id);
}

std::string
DiskFileManagerImpl::GetIndexIdentifier() {
    return GenIndexPathIdentifier(index_meta_.build_id,
                                  index_meta_.index_version);
}

std::string
DiskFileManagerImpl::GetTextIndexIdentifier() {
    return std::to_string(index_meta_.build_id) + "/" +
           std::to_string(index_meta_.index_version) + "/" +
           std::to_string(field_meta_.segment_id) +
           std::to_string(field_meta_.field_id);
}

std::string
DiskFileManagerImpl::GetLocalRawDataObjectPrefix() {
    auto local_chunk_manager = GetLocalChunkManager();
    return GenFieldRawDataPathPrefix(
        local_chunk_manager, field_meta_.segment_id, field_meta_.field_id);
}

bool
DiskFileManagerImpl::RemoveFile(const std::string& file) noexcept {
    // TODO: implement this interface
    return false;
}

std::optional<bool>
DiskFileManagerImpl::IsExisted(const std::string& file) noexcept {
    bool isExist = false;
    auto local_chunk_manager =
        LocalChunkManagerFactory::GetInstance().GetChunkManager();
    try {
        isExist = local_chunk_manager->Exist(file);
    } catch (std::exception& e) {
        // LOG_DEBUG("Exception:{}", e).what();
        return std::nullopt;
    }
    return isExist;
}

template std::string
DiskFileManagerImpl::CacheRawDataToDisk<float>(
    std::vector<std::string> remote_files);
template std::string
DiskFileManagerImpl::CacheRawDataToDisk<float16>(
    std::vector<std::string> remote_files);
template std::string
DiskFileManagerImpl::CacheRawDataToDisk<bfloat16>(
    std::vector<std::string> remote_files);
template std::string
DiskFileManagerImpl::CacheRawDataToDisk<bin1>(
    std::vector<std::string> remote_files);
}  // namespace milvus::storage
