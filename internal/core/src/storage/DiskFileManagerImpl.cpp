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
#include "index/Utils.h"
#include "log/Log.h"

#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"
#include "storage/IndexData.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/FileWriter.h"

#include "storage/RemoteOutputStream.h"
#include "storage/RemoteInputStream.h"

namespace milvus::storage {
DiskFileManagerImpl::DiskFileManagerImpl(
    const FileManagerContext& fileManagerContext)
    : FileManagerImpl(fileManagerContext.fieldDataMeta,
                      fileManagerContext.indexMeta) {
    rcm_ = fileManagerContext.chunkManagerPtr;
    fs_ = fileManagerContext.fs;
    plugin_context_ = fileManagerContext.plugin_context;
}

DiskFileManagerImpl::~DiskFileManagerImpl() {
    RemoveIndexFiles();
    RemoveTextLogFiles();
    RemoveJsonStatsFiles();
    RemoveNgramIndexFiles();
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
DiskFileManagerImpl::GetRemoteIndexPathV2(const std::string& file_name) const {
    std::string remote_prefix = GetRemoteIndexFilePrefixV2();
    return remote_prefix + "/" + file_name;
}

std::string
DiskFileManagerImpl::GetRemoteTextLogPath(const std::string& file_name,
                                          int64_t slice_num) const {
    auto remote_prefix = GetRemoteTextLogPrefix();
    return remote_prefix + "/" + file_name + "_" + std::to_string(slice_num);
}

std::string
DiskFileManagerImpl::GetRemoteJsonStatsSharedIndexPath(
    const std::string& file_name, int64_t slice_num) {
    namespace fs = std::filesystem;
    fs::path prefix = GetRemoteJsonStatsLogPrefix();
    fs::path suffix = JSON_STATS_SHARED_INDEX_PATH;
    fs::path file = file_name + "_" + std::to_string(slice_num);
    return (prefix / suffix / file).string();
}

std::string
DiskFileManagerImpl::GetRemoteJsonStatsShreddingPrefix() {
    namespace fs = std::filesystem;
    fs::path prefix = GetRemoteJsonStatsLogPrefix();
    fs::path suffix = JSON_STATS_SHREDDING_DATA_PATH;
    return (prefix / suffix).string();
}

bool
DiskFileManagerImpl::AddFileInternal(
    const std::string& file,
    const std::function<std::string(const std::string&, int)>&
        get_remote_path) noexcept {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    FILEMANAGER_TRY
    if (!local_chunk_manager->Exist(file)) {
        LOG_ERROR("local file {} not exists", file);
        return false;
    }

    // record local file path
    local_paths_.emplace_back(file);

    auto fileName = GetFileName(file);
    auto fileSize = local_chunk_manager->Size(file);
    added_total_file_size_ += fileSize;

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

        auto batch_size =
            std::min(FILE_SLICE_SIZE.load(), int64_t(fileSize) - offset);
        batch_remote_files.emplace_back(get_remote_path(fileName, slice_num));
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

std::shared_ptr<InputStream>
DiskFileManagerImpl::OpenInputStream(const std::string& filename) {
    auto local_file_name = GetFileName(filename);
    auto remote_file_path = GetRemoteIndexPathV2(local_file_name);

    auto fs = fs_;
    if (!fs) {
        fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                 .GetArrowFileSystem();
    }
    auto remote_file = fs->OpenInputFile(remote_file_path);
    AssertInfo(remote_file.ok(), "failed to open remote file");
    return std::static_pointer_cast<milvus::InputStream>(
        std::make_shared<milvus::storage::RemoteInputStream>(
            std::move(remote_file.ValueOrDie())));
}

std::shared_ptr<OutputStream>
DiskFileManagerImpl::OpenOutputStream(const std::string& filename) {
    auto local_file_name = GetFileName(filename);
    auto remote_file_path = GetRemoteIndexPathV2(local_file_name);

    auto fs = fs_;
    if (!fs) {
        fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                 .GetArrowFileSystem();
    }
    auto remote_stream = fs->OpenOutputStream(remote_file_path);
    AssertInfo(remote_stream.ok(),
               "failed to open remote stream, reason: {}",
               remote_stream.status().ToString());

    return std::make_shared<milvus::storage::RemoteOutputStream>(
        std::move(remote_stream.ValueOrDie()));
}

bool
DiskFileManagerImpl::AddFile(const std::string& file) noexcept {
    return AddFileInternal(file,
                           [this](const std::string& file_name, int slice_num) {
                               return GetRemoteIndexPath(file_name, slice_num);
                           });
}

bool
DiskFileManagerImpl::AddFileMeta(const FileMeta& file_meta) {
    auto local_file_name = GetFileName(file_meta.file_path);
    auto remote_file_path = GetRemoteIndexPathV2(local_file_name);

    remote_paths_to_size_[remote_file_path] = file_meta.file_size;
    return true;
}

bool
DiskFileManagerImpl::AddJsonSharedIndexLog(const std::string& file) noexcept {
    return AddFileInternal(
        file, [this](const std::string& file_name, int slice_num) {
            return GetRemoteJsonStatsSharedIndexPath(file_name, slice_num);
        });
}

bool
DiskFileManagerImpl::AddTextLog(const std::string& file) noexcept {
    return AddFileInternal(
        file, [this](const std::string& file_name, int slice_num) {
            return GetRemoteTextLogPath(file_name, slice_num);
        });
}

void
DiskFileManagerImpl::AddBatchIndexFiles(
    const std::string& local_file_name,
    const std::vector<int64_t>& local_file_offsets,
    const std::vector<std::string>& remote_files,
    const std::vector<int64_t>& remote_file_sizes) {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
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
                       index_meta_,
                       plugin_context_);
    for (auto& re : res) {
        remote_paths_to_size_[re.first] = re.second;
    }
}

void
DiskFileManagerImpl::CacheIndexToDiskInternal(
    const std::vector<std::string>& remote_files,
    const std::string& local_index_prefix,
    milvus::proto::common::LoadPriority priority) {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();

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
            local_index_prefix + prefix.substr(prefix.find_last_of('/') + 1);
        local_chunk_manager->CreateFile(local_index_file_name);

        // Get the remote files
        std::vector<std::string> batch_remote_files;
        batch_remote_files.reserve(slices.second.size());

        uint64_t max_parallel_degree =
            uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE.load());

        {
            auto file_writer = storage::FileWriter(
                local_index_file_name,
                storage::io::GetPriorityFromLoadPriority(priority));
            auto appendIndexFiles = [&]() {
                auto index_chunks_futures =
                    GetObjectData(rcm_.get(),
                                  batch_remote_files,
                                  milvus::PriorityForLoad(priority));
                for (auto& chunk_future : index_chunks_futures) {
                    auto chunk_codec = chunk_future.get();
                    file_writer.Write(chunk_codec->PayloadData(),
                                      chunk_codec->PayloadSize());
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
            file_writer.Finish();
        }

        local_paths_.emplace_back(local_index_file_name);
    }
}

void
DiskFileManagerImpl::CacheIndexToDisk(
    const std::vector<std::string>& remote_files,
    milvus::proto::common::LoadPriority priority) {
    return CacheIndexToDiskInternal(
        remote_files, GetLocalIndexObjectPrefix(), priority);
}

void
DiskFileManagerImpl::CacheTextLogToDisk(
    const std::vector<std::string>& remote_files,
    milvus::proto::common::LoadPriority priority) {
    return CacheIndexToDiskInternal(
        remote_files, GetLocalTextIndexPrefix(), priority);
}

void
DiskFileManagerImpl::CacheNgramIndexToDisk(
    const std::vector<std::string>& remote_files,
    milvus::proto::common::LoadPriority priority) {
    return CacheIndexToDiskInternal(
        remote_files, GetLocalNgramIndexPrefix(), priority);
}

void
DiskFileManagerImpl::CacheJsonStatsSharedIndexToDisk(
    const std::vector<std::string>& remote_files,
    milvus::proto::common::LoadPriority priority) {
    return CacheIndexToDiskInternal(
        remote_files, GetLocalJsonStatsSharedIndexPrefix(), priority);
}

template <typename DataType>
std::string
DiskFileManagerImpl::CacheRawDataToDisk(const Config& config) {
    auto storage_version =
        index::GetValueFromConfig<int64_t>(config, STORAGE_VERSION_KEY)
            .value_or(0);
    if (storage_version == STORAGE_V2) {
        return cache_raw_data_to_disk_storage_v2<DataType>(config);
    }
    return cache_raw_data_to_disk_internal<DataType>(config);
}

template <typename DataType>
std::string
DiskFileManagerImpl::cache_raw_data_to_disk_internal(const Config& config) {
    auto insert_files = index::GetValueFromConfig<std::vector<std::string>>(
        config, INSERT_FILES_KEY);
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when build index");
    auto remote_files = insert_files.value();
    SortByPath(remote_files);

    auto segment_id = GetFieldDataMeta().segment_id;
    auto field_id = GetFieldDataMeta().field_id;

    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    std::string local_data_path;
    bool file_created = false;

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
        for (int i = 0; i < batch_size; i++) {
            auto field_data = field_datas[i].get()->GetFieldData();
            num_rows += uint32_t(field_data->get_num_rows());
            cache_raw_data_to_disk_common<DataType>(field_data,
                                                    local_chunk_manager,
                                                    local_data_path,
                                                    file_created,
                                                    dim,
                                                    write_offset);
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

template <typename DataType>
void
DiskFileManagerImpl::cache_raw_data_to_disk_common(
    const FieldDataPtr& field_data,
    const std::shared_ptr<LocalChunkManager>& local_chunk_manager,
    std::string& local_data_path,
    bool& file_created,
    uint32_t& dim,
    int64_t& write_offset) {
    auto data_type = field_data->get_data_type();
    if (!file_created) {
        auto init_file_info = [&](milvus::DataType dt) {
            local_data_path = storage::GenFieldRawDataPathPrefix(
                                  local_chunk_manager,
                                  GetFieldDataMeta().segment_id,
                                  GetFieldDataMeta().field_id) +
                              "raw_data";
            if (dt == milvus::DataType::VECTOR_SPARSE_U32_F32) {
                local_data_path += ".sparse_u32_f32";
            }
            local_chunk_manager->CreateFile(local_data_path);
        };
        init_file_info(data_type);
        file_created = true;
    }
    if (data_type == milvus::DataType::VECTOR_SPARSE_U32_F32) {
        dim =
            (uint32_t)(std::dynamic_pointer_cast<FieldData<SparseFloatVector>>(
                           field_data)
                           ->Dim());
        auto sparse_rows =
            static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
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
            local_chunk_manager->Write(
                local_data_path, write_offset, row.data(), row_byte_size);
            write_offset += row_byte_size;
        }
    } else {
        dim = field_data->get_dim();
        auto data_size =
            field_data->get_num_rows() * milvus::GetVecRowSize<DataType>(dim);
        local_chunk_manager->Write(local_data_path,
                                   write_offset,
                                   const_cast<void*>(field_data->Data()),
                                   data_size);
        write_offset += data_size;
    }
}

template <typename T>
std::string
DiskFileManagerImpl::cache_raw_data_to_disk_storage_v2(const Config& config) {
    auto data_type = index::GetValueFromConfig<DataType>(config, DATA_TYPE_KEY);
    auto element_type =
        index::GetValueFromConfig<DataType>(config, ELEMENT_TYPE_KEY);
    AssertInfo(data_type.has_value(), "data type is empty when build index");
    AssertInfo(element_type.has_value(),
               "element type is empty when build index");
    auto dim = index::GetValueFromConfig<int64_t>(config, DIM_KEY).value_or(0);
    auto segment_insert_files =
        index::GetValueFromConfig<std::vector<std::vector<std::string>>>(
            config, SEGMENT_INSERT_FILES_KEY);
    AssertInfo(segment_insert_files.has_value(),
               "segment insert files is empty when build index");
    auto all_remote_files = segment_insert_files.value();
    for (auto& remote_files : all_remote_files) {
        SortByPath(remote_files);
    }

    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    std::string local_data_path;
    bool file_created = false;

    // file format
    // num_rows(uint32) | dim(uint32) | index_data ([]uint8_t)
    uint32_t num_rows = 0;
    uint32_t var_dim = 0;
    int64_t write_offset = sizeof(num_rows) + sizeof(var_dim);

    auto field_datas = GetFieldDatasFromStorageV2(all_remote_files,
                                                  GetFieldDataMeta().field_id,
                                                  data_type.value(),
                                                  element_type.value(),
                                                  dim,
                                                  fs_);
    for (auto& field_data : field_datas) {
        num_rows += uint32_t(field_data->get_num_rows());
        cache_raw_data_to_disk_common<T>(field_data,
                                         local_chunk_manager,
                                         local_data_path,
                                         file_created,
                                         var_dim,
                                         write_offset);
    }

    // write num_rows and dim value to file header
    write_offset = 0;
    local_chunk_manager->Write(
        local_data_path, write_offset, &num_rows, sizeof(num_rows));
    write_offset += sizeof(num_rows);
    local_chunk_manager->Write(
        local_data_path, write_offset, &var_dim, sizeof(var_dim));

    return local_data_path;
}

void
DiskFileManagerImpl::RemoveIndexFiles() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    local_chunk_manager->RemoveDir(GetLocalIndexObjectPrefix());
}

void
DiskFileManagerImpl::RemoveTextLogFiles() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    local_chunk_manager->RemoveDir(GetLocalTextIndexPrefix());
}

void
DiskFileManagerImpl::RemoveJsonStatsFiles() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    local_chunk_manager->RemoveDir(GetLocalJsonStatsPrefix());
}

void
DiskFileManagerImpl::RemoveNgramIndexFiles() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    local_chunk_manager->RemoveDir(GetLocalNgramIndexPrefix());
}

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
    if (mp.size() <= 1) {
        LOG_INFO("There are only one category, skip caching to local disk");
        return false;
    }

    LOG_INFO("Get opt fields with {} categories", mp.size());
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
        case DataType::TIMESTAMPTZ:
            return GENERATE_OPT_FIELD_IVF_IMPL(DataType::TIMESTAMPTZ);
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
DiskFileManagerImpl::CacheOptFieldToDisk(const Config& config) {
    auto storage_version =
        index::GetValueFromConfig<int64_t>(config, STORAGE_VERSION_KEY)
            .value_or(0);
    auto opt_fields =
        index::GetValueFromConfig<OptFieldT>(config, VEC_OPT_FIELDS);
    if (!opt_fields.has_value()) {
        return "";
    }

    std::vector<std::vector<std::string>> remote_files_storage_v2;
    if (storage_version == STORAGE_V2) {
        auto segment_insert_files =
            index::GetValueFromConfig<std::vector<std::vector<std::string>>>(
                config, SEGMENT_INSERT_FILES_KEY);
        AssertInfo(segment_insert_files.has_value(),
                   "segment insert files is empty when build index while "
                   "caching opt fields");
        remote_files_storage_v2 = segment_insert_files.value();
        for (auto& remote_files : remote_files_storage_v2) {
            SortByPath(remote_files);
        }
    }

    auto fields_map = opt_fields.value();
    const uint32_t num_of_fields = fields_map.size();
    if (0 == num_of_fields) {
        return "";
    } else if (num_of_fields > 1) {
        ThrowInfo(
            ErrorCode::NotImplemented,
            "vector index build with multiple fields is not supported yet");
    }

    auto segment_id = GetFieldDataMeta().segment_id;
    auto vec_field_id = GetFieldDataMeta().field_id;
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto local_data_path = storage::GenFieldRawDataPathPrefix(
                               local_chunk_manager, segment_id, vec_field_id) +
                           std::string(VEC_OPT_FIELDS);
    local_chunk_manager->CreateFile(local_data_path);
    uint64_t write_offset = 0;
    WriteOptFieldsIvfMeta(
        local_chunk_manager, local_data_path, num_of_fields, write_offset);

    std::unordered_set<int64_t> actual_field_ids;
    for (auto& [field_id, tup] : fields_map) {
        const auto& field_type = std::get<1>(tup);
        const auto& element_type = std::get<2>(tup);

        std::vector<FieldDataPtr> field_datas;
        // fetch scalar data from storage v2
        if (storage_version == STORAGE_V2) {
            field_datas = GetFieldDatasFromStorageV2(remote_files_storage_v2,
                                                     field_id,
                                                     field_type,
                                                     element_type,
                                                     1,
                                                     fs_);
        } else {  // original way
            auto& field_paths = std::get<3>(tup);
            if (0 == field_paths.size()) {
                LOG_WARN("optional field {} has no data", field_id);
                return "";
            }

            SortByPath(field_paths);
            field_datas = FetchFieldData(rcm_.get(), field_paths);
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
DiskFileManagerImpl::GetIndexIdentifier() {
    return GenIndexPathIdentifier(index_meta_.build_id,
                                  index_meta_.index_version,
                                  index_meta_.segment_id,
                                  index_meta_.field_id);
}

// path to store pre-built index contents downloaded from remote storage
std::string
DiskFileManagerImpl::GetLocalIndexObjectPrefix() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    return GenIndexPathPrefix(local_chunk_manager,
                              index_meta_.build_id,
                              index_meta_.index_version,
                              index_meta_.segment_id,
                              index_meta_.field_id,
                              false);
}

// temporary path used during index building
std::string
DiskFileManagerImpl::GetLocalTempIndexObjectPrefix() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    return GenIndexPathPrefix(local_chunk_manager,
                              index_meta_.build_id,
                              index_meta_.index_version,
                              index_meta_.segment_id,
                              index_meta_.field_id,
                              true);
}

// path to store pre-built index contents downloaded from remote storage
std::string
DiskFileManagerImpl::GetLocalTextIndexPrefix() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    return GenTextIndexPathPrefix(local_chunk_manager,
                                  index_meta_.build_id,
                                  index_meta_.index_version,
                                  field_meta_.segment_id,
                                  field_meta_.field_id,
                                  false);
}

// temporary path used during index building
std::string
DiskFileManagerImpl::GetLocalTempTextIndexPrefix() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    return GenIndexPathPrefix(local_chunk_manager,
                              index_meta_.build_id,
                              index_meta_.index_version,
                              field_meta_.segment_id,
                              field_meta_.field_id,
                              true);
}

std::string
DiskFileManagerImpl::GetLocalJsonStatsPrefix() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    return GenJsonStatsPathPrefix(local_chunk_manager,
                                  index_meta_.build_id,
                                  index_meta_.index_version,
                                  field_meta_.segment_id,
                                  field_meta_.field_id,
                                  false);
}

std::string
DiskFileManagerImpl::GetLocalTempJsonStatsPrefix() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    return GenJsonStatsPathPrefix(local_chunk_manager,
                                  index_meta_.build_id,
                                  index_meta_.index_version,
                                  field_meta_.segment_id,
                                  field_meta_.field_id,
                                  true);
}

std::string
DiskFileManagerImpl::GetLocalJsonStatsShreddingPrefix() {
    namespace fs = std::filesystem;
    fs::path prefix = GetLocalJsonStatsPrefix();
    fs::path suffix = JSON_STATS_SHREDDING_DATA_PATH;
    return (prefix / suffix).string();
}

std::string
DiskFileManagerImpl::GetLocalJsonStatsSharedIndexPrefix() {
    // make sure the path end with '/'
    namespace fs = std::filesystem;
    fs::path prefix = GetLocalJsonStatsPrefix();
    fs::path suffix = JSON_STATS_SHARED_INDEX_PATH;
    auto result = (prefix / suffix).string();
    if (!result.empty() && result.back() != fs::path::preferred_separator) {
        result += fs::path::preferred_separator;
    }
    return result;
}

std::string
DiskFileManagerImpl::GetLocalJsonStatsShreddingPath(
    const std::string& file_name) {
    namespace fs = std::filesystem;
    fs::path prefix = GetLocalJsonStatsShreddingPrefix();
    fs::path file = file_name;
    return (prefix / file).string();
}

std::string
DiskFileManagerImpl::GetLocalNgramIndexPrefix() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    return GenNgramIndexPrefix(local_chunk_manager,
                               index_meta_.build_id,
                               index_meta_.index_version,
                               field_meta_.segment_id,
                               field_meta_.field_id,
                               false);
}

std::string
DiskFileManagerImpl::GetLocalTempNgramIndexPrefix() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    return GenNgramIndexPrefix(local_chunk_manager,
                               index_meta_.build_id,
                               index_meta_.index_version,
                               field_meta_.segment_id,
                               field_meta_.field_id,
                               true);
}

std::string
DiskFileManagerImpl::GetRemoteJsonStatsLogPrefix() {
    return GenRemoteJsonStatsPathPrefix(rcm_,
                                        index_meta_.build_id,
                                        index_meta_.index_version,
                                        field_meta_.collection_id,
                                        field_meta_.partition_id,
                                        field_meta_.segment_id,
                                        field_meta_.field_id);
}

std::string
DiskFileManagerImpl::GetLocalRawDataObjectPrefix() {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
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
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    try {
        isExist = local_chunk_manager->Exist(file);
    } catch (std::exception& e) {
        // LOG_DEBUG("Exception:{}", e).what();
        return std::nullopt;
    }
    return isExist;
}

template std::string
DiskFileManagerImpl::CacheRawDataToDisk<float>(const Config& config);
template std::string
DiskFileManagerImpl::CacheRawDataToDisk<float16>(const Config& config);
template std::string
DiskFileManagerImpl::CacheRawDataToDisk<bfloat16>(const Config& config);
template std::string
DiskFileManagerImpl::CacheRawDataToDisk<bin1>(const Config& config);
template std::string
DiskFileManagerImpl::CacheRawDataToDisk<sparse_u32_f32>(const Config& config);
template std::string
DiskFileManagerImpl::CacheRawDataToDisk<int8_t>(const Config& config);

std::string
DiskFileManagerImpl::GetRemoteIndexFilePrefixV2() const {
    return FileManagerImpl::GetRemoteIndexFilePrefixV2();
}

}  // namespace milvus::storage
