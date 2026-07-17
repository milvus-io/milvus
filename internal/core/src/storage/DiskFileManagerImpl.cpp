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

#include <cxxabi.h>
#include "common/FastMem.h"
#include <string.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <future>
#include <iosfwd>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/filesystem/filesystem.h"
#include "boost/filesystem/path.hpp"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/TypeTraits.h"
#include "common/Types.h"
#include "common/VectorArray.h"
#include "common/VectorTrait.h"
#include "filemanager/FileManager.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "knowhere/sparse_utils.h"
#include "local/LegacyLocalChunkFiles.h"
#include "log/Log.h"
#include "milvus-storage/filesystem/fs.h"
#include "nlohmann/json.hpp"
#include "pb/schema.pb.h"
#include "storage/ChunkManager.h"
#include "storage/DataCodec.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"
#include "storage/FileWriter.h"
#include "storage/RemoteOutputStream.h"
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"

namespace milvus::storage {

namespace {
std::atomic<uint64_t> g_file_path_generation{0};

local::FileSystem
ResolveLocalFiles(const FileManagerContext& context) {
    if (context.local_files.has_value()) {
        return *context.local_files;
    }
    return local::LegacyLocalChunkFiles();
}

void
WriteBytesAt(local::io::WritableFile& file,
             uint64_t offset,
             const void* data,
             size_t size) {
    auto bytes = std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(data), size);
    auto bytes_written = file.WriteAt(offset, bytes);
    AssertInfo(bytes_written == size,
               "short write to local file {}, expected {}, got {}",
               file.DebugPath().string(),
               size,
               bytes_written);
}
}  // namespace

DiskFileManagerImpl::DiskFileManagerImpl(
    const FileManagerContext& fileManagerContext)
    : FileManagerImpl(fileManagerContext.fieldDataMeta,
                      fileManagerContext.indexMeta,
                      ResolveLocalFiles(fileManagerContext)),
      file_path_generation_(
          g_file_path_generation.fetch_add(1, std::memory_order_relaxed)) {
    rcm_ = fileManagerContext.chunkManagerPtr;
    fs_ = fileManagerContext.fs;
    plugin_context_ = fileManagerContext.plugin_context;
    loon_ffi_properties_ = fileManagerContext.loon_ffi_properties;
    stats_base_path_ = fileManagerContext.stats_base_path;
    storage_column_mappings_ = fileManagerContext.storage_column_mappings;
}

DiskFileManagerImpl::~DiskFileManagerImpl() {
    RemoveIndexFiles();
    CloseAndRemoveLocalDir(LocalIndexObjectPath(true));
    RemoveTextLogFiles();
    RemoveJsonStatsFiles();
    CloseAndRemoveLocalDir(LocalJsonStatsPath(true));
    RemoveNgramIndexFiles();
    CloseAndRemoveLocalDir(LocalNgramIndexPath(true));
    RemoveRawDataFiles();
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
    std::string remote_prefix = GetRemoteIndexObjectPrefix();
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

std::string
DiskFileManagerImpl::GetRemoteJsonStatsMetaPath(const std::string& file_name) {
    namespace fs = std::filesystem;
    fs::path prefix = GetRemoteJsonStatsLogPrefix();
    return (prefix / file_name).string();
}

std::string
DiskFileManagerImpl::GetLocalJsonStatsMetaPrefix() {
    return GetLocalJsonStatsPrefix();
}

std::string
DiskFileManagerImpl::ResolveLocalPrefix(const local::Path& path) const {
    namespace fs = std::filesystem;
    auto result = local_files_->ResolveNativePath(path).string();
    if (!result.empty() && result.back() != fs::path::preferred_separator) {
        result += fs::path::preferred_separator;
    }
    return result;
}

local::Path
DiskFileManagerImpl::LocalIndexObjectPath(bool temporary) const {
    auto prefix = temporary ? std::filesystem::path("tmp") / INDEX_ROOT_PATH
                            : std::filesystem::path(INDEX_ROOT_PATH);
    return local::Path((prefix / fmt::format("{}_{}_{}_{}_{}",
                                             index_meta_.build_id,
                                             index_meta_.index_version,
                                             index_meta_.segment_id,
                                             index_meta_.field_id,
                                             file_path_generation_))
                           .generic_string());
}

local::Path
DiskFileManagerImpl::LocalTextIndexPath(bool temporary) const {
    if (temporary) {
        return LocalIndexObjectPath(true);
    }
    return local::Path((std::filesystem::path(TEXT_LOG_ROOT_PATH) /
                        fmt::format("{}_{}_{}_{}_{}",
                                    index_meta_.build_id,
                                    index_meta_.index_version,
                                    field_meta_.segment_id,
                                    field_meta_.field_id,
                                    file_path_generation_))
                           .generic_string());
}

local::Path
DiskFileManagerImpl::LocalJsonStatsPath(bool temporary) const {
    auto prefix = temporary
                      ? std::filesystem::path("tmp") / JSON_STATS_ROOT_PATH
                      : std::filesystem::path(JSON_STATS_ROOT_PATH);
    return local::Path((prefix / fmt::format("{}_{}_{}_{}_{}",
                                             index_meta_.build_id,
                                             index_meta_.index_version,
                                             field_meta_.segment_id,
                                             field_meta_.field_id,
                                             file_path_generation_))
                           .generic_string());
}

local::Path
DiskFileManagerImpl::LocalNgramIndexPath(bool temporary) const {
    auto prefix = temporary ? std::filesystem::path("tmp") / NGRAM_LOG_ROOT_PATH
                            : std::filesystem::path(NGRAM_LOG_ROOT_PATH);
    return local::Path((prefix / fmt::format("{}_{}_{}_{}_{}",
                                             index_meta_.build_id,
                                             index_meta_.index_version,
                                             field_meta_.segment_id,
                                             field_meta_.field_id,
                                             file_path_generation_))
                           .generic_string());
}

local::Path
DiskFileManagerImpl::LocalRawDataPath() const {
    return local::Path((std::filesystem::path(RAWDATA_ROOT_PATH) /
                        fmt::format("{}_{}_{}",
                                    field_meta_.segment_id,
                                    field_meta_.field_id,
                                    file_path_generation_))
                           .generic_string());
}

std::shared_ptr<local::ManagedSubtree>
DiskFileManagerImpl::GetOrCreateManagedSubtree(const local::Path& path) {
    auto key = std::string(path.String());
    std::lock_guard<std::mutex> lock(managed_subtrees_mutex_);
    auto it = managed_subtrees_.find(key);
    if (it != managed_subtrees_.end()) {
        return it->second;
    }

    auto subtree = local_files_->ManageSubtree(path);
    managed_subtrees_.emplace(std::move(key), subtree);
    return subtree;
}

local::Path
DiskFileManagerImpl::LocalPathForResolvedPrefix(const std::string& dir) const {
    return local_files_->PathFromNativePath(dir);
}

DiskFileManagerImpl::LocalDirWriteLease
DiskFileManagerImpl::AcquireLocalDirWriteLease(const std::string& dir) {
    return AcquireLocalDirWriteLease(LocalPathForResolvedPrefix(dir));
}

DiskFileManagerImpl::LocalDirWriteLease
DiskFileManagerImpl::AcquireLocalDirWriteLease(const local::Path& path) {
    return GetOrCreateManagedSubtree(path)->AcquireWriter();
}

void
DiskFileManagerImpl::CloseAndRemoveLocalDir(const local::Path& path) noexcept {
    try {
        GetOrCreateManagedSubtree(path)->RemoveWhenIdle();
    } catch (const std::exception& error) {
        LOG_WARN("failed to close local subtree {}, error: {}",
                 path.String(),
                 error.what());
    } catch (...) {
        LOG_WARN("failed to close local subtree {}, unknown error",
                 path.String());
    }
}

bool
DiskFileManagerImpl::AddFileInternal(
    const std::string& file,
    const std::function<std::string(const std::string&, int)>&
        get_remote_path) noexcept {
    FILEMANAGER_TRY
    auto local_path = local_files_->PathFromNativePath(file);
    if (!local_files_->Exists(local_path)) {
        LOG_ERROR("local file {} not exists", file);
        return false;
    }

    // record local file path
    local_paths_.emplace_back(file);

    auto fileName = GetFileName(file);
    auto fileSize = local_files_->FileSize(local_path);
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
DiskFileManagerImpl::AddJsonStatsMetaLog(const std::string& file) noexcept {
    FILEMANAGER_TRY
    auto local_path = local_files_->PathFromNativePath(file);
    if (!local_files_->Exists(local_path)) {
        LOG_ERROR("local meta file {} not exists", file);
        return false;
    }

    local_paths_.emplace_back(file);
    auto fileName = GetFileName(file);
    auto local_file = local_files_->OpenForRead(local_path);
    auto fileSize = local_file.Size();
    added_total_file_size_ += fileSize;

    // Meta file is small, upload directly without slicing
    auto remote_path = GetRemoteJsonStatsMetaPath(fileName);
    auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[fileSize]);
    auto bytes =
        std::span<std::byte>(reinterpret_cast<std::byte*>(buf.get()), fileSize);
    AssertInfo(local_file.ReadAt(0, bytes) == fileSize,
               "failed to read complete local meta file {}",
               file);
    rcm_->Write(remote_path, buf.get(), fileSize);

    remote_paths_to_size_[remote_path] = fileSize;
    LOG_INFO("upload json stats meta file: {} to remote: {}, size: {}",
             file,
             remote_path,
             fileSize);

    FILEMANAGER_CATCH
    FILEMANAGER_END
    return true;
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
    auto local_path = local_files_->PathFromNativePath(local_file_name);
    auto local_file = std::make_shared<local::io::RandomAccessFile>(
        local_files_->OpenForRead(local_path));
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);

    std::vector<std::future<std::shared_ptr<uint8_t[]>>> futures;
    futures.reserve(remote_file_sizes.size());
    AssertInfo(local_file_offsets.size() == remote_files.size(),
               "inconsistent size of offset slices with file slices");
    AssertInfo(remote_files.size() == remote_file_sizes.size(),
               "inconsistent size of file slices with size slices");

    for (int64_t i = 0; i < remote_files.size(); ++i) {
        futures.push_back(pool.Submit(
            [local_file](const int64_t offset, const int64_t data_size)
                -> std::shared_ptr<uint8_t[]> {
                auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[data_size]);
                auto bytes = std::span<std::byte>(
                    reinterpret_cast<std::byte*>(buf.get()), data_size);
                auto bytes_read = local_file->ReadAt(offset, bytes);
                AssertInfo(
                    bytes_read == data_size,
                    "short read from local index file, expected {}, got {}",
                    data_size,
                    bytes_read);
                return buf;
            },
            local_file_offsets[i],
            remote_file_sizes[i]));
    }

    // hold index data util upload index file done.
    // WaitAllFutures drains every task before rethrowing, so a failed read
    // cannot unwind this frame while remaining tasks are still running.
    auto index_datas = WaitAllFutures(std::move(futures));
    std::vector<const uint8_t*> data_slices;
    data_slices.reserve(index_datas.size());
    for (auto& index_data : index_datas) {
        data_slices.emplace_back(index_data.get());
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
            LOG_ERROR("{}", err_message);
            throw std::logic_error(err_message);
        }
    }

    for (auto& slices : index_slices) {
        std::sort(slices.second.begin(), slices.second.end());
    }

    // TODO: remove this log when #45590 is solved
    LOG_INFO("CacheIndexToDisk: caching {} files to {}",
             index_slices.size(),
             local_index_prefix);

    for (auto& slices : index_slices) {
        auto prefix = slices.first;
        auto local_index_file_name =
            local_index_prefix + prefix.substr(prefix.find_last_of('/') + 1);
        auto local_path =
            local_files_->PathFromNativePath(local_index_file_name);

        // Get the remote files
        std::vector<std::string> batch_remote_files;
        batch_remote_files.reserve(slices.second.size());

        uint64_t max_parallel_degree =
            uint64_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE.load());

        {
            auto local_file = local_files_->OpenForWrite(
                local_path,
                local::WriteOptions{
                    .create = true, .truncate = true, .create_parent = true});
            auto file_writer = storage::FileWriter(
                std::move(local_file),
                storage::io::GetPriorityFromLoadPriority(priority));
            auto appendIndexFiles = [&]() {
                auto index_chunks_futures =
                    GetObjectData(rcm_.get(),
                                  batch_remote_files,
                                  milvus::PriorityForLoad(priority));
                storage::ProcessFuturesInOrder(
                    index_chunks_futures,
                    [&](std::unique_ptr<DataCodec> chunk_codec) {
                        file_writer.Write(chunk_codec->PayloadData(),
                                          chunk_codec->PayloadSize());
                    });
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
        // TODO: remove this log when #45590 is solved
        LOG_INFO("CacheIndexToDisk: cached file {}", local_index_file_name);
    }
}

void
DiskFileManagerImpl::CacheIndexToDisk(
    const std::vector<std::string>& remote_files,
    milvus::proto::common::LoadPriority priority) {
    auto local_prefix = GetLocalIndexObjectPrefix();
    auto lease = AcquireLocalDirWriteLease(LocalIndexObjectPath(false));
    CacheIndexToDiskInternal(remote_files, local_prefix, priority);
}

void
DiskFileManagerImpl::CacheTextLogToDisk(
    const std::vector<std::string>& remote_files,
    milvus::proto::common::LoadPriority priority) {
    auto local_prefix = GetLocalTextIndexPrefix();
    auto lease = AcquireLocalDirWriteLease(LocalTextIndexPath(false));
    CacheIndexToDiskInternal(remote_files, local_prefix, priority);
}

void
DiskFileManagerImpl::CacheNgramIndexToDisk(
    const std::vector<std::string>& remote_files,
    milvus::proto::common::LoadPriority priority) {
    auto local_prefix = GetLocalNgramIndexPrefix();
    auto lease = AcquireLocalDirWriteLease(LocalNgramIndexPath(false));
    CacheIndexToDiskInternal(remote_files, local_prefix, priority);
}

void
DiskFileManagerImpl::CacheJsonStatsSharedIndexToDisk(
    const std::vector<std::string>& remote_files,
    milvus::proto::common::LoadPriority priority) {
    auto local_prefix = GetLocalJsonStatsSharedIndexPrefix();
    auto lease = AcquireLocalDirWriteLease(LocalJsonStatsPath(false));
    CacheIndexToDiskInternal(remote_files, local_prefix, priority);
}

std::string
DiskFileManagerImpl::CacheJsonStatsMetaToDisk(
    const std::string& remote_file,
    milvus::proto::common::LoadPriority priority) {
    auto local_prefix = GetLocalJsonStatsMetaPrefix();
    auto lease = AcquireLocalDirWriteLease(LocalJsonStatsPath(false));

    auto file_name = remote_file.substr(remote_file.find_last_of('/') + 1);
    auto local_file =
        (std::filesystem::path(local_prefix) / file_name).string();

    // remote_file is an absolute remote path (basePath already prepended by caller)
    auto file_size = rcm_->Size(remote_file);
    auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[file_size]);
    rcm_->Read(remote_file, buf.get(), file_size);
    auto local_path = local_files_->PathFromNativePath(local_file);
    auto output = local_files_->OpenForWrite(
        local_path,
        local::WriteOptions{
            .create = true, .truncate = true, .create_parent = true});
    auto bytes = std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(buf.get()), file_size);
    AssertInfo(output.Write(bytes) == file_size,
               "failed to write complete json stats meta file {}",
               local_file);

    LOG_INFO("Cached json stats meta file from {} to {}, size: {}",
             remote_file,
             local_file,
             file_size);

    return local_file;
}

template <typename DataType>
std::string
DiskFileManagerImpl::CacheRawDataToDisk(const Config& config) {
    auto raw_data_lease = AcquireLocalDirWriteLease(LocalRawDataPath());
    std::optional<LocalDirWriteLease> valid_data_lease;
    if (index::GetValueFromConfig<std::string>(config,
                                               index::VALID_DATA_PATH_KEY)
            .has_value()) {
        valid_data_lease.emplace(
            AcquireLocalDirWriteLease(LocalIndexObjectPath(false)));
    }

    auto storage_version =
        index::GetValueFromConfig<int64_t>(config, STORAGE_VERSION_KEY)
            .value_or(0);
    if (storage_version == STORAGE_V2 || storage_version == STORAGE_V3) {
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

    std::string local_data_path;
    std::optional<local::io::WritableFile> local_file;

    // Check if we're dealing with embedding list (VECTOR_ARRAY)
    auto is_embedding_list =
        index::GetValueFromConfig<bool>(config, index::EMB_LIST);
    bool is_vector_array = is_embedding_list.value_or(false);
    std::vector<size_t> offsets;
    if (is_vector_array) {
        offsets.push_back(0);  // Initialize with 0 for cumulative offsets
    }

    auto valid_data_path = index::GetValueFromConfig<std::string>(
        config, index::VALID_DATA_PATH_KEY);
    std::vector<uint8_t> valid_bitmap;
    uint64_t total_num_rows = 0;
    bool nullable = false;

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
        storage::ProcessFuturesInOrder(
            field_datas, [&](std::unique_ptr<DataCodec> codec) {
                auto field_data = codec->GetFieldData();
                num_rows += uint32_t(field_data->get_valid_rows());

                if (valid_data_path.has_value() && field_data->IsNullable()) {
                    nullable = true;
                    auto rows = field_data->get_num_rows();
                    if (rows > 0) {
                        auto new_size = (total_num_rows + rows + 7) / 8;
                        if (new_size >
                            static_cast<int64_t>(valid_bitmap.size())) {
                            valid_bitmap.resize(new_size, 0);
                        }
                        for (int64_t i = 0; i < rows; ++i) {
                            if (field_data->is_valid(i)) {
                                set_bit(valid_bitmap, total_num_rows + i);
                            }
                        }
                        total_num_rows += rows;
                    }
                }

                cache_raw_data_to_disk_common<DataType>(
                    field_data,
                    local_file,
                    local_data_path,
                    dim,
                    write_offset,
                    is_vector_array ? &offsets : nullptr);
            });
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

    // For vector arrays, num_rows should be the total flattened vector count,
    // not the number of emb_lists, because DiskANN reads this from the data file header.
    if (is_vector_array) {
        num_rows = static_cast<uint32_t>(offsets.back());
    }

    // write num_rows and dim value to file header
    write_offset = 0;
    WriteBytesAt(*local_file, write_offset, &num_rows, sizeof(num_rows));
    write_offset += sizeof(num_rows);
    WriteBytesAt(*local_file, write_offset, &dim, sizeof(dim));

    // Write offsets file for VECTOR_ARRAY
    if (is_vector_array) {
        AssertInfo(
            !offsets.empty() && offsets.front() == 0,
            "invalid emb_list offsets: size {}, front {}",
            offsets.size(),
            offsets.empty() ? -1 : static_cast<int64_t>(offsets.front()));
        if (offsets.size() == 1) {
            AssertInfo(nullable || total_num_rows == 0,
                       "non-nullable emb_list offsets must include rows");
            AssertInfo(num_rows == 0,
                       "empty emb_list offsets cannot reference raw vectors");
        } else {
            // Get offsets path from config if provided, otherwise use default
            auto offsets_path = index::GetValueFromConfig<std::string>(
                                    config, index::EMB_LIST_OFFSETS_PATH)
                                    .value();
            auto offsets_file = local_files_->OpenForWrite(
                local_files_->PathFromNativePath(offsets_path),
                local::WriteOptions{
                    .create = true, .truncate = true, .create_parent = true});

            size_t num_offsets = offsets.size();
            int64_t offsets_write_pos = 0;

            WriteBytesAt(
                offsets_file, offsets_write_pos, &num_offsets, sizeof(size_t));
            offsets_write_pos += sizeof(size_t);

            WriteBytesAt(offsets_file,
                         offsets_write_pos,
                         offsets.data(),
                         offsets.size() * sizeof(size_t));
        }
    }

    if (nullable && valid_data_path.has_value() && total_num_rows > 0) {
        write_valid_data_file(
            valid_data_path.value(), valid_bitmap, total_num_rows);
    }

    return local_data_path;
}

template <typename DataType>
void
DiskFileManagerImpl::cache_raw_data_to_disk_common(
    const FieldDataPtr& field_data,
    std::optional<local::io::WritableFile>& local_file,
    std::string& local_data_path,
    uint32_t& dim,
    int64_t& write_offset,
    std::vector<size_t>* offsets) {
    auto data_type = field_data->get_data_type();
    if (!local_file.has_value()) {
        auto init_file_info = [&](milvus::DataType dt) {
            auto relative_path =
                std::string(LocalRawDataPath().String()) + "/raw_data";
            if (dt == milvus::DataType::VECTOR_SPARSE_U32_F32) {
                relative_path += ".sparse_u32_f32";
            }
            auto path = local::Path(std::move(relative_path));
            local_data_path = local_files_->ResolveNativePath(path).string();
            local_file.emplace(local_files_->OpenForWrite(
                path,
                local::WriteOptions{
                    .create = true, .truncate = true, .create_parent = true}));
        };
        init_file_info(data_type);
    }
    if (data_type == milvus::DataType::VECTOR_SPARSE_U32_F32) {
        dim =
            (uint32_t)(std::dynamic_pointer_cast<FieldData<SparseFloatVector>>(
                           field_data)
                           ->Dim());
        auto sparse_rows =
            static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
                field_data->Data());
        for (size_t i = 0; i < field_data->get_valid_rows(); ++i) {
            auto row = sparse_rows[i];
            auto row_byte_size = row.data_byte_size();
            uint32_t nnz = row.size();
            WriteBytesAt(*local_file, write_offset, &nnz, sizeof(nnz));
            write_offset += sizeof(nnz);
            WriteBytesAt(*local_file, write_offset, row.data(), row_byte_size);
            write_offset += row_byte_size;
        }
    } else if (data_type == milvus::DataType::VECTOR_ARRAY) {
        // Handle VECTOR_ARRAY - need to flatten the array data
        auto vec_array_data =
            dynamic_cast<FieldData<VectorArray>*>(field_data.get());
        AssertInfo(vec_array_data != nullptr,
                   "failed to cast field data to vector array");

        dim = field_data->get_dim();
        auto rows = vec_array_data->get_num_rows();

        // Calculate total data size needed
        int64_t total_size = 0;
        for (auto i = 0; i < vec_array_data->get_valid_rows(); ++i) {
            total_size += vec_array_data->DataSize(i);
        }

        // Allocate buffer and copy data
        auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[total_size]);
        int64_t buf_offset = 0;

        int64_t physical_row = 0;
        for (auto i = 0; i < rows; ++i) {
            if (vec_array_data->IsNullable() && !vec_array_data->is_valid(i)) {
                continue;
            }

            auto vec_array = vec_array_data->value_at(physical_row);
            auto size = vec_array_data->DataSize(physical_row);

            // Collect offsets information if needed (cumulative offsets)
            if (offsets != nullptr) {
                // Add cumulative offset (number of vectors processed so far)
                size_t last_offset = offsets->back();
                offsets->push_back(last_offset + vec_array->length());
            }

            if (size > 0) {
                milvus::fastmem::FastMemcpy(
                    buf.get() + buf_offset, vec_array->data(), size);
            }
            buf_offset += size;
            physical_row++;
        }

        // Write flattened data to disk
        WriteBytesAt(*local_file, write_offset, buf.get(), total_size);
        write_offset += total_size;
    } else {
        dim = field_data->get_dim();
        auto data_size =
            field_data->get_valid_rows() * milvus::GetVecRowSize<DataType>(dim);
        WriteBytesAt(*local_file, write_offset, field_data->Data(), data_size);
        write_offset += data_size;
    }
}

void
DiskFileManagerImpl::write_valid_data_file(const std::string& valid_data_path,
                                           std::vector<uint8_t>& valid_bitmap,
                                           uint64_t total_num_rows) {
    auto valid_file = local_files_->OpenForWrite(
        local_files_->PathFromNativePath(valid_data_path),
        local::WriteOptions{
            .create = true, .truncate = true, .create_parent = true});
    int64_t valid_write_pos = 0;

    WriteBytesAt(
        valid_file, valid_write_pos, &total_num_rows, sizeof(uint64_t));
    valid_write_pos += sizeof(uint64_t);

    WriteBytesAt(
        valid_file, valid_write_pos, valid_bitmap.data(), valid_bitmap.size());
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

    std::string local_data_path;
    std::optional<local::io::WritableFile> local_file;

    // Check if we're dealing with embedding list (VECTOR_ARRAY)
    auto is_embedding_list =
        index::GetValueFromConfig<bool>(config, index::EMB_LIST);
    bool is_vector_array = is_embedding_list.value_or(false);
    std::vector<size_t> offsets;
    if (is_vector_array) {
        offsets.push_back(0);  // Initialize with 0 for cumulative offsets
    }

    // Check if we need to track validity data for nullable vector fields
    auto valid_data_path = index::GetValueFromConfig<std::string>(
        config, index::VALID_DATA_PATH_KEY);

    // file format
    // num_rows(uint32) | dim(uint32) | index_data ([]uint8_t)
    uint32_t num_rows = 0;
    uint32_t var_dim = 0;
    int64_t write_offset = sizeof(num_rows) + sizeof(var_dim);

    std::vector<FieldDataPtr> field_datas;
    auto manifest =
        index::GetValueFromConfig<std::string>(config, SEGMENT_MANIFEST_KEY);
    auto manifest_path_str = manifest.value_or("");
    if (manifest_path_str != "") {
        AssertInfo(
            loon_ffi_properties_ != nullptr,
            "loon ffi properties is null when build index with manifest");
        field_datas = GetFieldDatasFromManifest(
            manifest_path_str,
            loon_ffi_properties_,
            field_meta_,
            data_type,
            dim,
            element_type,
            GetStorageColumnMapping(field_meta_.field_id));
    } else {
        field_datas = GetFieldDatasFromStorageV2(all_remote_files,
                                                 GetFieldDataMeta().field_id,
                                                 data_type.value(),
                                                 element_type.value(),
                                                 dim,
                                                 fs_);
    }

    bool nullable = false;
    uint64_t total_num_rows = 0;
    if (valid_data_path.has_value()) {
        for (auto& field_data : field_datas) {
            if (field_data->IsNullable()) {
                nullable = true;
            }
            total_num_rows += field_data->get_num_rows();
        }
    }

    std::vector<uint8_t> valid_bitmap;
    if (nullable) {
        valid_bitmap.resize((total_num_rows + 7) / 8, 0);
    }

    int64_t chunk_offset = 0;
    for (auto& field_data : field_datas) {
        num_rows += uint32_t(field_data->get_valid_rows());
        if (nullable) {
            auto rows = field_data->get_num_rows();
            for (int64_t i = 0; i < rows; ++i) {
                if (field_data->is_valid(i)) {
                    set_bit(valid_bitmap, chunk_offset + i);
                }
            }
            chunk_offset += rows;
        }

        cache_raw_data_to_disk_common<T>(field_data,
                                         local_file,
                                         local_data_path,
                                         var_dim,
                                         write_offset,
                                         is_vector_array ? &offsets : nullptr);
    }

    // For vector arrays, num_rows should be the total flattened vector count,
    // not the number of emb_lists, because DiskANN reads this from the data file header.
    if (is_vector_array) {
        num_rows = static_cast<uint32_t>(offsets.back());
    }

    // write num_rows and dim value to file header
    write_offset = 0;
    WriteBytesAt(*local_file, write_offset, &num_rows, sizeof(num_rows));
    write_offset += sizeof(num_rows);
    WriteBytesAt(*local_file, write_offset, &var_dim, sizeof(var_dim));

    // Write offsets file for VECTOR_ARRAY
    if (is_vector_array) {
        AssertInfo(
            !offsets.empty() && offsets.front() == 0,
            "invalid emb_list offsets: size {}, front {}",
            offsets.size(),
            offsets.empty() ? -1 : static_cast<int64_t>(offsets.front()));
        if (offsets.size() == 1) {
            AssertInfo(nullable || total_num_rows == 0,
                       "non-nullable emb_list offsets must include rows");
            AssertInfo(num_rows == 0,
                       "empty emb_list offsets cannot reference raw vectors");
        } else {
            // Get offsets path from config if provided, otherwise use default
            auto offsets_path = index::GetValueFromConfig<std::string>(
                                    config, index::EMB_LIST_OFFSETS_PATH)
                                    .value();

            auto offsets_file = local_files_->OpenForWrite(
                local_files_->PathFromNativePath(offsets_path),
                local::WriteOptions{
                    .create = true, .truncate = true, .create_parent = true});

            size_t num_offsets = offsets.size();
            int64_t offsets_write_pos = 0;

            WriteBytesAt(
                offsets_file, offsets_write_pos, &num_offsets, sizeof(size_t));
            offsets_write_pos += sizeof(size_t);

            WriteBytesAt(offsets_file,
                         offsets_write_pos,
                         offsets.data(),
                         offsets.size() * sizeof(size_t));
        }
    }

    if (nullable && valid_data_path.has_value() && total_num_rows > 0) {
        write_valid_data_file(
            valid_data_path.value(), valid_bitmap, total_num_rows);
    }

    return local_data_path;
}

void
DiskFileManagerImpl::RemoveIndexFiles() {
    CloseAndRemoveLocalDir(LocalIndexObjectPath(false));
}

void
DiskFileManagerImpl::RemoveTextLogFiles() {
    CloseAndRemoveLocalDir(LocalTextIndexPath(false));
}

void
DiskFileManagerImpl::RemoveJsonStatsSharedIndexFiles() {
    CloseAndRemoveLocalDir(LocalJsonStatsPath(false));
}

void
DiskFileManagerImpl::RemoveJsonStatsFiles() {
    CloseAndRemoveLocalDir(LocalJsonStatsPath(false));
}

void
DiskFileManagerImpl::RemoveNgramIndexFiles() {
    CloseAndRemoveLocalDir(LocalNgramIndexPath(false));
}

void
DiskFileManagerImpl::RemoveRawDataFiles() {
    CloseAndRemoveLocalDir(LocalRawDataPath());
}

template <DataType T>
bool
WriteOptFieldIvfDataImpl(const int64_t field_id,
                         local::io::WritableFile& local_file,
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
    WriteBytesAt(local_file, write_offset, &field_id, sizeof(field_id));
    write_offset += sizeof(field_id);
    const uint32_t num_of_unique_field_data = mp.size();
    WriteBytesAt(local_file,
                 write_offset,
                 &num_of_unique_field_data,
                 sizeof(num_of_unique_field_data));
    write_offset += sizeof(num_of_unique_field_data);
    for (const auto& [val, offsets] : mp) {
        const uint32_t offsets_cnt = offsets.size();
        WriteBytesAt(
            local_file, write_offset, &offsets_cnt, sizeof(offsets_cnt));
        write_offset += sizeof(offsets_cnt);
        const size_t data_size = offsets_cnt * sizeof(OffsetT);
        WriteBytesAt(local_file, write_offset, offsets.data(), data_size);
        write_offset += data_size;
    }
    return true;
}

#define GENERATE_OPT_FIELD_IVF_IMPL(DT) \
    WriteOptFieldIvfDataImpl<DT>(       \
        field_id, local_file, field_datas, write_offset)
bool
WriteOptFieldIvfData(const DataType& dt,
                     const int64_t field_id,
                     local::io::WritableFile& local_file,
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
WriteOptFieldsIvfMeta(local::io::WritableFile& local_file,
                      const uint32_t num_of_fields,
                      uint64_t& write_offset) {
    const uint8_t kVersion = 0;
    WriteBytesAt(local_file, write_offset, &kVersion, sizeof(kVersion));
    write_offset += sizeof(kVersion);
    WriteBytesAt(
        local_file, write_offset, &num_of_fields, sizeof(num_of_fields));
    write_offset += sizeof(num_of_fields);
}

std::string
DiskFileManagerImpl::CacheOptFieldToDisk(const Config& config) {
    auto opt_fields =
        index::GetValueFromConfig<OptFieldT>(config, VEC_OPT_FIELDS);
    if (!opt_fields.has_value() || opt_fields->empty()) {
        return "";
    }
    auto lease = AcquireLocalDirWriteLease(LocalRawDataPath());

    auto storage_version =
        index::GetValueFromConfig<int64_t>(config, STORAGE_VERSION_KEY)
            .value_or(0);
    if (storage_version == STORAGE_V3) {
        return cache_opt_field_to_disk_v3(config);
    }
    if (storage_version == STORAGE_V2) {
        return cache_opt_field_to_disk_v2(config);
    }

    // legacy path
    auto fields_map = opt_fields.value();
    const uint32_t num_of_fields = fields_map.size();
    if (num_of_fields > 1) {
        ThrowInfo(
            ErrorCode::NotImplemented,
            "vector index build with multiple fields is not supported yet");
    }

    auto local_data_path =
        GetLocalRawDataObjectPrefix() + std::string(VEC_OPT_FIELDS);
    auto local_file = local_files_->OpenForWrite(
        local_files_->PathFromNativePath(local_data_path),
        local::WriteOptions{
            .create = true, .truncate = true, .create_parent = true});
    uint64_t write_offset = 0;
    WriteOptFieldsIvfMeta(local_file, num_of_fields, write_offset);

    std::unordered_set<int64_t> actual_field_ids;
    for (auto& [field_id, tup] : fields_map) {
        const auto& field_type = std::get<1>(tup);

        auto& field_paths = std::get<3>(tup);
        if (0 == field_paths.size()) {
            LOG_WARN("optional field {} has no data", field_id);
            return "";
        }

        SortByPath(field_paths);
        std::vector<FieldDataPtr> field_datas =
            FetchFieldData(rcm_.get(), field_paths);

        if (WriteOptFieldIvfData(
                field_type, field_id, local_file, field_datas, write_offset)) {
            actual_field_ids.insert(field_id);
        }
    }

    if (actual_field_ids.size() != num_of_fields) {
        write_offset = 0;
        WriteOptFieldsIvfMeta(
            local_file, actual_field_ids.size(), write_offset);
        if (actual_field_ids.empty()) {
            return "";
        }
    }

    return local_data_path;
}

std::string
DiskFileManagerImpl::cache_opt_field_to_disk_v2(const Config& config) {
    auto opt_fields =
        index::GetValueFromConfig<OptFieldT>(config, VEC_OPT_FIELDS);
    if (!opt_fields.has_value()) {
        return "";
    }

    auto segment_insert_files =
        index::GetValueFromConfig<std::vector<std::vector<std::string>>>(
            config, SEGMENT_INSERT_FILES_KEY);
    AssertInfo(segment_insert_files.has_value(),
               "segment insert files is empty when build index while "
               "caching opt fields");
    auto remote_files_storage_v2 = segment_insert_files.value();
    for (auto& remote_files : remote_files_storage_v2) {
        SortByPath(remote_files);
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

    auto local_data_path =
        GetLocalRawDataObjectPrefix() + std::string(VEC_OPT_FIELDS);
    auto local_file = local_files_->OpenForWrite(
        local_files_->PathFromNativePath(local_data_path),
        local::WriteOptions{
            .create = true, .truncate = true, .create_parent = true});
    uint64_t write_offset = 0;
    WriteOptFieldsIvfMeta(local_file, num_of_fields, write_offset);

    std::unordered_set<int64_t> actual_field_ids;
    for (auto& [field_id, tup] : fields_map) {
        const auto& field_type = std::get<1>(tup);
        const auto& element_type = std::get<2>(tup);

        auto field_datas = GetFieldDatasFromStorageV2(remote_files_storage_v2,
                                                      field_id,
                                                      field_type,
                                                      element_type,
                                                      1,
                                                      fs_);

        if (WriteOptFieldIvfData(
                field_type, field_id, local_file, field_datas, write_offset)) {
            actual_field_ids.insert(field_id);
        }
    }

    if (actual_field_ids.size() != num_of_fields) {
        write_offset = 0;
        WriteOptFieldsIvfMeta(
            local_file, actual_field_ids.size(), write_offset);
        if (actual_field_ids.empty()) {
            return "";
        }
    }

    return local_data_path;
}

std::string
DiskFileManagerImpl::cache_opt_field_to_disk_v3(const Config& config) {
    auto opt_fields =
        index::GetValueFromConfig<OptFieldT>(config, VEC_OPT_FIELDS);
    if (!opt_fields.has_value()) {
        return "";
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

    auto manifest =
        index::GetValueFromConfig<std::string>(config, SEGMENT_MANIFEST_KEY);
    AssertInfo(manifest.has_value() && manifest.value() != "",
               "[StorageV3] manifest path is empty when build index");
    auto manifest_path_str = manifest.value();
    AssertInfo(loon_ffi_properties_ != nullptr,
               "[StorageV3] loon ffi properties is null when build index "
               "with manifest");

    auto local_data_path =
        GetLocalRawDataObjectPrefix() + std::string(VEC_OPT_FIELDS);
    auto local_file = local_files_->OpenForWrite(
        local_files_->PathFromNativePath(local_data_path),
        local::WriteOptions{
            .create = true, .truncate = true, .create_parent = true});
    uint64_t write_offset = 0;
    WriteOptFieldsIvfMeta(local_file, num_of_fields, write_offset);

    std::unordered_set<int64_t> actual_field_ids;
    for (auto& [field_id, tup] : fields_map) {
        const auto& field_type = std::get<1>(tup);
        const auto& element_type = std::get<2>(tup);

        // compose field schema for optional field
        proto::schema::FieldSchema field_schema;
        field_schema.set_fieldid(field_id);
        field_schema.set_nullable(true);  // use always nullable
        milvus::storage::FieldDataMeta field_meta{field_meta_.collection_id,
                                                  field_meta_.partition_id,
                                                  field_meta_.segment_id,
                                                  field_id,
                                                  field_schema};
        auto field_datas =
            GetFieldDatasFromManifest(manifest_path_str,
                                      loon_ffi_properties_,
                                      field_meta,
                                      field_type,
                                      1,  // scalar field
                                      element_type,
                                      GetStorageColumnMapping(field_id));

        if (WriteOptFieldIvfData(
                field_type, field_id, local_file, field_datas, write_offset)) {
            actual_field_ids.insert(field_id);
        }
    }

    if (actual_field_ids.size() != num_of_fields) {
        write_offset = 0;
        WriteOptFieldsIvfMeta(
            local_file, actual_field_ids.size(), write_offset);
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
    return ResolveLocalPrefix(LocalIndexObjectPath(false));
}

// temporary path used during index building
std::string
DiskFileManagerImpl::GetLocalTempIndexObjectPrefix() {
    return ResolveLocalPrefix(LocalIndexObjectPath(true));
}

// path to store pre-built index contents downloaded from remote storage
std::string
DiskFileManagerImpl::GetLocalTextIndexPrefix() {
    return ResolveLocalPrefix(LocalTextIndexPath(false));
}

// temporary path used during index building
std::string
DiskFileManagerImpl::GetLocalTempTextIndexPrefix() {
    return ResolveLocalPrefix(LocalTextIndexPath(true));
}

std::string
DiskFileManagerImpl::GetLocalJsonStatsPrefix() {
    return ResolveLocalPrefix(LocalJsonStatsPath(false));
}

std::string
DiskFileManagerImpl::GetLocalTempJsonStatsPrefix() {
    return ResolveLocalPrefix(LocalJsonStatsPath(true));
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
    return ResolveLocalPrefix(LocalNgramIndexPath(false));
}

std::string
DiskFileManagerImpl::GetLocalTempNgramIndexPrefix() {
    return ResolveLocalPrefix(LocalNgramIndexPath(true));
}

std::string
DiskFileManagerImpl::GetRemoteJsonStatsLogPrefix() {
    if (!stats_base_path_.empty()) {
        return stats_base_path_;
    }
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
    return ResolveLocalPrefix(LocalRawDataPath());
}

bool
DiskFileManagerImpl::RemoveFile(const std::string& file) noexcept {
    // TODO: implement this interface
    return false;
}

std::optional<bool>
DiskFileManagerImpl::IsExisted(const std::string& file) noexcept {
    try {
        return local_files_->Exists(local_files_->PathFromNativePath(file));
    } catch (std::exception& e) {
        // LOG_DEBUG("Exception:{}", e).what();
        return std::nullopt;
    }
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

}  // namespace milvus::storage
