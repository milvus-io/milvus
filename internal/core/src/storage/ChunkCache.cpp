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

#include <filesystem>
#include <future>
#include <memory>

#include "ChunkCache.h"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "log/Log.h"

namespace milvus::storage {
std::shared_ptr<ColumnBase>
ChunkCache::Read(const std::string& filepath,
                 const FieldMeta& field_meta,
                 bool mmap_enabled,
                 bool mmap_rss_not_need) {
    // use rlock to get future
    {
        std::shared_lock lck(mutex_);
        auto it = columns_.find(filepath);
        if (it != columns_.end()) {
            lck.unlock();
            auto result = it->second.second.get();
            AssertInfo(result, "unexpected null column, file={}", filepath);
            return result;
        }
    }

    // lock for mutation
    std::unique_lock lck(mutex_);
    // double check no-futurn
    auto it = columns_.find(filepath);
    if (it != columns_.end()) {
        lck.unlock();
        auto result = it->second.second.get();
        AssertInfo(result, "unexpected null column, file={}", filepath);
        return result;
    }

    std::promise<std::shared_ptr<ColumnBase>> p;
    std::shared_future<std::shared_ptr<ColumnBase>> f = p.get_future();
    columns_.emplace(filepath, std::make_pair(std::move(p), f));
    lck.unlock();

    // release lock and perform download and decode
    // other thread request same path shall get the future.
    bool allocate_success = false;
    ErrorCode err_code = Success;
    std::string err_msg = "";
    std::shared_ptr<ChunkedColumnBase> column;
    try {
        auto field_data =
            DownloadAndDecodeRemoteFile(cm_.get(), filepath, false);

        std::shared_ptr<Chunk> chunk;
        auto dim = IsSparseFloatVectorDataType(field_meta.get_data_type())
                       ? 1
                       : field_meta.get_dim();
        if (mmap_enabled) {
            auto path = std::filesystem::path(CachePath(filepath));
            auto dir = path.parent_path();
            std::filesystem::create_directories(dir);

            auto file = File::Open(path.string(), O_CREAT | O_TRUNC | O_RDWR);
            chunk = create_chunk(
                field_meta, dim, file, 0, field_data->GetReader()->reader);
            // unlink
            auto ok = unlink(path.c_str());
            AssertInfo(ok == 0,
                       "failed to unlink mmap data file {}, err: {}",
                       path.c_str(),
                       strerror(errno));
        } else {
            chunk =
                create_chunk(field_meta, dim, field_data->GetReader()->reader);
        }

        auto data_type = field_meta.get_data_type();
        if (IsSparseFloatVectorDataType(data_type)) {
            auto sparse_column =
                std::make_shared<ChunkedSparseFloatColumn>(field_meta);
            sparse_column->AddChunk(chunk);
            column = std::move(sparse_column);
        } else if (IsVariableDataType(data_type)) {
            AssertInfo(false,
                       "TODO: unimplemented for variable data type: {}",
                       data_type);
        } else {
            std::vector<std::shared_ptr<Chunk>> chunks{chunk};
            column = std::make_shared<ChunkedColumn>(field_meta, chunks);
        }
        if (mmap_enabled && mmap_rss_not_need) {
            auto ok = madvise(reinterpret_cast<void*>(
                                  const_cast<char*>(column->MmappedData())),
                              column->DataByteSize(),
                              ReadAheadPolicy_Map["dontneed"]);
            if (ok != 0) {
                LOG_WARN(
                    "failed to madvise to the data file {}, addr {}, size {}, "
                    "err: "
                    "{}",
                    filepath,
                    static_cast<const void*>(column->MmappedData()),
                    column->DataByteSize(),
                    strerror(errno));
            }
        }
    } catch (const SegcoreError& e) {
        err_code = e.get_error_code();
        err_msg = fmt::format("failed to read for chunkCache, seg_core_err:{}",
                              e.what());
    }
    std::unique_lock mmap_lck(mutex_);

    it = columns_.find(filepath);
    if (it != columns_.end()) {
        // check pair exists then set value
        it->second.first.set_value(column);
        if (allocate_success) {
            AssertInfo(column, "unexpected null column, file={}", filepath);
        }
    } else {
        PanicInfo(UnexpectedError,
                  "Wrong code, the thread to download for cache should get the "
                  "target entry");
    }
    if (err_code != Success) {
        columns_.erase(filepath);
        throw SegcoreError(err_code, err_msg);
    }
    return column;
}

void
ChunkCache::Remove(const std::string& filepath) {
    std::unique_lock lck(mutex_);
    columns_.erase(filepath);
}

void
ChunkCache::Prefetch(const std::string& filepath) {
    std::shared_lock lck(mutex_);
    auto it = columns_.find(filepath);
    if (it == columns_.end()) {
        return;
    }

    auto column = it->second.second.get();
    auto ok = madvise(
        reinterpret_cast<void*>(const_cast<char*>(column->MmappedData())),
        column->DataByteSize(),
        read_ahead_policy_);
    if (ok != 0) {
        LOG_WARN(
            "failed to madvise to the data file {}, addr {}, size {}, err: {}",
            filepath,
            static_cast<const void*>(column->MmappedData()),
            column->DataByteSize(),
            strerror(errno));
    }
}

// TODO(sunby): use mmap chunk manager to create chunk
std::string
ChunkCache::CachePath(const std::string& filepath) {
    auto path = std::filesystem::path(filepath);
    auto prefix = std::filesystem::path(path_prefix_);

    // Cache path shall not use absolute filepath direct, it shall always under path_prefix_
    if (path.is_absolute()) {
        return (prefix /
                filepath.substr(path.root_directory().string().length(),
                                filepath.length()))
            .string();
    }

    return (prefix / filepath).string();
}

}  // namespace milvus::storage
