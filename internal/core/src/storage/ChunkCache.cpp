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

#include "ChunkCache.h"
#include <future>
#include <memory>
#include "common/Types.h"

namespace milvus::storage {
std::shared_ptr<ColumnBase>
ChunkCache::Read(const std::string& filepath,
                 const MmapChunkDescriptorPtr& descriptor,
                 const bool mmap_rss_not_need) {
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
    std::unique_ptr<DataCodec> field_data;
    std::shared_ptr<ColumnBase> column;
    bool allocate_success = false;
    ErrorCode err_code = Success;
    std::string err_msg = "";
    try {
        field_data = DownloadAndDecodeRemoteFile(cm_.get(), filepath);
        column = Mmap(field_data->GetFieldData(), descriptor);
        if (mmap_rss_not_need) {
            auto ok = madvise(reinterpret_cast<void*>(
                                  const_cast<char*>(column->MmappedData())),
                              column->ByteSize(),
                              ReadAheadPolicy_Map["dontneed"]);
            if (ok != 0) {
                LOG_WARN(
                    "failed to madvise to the data file {}, addr {}, size {}, "
                    "err: "
                    "{}",
                    filepath,
                    static_cast<const void*>(column->MmappedData()),
                    column->ByteSize(),
                    strerror(errno));
            }
        }
        allocate_success = true;
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
        column->ByteSize(),
        read_ahead_policy_);
    if (ok != 0) {
        LOG_WARN(
            "failed to madvise to the data file {}, addr {}, size {}, err: {}",
            filepath,
            static_cast<const void*>(column->MmappedData()),
            column->ByteSize(),
            strerror(errno));
    }
}

std::shared_ptr<ColumnBase>
ChunkCache::Mmap(const FieldDataPtr& field_data,
                 const MmapChunkDescriptorPtr& descriptor) {
    auto dim = field_data->get_dim();
    auto data_type = field_data->get_data_type();

    auto data_size = field_data->Size();

    std::shared_ptr<ColumnBase> column{};

    if (IsSparseFloatVectorDataType(data_type)) {
        std::vector<uint64_t> indices{};
        uint64_t offset = 0;
        for (auto i = 0; i < field_data->get_num_rows(); ++i) {
            indices.push_back(offset);
            offset += field_data->Size(i);
        }
        auto sparse_column = std::make_shared<SparseFloatColumn>(
            data_size, dim, data_type, mcm_, descriptor);
        sparse_column->AppendBatchMmap(field_data);
        sparse_column->Seal(std::move(indices));
        column = std::move(sparse_column);
    } else if (IsVariableDataType(data_type)) {
        AssertInfo(
            false, "TODO: unimplemented for variable data type: {}", data_type);
    } else {
        column = std::make_shared<Column>(
            data_size, dim, data_type, mcm_, descriptor);
        column->AppendBatch(field_data);
    }
    return column;
}
}  // namespace milvus::storage
