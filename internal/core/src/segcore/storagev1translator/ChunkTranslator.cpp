// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "segcore/storagev1translator/ChunkTranslator.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "cachinglayer/Utils.h"
#include "common/ChunkWriter.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/SystemProperty.h"
#include "segcore/Utils.h"
#include "storage/ThreadPools.h"
#include "mmap/Types.h"

namespace milvus::segcore::storagev1translator {

ChunkTranslator::ChunkTranslator(
    int64_t segment_id,
    FieldMeta field_meta,
    FieldDataInfo field_data_info,
    std::vector<std::pair<std::string, int64_t>>&& files_and_rows,
    bool use_mmap)
    : segment_id_(segment_id),
      field_id_(field_data_info.field_id),
      field_meta_(field_meta),
      key_(fmt::format("seg_{}_f_{}", segment_id, field_meta.get_id().get())),
      use_mmap_(use_mmap),
      files_and_rows_(std::move(files_and_rows)),
      mmap_dir_path_(field_data_info.mmap_dir_path),
      meta_(use_mmap ? milvus::cachinglayer::StorageType::DISK
                     : milvus::cachinglayer::StorageType::MEMORY,
            milvus::segcore::getCacheWarmupPolicy(
                IsVectorDataType(field_meta.get_data_type()),
                /* is_index */ false),
            /* support_eviction */ false) {
    AssertInfo(!SystemProperty::Instance().IsSystem(FieldId(field_id_)),
               "ChunkTranslator not supported for system field");
    meta_.num_rows_until_chunk_.push_back(0);
    for (auto& [file, rows] : files_and_rows_) {
        meta_.num_rows_until_chunk_.push_back(
            meta_.num_rows_until_chunk_.back() + rows);
    }
    AssertInfo(meta_.num_rows_until_chunk_.back() == field_data_info.row_count,
               fmt::format("data lost while loading column {}: found "
                           "num rows {} but expected {}",
                           field_data_info.field_id,
                           meta_.num_rows_until_chunk_.back(),
                           field_data_info.row_count));
}

std::unique_ptr<milvus::Chunk>
ChunkTranslator::load_chunk(milvus::cachinglayer::cid_t cid) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    auto channel = std::make_shared<ArrowReaderChannel>();
    pool.Submit(LoadArrowReaderFromRemote,
                std::vector<std::string>{files_and_rows_[cid].first},
                channel);
    LOG_DEBUG("segment {} submits load field {} chunk {} task to thread pool",
              segment_id_,
              field_id_,
              cid);

    auto data_type = field_meta_.get_data_type();

    if (!use_mmap_) {
        std::shared_ptr<milvus::ArrowDataWrapper> r;
        while (channel->pop(r)) {
            arrow::ArrayVector array_vec =
                read_single_column_batches(r->reader);
            return create_chunk(field_meta_,
                                IsVectorDataType(data_type) &&
                                        !IsSparseFloatVectorDataType(data_type)
                                    ? field_meta_.get_dim()
                                    : 1,
                                array_vec);
        }
    } else {
        // we don't know the resulting file size beforehand, thus using a separate file for each chunk.
        auto filepath = std::filesystem::path(mmap_dir_path_) /
                        std::to_string(segment_id_) /
                        std::to_string(field_id_) / std::to_string(cid);

        LOG_INFO("segment {} mmaping field {} chunk {} to path {}",
                 segment_id_,
                 field_id_,
                 cid,
                 filepath.string());

        std::filesystem::create_directories(filepath.parent_path());

        auto file = File::Open(filepath.string(), O_CREAT | O_TRUNC | O_RDWR);

        std::shared_ptr<milvus::ArrowDataWrapper> r;
        while (channel->pop(r)) {
            arrow::ArrayVector array_vec =
                read_single_column_batches(r->reader);
            auto chunk = create_chunk(field_meta_,
                                IsVectorDataType(data_type) &&
                                        !IsSparseFloatVectorDataType(data_type)
                                    ? field_meta_.get_dim()
                                    : 1,
                                file,
                                /*file_offset*/ 0,
                                array_vec);
            auto ok = unlink(filepath.c_str());
            AssertInfo(ok == 0,
                    fmt::format("failed to unlink mmap data file {}, err: {}",
                                filepath.c_str(),
                                strerror(errno)));
            return chunk;
        }
    }
}

size_t
ChunkTranslator::num_cells() const {
    return files_and_rows_.size();
}

milvus::cachinglayer::cid_t
ChunkTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return uid;
}

milvus::cachinglayer::ResourceUsage
ChunkTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    return {0, 0};
}

const std::string&
ChunkTranslator::key() const {
    return key_;
}

std::vector<
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
ChunkTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    std::vector<
        std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
        cells;
    cells.reserve(cids.size());
    for (auto cid : cids) {
        cells.emplace_back(cid, load_chunk(cid));
    }
    return cells;
}

}  // namespace milvus::segcore::storagev1translator
