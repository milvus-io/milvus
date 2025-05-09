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

ChunkTranslator::ChunkTranslator(int64_t segment_id,
                                 FieldMeta field_meta,
                                 FieldDataInfo field_data_info,
                                 std::vector<std::string> insert_files,
                                 bool use_mmap)
    : segment_id_(segment_id),
      key_(fmt::format("seg_{}_f_{}", segment_id, field_data_info.field_id)),
      use_mmap_(use_mmap),
      meta_(use_mmap ? milvus::cachinglayer::StorageType::DISK
                     : milvus::cachinglayer::StorageType::MEMORY) {
    chunks_.resize(insert_files.size());
    AssertInfo(
        !SystemProperty::Instance().IsSystem(FieldId(field_data_info.field_id)),
        "ChunkTranslator not supported for system field");

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    pool.Submit(LoadArrowReaderFromRemote,
                insert_files,
                field_data_info.arrow_reader_channel);
    LOG_INFO("segment {} submits load field {} task to thread pool",
             segment_id_,
             field_data_info.field_id);

    auto data_type = field_meta.get_data_type();
    auto cid = 0;
    auto row_count = 0;
    meta_.num_rows_until_chunk_.push_back(0);
    if (!use_mmap_) {
        std::shared_ptr<milvus::ArrowDataWrapper> r;
        while (field_data_info.arrow_reader_channel->pop(r)) {
            arrow::ArrayVector array_vec =
                read_single_column_batches(r->reader);
            auto chunk =
                create_chunk(field_meta,
                             IsVectorDataType(data_type) &&
                                     !IsSparseFloatVectorDataType(data_type)
                                 ? field_meta.get_dim()
                                 : 1,
                             array_vec)
                    .release();
            chunks_[cid] = chunk;
            row_count += chunk->RowNums();
            meta_.num_rows_until_chunk_.push_back(row_count);
            cid++;
        }
    } else {
        auto filepath = std::filesystem::path(field_data_info.mmap_dir_path) /
                        std::to_string(segment_id_) /
                        std::to_string(field_data_info.field_id);
        auto dir = filepath.parent_path();
        std::filesystem::create_directories(dir);

        auto file = File::Open(filepath.string(), O_CREAT | O_TRUNC | O_RDWR);

        std::shared_ptr<milvus::ArrowDataWrapper> r;
        size_t file_offset = 0;
        std::vector<std::shared_ptr<Chunk>> chunks;
        while (field_data_info.arrow_reader_channel->pop(r)) {
            arrow::ArrayVector array_vec =
                read_single_column_batches(r->reader);
            auto chunk =
                create_chunk(field_meta,
                             IsVectorDataType(data_type) &&
                                     !IsSparseFloatVectorDataType(data_type)
                                 ? field_meta.get_dim()
                                 : 1,
                             file,
                             file_offset,
                             array_vec)
                    .release();
            chunks_[cid] = chunk;
            row_count += chunk->RowNums();
            meta_.num_rows_until_chunk_.push_back(row_count);
            cid++;
            file_offset += chunk->Size();
        }
    }
    AssertInfo(row_count == field_data_info.row_count,
               fmt::format("data lost while loading column {}: loaded "
                           "num rows {} but expected {}",
                           field_data_info.field_id,
                           row_count,
                           field_data_info.row_count));
}

ChunkTranslator::~ChunkTranslator() {
    for (auto chunk : chunks_) {
        if (chunk != nullptr) {
            // let the Chunk to be deleted by the unique_ptr
            auto chunk_ptr = std::unique_ptr<Chunk>(chunk);
        }
    }
}

size_t
ChunkTranslator::num_cells() const {
    return chunks_.size();
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
    for (auto cid : cids) {
        AssertInfo(chunks_[cid] != nullptr,
                   "ChunkTranslator::get_cells called again on cell {} of "
                   "CacheSlot {}.",
                   cid,
                   key_);
        cells.emplace_back(cid, std::unique_ptr<milvus::Chunk>(chunks_[cid]));
        chunks_[cid] = nullptr;
    }
    return cells;
}

}  // namespace milvus::segcore::storagev1translator
