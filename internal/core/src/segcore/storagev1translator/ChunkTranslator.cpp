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

void
virtual_chunk_config(int64_t total_row_count,
                     int64_t nr_chunks,
                     const std::vector<int64_t>& num_rows_until_chunk,
                     int64_t& virt_chunk_order,
                     std::vector<int64_t>& vcid_to_cid_arr) {
    // if there is no chunks, just use a single virtual chunk
    if (nr_chunks == 0) {
        virt_chunk_order = 0;
        vcid_to_cid_arr.resize(1);
        vcid_to_cid_arr[0] = 0;
        return;
    }

    // simply assume `avg_num_rows_per_chunk` is far less than 2^64, even far less than 2^32
    auto avg_num_rows_per_chunk = total_row_count / nr_chunks;
    virt_chunk_order = 0;
    while (avg_num_rows_per_chunk >= 2 * (1 << virt_chunk_order)) {
        virt_chunk_order++;
    }
    auto num_rows_per_virt_chunk = 1 << virt_chunk_order;
    auto nr_virt_chunks = total_row_count / num_rows_per_virt_chunk;
    if (total_row_count % num_rows_per_virt_chunk != 0) {
        ++nr_virt_chunks;
    }
    vcid_to_cid_arr.resize(nr_virt_chunks);
    size_t cid = 0;
    for (size_t i = 0; i < vcid_to_cid_arr.size(); i++) {
        // svc is the start row of the virtual chunk, it must be less than `total_row_count`
        int64_t svc = i * num_rows_per_virt_chunk;
        // find the first cid whose end row is greater than svc
        // cid will not be out of range, because svc is always less than `total_row_count`
        // and the last item of `num_rows_until_chunk` is `total_row_count`
        while (svc >= num_rows_until_chunk[cid + 1]) {
            ++cid;
        }
        vcid_to_cid_arr[i] = cid;
    }
}

ChunkTranslator::ChunkTranslator(
    int64_t segment_id,
    FieldMeta field_meta,
    FieldDataInfo field_data_info,
    std::vector<FileInfo>&& file_infos,
    bool use_mmap,
    milvus::proto::common::LoadPriority load_priority)
    : segment_id_(segment_id),
      field_id_(field_data_info.field_id),
      field_meta_(field_meta),
      key_(fmt::format("seg_{}_f_{}", segment_id, field_meta.get_id().get())),
      use_mmap_(use_mmap),
      file_infos_(std::move(file_infos)),
      mmap_dir_path_(field_data_info.mmap_dir_path),
      meta_(use_mmap ? milvus::cachinglayer::StorageType::DISK
                     : milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::IDENTICAL,
            milvus::segcore::getCellDataType(
                IsVectorDataType(field_meta.get_data_type()),
                /* is_index */ false),
            milvus::segcore::getCacheWarmupPolicy(
                IsVectorDataType(field_meta.get_data_type()),
                /* is_index */ false,
                /* in_load_list*/ field_data_info.in_load_list),
            /* support_eviction */ true),
      load_priority_(load_priority) {
    AssertInfo(!SystemProperty::Instance().IsSystem(FieldId(field_id_)),
               "ChunkTranslator not supported for system field");
    meta_.num_rows_until_chunk_.push_back(0);
    for (const auto& info : file_infos_) {
        meta_.num_rows_until_chunk_.push_back(
            meta_.num_rows_until_chunk_.back() + info.row_count);
    }
    AssertInfo(meta_.num_rows_until_chunk_.back() == field_data_info.row_count,
               fmt::format("data lost while loading column {}: found "
                           "num rows {} but expected {}",
                           field_data_info.field_id,
                           meta_.num_rows_until_chunk_.back(),
                           field_data_info.row_count));
    virtual_chunk_config(field_data_info.row_count,
                         file_infos_.size(),
                         meta_.num_rows_until_chunk_,
                         meta_.virt_chunk_order_,
                         meta_.vcid_to_cid_arr_);
}

size_t
ChunkTranslator::num_cells() const {
    return file_infos_.size();
}

milvus::cachinglayer::cid_t
ChunkTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    // For now, the cell id is identical to the uid, so the meta_.cell_id_mapping_mode is IDENTICAL.
    // Note: if you want to use a customized cell id mapping mode, don't forget to change the meta_.cell_id_mapping_mode to CUSTOMIZED.
    return uid;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
ChunkTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    AssertInfo(cid < file_infos_.size(), "cid out of range");

    int64_t memory_size = file_infos_[cid].memory_size;
    if (use_mmap_) {
        // For mmap, the memory is counted as disk usage
        return {{0, memory_size}, {memory_size * 2, memory_size * 2}};
    } else {
        // For non-mmap, the memory is counted as memory usage
        return {{memory_size, 0}, {memory_size * 2, 0}};
    }
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

    std::vector<std::string> remote_files;
    remote_files.reserve(cids.size());
    for (auto cid : cids) {
        remote_files.push_back(file_infos_[cid].file_path);
    }

    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    auto channel = std::make_shared<ArrowReaderChannel>();
    LOG_INFO("segment {} submits load field {} chunks {} task to thread pool",
             segment_id_,
             field_id_,
             fmt::format("{}", fmt::join(cids, " ")));
    pool.Submit(
        LoadArrowReaderFromRemote, remote_files, channel, load_priority_);

    auto data_type = field_meta_.get_data_type();

    std::filesystem::path folder;

    if (use_mmap_) {
        folder = std::filesystem::path(mmap_dir_path_) /
                 std::to_string(segment_id_) / std::to_string(field_id_);
        std::filesystem::create_directories(folder);
    }

    for (auto cid : cids) {
        std::unique_ptr<milvus::Chunk> chunk = nullptr;
        if (!use_mmap_) {
            std::shared_ptr<milvus::ArrowDataWrapper> r;
            // this relies on the fact that channel is blocked when there is no data to pop
            bool popped = channel->pop(r);
            AssertInfo(popped, "failed to pop arrow reader from channel");
            arrow::ArrayVector array_vec =
                read_single_column_batches(r->reader);
            chunk = create_chunk(field_meta_, array_vec);
        } else {
            // we don't know the resulting file size beforehand, thus using a separate file for each chunk.
            auto filepath = folder / std::to_string(cid);

            LOG_INFO("segment {} mmaping field {} chunk {} to path {}",
                     segment_id_,
                     field_id_,
                     cid,
                     filepath.string());

            std::shared_ptr<milvus::ArrowDataWrapper> r;
            bool popped = channel->pop(r);
            AssertInfo(popped, "failed to pop arrow reader from channel");
            arrow::ArrayVector array_vec =
                read_single_column_batches(r->reader);
            chunk = create_chunk(field_meta_, array_vec, filepath.string());
        }
        cells.emplace_back(cid, std::move(chunk));
    }

    return cells;
}

}  // namespace milvus::segcore::storagev1translator
