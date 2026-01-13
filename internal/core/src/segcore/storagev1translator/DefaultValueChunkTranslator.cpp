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

#include "segcore/storagev1translator/DefaultValueChunkTranslator.h"

#include "common/ChunkWriter.h"
#include "common/Types.h"
#include "segcore/Utils.h"
#include "segcore/Utils.h"
#include "storage/Util.h"

namespace milvus::segcore::storagev1translator {

DefaultValueChunkTranslator::DefaultValueChunkTranslator(
    int64_t segment_id,
    FieldMeta field_meta,
    FieldDataInfo field_data_info,
    bool use_mmap,
    bool mmap_populate,
    const std::string& warmup_policy)
    : total_rows_(field_data_info.row_count),
      segment_id_(segment_id),
      key_(
          fmt::format("seg_{}_f_{}_def", segment_id, field_data_info.field_id)),
      use_mmap_(use_mmap),
      mmap_populate_(mmap_populate),
      mmap_dir_path_(field_data_info.mmap_dir_path),
      field_meta_(field_meta),
      meta_(use_mmap ? milvus::cachinglayer::StorageType::DISK
                     : milvus::cachinglayer::StorageType::MEMORY,
            // For default-value fields, one logical chunk per caching cell.
            // Cell IDs are identical to chunk IDs.
            milvus::cachinglayer::CellIdMappingMode::IDENTICAL,
            milvus::segcore::getCellDataType(
                IsVectorDataType(field_meta.get_data_type()),
                /* is_index */ false),
            milvus::segcore::getCacheWarmupPolicy(
                warmup_policy,
                IsVectorDataType(field_meta.get_data_type()),
                /* is_index */ false,
                /* in_load_list, set to false to reduce memory usage */ false),
            /* support_eviction */ false) {
    // Split rows into ~64KB cells according to value_size().
    // Fallback to single-cell if value_size() is not well-defined.
    auto vsize = this->value_size();
    int64_t rows_per_cell = total_rows_;
    if (vsize > 0) {
        rows_per_cell = std::max<int64_t>(
            1, kTargetCellBytes / static_cast<int64_t>(vsize));
    }
    // primary_cell_rows_ is the standard cell row count, but should not exceed
    // total_rows_.
    primary_cell_rows_ = std::min(rows_per_cell, total_rows_);

    meta_.num_rows_until_chunk_.clear();
    meta_.num_rows_until_chunk_.reserve(
        static_cast<size_t>(total_rows_ / rows_per_cell) + 2);
    meta_.num_rows_until_chunk_.push_back(0);
    while (meta_.num_rows_until_chunk_.back() < total_rows_) {
        auto prev = meta_.num_rows_until_chunk_.back();
        auto remain = total_rows_ - prev;
        auto this_rows = std::min(remain, rows_per_cell);
        meta_.num_rows_until_chunk_.push_back(prev + this_rows);
    }

    auto nr_chunks =
        static_cast<int64_t>(meta_.num_rows_until_chunk_.size() - 1);
    virtual_chunk_config(total_rows_,
                         nr_chunks,
                         meta_.num_rows_until_chunk_,
                         meta_.virt_chunk_order_,
                         meta_.vcid_to_cid_arr_);

    field_id_ = field_data_info.field_id;

    // Buffer sharing is only safe for non-nullable fixed-width types, where
    // there is no null bitmap and data_start_ == data_ regardless of row count.
    // For nullable types, the null bitmap size (ceil(row_nums/8)) affects
    // data_start_ offset in FixedWidthChunk, so sharing a buffer built for N
    // rows with a chunk claiming M rows would cause data_start_ to be wrong.
    // For variable-length types, buffer layout (offsets position) also depends
    // on exact row count.
    can_share_buffer_ = !IsVariableDataType(field_meta_.get_data_type()) &&
                        !field_meta_.is_nullable();

    // Pre-build primary buffer for all primary cells
    if (primary_cell_rows_ > 0) {
        primary_buffer_ = build_buffer_for_rows(primary_cell_rows_, "");
    }

    // For variable-length types, check if tail cell has different row count
    if (!can_share_buffer_ && num_cells() > 1) {
        auto last_cid = num_cells() - 1;
        auto tail_rows = meta_.num_rows_until_chunk_[last_cid + 1] -
                         meta_.num_rows_until_chunk_[last_cid];
        if (tail_rows != primary_cell_rows_) {
            tail_buffer_ = build_buffer_for_rows(tail_rows, "_tail");
            tail_cell_rows_ = tail_rows;
        }
    }
}

DefaultValueChunkTranslator::~DefaultValueChunkTranslator() {
}

size_t
DefaultValueChunkTranslator::num_cells() const {
    return meta_.num_rows_until_chunk_.size() > 0
               ? meta_.num_rows_until_chunk_.size() - 1
               : 0;
}

milvus::cachinglayer::cid_t
DefaultValueChunkTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return uid;
}

int64_t
DefaultValueChunkTranslator::value_size() const {
    int64_t value_size = 0;
    switch (field_meta_.get_data_type()) {
        case milvus::DataType::BOOL:
            value_size = sizeof(bool);
            break;
        case milvus::DataType::INT8:
            value_size = sizeof(int8_t);
            break;
        case milvus::DataType::INT16:
            value_size = sizeof(int16_t);
            break;
        case milvus::DataType::INT32:
            value_size = sizeof(int32_t);
            break;
        case milvus::DataType::INT64:
            value_size = sizeof(int64_t);
            break;
        case milvus::DataType::TIMESTAMPTZ:
            value_size = sizeof(int64_t);
            break;
        case milvus::DataType::FLOAT:
            value_size = sizeof(float);
            break;
        case milvus::DataType::DOUBLE:
            value_size = sizeof(double);
            break;
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING:
        case milvus::DataType::TEXT:
            if (field_meta_.default_value().has_value()) {
                auto default_value = field_meta_.default_value().value();
                value_size = default_value.string_data().size() +
                             1;  // +1 for null terminator
            } else {
                value_size = 1;  // 1 for null
            }
            break;
        case milvus::DataType::JSON:
            value_size = sizeof(Json);
            break;
        case milvus::DataType::ARRAY:
            value_size = sizeof(Array);
            break;
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported default value data type {}",
                      field_meta_.get_data_type());
    }
    return value_size;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
DefaultValueChunkTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    // TODO: actually only the first cell is used, other cells share the same buffer,
    // but for now we estimate the same size for all cells
    auto value_size = this->value_size();
    auto rows_begin = meta_.num_rows_until_chunk_[cid];
    auto rows_end = meta_.num_rows_until_chunk_[cid + 1];
    auto rows = rows_end - rows_begin;
    auto cell_bytes = value_size * rows;
    if (use_mmap_) {
        return {{0, cell_bytes}, {0, cell_bytes}};
    } else {
        return {{cell_bytes, 0}, {cell_bytes, 0}};
    }
}

const std::string&
DefaultValueChunkTranslator::key() const {
    return key_;
}

milvus::ChunkBuffer
DefaultValueChunkTranslator::build_buffer_for_rows(
    int64_t num_rows, const std::string& suffix) const {
    auto data_type = field_meta_.get_data_type();
    std::shared_ptr<arrow::ArrayBuilder> builder;

    if (IsVectorDataType(data_type)) {
        AssertInfo(field_meta_.is_nullable(),
                   "only nullable vector fields can be dynamically added");
        builder = milvus::storage::CreateArrowBuilder(
            data_type, field_meta_.get_element_type(), field_meta_.get_dim());
    } else {
        builder = milvus::storage::CreateArrowBuilder(data_type);
    }

    arrow::Status ast;
    if (field_meta_.default_value().has_value()) {
        ast = builder->Reserve(num_rows);
        AssertInfo(
            ast.ok(), "reserve arrow builder failed: {}", ast.ToString());
        auto default_scalar =
            storage::CreateArrowScalarFromDefaultValue(field_meta_);
        ast = builder->AppendScalar(*default_scalar, num_rows);
    } else {
        ast = builder->AppendNulls(num_rows);
    }
    AssertInfo(ast.ok(),
               "append null/default values to arrow builder failed: {}",
               ast.ToString());

    arrow::ArrayVector array_vec;
    array_vec.emplace_back(builder->Finish().ValueOrDie());

    if (!use_mmap_ || mmap_dir_path_.empty()) {
        return milvus::create_chunk_buffer(
            field_meta_, array_vec, mmap_populate_);
    } else {
        auto filepath =
            std::filesystem::path(mmap_dir_path_) /
            fmt::format("seg_{}_f_{}_def{}", segment_id_, field_id_, suffix);
        std::filesystem::create_directories(filepath.parent_path());
        return milvus::create_chunk_buffer(
            field_meta_, array_vec, mmap_populate_, filepath.string());
    }
}

std::vector<
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
DefaultValueChunkTranslator::get_cells(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    std::vector<
        std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
        res;
    res.reserve(cids.size());

    for (auto cid : cids) {
        assert(cid + 1 < meta_.num_rows_until_chunk_.size());

        auto rows_begin = meta_.num_rows_until_chunk_[cid];
        auto rows_end = meta_.num_rows_until_chunk_[cid + 1];
        auto num_rows = rows_end - rows_begin;

        std::unique_ptr<milvus::Chunk> chunk;
        if (can_share_buffer_) {
            // Fixed-width types: share the pre-built buffer, override row count
            AssertInfo(primary_buffer_.has_value(),
                       "primary buffer is not initialized");
            chunk = milvus::make_chunk_from_buffer(
                field_meta_, primary_buffer_.value(), num_rows);
        } else {
            // Variable-length types: use matching buffer (primary or tail)
            // because buffer layout (null bitmap, offsets) must match row count.
            // row_nums_override=0: use the buffer's own row_nums.
            if (tail_buffer_.has_value() && num_rows == tail_cell_rows_) {
                chunk = milvus::make_chunk_from_buffer(
                    field_meta_, tail_buffer_.value(), 0);
            } else {
                AssertInfo(primary_buffer_.has_value(),
                           "primary buffer is not initialized");
                chunk = milvus::make_chunk_from_buffer(
                    field_meta_, primary_buffer_.value(), 0);
            }
        }
        res.emplace_back(cid, std::move(chunk));
    }

    return res;
}

}  // namespace milvus::segcore::storagev1translator
