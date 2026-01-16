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
    bool mmap_populate)
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

    // Pre-build shared buffer for default-value cells: all cells, including
    // the tail one, will share this buffer. Tail cells will use a smaller
    // logical row count while reusing the same underlying memory.
    auto build_buffer_for_rows = [&](int64_t num_rows) -> milvus::ChunkBuffer {
        auto data_type = field_meta_.get_data_type();
        std::shared_ptr<arrow::ArrayBuilder> builder;
        if (IsVectorDataType(data_type)) {
            AssertInfo(field_meta_.is_nullable(),
                       "only nullable vector fields can be dynamically added");
            builder = std::make_shared<arrow::BinaryBuilder>();
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
                fmt::format(
                    "seg_{}_f_{}_def", segment_id_, field_data_info.field_id);
            std::filesystem::create_directories(filepath.parent_path());
            // just use default load priority: proto::common::LoadPriority::HIGH
            return milvus::create_chunk_buffer(
                field_meta_, array_vec, mmap_populate_, filepath.string());
        }
    };

    if (primary_cell_rows_ > 0) {
        primary_buffer_ = build_buffer_for_rows(primary_cell_rows_);
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
        case milvus::DataType::VECTOR_FLOAT:
        case milvus::DataType::VECTOR_BINARY:
        case milvus::DataType::VECTOR_FLOAT16:
        case milvus::DataType::VECTOR_BFLOAT16:
        case milvus::DataType::VECTOR_INT8:
        case milvus::DataType::VECTOR_SPARSE_U32_F32:
            AssertInfo(field_meta_.is_nullable(),
                       "only nullable vector fields can be dynamically added");
            value_size = 0;
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

std::vector<
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
DefaultValueChunkTranslator::get_cells(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    AssertInfo(primary_buffer_.has_value(),
               "primary buffer is not initialized");

    std::vector<
        std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
        res;
    res.reserve(cids.size());

    for (auto cid : cids) {
        assert(cid + 1 < meta_.num_rows_until_chunk_.size());

        auto rows_begin = meta_.num_rows_until_chunk_[cid];
        auto rows_end = meta_.num_rows_until_chunk_[cid + 1];
        auto num_rows = rows_end - rows_begin;

        const milvus::ChunkBuffer& buffer = primary_buffer_.value();

        auto chunk =
            milvus::make_chunk_from_buffer(field_meta_, buffer, num_rows);
        res.emplace_back(cid, std::move(chunk));
    }

    return res;
}

}  // namespace milvus::segcore::storagev1translator
