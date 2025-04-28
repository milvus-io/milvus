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
#include "storage/Util.h"

namespace milvus::segcore::storagev1translator {

DefaultValueChunkTranslator::DefaultValueChunkTranslator(
    int64_t segment_id,
    FieldMeta field_meta,
    FieldDataInfo field_data_info,
    bool use_mmap)
    : segment_id_(segment_id),
      key_(fmt::format("seg_{}_f_{}", segment_id, field_data_info.field_id)),
      use_mmap_(use_mmap),
      field_meta_(field_meta),
      meta_(use_mmap ? milvus::cachinglayer::StorageType::DISK
                     : milvus::cachinglayer::StorageType::MEMORY) {
    meta_.num_rows_until_chunk_.push_back(0);
    meta_.num_rows_until_chunk_.push_back(field_data_info.row_count);
}

DefaultValueChunkTranslator::~DefaultValueChunkTranslator() {
}

size_t
DefaultValueChunkTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
DefaultValueChunkTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return 0;
}

milvus::cachinglayer::ResourceUsage
DefaultValueChunkTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    return milvus::cachinglayer::ResourceUsage{0, 0};
}

const std::string&
DefaultValueChunkTranslator::key() const {
    return key_;
}

std::vector<
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
DefaultValueChunkTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    AssertInfo(cids.size() == 1 && cids[0] == 0,
               "DefaultValueChunkTranslator only supports one cell");
    auto num_rows = meta_.num_rows_until_chunk_[1];
    auto builder =
        milvus::storage::CreateArrowBuilder(field_meta_.get_data_type());
    arrow::Status ast;
    if (field_meta_.default_value().has_value()) {
        builder->Reserve(num_rows);
        auto scalar = storage::CreateArrowScalarFromDefaultValue(field_meta_);
        ast = builder->AppendScalar(*scalar, num_rows);
    } else {
        ast = builder->AppendNulls(num_rows);
    }
    AssertInfo(ast.ok(),
               "append null/default values to arrow builder failed: {}",
               ast.ToString());
    arrow::ArrayVector array_vec;
    array_vec.emplace_back(builder->Finish().ValueOrDie());
    auto chunk = create_chunk(
        field_meta_,
        IsVectorDataType(field_meta_.get_data_type()) &&
                !IsSparseFloatVectorDataType(field_meta_.get_data_type())
            ? field_meta_.get_dim()
            : 1,
        array_vec);

    std::vector<
        std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
        res;
    res.reserve(1);
    res.emplace_back(0, std::move(chunk));
    return res;
}

}  // namespace milvus::segcore::storagev1translator
