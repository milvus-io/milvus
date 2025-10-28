// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "SkipIndex.h"

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Utils.h"

namespace milvus {

static const index::NoneFieldChunkMetrics defaultFieldChunkMetrics{};

const cachinglayer::PinWrapper<const index::FieldChunkMetrics*>
SkipIndex::GetFieldChunkMetrics(milvus::FieldId field_id, int chunk_id) const {
    // skip index structure must be setup before using, thus we do not lock here.
    auto field_metrics = fieldChunkMetrics_.find(field_id);
    if (field_metrics != fieldChunkMetrics_.end()) {
        auto& field_chunk_metrics = field_metrics->second;
        auto ca = cachinglayer::SemiInlineGet(
            field_chunk_metrics->PinCells(nullptr, {chunk_id}));
        auto metrics = ca->get_cell_of(chunk_id);
        return cachinglayer::PinWrapper<const index::FieldChunkMetrics*>(
            ca, metrics);
    }
    return cachinglayer::PinWrapper<const index::FieldChunkMetrics*>(
        &defaultFieldChunkMetrics);
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<index::FieldChunkMetrics>>>
FieldChunkMetricsTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<index::FieldChunkMetrics>>>
        cells;
    cells.reserve(cids.size());
    for (auto chunk_id : cids) {
        auto pw = column_->GetChunk(nullptr, chunk_id);
        auto chunk_metrics = builder_.Build(data_type_, pw.get());
        cells.emplace_back(chunk_id, std::move(chunk_metrics));
    }
    return cells;
}

}  // namespace milvus
