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

static const FieldChunkMetrics defaultFieldChunkMetrics;

const cachinglayer::PinWrapper<const FieldChunkMetrics*>
SkipIndex::GetFieldChunkMetrics(milvus::FieldId field_id, int chunk_id) const {
    // skip index structure must be setup before using, thus we do not lock here.
    auto field_metrics = fieldChunkMetrics_.find(field_id);
    if (field_metrics != fieldChunkMetrics_.end()) {
        auto& field_chunk_metrics = field_metrics->second;
        auto ca = cachinglayer::SemiInlineGet(
            field_chunk_metrics->PinCells({chunk_id}));
        auto metrics = ca->get_cell_of(chunk_id);
        return cachinglayer::PinWrapper<const FieldChunkMetrics*>(ca, metrics);
    }
    return cachinglayer::PinWrapper<const FieldChunkMetrics*>(
        &defaultFieldChunkMetrics);
}

std::vector<
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<FieldChunkMetrics>>>
FieldChunkMetricsTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<FieldChunkMetrics>>>
        cells;
    cells.reserve(cids.size());
    if (data_type_ == milvus::DataType::VARCHAR) {
        for (auto chunk_id : cids) {
            auto pw = column_->GetChunk(chunk_id);
            auto chunk = static_cast<StringChunk*>(pw.get());

            int num_rows = chunk->RowNums();
            auto chunk_metrics = std::make_unique<FieldChunkMetrics>();
            if (num_rows > 0) {
                auto info = ProcessStringFieldMetrics(chunk);
                chunk_metrics->min_ = Metrics(info.min_);
                chunk_metrics->max_ = Metrics(info.max_);
                chunk_metrics->null_count_ = info.null_count_;
            }
            chunk_metrics->hasValue_ = chunk_metrics->null_count_ != num_rows;
            cells.emplace_back(chunk_id, std::move(chunk_metrics));
        }
    } else {
        for (auto chunk_id : cids) {
            auto pw = column_->GetChunk(chunk_id);
            auto chunk = static_cast<FixedWidthChunk*>(pw.get());
            auto span = chunk->Span();

            const void* chunk_data = span.data();
            const bool* valid_data = span.valid_data();
            int64_t count = span.row_count();

            auto chunk_metrics = std::make_unique<FieldChunkMetrics>();

            if (count > 0) {
                switch (data_type_) {
                    case DataType::INT8: {
                        const int8_t* typedData =
                            static_cast<const int8_t*>(chunk_data);
                        auto info = ProcessFieldMetrics<int8_t>(
                            typedData, valid_data, count);
                        chunk_metrics->min_ = Metrics(info.min_);
                        chunk_metrics->max_ = Metrics(info.max_);
                        chunk_metrics->null_count_ = info.null_count_;
                        break;
                    }
                    case DataType::INT16: {
                        const int16_t* typedData =
                            static_cast<const int16_t*>(chunk_data);
                        auto info = ProcessFieldMetrics<int16_t>(
                            typedData, valid_data, count);
                        chunk_metrics->min_ = Metrics(info.min_);
                        chunk_metrics->max_ = Metrics(info.max_);
                        chunk_metrics->null_count_ = info.null_count_;
                        break;
                    }
                    case DataType::INT32: {
                        const int32_t* typedData =
                            static_cast<const int32_t*>(chunk_data);
                        auto info = ProcessFieldMetrics<int32_t>(
                            typedData, valid_data, count);
                        chunk_metrics->min_ = Metrics(info.min_);
                        chunk_metrics->max_ = Metrics(info.max_);
                        chunk_metrics->null_count_ = info.null_count_;
                        break;
                    }
                    case DataType::INT64: {
                        const int64_t* typedData =
                            static_cast<const int64_t*>(chunk_data);
                        auto info = ProcessFieldMetrics<int64_t>(
                            typedData, valid_data, count);
                        chunk_metrics->min_ = Metrics(info.min_);
                        chunk_metrics->max_ = Metrics(info.max_);
                        chunk_metrics->null_count_ = info.null_count_;
                        break;
                    }
                    case DataType::FLOAT: {
                        const float* typedData =
                            static_cast<const float*>(chunk_data);
                        auto info = ProcessFieldMetrics<float>(
                            typedData, valid_data, count);
                        chunk_metrics->min_ = Metrics(info.min_);
                        chunk_metrics->max_ = Metrics(info.max_);
                        chunk_metrics->null_count_ = info.null_count_;
                        break;
                    }
                    case DataType::DOUBLE: {
                        const double* typedData =
                            static_cast<const double*>(chunk_data);
                        auto info = ProcessFieldMetrics<double>(
                            typedData, valid_data, count);
                        chunk_metrics->min_ = Metrics(info.min_);
                        chunk_metrics->max_ = Metrics(info.max_);
                        chunk_metrics->null_count_ = info.null_count_;
                        break;
                    }
                }
            }
            chunk_metrics->hasValue_ = chunk_metrics->null_count_ != count;
            cells.emplace_back(chunk_id, std::move(chunk_metrics));
        }
    }
    return cells;
}

}  // namespace milvus
