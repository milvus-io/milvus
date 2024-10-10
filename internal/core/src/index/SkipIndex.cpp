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

namespace milvus {

static const FieldChunkMetrics defaultFieldChunkMetrics;

const FieldChunkMetrics&
SkipIndex::GetFieldChunkMetrics(milvus::FieldId field_id, int chunk_id) const {
    std::shared_lock lck(mutex_);
    auto field_metrics = fieldChunkMetrics_.find(field_id);
    if (field_metrics != fieldChunkMetrics_.end()) {
        auto field_chunk_metrics = field_metrics->second.find(chunk_id);
        if (field_chunk_metrics != field_metrics->second.end()) {
            return *(field_chunk_metrics->second.get());
        }
    }
    return defaultFieldChunkMetrics;
}

void
SkipIndex::LoadPrimitive(milvus::FieldId field_id,
                         int64_t chunk_id,
                         milvus::DataType data_type,
                         const void* chunk_data,
                         const bool* valid_data,
                         int64_t count) {
    auto chunkMetrics = std::make_unique<FieldChunkMetrics>();

    if (count > 0) {
        switch (data_type) {
            case DataType::INT8: {
                const int8_t* typedData =
                    static_cast<const int8_t*>(chunk_data);
                auto info =
                    ProcessFieldMetrics<int8_t>(typedData, valid_data, count);
                chunkMetrics->min_ = Metrics(info.min_);
                chunkMetrics->max_ = Metrics(info.max_);
                chunkMetrics->null_count_ = info.null_count_;
                break;
            }
            case DataType::INT16: {
                const int16_t* typedData =
                    static_cast<const int16_t*>(chunk_data);
                auto info =
                    ProcessFieldMetrics<int16_t>(typedData, valid_data, count);
                chunkMetrics->min_ = Metrics(info.min_);
                chunkMetrics->max_ = Metrics(info.max_);
                chunkMetrics->null_count_ = info.null_count_;
                break;
            }
            case DataType::INT32: {
                const int32_t* typedData =
                    static_cast<const int32_t*>(chunk_data);
                auto info =
                    ProcessFieldMetrics<int32_t>(typedData, valid_data, count);
                chunkMetrics->min_ = Metrics(info.min_);
                chunkMetrics->max_ = Metrics(info.max_);
                chunkMetrics->null_count_ = info.null_count_;
                break;
            }
            case DataType::INT64: {
                const int64_t* typedData =
                    static_cast<const int64_t*>(chunk_data);
                auto info =
                    ProcessFieldMetrics<int64_t>(typedData, valid_data, count);
                chunkMetrics->min_ = Metrics(info.min_);
                chunkMetrics->max_ = Metrics(info.max_);
                chunkMetrics->null_count_ = info.null_count_;
                break;
            }
            case DataType::FLOAT: {
                const float* typedData = static_cast<const float*>(chunk_data);
                auto info =
                    ProcessFieldMetrics<float>(typedData, valid_data, count);
                chunkMetrics->min_ = Metrics(info.min_);
                chunkMetrics->max_ = Metrics(info.max_);
                chunkMetrics->null_count_ = info.null_count_;
                break;
            }
            case DataType::DOUBLE: {
                const double* typedData =
                    static_cast<const double*>(chunk_data);
                auto info =
                    ProcessFieldMetrics<double>(typedData, valid_data, count);
                chunkMetrics->min_ = Metrics(info.min_);
                chunkMetrics->max_ = Metrics(info.max_);
                chunkMetrics->null_count_ = info.null_count_;
                break;
            }
        }
    }
    chunkMetrics->hasValue_ = chunkMetrics->null_count_ == count ? false : true;
    std::unique_lock lck(mutex_);
    if (fieldChunkMetrics_.count(field_id) == 0) {
        fieldChunkMetrics_.insert(std::make_pair(
            field_id,
            std::unordered_map<int64_t, std::unique_ptr<FieldChunkMetrics>>()));
    }

    fieldChunkMetrics_[field_id].emplace(chunk_id, std::move(chunkMetrics));
}

void
SkipIndex::LoadString(
    milvus::FieldId field_id,
    int64_t chunk_id,
    const milvus::ChunkedVariableColumn<std::string>& var_column) {
    int num_rows = var_column.NumRows();
    auto chunkMetrics = std::make_unique<FieldChunkMetrics>();
    if (num_rows > 0) {
        auto info = ProcessStringFieldMetrics(var_column);
        chunkMetrics->min_ = Metrics(info.min_);
        chunkMetrics->max_ = Metrics(info.max_);
        chunkMetrics->null_count_ = info.null_count_;
    }

    chunkMetrics->hasValue_ =
        chunkMetrics->null_count_ == num_rows ? false : true;

    std::unique_lock lck(mutex_);
    if (fieldChunkMetrics_.count(field_id) == 0) {
        fieldChunkMetrics_.insert(std::make_pair(
            field_id,
            std::unordered_map<int64_t, std::unique_ptr<FieldChunkMetrics>>()));
    }
    fieldChunkMetrics_[field_id].emplace(chunk_id, std::move(chunkMetrics));
}

void
SkipIndex::LoadString(
    milvus::FieldId field_id,
    int64_t chunk_id,
    const milvus::SingleChunkVariableColumn<std::string>& var_column) {
    int num_rows = var_column.NumRows();
    auto chunkMetrics = std::make_unique<FieldChunkMetrics>();
    if (num_rows > 0) {
        auto info = ProcessStringFieldMetrics(var_column);
        chunkMetrics->min_ = Metrics(std::move(info.min_));
        chunkMetrics->max_ = Metrics(std::move(info.max_));
        chunkMetrics->null_count_ = info.null_count_;
    }

    chunkMetrics->hasValue_ =
        chunkMetrics->null_count_ == num_rows ? false : true;

    std::unique_lock lck(mutex_);
    if (fieldChunkMetrics_.count(field_id) == 0) {
        fieldChunkMetrics_.insert(std::make_pair(
            field_id,
            std::unordered_map<int64_t, std::unique_ptr<FieldChunkMetrics>>()));
    }
    fieldChunkMetrics_[field_id].emplace(chunk_id, std::move(chunkMetrics));
}

}  // namespace milvus
