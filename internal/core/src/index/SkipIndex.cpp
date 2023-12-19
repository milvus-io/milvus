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
                         int64_t count) {
    auto chunkMetrics = std::make_unique<FieldChunkMetrics>();

    if (count > 0) {
        chunkMetrics->hasValue_ = true;
        switch (data_type) {
            case DataType::INT8: {
                const int8_t* typedData =
                    static_cast<const int8_t*>(chunk_data);
                std::pair<int8_t, int8_t> minMax =
                    ProcessFieldMetrics<int8_t>(typedData, count);
                chunkMetrics->min_ = Metrics(minMax.first);
                chunkMetrics->max_ = Metrics(minMax.second);
                break;
            }
            case DataType::INT16: {
                const int16_t* typedData =
                    static_cast<const int16_t*>(chunk_data);
                std::pair<int16_t, int16_t> minMax =
                    ProcessFieldMetrics<int16_t>(typedData, count);
                chunkMetrics->min_ = Metrics(minMax.first);
                chunkMetrics->max_ = Metrics(minMax.second);
                break;
            }
            case DataType::INT32: {
                const int32_t* typedData =
                    static_cast<const int32_t*>(chunk_data);
                std::pair<int32_t, int32_t> minMax =
                    ProcessFieldMetrics<int32_t>(typedData, count);
                chunkMetrics->min_ = Metrics(minMax.first);
                chunkMetrics->max_ = Metrics(minMax.second);
                break;
            }
            case DataType::INT64: {
                const int64_t* typedData =
                    static_cast<const int64_t*>(chunk_data);
                std::pair<int64_t, int64_t> minMax =
                    ProcessFieldMetrics<int64_t>(typedData, count);
                chunkMetrics->min_ = Metrics(minMax.first);
                chunkMetrics->max_ = Metrics(minMax.second);
                break;
            }
            case DataType::FLOAT: {
                const float* typedData = static_cast<const float*>(chunk_data);
                std::pair<float, float> minMax =
                    ProcessFieldMetrics<float>(typedData, count);
                chunkMetrics->min_ = Metrics(minMax.first);
                chunkMetrics->max_ = Metrics(minMax.second);
                break;
            }
            case DataType::DOUBLE: {
                const double* typedData =
                    static_cast<const double*>(chunk_data);
                std::pair<double, double> minMax =
                    ProcessFieldMetrics<double>(typedData, count);
                chunkMetrics->min_ = Metrics(minMax.first);
                chunkMetrics->max_ = Metrics(minMax.second);
                break;
            }
        }
    }
    std::unique_lock lck(mutex_);
    if (fieldChunkMetrics_.count(field_id) == 0) {
        fieldChunkMetrics_.insert(std::make_pair(
            field_id,
            std::unordered_map<int64_t, std::unique_ptr<FieldChunkMetrics>>()));
    }

    fieldChunkMetrics_[field_id].emplace(chunk_id, std::move(chunkMetrics));
}

void
SkipIndex::LoadString(milvus::FieldId field_id,
                      int64_t chunk_id,
                      const milvus::VariableColumn<std::string>& var_column) {
    int num_rows = var_column.NumRows();
    auto chunkMetrics = std::make_unique<FieldChunkMetrics>();
    if (num_rows > 0) {
        chunkMetrics->hasValue_ = true;
        std::string_view min_string = var_column.RawAt(0);
        std::string_view max_string = var_column.RawAt(0);
        for (size_t i = 1; i < num_rows; i++) {
            const auto& val = var_column.RawAt(i);
            if (val < min_string) {
                min_string = val;
            }
            if (val > max_string) {
                max_string = val;
            }
        }
        chunkMetrics->min_ = Metrics(min_string);
        chunkMetrics->max_ = Metrics(max_string);
    }
    std::unique_lock lck(mutex_);
    if (fieldChunkMetrics_.count(field_id) == 0) {
        fieldChunkMetrics_.insert(std::make_pair(
            field_id,
            std::unordered_map<int64_t, std::unique_ptr<FieldChunkMetrics>>()));
    }
    fieldChunkMetrics_[field_id].emplace(chunk_id, std::move(chunkMetrics));
}

}  // namespace milvus
