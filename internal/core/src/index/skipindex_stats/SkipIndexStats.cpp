// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/skipindex_stats/SkipIndexStats.h"
#include <cstring>
#include "parquet/types.h"

namespace milvus::index {

std::unique_ptr<FieldChunkMetrics>
SkipIndexStatsBuilder::Build(
    DataType data_type,
    const std::shared_ptr<parquet::Statistics>& statistic) const {
    std::unique_ptr<FieldChunkMetrics> chunk_metrics;
    switch (data_type) {
        case DataType::INT8: {
            auto info =
                ProcessFieldMetrics<parquet::Int32Type, int8_t>(statistic);
            chunk_metrics = std::make_unique<IntFieldChunkMetrics<int8_t>>(
                info.min_, info.max_, nullptr);
            break;
        }
        case milvus::DataType::INT16: {
            auto info =
                ProcessFieldMetrics<parquet::Int32Type, int16_t>(statistic);
            chunk_metrics = std::make_unique<IntFieldChunkMetrics<int16_t>>(
                info.min_, info.max_, nullptr);
            break;
        }
        case milvus::DataType::INT32: {
            auto info =
                ProcessFieldMetrics<parquet::Int32Type, int32_t>(statistic);
            chunk_metrics = std::make_unique<IntFieldChunkMetrics<int32_t>>(
                info.min_, info.max_, nullptr);
            break;
        }
        case milvus::DataType::INT64: {
            auto info =
                ProcessFieldMetrics<parquet::Int64Type, int64_t>(statistic);
            chunk_metrics = std::make_unique<IntFieldChunkMetrics<int64_t>>(
                info.min_, info.max_, nullptr);
            break;
        }
        case milvus::DataType::FLOAT: {
            auto info =
                ProcessFieldMetrics<parquet::FloatType, float>(statistic);
            chunk_metrics = std::make_unique<FloatFieldChunkMetrics<float>>(
                info.min_, info.max_);
            break;
        }
        case milvus::DataType::DOUBLE: {
            auto info =
                ProcessFieldMetrics<parquet::DoubleType, double>(statistic);
            chunk_metrics = std::make_unique<FloatFieldChunkMetrics<double>>(
                info.min_, info.max_);
            break;
        }
        case milvus::DataType::VARCHAR: {
            auto info =
                ProcessFieldMetrics<parquet::ByteArrayType, std::string>(
                    statistic);
            chunk_metrics = std::make_unique<StringFieldChunkMetrics>(
                std::string(info.min_),
                std::string(info.max_),
                nullptr,
                nullptr);
            break;
        }
        default: {
            chunk_metrics = std::make_unique<NoneFieldChunkMetrics>();
            break;
        }
    }
    return chunk_metrics;
}

std::unique_ptr<FieldChunkMetrics>
SkipIndexStatsBuilder::Build(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    int col_idx,
    arrow::Type::type data_type) const {
    auto none_ptr = std::make_unique<NoneFieldChunkMetrics>();
    if (batches.empty()) {
        return none_ptr;
    }
    switch (data_type) {
        case arrow::Type::BOOL: {
            metricsInfo<bool> info =
                ProcessFieldMetrics<bool, arrow::BooleanArray>(batches,
                                                               col_idx);
            return LoadMetrics<bool>(info);
        }
        case arrow::Type::INT8: {
            auto info =
                ProcessFieldMetrics<int8_t, arrow::Int8Array>(batches, col_idx);
            return LoadMetrics<int8_t>(info);
        }
        case arrow::Type::INT16: {
            auto info = ProcessFieldMetrics<int16_t, arrow::Int16Array>(
                batches, col_idx);
            return LoadMetrics<int16_t>(info);
        }
        case arrow::Type::INT32: {
            auto info = ProcessFieldMetrics<int32_t, arrow::Int32Array>(
                batches, col_idx);
            return LoadMetrics<int32_t>(info);
        }
        case arrow::Type::INT64: {
            auto info = ProcessFieldMetrics<int64_t, arrow::Int64Array>(
                batches, col_idx);
            return LoadMetrics<int64_t>(info);
        }
        case arrow::Type::FLOAT: {
            auto info =
                ProcessFieldMetrics<float, arrow::FloatArray>(batches, col_idx);
            return LoadMetrics<float>(info);
        }
        case arrow::Type::DOUBLE: {
            auto info = ProcessFieldMetrics<double, arrow::DoubleArray>(
                batches, col_idx);
            return LoadMetrics<double>(info);
        }
        case arrow::Type::STRING: {
            const metricsInfo<std::string>& info =
                ProcessStringFieldMetrics(batches, col_idx);
            return LoadMetrics<std::string>(info);
        }
    }
    return none_ptr;
}

std::unique_ptr<FieldChunkMetrics>
SkipIndexStatsBuilder::Build(DataType data_type, const Chunk* chunk) const {
    auto none_ptr = std::make_unique<NoneFieldChunkMetrics>();
    if (chunk == nullptr || chunk->RowNums() == 0) {
        return none_ptr;
    }
    if (data_type == DataType::VARCHAR) {
        auto string_chunk = static_cast<const StringChunk*>(chunk);
        metricsInfo<std::string> info = ProcessStringFieldMetrics(string_chunk);
        return LoadMetrics<std::string>(info);
    }
    auto fixed_chunk = static_cast<const FixedWidthChunk*>(chunk);
    auto span = fixed_chunk->Span();

    const void* chunk_data = span.data();
    const bool* valid_data = span.valid_data();
    int64_t count = span.row_count();
    switch (data_type) {
        case DataType::BOOL: {
            const bool* typedData = static_cast<const bool*>(chunk_data);
            auto info = ProcessFieldMetrics<bool>(typedData, valid_data, count);
            return LoadMetrics<bool>(info);
        }
        case DataType::INT8: {
            const int8_t* typedData = static_cast<const int8_t*>(chunk_data);
            auto info =
                ProcessFieldMetrics<int8_t>(typedData, valid_data, count);
            return LoadMetrics<int8_t>(info);
        }
        case DataType::INT16: {
            const int16_t* typedData = static_cast<const int16_t*>(chunk_data);
            auto info =
                ProcessFieldMetrics<int16_t>(typedData, valid_data, count);
            return LoadMetrics<int16_t>(info);
        }
        case DataType::INT32: {
            const int32_t* typedData = static_cast<const int32_t*>(chunk_data);
            auto info =
                ProcessFieldMetrics<int32_t>(typedData, valid_data, count);
            return LoadMetrics<int32_t>(info);
        }
        case DataType::INT64: {
            const int64_t* typedData = static_cast<const int64_t*>(chunk_data);
            auto info =
                ProcessFieldMetrics<int64_t>(typedData, valid_data, count);
            return LoadMetrics<int64_t>(info);
        }
        case DataType::FLOAT: {
            const float* typedData = static_cast<const float*>(chunk_data);
            auto info =
                ProcessFieldMetrics<float>(typedData, valid_data, count);
            return LoadMetrics<float>(info);
        }
        case DataType::DOUBLE: {
            const double* typedData = static_cast<const double*>(chunk_data);
            auto info =
                ProcessFieldMetrics<double>(typedData, valid_data, count);
            return LoadMetrics<double>(info);
        }
    }
    return none_ptr;
}
}  // namespace milvus::index