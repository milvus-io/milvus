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

#include <cstdint>
#include <cstring>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "common/Span.h"
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
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING:
        case milvus::DataType::TEXT: {
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
        case milvus::DataType::UUID: {
            // UUID is 16B FixedLenByteArray — reuse ByteArray stats but
            // ordering is byte-wise big-endian via memcmp 16B same as
            // UUID::operator< (data < other.data). Parquet TypedStatistics
            // for FixedLenByteArray also exposes min/max as ByteArray.
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
        case arrow::Type::FIXED_SIZE_BINARY: {
            // UUID: 16B fixed_size_binary(16), min/max via memcmp 16B
            // (same as UUID::operator<). Bloom via 16B bytes.
            int64_t total_rows = 0;
            int64_t null_count = 0;
            milvus::UUID min{};
            milvus::UUID max{};
            ankerl::unordered_dense::set<milvus::UUID> unique_values;
            bool has_first_valid = false;
            for (const auto& batch : batches) {
                auto arr = batch->column(col_idx);
                auto array =
                    std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arr);
                for (int64_t i = 0; i < array->length(); ++i) {
                    if (array->IsNull(i)) {
                        null_count++;
                        continue;
                    }
                    auto view = array->GetView(i);
                    milvus::UUID val{};
                    std::memcpy(val.data.data(), view.data(), 16);
                    if (!has_first_valid) {
                        min = val;
                        max = val;
                        has_first_valid = true;
                    } else {
                        if (std::memcmp(val.data.data(), min.data.data(), 16) <
                            0) {
                            min = val;
                        }
                        if (std::memcmp(val.data.data(), max.data.data(), 16) >
                            0) {
                            max = val;
                        }
                    }
                    if (enable_bloom_filter_) {
                        unique_values.insert(val);
                    }
                }
                total_rows += array->length();
            }
            metricsInfo<milvus::UUID> info{total_rows,
                                           null_count,
                                           min,
                                           max,
                                           false,
                                           false,
                                           std::move(unique_values)};
            return LoadMetrics<milvus::UUID>(info);
        }
        default:
            break;
    }
    return none_ptr;
}

std::unique_ptr<FieldChunkMetrics>
SkipIndexStatsBuilder::Build(DataType data_type, const Chunk* chunk) const {
    auto none_ptr = std::make_unique<NoneFieldChunkMetrics>();
    if (chunk == nullptr || chunk->RowNums() == 0) {
        return none_ptr;
    }
    if (IsStringDataType(data_type)) {
        auto string_chunk = static_cast<const StringChunk*>(chunk);
        metricsInfo<std::string> info = ProcessStringFieldMetrics(string_chunk);
        return LoadMetrics<std::string>(info);
    }
    auto fixed_chunk = static_cast<const FixedWidthChunk*>(chunk);
    auto span = fixed_chunk->Span();

    const void* chunk_data = span.data();
    const auto validity = span.validity();
    int64_t count = span.row_count();
    switch (data_type) {
        case DataType::BOOL: {
            const bool* typedData = static_cast<const bool*>(chunk_data);
            auto info = ProcessFieldMetrics<bool>(typedData, validity, count);
            return LoadMetrics<bool>(info);
        }
        case DataType::INT8: {
            const int8_t* typedData = static_cast<const int8_t*>(chunk_data);
            auto info = ProcessFieldMetrics<int8_t>(typedData, validity, count);
            return LoadMetrics<int8_t>(info);
        }
        case DataType::INT16: {
            const int16_t* typedData = static_cast<const int16_t*>(chunk_data);
            auto info =
                ProcessFieldMetrics<int16_t>(typedData, validity, count);
            return LoadMetrics<int16_t>(info);
        }
        case DataType::INT32: {
            const int32_t* typedData = static_cast<const int32_t*>(chunk_data);
            auto info =
                ProcessFieldMetrics<int32_t>(typedData, validity, count);
            return LoadMetrics<int32_t>(info);
        }
        case DataType::INT64: {
            const int64_t* typedData = static_cast<const int64_t*>(chunk_data);
            auto info =
                ProcessFieldMetrics<int64_t>(typedData, validity, count);
            return LoadMetrics<int64_t>(info);
        }
        case DataType::FLOAT: {
            const float* typedData = static_cast<const float*>(chunk_data);
            auto info = ProcessFieldMetrics<float>(typedData, validity, count);
            return LoadMetrics<float>(info);
        }
        case DataType::DOUBLE: {
            const double* typedData = static_cast<const double*>(chunk_data);
            auto info = ProcessFieldMetrics<double>(typedData, validity, count);
            return LoadMetrics<double>(info);
        }
        case DataType::UUID: {
            // UUID min/max via memcmp 16B (same as UUID::operator<).
            const milvus::UUID* typedData =
                static_cast<const milvus::UUID*>(chunk_data);
            bool has_first_valid = false;
            milvus::UUID min{};
            milvus::UUID max{};
            int64_t null_count = 0;
            ankerl::unordered_dense::set<milvus::UUID> unique_values;
            for (int64_t i = 0; i < count; ++i) {
                if (validity && !validity[i]) {
                    null_count++;
                    continue;
                }
                const milvus::UUID& val = typedData[i];
                if (!has_first_valid) {
                    min = val;
                    max = val;
                    has_first_valid = true;
                } else {
                    if (std::memcmp(val.data.data(), min.data.data(), 16) < 0) {
                        min = val;
                    }
                    if (std::memcmp(val.data.data(), max.data.data(), 16) > 0) {
                        max = val;
                    }
                }
                if (enable_bloom_filter_) {
                    unique_values.insert(val);
                }
            }
            if (count - null_count == 0) {
                return std::make_unique<NoneFieldChunkMetrics>();
            }
            metricsInfo<milvus::UUID> info{count,
                                           null_count,
                                           min,
                                           max,
                                           false,
                                           false,
                                           std::move(unique_values)};
            return LoadMetrics<milvus::UUID>(info);
        }
        default:
            break;
    }
    return none_ptr;
}
}  // namespace milvus::index
