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
#include <memory>

#include "arrow/type_fwd.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Utils.h"
#include "common/FieldDataInterface.h"

namespace milvus {

static const FieldChunkMetrics defaultFieldChunkMetrics;

FieldChunkMetrics::FieldChunkMetrics() = default;

std::unique_ptr<FieldChunkMetric>
FieldChunkMetrics::LoadMetric(arrow::Type::type data_type,
                              FieldChunkMetricType metric_type,
                              const std::string& data) {
    switch (data_type) {
        case arrow::Type::BOOL:
            switch (metric_type) {
                case FieldChunkMetricType::SET:
                    return std::make_unique<SetFieldChunkMetric<bool>>(data);
                default:
                    return nullptr;
            }
        case arrow::Type::INT8:
            switch (metric_type) {
                case FieldChunkMetricType::MINMAX:
                    return std::make_unique<MinMaxFieldChunkMetric<int8_t>>(
                        data);
                case FieldChunkMetricType::SET:
                    return std::make_unique<SetFieldChunkMetric<int8_t>>(data);
                case FieldChunkMetricType::BLOOM_FILTER:
                    return std::make_unique<
                        BloomFilterFieldChunkMetric<int8_t>>(data);
                default:
                    return nullptr;
            }
        case arrow::Type::INT16:
            switch (metric_type) {
                case FieldChunkMetricType::MINMAX:
                    return std::make_unique<MinMaxFieldChunkMetric<int16_t>>(
                        data);
                case FieldChunkMetricType::SET:
                    return std::make_unique<SetFieldChunkMetric<int16_t>>(data);
                case FieldChunkMetricType::BLOOM_FILTER:
                    return std::make_unique<
                        BloomFilterFieldChunkMetric<int16_t>>(data);
                default:
                    return nullptr;
            }
        case arrow::Type::INT32:
            switch (metric_type) {
                case FieldChunkMetricType::MINMAX:
                    return std::make_unique<MinMaxFieldChunkMetric<int32_t>>(
                        data);
                case FieldChunkMetricType::SET:
                    return std::make_unique<SetFieldChunkMetric<int32_t>>(data);
                case FieldChunkMetricType::BLOOM_FILTER:
                    return std::make_unique<
                        BloomFilterFieldChunkMetric<int32_t>>(data);
                default:
                    return nullptr;
            }
        case arrow::Type::INT64:
            switch (metric_type) {
                case FieldChunkMetricType::MINMAX:
                    return std::make_unique<MinMaxFieldChunkMetric<int64_t>>(
                        data);
                case FieldChunkMetricType::SET:
                    return std::make_unique<SetFieldChunkMetric<int64_t>>(data);
                case FieldChunkMetricType::BLOOM_FILTER:
                    return std::make_unique<
                        BloomFilterFieldChunkMetric<int64_t>>(data);
                default:
                    return nullptr;
            }
        case arrow::Type::FLOAT:
            switch (metric_type) {
                case FieldChunkMetricType::MINMAX:
                    return std::make_unique<MinMaxFieldChunkMetric<float>>(
                        data);
                case FieldChunkMetricType::SET:
                    return std::make_unique<SetFieldChunkMetric<float>>(data);
                case FieldChunkMetricType::BLOOM_FILTER:
                    return std::make_unique<BloomFilterFieldChunkMetric<float>>(
                        data);
                default:
                    return nullptr;
            }
        case arrow::Type::DOUBLE:
            switch (metric_type) {
                case FieldChunkMetricType::MINMAX:
                    return std::make_unique<MinMaxFieldChunkMetric<double>>(
                        data);
                case FieldChunkMetricType::SET:
                    return std::make_unique<SetFieldChunkMetric<double>>(data);
                case FieldChunkMetricType::BLOOM_FILTER:
                    return std::make_unique<
                        BloomFilterFieldChunkMetric<double>>(data);
                default:
                    return nullptr;
            }
        case arrow::Type::STRING:
            switch (metric_type) {
                case FieldChunkMetricType::MINMAX:
                    return std::make_unique<
                        MinMaxFieldChunkMetric<std::string>>(data);
                case FieldChunkMetricType::SET:
                    return std::make_unique<SetFieldChunkMetric<std::string>>(
                        data);
                case FieldChunkMetricType::BLOOM_FILTER:
                    return std::make_unique<
                        BloomFilterFieldChunkMetric<std::string>>(data);
                case FieldChunkMetricType::NGRAM_FILTER:
                    return std::make_unique<NgramFilterFieldChunkMetric>(data);
                default:
                    return nullptr;
            }
        default:
            return nullptr;
    }
}

std::string
FieldChunkMetrics::Serialize() const {
    std::stringstream ss(std::ios::binary | std::ios::out);

    uint32_t count = metrics_.size();
    ss.write(reinterpret_cast<const char*>(&count), sizeof(count));

    for (const auto& metric : metrics_) {
        auto type = metric->GetType();
        ss.write(reinterpret_cast<const char*>(&type), sizeof(type));

        std::string data = metric->Serialize();
        uint32_t len = data.length();
        ss.write(reinterpret_cast<const char*>(&len), sizeof(len));
        ss.write(data.data(), len);
    }
    return ss.str();
}

void
FieldChunkMetrics::Deserialize(const std::string& data) {
    std::stringstream ss(data, std::ios::binary | std::ios::in);
    uint32_t metric_count;
    ss.read(reinterpret_cast<char*>(&metric_count), sizeof(metric_count));
    metrics_.reserve(metric_count);

    for (uint32_t i = 0; i < metric_count; ++i) {
        FieldChunkMetricType metric_type;
        ss.read(reinterpret_cast<char*>(&metric_type), sizeof(metric_type));

        uint32_t metric_len;
        ss.read(reinterpret_cast<char*>(&metric_len), sizeof(metric_len));
        std::string metric_data(metric_len, '\0');
        ss.read(&metric_data[0], metric_len);

        auto metric = LoadMetric(data_type_, metric_type, metric_data);
        if (metric && metric->hasValue_) {
            metrics_.emplace_back(std::move(metric));
        }
    }
}

const cachinglayer::PinWrapper<const FieldChunkMetrics*>
SkipIndex::GetFieldChunkMetricsV1(milvus::FieldId field_id,
                                  int chunk_id) const {
    // skip index structure must be setup before using, thus we do not lock here.
    auto field_metrics = v1_fieldChunkMetrics_.find(field_id);
    if (field_metrics != v1_fieldChunkMetrics_.end()) {
        auto& field_chunk_metrics = field_metrics->second;
        auto ca = cachinglayer::SemiInlineGet(
            field_chunk_metrics->PinCells(nullptr, {chunk_id}));
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
    for (auto chunk_id : cids) {
        auto pw = column_->GetChunk(nullptr, chunk_id);
        auto chunk_metrics =
            std::make_unique<FieldChunkMetrics>(data_type_, pw.get());
        cells.emplace_back(chunk_id, std::move(chunk_metrics));
    }
    return cells;
}

arrow::Type::type
FieldChunkMetrics::ToArrowType(DataType type) {
    switch (type) {
        case DataType::BOOL:
            return arrow::Type::BOOL;
        case DataType::INT8:
            return arrow::Type::INT8;
        case DataType::INT16:
            return arrow::Type::INT16;
        case DataType::INT32:
            return arrow::Type::INT32;
        case DataType::INT64:
            return arrow::Type::INT64;
        case DataType::FLOAT:
            return arrow::Type::FLOAT;
        case DataType::DOUBLE:
            return arrow::Type::DOUBLE;
        case DataType::VARCHAR:
            return arrow::Type::STRING;
        default:
            throw std::runtime_error("Unsupported data type for skip index");
    }
}

FieldChunkMetrics::FieldChunkMetrics(DataType data_type, const Chunk* chunk)
    : data_type_(ToArrowType(data_type)) {
    if (chunk == nullptr || chunk->RowNums() == 0) {
        return;
    }
    if (data_type == DataType::VARCHAR) {
        auto string_chunk = static_cast<const StringChunk*>(chunk);
        MetricsInfo<std::string> info = ProcessStringFieldMetrics(string_chunk);
        LoadStringMetrics(info);
        return;
    }
    auto fixed_chunk = static_cast<const FixedWidthChunk*>(chunk);
    auto span = fixed_chunk->Span();

    const void* chunk_data = span.data();
    const bool* valid_data = span.valid_data();
    int64_t count = span.row_count();
    switch (data_type) {
        case DataType::INT8: {
            const int8_t* typedData = static_cast<const int8_t*>(chunk_data);
            auto info =
                ProcessIntFieldMetrics<int8_t>(typedData, valid_data, count);
            LoadIntMetrics<int8_t>(info);
            break;
        }
        case DataType::INT16: {
            const int16_t* typedData = static_cast<const int16_t*>(chunk_data);
            auto info =
                ProcessIntFieldMetrics<int16_t>(typedData, valid_data, count);
            LoadIntMetrics<int16_t>(info);
            break;
        }
        case DataType::INT32: {
            const int32_t* typedData = static_cast<const int32_t*>(chunk_data);
            auto info =
                ProcessIntFieldMetrics<int32_t>(typedData, valid_data, count);
            LoadIntMetrics<int32_t>(info);
            break;
        }
        case DataType::INT64: {
            const int64_t* typedData = static_cast<const int64_t*>(chunk_data);
            auto info =
                ProcessIntFieldMetrics<int64_t>(typedData, valid_data, count);
            LoadIntMetrics<int64_t>(info);
            break;
        }
        case DataType::FLOAT: {
            const float* typedData = static_cast<const float*>(chunk_data);
            auto info =
                ProcessFloatFieldMetrics<float>(typedData, valid_data, count);
            LoadFloatMetrics<float>(info);
            break;
        }
        case DataType::DOUBLE: {
            const double* typedData = static_cast<const double*>(chunk_data);
            auto info =
                ProcessFloatFieldMetrics<double>(typedData, valid_data, count);
            LoadFloatMetrics<double>(info);
            break;
        }
        case DataType::BOOL: {
            const bool* typedData = static_cast<const bool*>(chunk_data);
            auto info =
                ProcessBooleanFieldMetrics(typedData, valid_data, count);
            LoadBooleanMetrics(info);
            break;
        }
    }
}

FieldChunkMetrics::FieldChunkMetrics(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    int col_idx,
    arrow::Type::type data_type) {
    switch (data_type) {
        case arrow::Type::BOOL: {
            MetricsInfo<bool> info =
                ProcessBooleanFieldMetrics(batches, col_idx);
            LoadBooleanMetrics(info);
            break;
        }
        case arrow::Type::INT8: {
            auto info = ProcessIntFieldMetrics<int8_t>(batches, col_idx);
            LoadIntMetrics<int8_t>(info);
            break;
        }
        case arrow::Type::INT16: {
            auto info = ProcessIntFieldMetrics<int16_t>(batches, col_idx);
            LoadIntMetrics<int16_t>(info);
            break;
        }
        case arrow::Type::INT32: {
            auto info = ProcessIntFieldMetrics<int32_t>(batches, col_idx);
            LoadIntMetrics<int32_t>(info);
            break;
        }
        case arrow::Type::INT64: {
            auto info = ProcessIntFieldMetrics<int64_t>(batches, col_idx);
            LoadIntMetrics<int64_t>(info);
            break;
        }
        case arrow::Type::FLOAT: {
            auto info = ProcessFloatFieldMetrics<float>(batches, col_idx);
            LoadFloatMetrics<float>(info);
            break;
        }
        case arrow::Type::DOUBLE: {
            auto info = ProcessFloatFieldMetrics<double>(batches, col_idx);
            LoadFloatMetrics<double>(info);
            break;
        }
        case arrow::Type::STRING: {
            const MetricsInfo<std::string>& info =
                ProcessStringFieldMetrics(batches, col_idx);
            LoadStringMetrics(info);
            break;
        }
    }
}
}  // namespace milvus
