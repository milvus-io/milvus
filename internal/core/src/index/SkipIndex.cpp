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
}  // namespace milvus
