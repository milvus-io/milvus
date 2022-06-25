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

#include <map>

#include "ArrowConverter.h"
#include "Consts.h"

namespace milvus {

milvus::FieldId
GetFieldId(std::shared_ptr<arrow::Field> field) {
    auto field_id_result = field->metadata()->Get(milvus::METADATA_FIELD_ID_KEY);
    AssertInfo(field_id_result.ok(), "Cannot find field id");
    return FieldId(std::stoi(field_id_result.ValueOrDie()));
}

milvus::DataType
GetDataType(std::shared_ptr<arrow::Field> field) {
    auto data_type_result = field->metadata()->Get(METADATA_FIELD_TYPE_KEY);
    AssertInfo(data_type_result.ok(), "Cannot find field type");
    return DataType(std::stoi(data_type_result.ValueOrDie()));
}

std::shared_ptr<arrow::Field>
ToArrowField(const milvus::FieldMeta& field_meta) {
    std::shared_ptr<arrow::DataType> type;
    auto field_id = field_meta.get_id();
    auto data_type_str = std::to_string(static_cast<int>(field_meta.get_data_type()));
    std::unordered_map<std::string, std::string> kv_meta = {
        {milvus::METADATA_FIELD_ID_KEY, std::to_string(field_id.get())},
        {milvus::METADATA_FIELD_TYPE_KEY, data_type_str}};
    switch (field_meta.get_data_type()) {
        case DataType::BOOL:
            type = arrow::boolean();
            break;
        case DataType::INT8:
            type = arrow::int8();
            break;
        case DataType::INT16:
            type = arrow::int16();
            break;
        case DataType::INT32:
            type = arrow::int32();
            break;
        case DataType::INT64:
            type = arrow::int64();
            break;
        case DataType::FLOAT:
            type = arrow::float32();
            break;
        case DataType::DOUBLE:
            type = arrow::float64();
            break;
        case DataType::STRING:
        case DataType::VARCHAR:
            type = arrow::utf8();
            break;
        case DataType::VECTOR_BINARY:
            type = arrow::fixed_size_binary(field_meta.get_dim() / 8);
            break;
        case DataType::VECTOR_FLOAT:
            type = arrow::fixed_size_binary(field_meta.get_dim() * 4);
            break;
        default:
            PanicInfo("Unsupported DataType(" + data_type_str + ")");
    }
    if (field_meta.is_vector()) {
        kv_meta.insert({{milvus::METADATA_DIM_KEY, std::to_string(field_meta.get_dim())}});
        if (field_meta.get_metric_type().has_value()) {
            kv_meta.insert({{milvus::METADATA_METRIC_TYPE_KEY, field_meta.get_metric_type().value()}});
        }
    }
    if (field_meta.is_string()) {
        kv_meta.insert({{milvus::METADATA_MAX_LEN_KEY, std::to_string(field_meta.get_max_len())}});
    }
    std::shared_ptr<arrow::KeyValueMetadata> metadata = arrow::key_value_metadata(kv_meta);
    return arrow::field(field_meta.get_name().get(), type, metadata);
}

std::shared_ptr<arrow::Schema>
ToArrowSchema(milvus::SchemaPtr milvus_schema) {
    auto builder = arrow::SchemaBuilder();
    std::vector<FieldMeta> pairs;
    for (const auto& iter : *milvus_schema) {
        pairs.emplace_back(iter.second);
    }
    std::sort(pairs.begin(), pairs.end(), [](FieldMeta x, FieldMeta y) { return x.get_id().get() < y.get_id().get(); });
    std::for_each(pairs.begin(), pairs.end(), [&builder](FieldMeta x) { builder.AddField(ToArrowField(x)); });
    return builder.Finish().ValueOrDie();
}

milvus::SchemaPtr
FromArrowSchema(std::shared_ptr<arrow::Schema> arrow_schema) {
    SchemaPtr schema = std::make_shared<milvus::Schema>();
    for (const auto& field : arrow_schema->fields()) {
        auto field_id = GetFieldId(field);
        auto milvus_type = GetDataType(field);
        switch (milvus_type) {
            case DataType::BOOL:
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
            case DataType::FLOAT:
            case DataType::DOUBLE:
                schema->AddField(milvus::FieldName(field->name()), field_id, milvus_type);
                break;
            case DataType::STRING:
            case DataType::VARCHAR: {
                auto max_len = std::stoll(field->metadata()->Get(milvus::METADATA_MAX_LEN_KEY).ValueOrDie());
                schema->AddField(milvus::FieldName(field->name()), field_id, milvus_type, max_len);
                break;
            }
            case DataType::VECTOR_BINARY:
            case DataType::VECTOR_FLOAT: {
                auto dim = std::stoll(field->metadata()->Get(milvus::METADATA_DIM_KEY).ValueOrDie());
                auto metric_type = field->metadata()->Get(milvus::METADATA_METRIC_TYPE_KEY).ValueOr("");
                auto maybe_type = !metric_type.empty() ? std::make_optional(static_cast<MetricType>(metric_type))
                                                       : std::optional<MetricType>();
                schema->AddField(milvus::FieldName(field->name()), field_id, milvus_type, dim, maybe_type);
                break;
            }
            default:
                PanicInfo("Unsupported DataType(" + std::to_string((int)milvus_type) + ")");
        }
    }
    return schema;
}

}  // namespace milvus
