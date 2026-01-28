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

#include "index/json_stats/utils.h"
#include <boost/filesystem.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <nlohmann/json.hpp>
#include "milvus-storage/common/constants.h"

namespace milvus::index {

std::shared_ptr<arrow::ArrayBuilder>
CreateSharedArrowBuilder() {
    return std::make_shared<arrow::BinaryBuilder>();
}

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(JSONType type) {
    std::shared_ptr<arrow::ArrayBuilder> builder;
    switch (type) {
        case JSONType::INT8:
            builder = std::make_shared<arrow::Int8Builder>();
            break;
        case JSONType::INT16:
            builder = std::make_shared<arrow::Int16Builder>();
            break;
        case JSONType::INT32:
            builder = std::make_shared<arrow::Int32Builder>();
            break;
        case JSONType::INT64:
            builder = std::make_shared<arrow::Int64Builder>();
            break;
        case JSONType::FLOAT:
            builder = std::make_shared<arrow::FloatBuilder>();
            break;
        case JSONType::DOUBLE:
            builder = std::make_shared<arrow::DoubleBuilder>();
            break;
        case JSONType::BOOL:
            builder = std::make_shared<arrow::BooleanBuilder>();
            break;
        case JSONType::STRING:
            builder = std::make_shared<arrow::StringBuilder>();
            break;
        case JSONType::ARRAY:
            // Store array as bson binary in a dedicated column
            builder = std::make_shared<arrow::BinaryBuilder>();
            break;
        default:
            ThrowInfo(ErrorCode::Unsupported,
                      "Unsupported JSON type:{} ",
                      ToString(type));
    }
    return builder;
}

std::shared_ptr<arrow::Field>
CreateSharedArrowField(const std::string& field_name, int64_t field_id) {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append(milvus_storage::ARROW_FIELD_ID_KEY,
                     std::to_string(field_id));
    metadata->Append(JSON_STATS_META_KEY_LAYOUT_TYPE_MAP,
                     ToString(JsonKeyLayoutType::SHARED));
    return arrow::field(field_name, arrow::binary(), true, metadata);
}

std::shared_ptr<arrow::Field>
CreateArrowField(const JsonKey& key,
                 const JsonKeyLayoutType& key_type,
                 int64_t field_id) {
    if (key_type == JsonKeyLayoutType::SHARED) {
        ThrowInfo(ErrorCode::Unsupported,
                  "Shared field is not supported in CreateArrowField");
    }

    std::string field_name = key.ToColumnName();
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    // metadata->Append(JSON_STATS_META_SHREDDING_COLUMN_KEY_MAP, key.key_);
    // metadata->Append(JSON_STATS_META_KEY_LAYOUT_TYPE_MAP, ToString(key_type));
    metadata->Append(milvus_storage::ARROW_FIELD_ID_KEY,
                     std::to_string(field_id));

    std::shared_ptr<arrow::Field> field;
    switch (key.type_) {
        case JSONType::INT8:
            field = arrow::field(field_name, arrow::int8(), true, metadata);
            break;
        case JSONType::INT16:
            field = arrow::field(field_name, arrow::int16(), true, metadata);
            break;
        case JSONType::INT32:
            field = arrow::field(field_name, arrow::int32(), true, metadata);
            break;
        case JSONType::INT64:
            field = arrow::field(field_name, arrow::int64(), true, metadata);
            break;
        case JSONType::DOUBLE:
            field = arrow::field(field_name, arrow::float64(), true, metadata);
            break;
        case JSONType::FLOAT:
            field = arrow::field(field_name, arrow::float32(), true, metadata);
            break;
        case JSONType::BOOL:
            field = arrow::field(field_name, arrow::boolean(), true, metadata);
            break;
        case JSONType::STRING:
            field = arrow::field(field_name, arrow::utf8(), true, metadata);
            break;
        case JSONType::ARRAY:
            // Store array payload as binary column (bson format)
            field = arrow::field(field_name, arrow::binary(), true, metadata);
            break;
        default:
            ThrowInfo(ErrorCode::Unsupported,
                      "Unsupported JSON type: {} ",
                      ToString(key.type_));
    }
    return field;
}

std::pair<std::vector<std::shared_ptr<arrow::ArrayBuilder>>,
          std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>>
CreateArrowBuilders(const std::map<JsonKey, JsonKeyLayoutType>& column_map) {
    std::shared_ptr<arrow::ArrayBuilder> shared_builder =
        CreateSharedArrowBuilder();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>> builders_map;
    for (const auto& [key, type] : column_map) {
        switch (type) {
            case JsonKeyLayoutType::SHARED:
                builders_map[key.ToColumnName()] = shared_builder;
                break;
            case JsonKeyLayoutType::TYPED:
            case JsonKeyLayoutType::TYPED_NOT_ALL:
            case JsonKeyLayoutType::DYNAMIC:
            case JsonKeyLayoutType::DYNAMIC_ONLY: {
                auto builder = CreateArrowBuilder(key.type_);
                builders.push_back(builder);
                builders_map[key.ToColumnName()] = builder;
                break;
            }
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "Unsupported JSON key type: {}",
                          ToString(type));
        }
    }
    builders.push_back(shared_builder);
    return std::make_pair(builders, builders_map);
}

std::shared_ptr<arrow::Schema>
CreateArrowSchema(const std::map<JsonKey, JsonKeyLayoutType>& column_map) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::shared_ptr<arrow::Field> shared_field = nullptr;
    bool shared_field_name_conflict = false;
    std::vector<std::string> may_conflict_shared_field_names;
    auto field_id = START_JSON_STATS_FIELD_ID;
    for (const auto& [key, type] : column_map) {
        switch (type) {
            case JsonKeyLayoutType::TYPED:
            case JsonKeyLayoutType::TYPED_NOT_ALL:
            case JsonKeyLayoutType::DYNAMIC:
            case JsonKeyLayoutType::DYNAMIC_ONLY:
                fields.push_back(CreateArrowField(key, type, field_id++));
                if (field_id > END_JSON_STATS_FIELD_ID) {
                    ThrowInfo(ErrorCode::UnexpectedError,
                              "Field ID exceeds the limit: {}, field_id: {}",
                              END_JSON_STATS_FIELD_ID,
                              field_id);
                }
                if (key.key_ == JSON_KEY_STATS_SHARED_FIELD_NAME) {
                    shared_field_name_conflict = true;
                }
                break;
            case JsonKeyLayoutType::SHARED:
                break;
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "Unsupported JSON key type: {}",
                          ToString(type));
        }
    }
    std::string field_name = JSON_KEY_STATS_SHARED_FIELD_NAME;
    if (shared_field_name_conflict) {
        boost::uuids::random_generator generator;
        auto uuid = generator();
        auto suffix = boost::uuids::to_string(uuid).substr(0, 5);
        field_name = suffix + "_" + field_name;
    }
    fields.push_back(CreateSharedArrowField(field_name, field_id++));
    return arrow::schema(fields);
}

std::vector<std::pair<std::string, std::string>>
CreateParquetKVMetadata(
    const std::map<JsonKey, JsonKeyLayoutType>& column_map) {
    // layout type map is now stored in a separate meta file to reduce parquet file size.
    // return empty metadata vector.
    return {};
}

std::string
JsonStatsMeta::Serialize() const {
    nlohmann::json root;

    // Serialize string values
    for (const auto& [key, value] : string_values_) {
        root[key] = value;
    }

    // Serialize int64 values
    for (const auto& [key, value] : int64_values_) {
        root[key] = value;
    }

    // Serialize layout type map
    if (!layout_type_map_.empty()) {
        nlohmann::json layout_map;
        for (const auto& [json_key, layout_type] : layout_type_map_) {
            layout_map[json_key.ToColumnName()] = ToString(layout_type);
        }
        root[META_KEY_LAYOUT_TYPE_MAP] = layout_map;
    }

    return root.dump();
}

JsonStatsMeta
JsonStatsMeta::Deserialize(const std::string& json_str) {
    JsonStatsMeta meta;

    try {
        nlohmann::json root = nlohmann::json::parse(json_str);

        for (auto it = root.begin(); it != root.end(); ++it) {
            const std::string& key = it.key();

            if (key == META_KEY_LAYOUT_TYPE_MAP) {
                // Parse layout type map
                std::map<JsonKey, JsonKeyLayoutType> layout_map;
                for (auto& [column_name, layout_type_str] :
                     it.value().items()) {
                    auto json_type = GetJsonTypeFromKeyName(column_name);
                    auto json_pointer = GetKeyFromColumnName(column_name);
                    JsonKey json_key(json_pointer, json_type);
                    auto layout_type =
                        JsonKeyLayoutTypeFromString(layout_type_str);
                    layout_map[json_key] = layout_type;
                }
                meta.SetLayoutTypeMap(layout_map);
            } else if (it.value().is_string()) {
                meta.SetString(key, it.value().get<std::string>());
            } else if (it.value().is_number_integer()) {
                meta.SetInt64(key, it.value().get<int64_t>());
            }
            // Other types can be added as needed
        }
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Failed to deserialize JsonStatsMeta: {}",
                  e.what());
    }

    return meta;
}

std::unordered_map<std::string, std::set<std::string>>
JsonStatsMeta::DeserializeToKeyFieldMap(const std::string& json_str) {
    std::unordered_map<std::string, std::set<std::string>> key_field_map;

    try {
        nlohmann::json root = nlohmann::json::parse(json_str);

        auto it = root.find(META_KEY_LAYOUT_TYPE_MAP);
        if (it != root.end()) {
            for (auto& [column_name, layout_type_str] : it.value().items()) {
                auto layout_type = JsonKeyLayoutTypeFromString(layout_type_str);
                if (layout_type == JsonKeyLayoutType::SHARED) {
                    continue;
                }
                auto json_pointer = GetKeyFromColumnName(column_name);
                key_field_map[json_pointer].insert(column_name);
            }
        }
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Failed to deserialize JsonStatsMeta to key_field_map: {}",
                  e.what());
    }

    return key_field_map;
}

}  // namespace milvus::index