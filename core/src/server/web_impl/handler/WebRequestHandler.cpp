// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "server/web_impl/handler/WebRequestHandler.h"

#include <algorithm>
#include <ctime>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include <fiu/fiu-local.h>

#include "config/ConfigMgr.h"
#include "config/ServerConfig.h"
#include "db/Utils.h"
#include "metrics/SystemInfo.h"
#include "query/BinaryQuery.h"
#include "server/ValidationUtil.h"
#include "server/delivery/request/BaseReq.h"
#include "server/web_impl/Constants.h"
#include "server/web_impl/Types.h"
#include "server/web_impl/dto/PartitionDto.hpp"
#include "server/web_impl/utils/Util.h"
#include "thirdparty/nlohmann/json.hpp"
#include "utils/ConfigUtils.h"
#include "utils/StringHelpFunctions.h"

namespace milvus {
namespace server {
namespace web {

StatusCode
WebErrorMap(ErrorCode code) {
    static const std::map<ErrorCode, StatusCode> code_map = {
        {SERVER_UNEXPECTED_ERROR, StatusCode::UNEXPECTED_ERROR},
        {SERVER_UNSUPPORTED_ERROR, StatusCode::UNEXPECTED_ERROR},
        {SERVER_NULL_POINTER, StatusCode::UNEXPECTED_ERROR},
        {SERVER_INVALID_ARGUMENT, StatusCode::ILLEGAL_ARGUMENT},
        {SERVER_FILE_NOT_FOUND, StatusCode::FILE_NOT_FOUND},
        {SERVER_NOT_IMPLEMENT, StatusCode::UNEXPECTED_ERROR},
        {SERVER_CANNOT_CREATE_FOLDER, StatusCode::CANNOT_CREATE_FOLDER},
        {SERVER_CANNOT_CREATE_FILE, StatusCode::CANNOT_CREATE_FILE},
        {SERVER_CANNOT_DELETE_FOLDER, StatusCode::CANNOT_DELETE_FOLDER},
        {SERVER_CANNOT_DELETE_FILE, StatusCode::CANNOT_DELETE_FILE},
        {SERVER_COLLECTION_NOT_EXIST, StatusCode::COLLECTION_NOT_EXISTS},
        {SERVER_INVALID_COLLECTION_NAME, StatusCode::ILLEGAL_COLLECTION_NAME},
        {SERVER_INVALID_COLLECTION_DIMENSION, StatusCode::ILLEGAL_DIMENSION},
        {SERVER_INVALID_VECTOR_DIMENSION, StatusCode::ILLEGAL_DIMENSION},

        {SERVER_INVALID_INDEX_TYPE, StatusCode::ILLEGAL_INDEX_TYPE},
        {SERVER_INVALID_ROWRECORD, StatusCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_ROWRECORD_ARRAY, StatusCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_TOPK, StatusCode::ILLEGAL_TOPK},
        {SERVER_INVALID_NPROBE, StatusCode::ILLEGAL_ARGUMENT},
        {SERVER_INVALID_INDEX_NLIST, StatusCode::ILLEGAL_NLIST},
        {SERVER_INVALID_INDEX_METRIC_TYPE, StatusCode::ILLEGAL_METRIC_TYPE},
        {SERVER_INVALID_SEGMENT_ROW_COUNT, StatusCode::ILLEGAL_ARGUMENT},
        {SERVER_ILLEGAL_VECTOR_ID, StatusCode::ILLEGAL_VECTOR_ID},
        {SERVER_ILLEGAL_SEARCH_RESULT, StatusCode::ILLEGAL_SEARCH_RESULT},
        {SERVER_CACHE_FULL, StatusCode::CACHE_FAILED},
        {SERVER_BUILD_INDEX_ERROR, StatusCode::BUILD_INDEX_ERROR},
        {SERVER_OUT_OF_MEMORY, StatusCode::OUT_OF_MEMORY},

        {DB_NOT_FOUND, StatusCode::COLLECTION_NOT_EXISTS},
        {DB_META_TRANSACTION_FAILED, StatusCode::META_FAILED},
    };
    if (code < StatusCode::MAX) {
        return StatusCode(code);
    } else if (code_map.find(code) != code_map.end()) {
        return code_map.at(code);
    } else {
        return StatusCode::UNEXPECTED_ERROR;
    }
}

template <typename T>
void
CopyStructuredData(const nlohmann::json& json, std::vector<uint8_t>& raw) {
    std::vector<T> values;
    auto size = json.size();
    values.resize(size);
    raw.resize(size * sizeof(T));
    size_t offset = 0;
    for (const auto& data : json) {
        values[offset] = data.get<T>();
        ++offset;
    }
    memcpy(raw.data(), values.data(), size * sizeof(T));
}

void
CopyRowVectorFromJson(const nlohmann::json& json, std::vector<uint8_t>& vectors_data, bool bin) {
    //    if (!json.is_array()) {
    //        return Status(ILLEGAL_BODY, "field \"vectors\" must be a array");
    //    }
    std::vector<float> float_vector;
    if (!bin) {
        for (auto& data : json) {
            float_vector.emplace_back(data.get<float>());
        }
        auto size = float_vector.size() * sizeof(float);
        vectors_data.resize(size);
        memcpy(vectors_data.data(), float_vector.data(), size);
    } else {
        for (auto& data : json) {
            vectors_data.emplace_back(data.get<uint8_t>());
        }
    }
}

template <typename T>
void
CopyRowStructuredData(const nlohmann::json& entity_json, const std::string& field_name, const int64_t offset,
                      const int64_t row_num, std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data) {
    T value = entity_json.get<T>();
    std::vector<uint8_t> temp_data(sizeof(T), 0);
    memcpy(temp_data.data(), &value, sizeof(T));
    if (chunk_data.find(field_name) == chunk_data.end()) {
        std::vector<uint8_t> T_data(row_num * sizeof(T), 0);
        memcpy(T_data.data(), temp_data.data(), sizeof(T));
        chunk_data.insert({field_name, T_data});
    } else {
        int64_t T_offset = offset * sizeof(T);
        memcpy(chunk_data.at(field_name).data() + T_offset, temp_data.data(), sizeof(T));
    }
}

using FloatJson = nlohmann::basic_json<std::map, std::vector, std::string, bool, std::int64_t, std::uint64_t, float>;

/////////////////////////////////// Private methods ///////////////////////////////////////
void
WebRequestHandler::AddStatusToJson(nlohmann::json& json, int64_t code, const std::string& msg) {
    json["code"] = code;
    json["message"] = msg;
}

Status
WebRequestHandler::IsBinaryCollection(const std::string& collection_name, bool& bin) {
    CollectionSchema schema;
    auto status = req_handler_.GetCollectionInfo(context_ptr_, collection_name, schema);
    if (status.ok()) {
        std::string metric_type = schema.extra_params_[engine::PARAM_INDEX_METRIC_TYPE];
        bin = engine::utils::IsBinaryMetricType(metric_type);
    }

    return status;
}

Status
WebRequestHandler::CopyRecordsFromJson(const nlohmann::json& json, std::vector<uint8_t>& vectors_data, bool bin) {
    if (!json.is_array()) {
        return Status(ILLEGAL_BODY, "field \"vectors\" must be a array");
    }

    std::vector<float> float_vector;
    if (!bin) {
        for (auto& vec : json) {
            if (!vec.is_array()) {
                return Status(ILLEGAL_BODY, "A vector in field \"vectors\" must be a float array");
            }
            for (auto& data : vec) {
                float_vector.emplace_back(data.get<float>());
            }
        }
        auto size = float_vector.size() * sizeof(float);
        vectors_data.resize(size);
        memcpy(vectors_data.data(), float_vector.data(), size);
    } else {
        for (auto& vec : json) {
            if (!vec.is_array()) {
                return Status(ILLEGAL_BODY, "A vector in field \"vectors\" must be a float array");
            }
            for (auto& data : vec) {
                vectors_data.emplace_back(data.get<uint8_t>());
            }
        }
    }

    return Status::OK();
}

Status
WebRequestHandler::CopyData2Json(const milvus::engine::DataChunkPtr& data_chunk,
                                 const milvus::engine::snapshot::FieldElementMappings& field_mappings,
                                 const std::vector<int64_t>& id_array, nlohmann::json& json_res) {
    int64_t id_size = id_array.size();
    for (int i = 0; i < id_size; i++) {
        nlohmann::json one_json;
        nlohmann::json entity_json;
        for (const auto& it : field_mappings) {
            auto type = it.first->GetFtype();
            std::string name = it.first->GetName();

            engine::BinaryDataPtr data = data_chunk->fixed_fields_[name];
            if (data == nullptr || data->data_.empty()) {
                continue;
            }

            auto single_size = data->data_.size() / id_size;

            switch (static_cast<engine::DataType>(type)) {
                case engine::DataType::INT32: {
                    int32_t int32_value;
                    int64_t offset = sizeof(int32_t) * i;
                    memcpy(&int32_value, data->data_.data() + offset, sizeof(int32_t));
                    entity_json[name] = int32_value;
                    break;
                }
                case engine::DataType::INT64: {
                    int64_t int64_value;
                    int64_t offset = sizeof(int64_t) * i;
                    memcpy(&int64_value, data->data_.data() + offset, sizeof(int64_t));
                    entity_json[name] = int64_value;
                    break;
                }
                case engine::DataType::FLOAT: {
                    float float_value;
                    int64_t offset = sizeof(float) * i;
                    memcpy(&float_value, data->data_.data() + offset, sizeof(float));
                    entity_json[name] = float_value;
                    break;
                }
                case engine::DataType::DOUBLE: {
                    double double_value;
                    int64_t offset = sizeof(double) * i;
                    memcpy(&double_value, data->data_.data() + offset, sizeof(double));
                    entity_json[name] = double_value;
                    break;
                }
                case engine::DataType::VECTOR_BINARY: {
                    std::vector<int8_t> binary_vector;
                    auto vector_size = single_size * sizeof(int8_t) / sizeof(int8_t);
                    binary_vector.resize(vector_size);
                    int64_t offset = vector_size * i;
                    memcpy(binary_vector.data(), data->data_.data() + offset, vector_size);
                    entity_json[name] = binary_vector;
                    break;
                }
                case engine::DataType::VECTOR_FLOAT: {
                    std::vector<float> float_vector;
                    auto vector_size = single_size * sizeof(int8_t) / sizeof(float);
                    float_vector.resize(vector_size);
                    int64_t offset = vector_size * i;
                    memcpy(float_vector.data(), data->data_.data() + offset, vector_size * sizeof(float));
                    entity_json[name] = float_vector;
                    break;
                }
                default:
                    break;
            }
        }
        one_json["entity"] = entity_json;
        one_json["id"] = std::to_string(id_array[i]);
        json_res["entities"].push_back(one_json);
    }
    return Status::OK();
}

///////////////////////// WebRequestHandler methods ///////////////////////////////////////
Status
WebRequestHandler::GetCollectionMetaInfo(const std::string& collection_name, nlohmann::json& json_out) {
    CollectionSchema schema;
    STATUS_CHECK(req_handler_.GetCollectionInfo(context_ptr_, collection_name, schema));

    int64_t count;
    STATUS_CHECK(req_handler_.CountEntities(context_ptr_, collection_name, count));

    json_out["collection_name"] = schema.collection_name_;
    for (const auto& field : schema.fields_) {
        if (field.first == engine::FIELD_UID) {
            continue;
        }
        nlohmann::json field_json;
        field_json["field_name"] = field.first;
        field_json["field_type"] = type2str.at(field.second.field_type_);
        field_json["index_params"] = field.second.index_params_;
        field_json["extra_params"] = field.second.field_params_;
        json_out["fields"].push_back(field_json);
    }
    if (schema.extra_params_.contains(engine::PARAM_SEGMENT_ROW_COUNT)) {
        json_out[engine::PARAM_SEGMENT_ROW_COUNT] = schema.extra_params_[engine::PARAM_SEGMENT_ROW_COUNT];
    }
    if (schema.extra_params_.contains(engine::PARAM_UID_AUTOGEN)) {
        json_out[engine::PARAM_UID_AUTOGEN] = schema.extra_params_[engine::PARAM_UID_AUTOGEN];
    }
    json_out["count"] = count;
    return Status::OK();
}

Status
WebRequestHandler::GetCollectionStat(const std::string& collection_name, nlohmann::json& json_out) {
    std::string collection_stats;
    auto status = req_handler_.GetCollectionStats(context_ptr_, collection_name, collection_stats);

    if (status.ok()) {
        try {
            json_out = nlohmann::json::parse(collection_stats);
        } catch (std::exception& e) {
            return Status(SERVER_UNEXPECTED_ERROR,
                          "Error occurred when parsing collection stat information: " + std::string(e.what()));
        }
    }

    return status;
}

Status
WebRequestHandler::GetPageEntities(const std::string& collection_name, const std::string& partition_tag,
                                   const int64_t page_size, const int64_t offset, nlohmann::json& json_out) {
    std::string collection_info;
    STATUS_CHECK(req_handler_.GetCollectionStats(context_ptr_, collection_name, collection_info));
    nlohmann::json json_info = nlohmann::json::parse(collection_info);

    if (!json_info.contains("partitions")) {
        return Status(SERVER_UNEXPECTED_ERROR, "Collection info does not include partitions");
    }
    if (!json_info["partitions"].is_array()) {
        return Status(SERVER_UNEXPECTED_ERROR, "Collection info partition json is not an array");
    }
    int64_t entity_num = offset;
    std::vector<int64_t> segment_ids;
    int64_t row_count = 0;
    bool already_find = false;
    for (auto& json_partition : json_info["partitions"]) {
        if (!partition_tag.empty() && json_partition["tag"] != partition_tag) {
            continue;
        }
        for (auto& json_segment : json_partition["segments"]) {
            auto count = json_segment["row_count"].get<int64_t>();
            row_count += count;
            if (entity_num < count) {
                already_find = true;
            }
            if (not already_find && entity_num >= count) {
                entity_num -= count;
            }
            if (already_find) {
                segment_ids.emplace_back(json_segment["id"].get<int64_t>());
            }
        }
    }
    json_out["count"] = row_count;
    int64_t real_offset = entity_num;
    int64_t real_page_size = page_size;

    engine::IDNumbers entity_ids;
    for (const auto seg_id : segment_ids) {
        engine::IDNumbers temp_ids;
        STATUS_CHECK(req_handler_.ListIDInSegment(context_ptr_, collection_name, seg_id, temp_ids));
        auto ids_begin = real_offset;
        auto ids_end = std::min(temp_ids.size(), static_cast<size_t>(real_offset + real_page_size));
        auto new_ids = std::vector<int64_t>(temp_ids.begin() + ids_begin, temp_ids.begin() + ids_end);
        auto cur_size = entity_ids.size();
        auto new_size = new_ids.size();
        entity_ids.resize(cur_size + new_size);
        memcpy(entity_ids.data() + cur_size, new_ids.data(), new_size * sizeof(int64_t));

        real_page_size -= (ids_end - ids_begin);
        real_offset = 0;
    }
    if (segment_ids.empty()) {
        json_out["entities"] = json::array();
        return Status::OK();
    }
    std::vector<std::string> field_names;
    STATUS_CHECK(GetEntityByIDs(collection_name, entity_ids, field_names, json_out));
    return Status::OK();
}

Status
WebRequestHandler::GetSegmentVectors(const std::string& collection_name, int64_t segment_id, int64_t page_size,
                                     int64_t offset, nlohmann::json& json_out) {
    engine::IDNumbers vector_ids;
    STATUS_CHECK(req_handler_.ListIDInSegment(context_ptr_, nullptr, segment_id, vector_ids));

    auto ids_begin = std::min(vector_ids.size(), static_cast<size_t>(offset));
    auto ids_end = std::min(vector_ids.size(), static_cast<size_t>(offset + page_size));

    auto new_ids = std::vector<int64_t>(vector_ids.begin() + ids_begin, vector_ids.begin() + ids_end);
    nlohmann::json vectors_json;
    //    auto status = GetVectorsByIDs(collection_name, new_ids, vectors_json);

    nlohmann::json result_json;
    if (vectors_json.empty()) {
        json_out["vectors"] = std::vector<int64_t>();
    } else {
        json_out["vectors"] = vectors_json;
    }
    json_out["count"] = vector_ids.size();

    //    AddStatusToJson(json_out, status.code(), status.message());

    return Status::OK();
}

Status
WebRequestHandler::GetSegmentIds(const std::string& collection_name, int64_t segment_id, int64_t page_size,
                                 int64_t offset, nlohmann::json& json_out) {
    std::vector<int64_t> ids;
    auto status = req_handler_.ListIDInSegment(context_ptr_, collection_name, segment_id, ids);
    if (status.ok()) {
        auto ids_begin = std::min(ids.size(), static_cast<size_t>(offset));
        auto ids_end = std::min(ids.size(), static_cast<size_t>(offset + page_size));

        if (ids_begin >= ids_end) {
            json_out["ids"] = std::vector<int64_t>();
        } else {
            for (size_t i = ids_begin; i < ids_end; i++) {
                json_out["ids"].push_back(std::to_string(ids.at(i)));
            }
        }
        json_out["count"] = ids.size();
    }

    return status;
}

Status
WebRequestHandler::CommandLine(const std::string& cmd, std::string& reply) {
    return req_handler_.Cmd(context_ptr_, cmd, reply);
}

Status
WebRequestHandler::Cmd(const std::string& cmd, std::string& result_str) {
    std::string reply;
    auto status = CommandLine(cmd, reply);

    if (status.ok()) {
        nlohmann::json result;
        AddStatusToJson(result, status.code(), status.message());
        result["reply"] = reply;
        result_str = result.dump();
    }

    return status;
}

Status
WebRequestHandler::PreLoadCollection(const nlohmann::json& json, std::string& result_str) {
    if (!json.contains("collection_name")) {
        return Status(BODY_FIELD_LOSS, "Field \"load\" must contains collection_name");
    }

    auto collection_name = json["collection_name"];
    auto status = req_handler_.LoadCollection(context_ptr_, collection_name.get<std::string>());
    if (status.ok()) {
        nlohmann::json result;
        AddStatusToJson(result, status.code(), status.message());
        result_str = result.dump();
    }

    return status;
}

Status
WebRequestHandler::Flush(const nlohmann::json& json, std::string& result_str) {
    if (!json.contains("collection_names")) {
        return Status(BODY_FIELD_LOSS, "Field \"flush\" must contains collection_names");
    }

    auto collection_names = json["collection_names"];
    if (!collection_names.is_array()) {
        return Status(BODY_FIELD_LOSS, "Field \"collection_names\" must be and array");
    }

    std::vector<std::string> names;
    for (auto& name : collection_names) {
        names.emplace_back(name.get<std::string>());
    }

    auto status = req_handler_.Flush(context_ptr_, names);
    if (status.ok()) {
        nlohmann::json result;
        AddStatusToJson(result, status.code(), status.message());
        result_str = result.dump();
    }

    return status;
}

Status
WebRequestHandler::Compact(const nlohmann::json& json, std::string& result_str) {
    if (!json.contains("collection_name")) {
        return Status(BODY_FIELD_LOSS, "Field \"compact\" must contains collection_names");
    }

    auto collection_name = json["collection_name"];
    if (!collection_name.is_string()) {
        return Status(BODY_FIELD_LOSS, "Field \"collection_names\" must be a string");
    }

    auto name = collection_name.get<std::string>();

    double compact_threshold =
        json["threshold"].get<double>();  // compact trigger threshold: delete_counts/segment_counts
    auto status = req_handler_.Compact(context_ptr_, name, compact_threshold);

    if (status.ok()) {
        nlohmann::json result;
        AddStatusToJson(result, status.code(), status.message());
        result_str = result.dump();
    }

    return status;
}

Status
WebRequestHandler::GetConfig(std::string& result_str) {
    std::string cmd = "get_milvus_config";
    std::string reply;
    auto status = CommandLine(cmd, reply);
    if (status.ok()) {
        nlohmann::json j = nlohmann::json::parse(reply);
#ifdef MILVUS_GPU_VERSION
        if (j.contains("gpu_resource_config")) {
            std::vector<std::string> gpus;
            if (j["gpu_resource_config"].contains("search_resources")) {
                auto gpu_search_res = j["gpu_resource_config"]["search_resources"].get<std::string>();
                StringHelpFunctions::SplitStringByDelimeter(gpu_search_res, ",", gpus);
                j["gpu_resource_config"]["search_resources"] = gpus;
            }
            if (j["gpu_resource_config"].contains("build_index_resources")) {
                auto gpu_build_res = j["gpu_resource_config"]["build_index_resources"].get<std::string>();
                gpus.clear();
                StringHelpFunctions::SplitStringByDelimeter(gpu_build_res, ",", gpus);
                j["gpu_resource_config"]["build_index_resources"] = gpus;
            }
        }
#endif
        result_str = j.dump();
    }

    return Status::OK();
}

Status
WebRequestHandler::SetConfig(const nlohmann::json& json, std::string& result_str) {
    if (!json.is_object()) {
        return Status(ILLEGAL_BODY, "Payload must be a map");
    }

    std::vector<std::string> cmds;
    for (auto& el : json.items()) {
        auto ekey = el.key();
        auto evalue = el.value();

        std::ostringstream ss;
        if (evalue.is_string()) {
            std::string vle = evalue;
            ss << "set " << el.key() << " " << vle;
        } else {
            ss << "set " << el.key() << " " << evalue;
        }
        cmds.emplace_back(ss.str());
    }

    std::string msg;

    for (auto& c : cmds) {
        std::string reply;
        auto status = CommandLine(c, reply);
        if (!status.ok()) {
            return status;
        }
        msg += c + " successfully;";
    }

    nlohmann::json result;
    AddStatusToJson(result, StatusCode::SUCCESS, msg);
    result_str = result.dump();

    return Status::OK();
}

Status
WebRequestHandler::ProcessLeafQueryJson(const nlohmann::json& json, milvus::query::BooleanQueryPtr& query,
                                        std::string& field_name, query::QueryPtr& query_ptr) {
    auto status = Status::OK();
    if (json.contains("term")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto term_query = std::make_shared<query::TermQuery>();
        nlohmann::json json_obj = json["term"];
        JSON_NULL_CHECK(json_obj);
        JSON_OBJECT_CHECK(json_obj);
        term_query->json_obj = json_obj;
        nlohmann::json::iterator json_it = json_obj.begin();
        field_name = json_it.key();

        leaf_query->term_query = term_query;
        query->AddLeafQuery(leaf_query);
    } else if (json.contains("range")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto range_query = std::make_shared<query::RangeQuery>();
        nlohmann::json json_obj = json["range"];
        JSON_NULL_CHECK(json_obj);
        JSON_OBJECT_CHECK(json_obj);
        range_query->json_obj = json_obj;
        nlohmann::json::iterator json_it = json_obj.begin();
        field_name = json_it.key();

        leaf_query->range_query = range_query;
        query->AddLeafQuery(leaf_query);
    } else if (json.contains("vector")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto vector_json = json["vector"];
        JSON_NULL_CHECK(vector_json);

        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> dist(0, 64);
        int64_t place_number = dist(rng);
        std::string placeholder = "placeholder" + std::to_string(place_number);
        leaf_query->vector_placeholder = placeholder;
        query->AddLeafQuery(leaf_query);

        auto vector_query = std::make_shared<query::VectorQuery>();
        json::iterator vector_param_it = vector_json.begin();
        if (vector_param_it != vector_json.end()) {
            const std::string& vector_name = vector_param_it.key();
            vector_query->field_name = vector_name;
            nlohmann::json param_json = vector_param_it.value();
            int64_t topk = param_json["topk"];
            status = server::ValidateSearchTopk(topk);
            if (!status.ok()) {
                return status;
            }
            vector_query->topk = topk;
            if (param_json.contains("metric_type")) {
                std::string metric_type = param_json["metric_type"];
                vector_query->metric_type = metric_type;
                query_ptr->metric_types.insert({vector_name, metric_type});
            }
            if (!vector_param_it.value()["params"].empty()) {
                vector_query->extra_params = vector_param_it.value()["params"];
            }

            auto& values = vector_param_it.value()["values"];
            vector_query->query_vector.vector_count = values.size();
            for (auto& vector_records : values) {
                if (field_type_.find(vector_name) != field_type_.end()) {
                    if (field_type_.at(vector_name) == engine::DataType::VECTOR_FLOAT) {
                        for (auto& data : vector_records) {
                            vector_query->query_vector.float_data.emplace_back(data.get<float>());
                        }
                    } else if (field_type_.at(vector_name) == engine::DataType::VECTOR_BINARY) {
                        for (auto& data : vector_records) {
                            vector_query->query_vector.binary_data.emplace_back(data.get<int8_t>());
                        }
                    }
                }
            }
            query_ptr->index_fields.insert(vector_name);
        }

        query_ptr->vectors.insert(std::make_pair(placeholder, vector_query));

    } else {
        return Status{SERVER_INVALID_ARGUMENT, "Leaf query get wrong key"};
    }
    return status;
}

Status
WebRequestHandler::ProcessBooleanQueryJson(const nlohmann::json& query_json, query::BooleanQueryPtr& boolean_query,
                                           query::QueryPtr& query_ptr) {
    auto status = Status::OK();
    if (query_json.empty()) {
        return Status{SERVER_INVALID_ARGUMENT, "BoolQuery is null"};
    }
    for (auto& el : query_json.items()) {
        if (el.key() == "must") {
            boolean_query->SetOccur(query::Occur::MUST);
            auto must_json = el.value();
            if (!must_json.is_array()) {
                std::string msg = "Must json string is not an array";
                return Status{SERVER_INVALID_DSL_PARAMETER, msg};
            }

            for (auto& json : must_json) {
                auto must_query = std::make_shared<query::BooleanQuery>();
                if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                    STATUS_CHECK(ProcessBooleanQueryJson(json, must_query, query_ptr));
                    boolean_query->AddBooleanQuery(must_query);
                } else {
                    std::string field_name;
                    STATUS_CHECK(ProcessLeafQueryJson(json, boolean_query, field_name, query_ptr));
                    if (!field_name.empty()) {
                        query_ptr->index_fields.insert(field_name);
                    }
                }
            }
        } else if (el.key() == "should") {
            boolean_query->SetOccur(query::Occur::SHOULD);
            auto should_json = el.value();
            if (!should_json.is_array()) {
                std::string msg = "Should json string is not an array";
                return Status{SERVER_INVALID_DSL_PARAMETER, msg};
            }

            for (auto& json : should_json) {
                auto should_query = std::make_shared<query::BooleanQuery>();
                if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                    STATUS_CHECK(ProcessBooleanQueryJson(json, should_query, query_ptr));
                    boolean_query->AddBooleanQuery(should_query);
                } else {
                    std::string field_name;
                    STATUS_CHECK(ProcessLeafQueryJson(json, boolean_query, field_name, query_ptr));
                    if (!field_name.empty()) {
                        query_ptr->index_fields.insert(field_name);
                    }
                }
            }
        } else if (el.key() == "must_not") {
            boolean_query->SetOccur(query::Occur::MUST_NOT);
            auto should_json = el.value();
            if (!should_json.is_array()) {
                std::string msg = "Must_not json string is not an array";
                return Status{SERVER_INVALID_DSL_PARAMETER, msg};
            }

            for (auto& json : should_json) {
                if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                    auto must_not_query = std::make_shared<query::BooleanQuery>();
                    STATUS_CHECK(ProcessBooleanQueryJson(json, must_not_query, query_ptr));
                    boolean_query->AddBooleanQuery(must_not_query);
                } else {
                    std::string field_name;
                    STATUS_CHECK(ProcessLeafQueryJson(json, boolean_query, field_name, query_ptr));
                    if (!field_name.empty()) {
                        query_ptr->index_fields.insert(field_name);
                    }
                }
            }
        } else {
            std::string msg = "BoolQuery json string does not include bool query";
            return Status{SERVER_INVALID_DSL_PARAMETER, msg};
        }
    }

    return status;
}

Status
WebRequestHandler::Search(const std::string& collection_name, const nlohmann::json& json, std::string& result_str) {
    Status status;

    milvus::server::CollectionSchema collection_schema;
    status = req_handler_.GetCollectionInfo(context_ptr_, collection_name, collection_schema);
    if (!status.ok()) {
        return Status{UNEXPECTED_ERROR, "DescribeHybridCollection failed"};
    }
    for (const auto& field : collection_schema.fields_) {
        field_type_.insert({field.first, field.second.field_type_});
    }

    milvus::json extra_params;
    if (json.contains("fields")) {
        if (json["fields"].is_array()) {
            extra_params["fields"] = json["fields"];
        }
    }
    auto query_json = json["query"];

    std::vector<std::string> partition_tags;
    if (query_json.contains("partition_tags")) {
        auto tags = query_json["partition_tags"];
        if (!tags.is_null() && !tags.is_array()) {
            return Status(BODY_PARSE_FAIL, "Field \"partition_tags\" must be a array");
        }

        for (auto& tag : tags) {
            partition_tags.emplace_back(tag.get<std::string>());
        }
    }

    if (query_json.contains("bool")) {
        auto boolean_query_json = query_json["bool"];
        auto boolean_query = std::make_shared<query::BooleanQuery>();
        query_ptr_ = std::make_shared<query::Query>();
        query_ptr_->collection_id = collection_name;

        status = ProcessBooleanQueryJson(boolean_query_json, boolean_query, query_ptr_);
        if (!status.ok()) {
            return status;
        }
        auto general_query = std::make_shared<query::GeneralQuery>();
        query::GenBinaryQuery(boolean_query, general_query->bin);

        query_ptr_->root = general_query;

        engine::QueryResultPtr result = std::make_shared<engine::QueryResult>();
        engine::snapshot::FieldElementMappings field_mappings;
        status = req_handler_.Search(context_ptr_, query_ptr_, extra_params, field_mappings, result);

        if (!status.ok()) {
            return status;
        }

        nlohmann::json result_json;
        result_json["num"] = result->row_num_;
        if (result->row_num_ == 0) {
            result_json["result"] = std::vector<int64_t>();
            result_str = result_json.dump();
            return Status::OK();
        }

        auto step = result->result_ids_.size() / result->row_num_;  // topk
        for (int64_t i = 0; i < result->row_num_; i++) {
            nlohmann::json raw_result_json;
            for (size_t j = 0; j < step; j++) {
                nlohmann::json one_result_json;
                one_result_json["id"] = std::to_string(result->result_ids_.at(i * step + j));
                one_result_json["distance"] = std::to_string(result->result_distances_.at(i * step + j));
                FloatJson one_entity_json;
                for (const auto& field : field_mappings) {
                    auto field_name = field.first->GetName();
                    auto field_data = result->data_chunk_->fixed_fields_;
                    switch (field.first->GetFtype()) {
                        case engine::DataType::INT32: {
                            int32_t int32_value;
                            int64_t offset = (i * step + j) * sizeof(int32_t);
                            memcpy(&int32_value, field_data.at(field_name)->data_.data() + offset, sizeof(int32_t));
                            one_entity_json[field_name] = int32_value;
                            break;
                        }
                        case engine::DataType::INT64: {
                            int64_t int64_value;
                            int64_t offset = (i * step + j) * sizeof(int64_t);
                            memcpy(&int64_value, field_data.at(field_name)->data_.data() + offset, sizeof(int64_t));
                            one_entity_json[field_name] = int64_value;
                            break;
                        }
                        case engine::DataType::FLOAT: {
                            float float_value;
                            int64_t offset = (i * step + j) * sizeof(float);
                            memcpy(&float_value, field_data.at(field_name)->data_.data() + offset, sizeof(float));
                            one_entity_json[field_name] = float_value;
                            break;
                        }
                        case engine::DataType::DOUBLE: {
                            double double_value;
                            int64_t offset = (i * step + j) * sizeof(double);
                            memcpy(&double_value, field_data.at(field_name)->data_.data() + offset, sizeof(double));
                            one_entity_json[field_name] = double_value;
                            break;
                        }
                        case engine::DataType::VECTOR_FLOAT: {
                            std::vector<float> float_vector;
                            auto dim =
                                field_data.at(field_name)->data_.size() / (result->result_ids_.size() * sizeof(float));
                            int64_t offset = (i * step + j) * dim * sizeof(float);
                            float_vector.resize(dim);
                            memcpy(float_vector.data(), field_data.at(field_name)->data_.data() + offset,
                                   dim * sizeof(float));
                            one_entity_json[field_name] = float_vector;
                            break;
                        }
                        case engine::DataType::VECTOR_BINARY: {
                            std::vector<int8_t> binary_vector;
                            auto dim = field_data.at(field_name)->data_.size() / (result->result_ids_.size());
                            int64_t offset = (i * step + j) * dim;
                            binary_vector.resize(dim);
                            memcpy(binary_vector.data(), field_data.at(field_name)->data_.data() + offset,
                                   dim * sizeof(int8_t));
                            one_entity_json[field_name] = binary_vector;
                            break;
                        }
                        default: { return Status(SERVER_UNEXPECTED_ERROR, "Return field data type is wrong"); }
                    }
                }
                if (!one_entity_json.empty()) {
                    one_result_json["entity"] = one_entity_json;
                }
                raw_result_json.push_back(one_result_json);
            }
            result_json["result"].push_back(raw_result_json);
        }
        result_str = result_json.dump();
    }

    return Status::OK();
}

StatusDtoT
WebRequestHandler::DeleteByIDs(const OString& collection_name, const OString& payload, OString& response) {
    auto json = nlohmann::json::parse(payload->std_str());
    std::vector<int64_t> entity_ids;
    if (!json.contains("ids")) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, R"(Field "delete" must contains "ids")");
    }
    auto ids = json["ids"];
    if (!ids.is_array()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, R"("ids" must be an array)");
    }

    for (auto& id : ids) {
        auto id_str = id.get<std::string>();
        if (!ValidateStringIsNumber(id_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_BODY, R"(Members in "ids" must be integer string)")
        }
        entity_ids.emplace_back(std::stol(id_str));
    }

    auto status = req_handler_.DeleteEntityByID(context_ptr_, collection_name->std_str(), entity_ids);

    nlohmann::json result_json;
    AddStatusToJson(result_json, status.code(), status.message());
    std::string result_str = result_json.dump();

    response = status.ok() ? result_str.c_str() : "NULL";

    ASSIGN_RETURN_STATUS_DTO(status);
}

Status
WebRequestHandler::GetEntityByIDs(const std::string& collection_name, const std::vector<int64_t>& ids,
                                  std::vector<std::string>& field_names, nlohmann::json& json_out) {
    std::vector<bool> valid_row;
    engine::DataChunkPtr data_chunk;
    engine::snapshot::FieldElementMappings field_mappings;

    if (ids.empty()) {
        json_out["entities"] = {};
        return Status::OK();
    }

    auto status = req_handler_.GetEntityByID(context_ptr_, collection_name, ids, field_names, valid_row, field_mappings,
                                             data_chunk);
    if (!status.ok()) {
        return status;
    }

    int64_t valid_size = 0;
    for (auto row : valid_row) {
        if (row) {
            valid_size++;
        }
    }

    CopyData2Json(data_chunk, field_mappings, ids, json_out);
    return Status::OK();
}

////////////////////////////////// Router methods ////////////////////////////////////////////
StatusDtoT
WebRequestHandler::GetDevices(DevicesDtoT& devices_dto) {
    auto& system_info = SystemInfo::GetInstance();

    devices_dto->cpu = devices_dto->cpu->createShared();
    devices_dto->cpu->memory = system_info.GetPhysicalMemory();

    devices_dto->gpus = devices_dto->gpus.createShared();

#ifdef MILVUS_GPU_VERSION
    size_t count = system_info.num_device();
    std::vector<int64_t> device_mems = system_info.GPUMemoryTotal();

    if (count != device_mems.size()) {
        RETURN_STATUS_DTO(UNEXPECTED_ERROR, "Can't obtain GPU info");
    }

    for (size_t i = 0; i < count; i++) {
        auto device_dto = DeviceInfoDto::createShared();
        device_dto->memory = device_mems.at(i);
        devices_dto->gpus->emplace_back("GPU" + OString(std::to_string(i).c_str()), device_dto);
    }
#endif

    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}

StatusDtoT
WebRequestHandler::GetAdvancedConfig(AdvancedConfigDtoT& advanced_config) {
    //    std::string reply;
    //    std::string cache_cmd_prefix = "get_config " + std::string(CONFIG_CACHE) + ".";
    //
    //    std::string cache_cmd_string = cache_cmd_prefix + std::string(CONFIG_CACHE_CPU_CACHE_CAPACITY);
    //    auto status = CommandLine(cache_cmd_string, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status)
    //    }
    //    advanced_config->cpu_cache_capacity = std::stol(reply);
    //
    //    cache_cmd_string = cache_cmd_prefix + std::string(CONFIG_CACHE_CACHE_INSERT_DATA);
    //    CommandLine(cache_cmd_string, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status)
    //    }
    //    advanced_config->cache_insert_data = ("1" == reply || "true" == reply);
    //
    //    auto engine_cmd_prefix = "get_config " + std::string(CONFIG_ENGINE) + ".";
    //    auto engine_cmd_string = engine_cmd_prefix + std::string(CONFIG_ENGINE_USE_BLAS_THRESHOLD);
    //    CommandLine(engine_cmd_string, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status)
    //    }
    //    advanced_config->use_blas_threshold = std::stol(reply);
    //
    //#ifdef MILVUS_GPU_VERSION
    //    engine_cmd_string = engine_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_GPU_SEARCH_THRESHOLD);
    //    CommandLine(engine_cmd_string, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status)
    //    }
    //    advanced_config->gpu_search_threshold = std::stol(reply);
    //#endif
    //
    //    ASSIGN_RETURN_STATUS_DTO(status)
    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}

StatusDtoT
WebRequestHandler::SetAdvancedConfig(const AdvancedConfigDtoT& advanced_config) {
    //    if (nullptr == advanced_config->cpu_cache_capacity.get()) {
    //        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'cpu_cache_capacity\' miss.");
    //    }
    //
    //    if (nullptr == advanced_config->cache_insert_data.get()) {
    //        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'cache_insert_data\' miss.");
    //    }
    //
    //    if (nullptr == advanced_config->use_blas_threshold.get()) {
    //        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'use_blas_threshold\' miss.");
    //    }
    //
    //#ifdef MILVUS_GPU_VERSION
    //    if (nullptr == advanced_config->gpu_search_threshold.get()) {
    //        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'gpu_search_threshold\' miss.");
    //    }
    //#endif
    //
    //    std::string reply;
    //    std::string cache_cmd_prefix = "set_config " + std::string(CONFIG_CACHE) + ".";
    //
    //    std::string cache_cmd_string = cache_cmd_prefix + std::string(CONFIG_CACHE_CPU_CACHE_CAPACITY) + " " +
    //                                   std::to_string(advanced_config->cpu_cache_capacity->getValue());
    //    auto status = CommandLine(cache_cmd_string, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status)
    //    }
    //
    //    cache_cmd_string = cache_cmd_prefix + std::string(CONFIG_CACHE_CACHE_INSERT_DATA) + " " +
    //                       std::to_string(advanced_config->cache_insert_data->getValue());
    //    status = CommandLine(cache_cmd_string, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status)
    //    }
    //
    //    auto engine_cmd_prefix = "set_config " + std::string(CONFIG_ENGINE) + ".";
    //
    //    auto engine_cmd_string = engine_cmd_prefix + std::string(CONFIG_ENGINE_USE_BLAS_THRESHOLD) + " " +
    //                             std::to_string(advanced_config->use_blas_threshold->getValue());
    //    status = CommandLine(engine_cmd_string, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status)
    //    }
    //
    //#ifdef MILVUS_GPU_VERSION
    //    auto gpu_cmd_prefix = "set_config " + std::string(CONFIG_GPU_RESOURCE) + ".";
    //    auto gpu_cmd_string = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_GPU_SEARCH_THRESHOLD) + " " +
    //                          std::to_string(advanced_config->gpu_search_threshold->getValue());
    //    status = CommandLine(gpu_cmd_string, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status)
    //    }
    //#endif
    //
    //    ASSIGN_RETURN_STATUS_DTO(status)
    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}

#ifdef MILVUS_GPU_VERSION

StatusDtoT
WebRequestHandler::GetGpuConfig(GPUConfigDtoT& gpu_config_dto) {
    //    std::string reply;
    //    std::string gpu_cmd_prefix = "get_config " + std::string(CONFIG_GPU_RESOURCE) + ".";
    //
    //    std::string gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_ENABLE);
    //    auto status = CommandLine(gpu_cmd_request, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status);
    //    }
    //    gpu_config_dto->enable = reply == "1" || reply == "true";
    //
    //    if (!gpu_config_dto->enable->getValue()) {
    //        ASSIGN_RETURN_STATUS_DTO(Status::OK());
    //    }
    //
    //    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_CACHE_CAPACITY);
    //    status = CommandLine(gpu_cmd_request, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status);
    //    }
    //    gpu_config_dto->cache_capacity = std::stol(reply);
    //
    //    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_SEARCH_RESOURCES);
    //    status = CommandLine(gpu_cmd_request, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status);
    //    }
    //
    //    std::vector<std::string> gpu_entry;
    //    StringHelpFunctions::SplitStringByDelimeter(reply, ",", gpu_entry);
    //
    //    gpu_config_dto->search_resources = gpu_config_dto->search_resources->createShared();
    //    for (auto& device_id : gpu_entry) {
    //        gpu_config_dto->search_resources->pushBack(OString(device_id.c_str())->toUpperCase());
    //    }
    //    gpu_entry.clear();
    //
    //    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES);
    //    status = CommandLine(gpu_cmd_request, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status);
    //    }
    //
    //    StringHelpFunctions::SplitStringByDelimeter(reply, ",", gpu_entry);
    //    gpu_config_dto->build_index_resources = gpu_config_dto->build_index_resources->createShared();
    //    for (auto& device_id : gpu_entry) {
    //        gpu_config_dto->build_index_resources->pushBack(OString(device_id.c_str())->toUpperCase());
    //    }
    //
    //    ASSIGN_RETURN_STATUS_DTO(Status::OK());
    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}

StatusDtoT
WebRequestHandler::SetGpuConfig(const GPUConfigDtoT& gpu_config_dto) {
    //    // Step 1: Check config param
    //    if (nullptr == gpu_config_dto->enable.get()) {
    //        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'enable\' miss")
    //    }
    //
    //    if (nullptr == gpu_config_dto->cache_capacity.get()) {
    //        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'cache_capacity\' miss")
    //    }
    //
    //    if (nullptr == gpu_config_dto->search_resources.get()) {
    //        gpu_config_dto->search_resources = gpu_config_dto->search_resources->createShared();
    //        gpu_config_dto->search_resources->pushBack("GPU0");
    //    }
    //
    //    if (nullptr == gpu_config_dto->build_index_resources.get()) {
    //        gpu_config_dto->build_index_resources = gpu_config_dto->build_index_resources->createShared();
    //        gpu_config_dto->build_index_resources->pushBack("GPU0");
    //    }
    //
    //    // Step 2: Set config
    //    std::string reply;
    //    std::string gpu_cmd_prefix = "set_config " + std::string(CONFIG_GPU_RESOURCE) + ".";
    //    std::string gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_ENABLE) + " " +
    //                                  std::to_string(gpu_config_dto->enable->getValue());
    //    auto status = CommandLine(gpu_cmd_request, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status);
    //    }
    //
    //    if (!gpu_config_dto->enable->getValue()) {
    //        RETURN_STATUS_DTO(SUCCESS, "Set Gpu resources to false");
    //    }
    //
    //    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_CACHE_CAPACITY) + " " +
    //                      std::to_string(gpu_config_dto->cache_capacity->getValue());
    //    status = CommandLine(gpu_cmd_request, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status);
    //    }
    //
    //    std::vector<std::string> search_resources;
    //    gpu_config_dto->search_resources->forEach(
    //        [&search_resources](const OString& res) { search_resources.emplace_back(res->toLowerCase()->std_str());
    //        });
    //
    //    std::string search_resources_value;
    //    for (auto& res : search_resources) {
    //        search_resources_value += res + ",";
    //    }
    //    auto len = search_resources_value.size();
    //    if (len > 0) {
    //        search_resources_value.erase(len - 1);
    //    }
    //
    //    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_SEARCH_RESOURCES) + " " +
    //    search_resources_value; status = CommandLine(gpu_cmd_request, reply); if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status);
    //    }
    //
    //    std::vector<std::string> build_resources;
    //    gpu_config_dto->build_index_resources->forEach(
    //        [&build_resources](const OString& res) { build_resources.emplace_back(res->toLowerCase()->std_str()); });
    //
    //    std::string build_resources_value;
    //    for (auto& res : build_resources) {
    //        build_resources_value += res + ",";
    //    }
    //    len = build_resources_value.size();
    //    if (len > 0) {
    //        build_resources_value.erase(len - 1);
    //    }
    //
    //    gpu_cmd_request =
    //        gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES) + " " + build_resources_value;
    //    status = CommandLine(gpu_cmd_request, reply);
    //    if (!status.ok()) {
    //        ASSIGN_RETURN_STATUS_DTO(status);
    //    }
    //
    //    ASSIGN_RETURN_STATUS_DTO(Status::OK());
    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}

#endif

/*************
 *
 * Collection {
 */
StatusDtoT
WebRequestHandler::CreateCollection(const milvus::server::web::OString& body) {
    auto json_str = nlohmann::json::parse(body->c_str());
    std::string collection_name = json_str["collection_name"];

    // TODO(yukun): do checking
    std::unordered_map<std::string, FieldSchema> fields;
    for (auto& field : json_str["fields"]) {
        FieldSchema field_schema;
        std::string field_name = field["field_name"];

        if (fields.find(field_name) != fields.end()) {
            auto status = Status(SERVER_INVALID_FIELD_NAME, "Collection mapping has duplicate field names");
            ASSIGN_RETURN_STATUS_DTO(status)
        }

        field_schema.field_params_ = field["extra_params"];

        std::string field_type = field["field_type"];
        std::transform(field_type.begin(), field_type.end(), field_type.begin(), ::tolower);

        if (str2type.find(field_type) == str2type.end()) {
            std::string msg = field_name + " has wrong field_type";
            RETURN_STATUS_DTO(BODY_PARSE_FAIL, msg.c_str());
        }
        field_schema.field_type_ = str2type.at(field_type);

        fields[field_name] = field_schema;
    }

    milvus::json json_params;
    if (json_str.contains(engine::PARAM_SEGMENT_ROW_COUNT)) {
        json_params[engine::PARAM_SEGMENT_ROW_COUNT] = json_str[engine::PARAM_SEGMENT_ROW_COUNT];
    }
    if (json_str.contains(engine::PARAM_UID_AUTOGEN)) {
        json_params[engine::PARAM_UID_AUTOGEN] = json_str[engine::PARAM_UID_AUTOGEN];
    }

    auto status = req_handler_.CreateCollection(context_ptr_, collection_name, fields, json_params);

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDtoT
WebRequestHandler::ShowCollections(const OQueryParams& query_params, OString& result) {
    int64_t offset = 0;
    auto status = ParseQueryInteger(query_params, "offset", offset);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    int64_t page_size = 10;
    status = ParseQueryInteger(query_params, "page_size", page_size);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    if (offset < 0 || page_size < 0) {
        RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, "Query param 'offset' or 'page_size' should equal or bigger than 0");
    }

    bool all_required = false;
    ParseQueryBool(query_params, "all_required", all_required);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    std::vector<std::string> collections;
    status = req_handler_.ListCollections(context_ptr_, collections);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    if (all_required) {
        offset = 0;
        page_size = collections.size();
    } else {
        offset = std::min(static_cast<size_t>(offset), collections.size());
        page_size = std::min(collections.size() - offset, static_cast<size_t>(page_size));
    }

    nlohmann::json collections_json;
    for (int64_t i = offset; i < page_size + offset; i++) {
        nlohmann::json collection_json;
        status = GetCollectionMetaInfo(collections.at(i), collection_json);
        if (!status.ok()) {
            ASSIGN_RETURN_STATUS_DTO(status)
        }
        collections_json.push_back(collection_json);
    }

    nlohmann::json result_json;
    result_json["count"] = collections.size();
    if (collections_json.empty()) {
        result_json["collections"] = std::vector<int64_t>();
    } else {
        result_json["collections"] = collections_json;
    }

    AddStatusToJson(result_json, status.code(), status.message());
    result = result_json.dump().c_str();
    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDtoT
WebRequestHandler::GetCollection(const OString& collection_name, const OQueryParams& query_params, OString& result) {
    if (nullptr == collection_name.get()) {
        RETURN_STATUS_DTO(PATH_PARAM_LOSS, "Path param \'collection_name\' is required!");
    }

    std::string stat;
    auto status = ParseQueryStr(query_params, "info", stat);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    if (!stat.empty() && stat == "stat") {
        nlohmann::json json;
        status = GetCollectionStat(collection_name->std_str(), json);
        result = status.ok() ? json.dump().c_str() : "NULL";
    } else {
        nlohmann::json json;
        status = GetCollectionMetaInfo(collection_name->std_str(), json);
        result = status.ok() ? json.dump().c_str() : "NULL";
    }

    ASSIGN_RETURN_STATUS_DTO(status);
}

StatusDtoT
WebRequestHandler::DropCollection(const OString& collection_name) {
    auto status = req_handler_.DropCollection(context_ptr_, collection_name->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

/***********
 *
 * Index {
 */

StatusDtoT
WebRequestHandler::CreateIndex(const OString& collection_name, const OString& field_name, const OString& body) {
    try {
        auto request_json = nlohmann::json::parse(body->std_str());
        if (!request_json.contains("index_type")) {
            RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'index_type\' is required");
        }

        auto status =
            req_handler_.CreateIndex(context_ptr_, collection_name->std_str(), field_name->std_str(), "", request_json);
        ASSIGN_RETURN_STATUS_DTO(status);
    } catch (nlohmann::detail::parse_error& e) {
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, e.what())
    } catch (nlohmann::detail::type_error& e) {
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, e.what())
    }

    ASSIGN_RETURN_STATUS_DTO(Status::OK())
}

StatusDtoT
WebRequestHandler::DropIndex(const OString& collection_name, const OString& field_name) {
    auto status = req_handler_.DropIndex(context_ptr_, collection_name->std_str(), field_name->std_str(), "");
    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDtoT
WebRequestHandler::CreatePartition(const OString& collection_name, const PartitionRequestDtoT& param) {
    if (nullptr == param->partition_tag.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'partition_tag\' is required")
    }

    auto status =
        req_handler_.CreatePartition(context_ptr_, collection_name->std_str(), param->partition_tag->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDtoT
WebRequestHandler::ShowPartitions(const OString& collection_name, const OQueryParams& query_params,
                                  PartitionListDtoT& partition_list_dto) {
    int64_t offset = 0;
    auto status = ParseQueryInteger(query_params, "offset", offset);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    int64_t page_size = 10;
    status = ParseQueryInteger(query_params, "page_size", page_size);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    if (offset < 0 || page_size < 0) {
        ASSIGN_RETURN_STATUS_DTO(
            Status(SERVER_UNEXPECTED_ERROR, "Query param 'offset' or 'page_size' should equal or bigger than 0"));
    }

    bool all_required = false;
    auto required = query_params.get("all_required");
    if (nullptr != required.get()) {
        auto required_str = required->std_str();
        if (!ValidateStringIsBool(required_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, "Query param \'all_required\' must be a bool")
        }
        all_required = required_str == "True" || required_str == "true";
    }

    std::vector<std::string> partition_names;
    status = req_handler_.ListPartitions(context_ptr_, collection_name->std_str(), partition_names);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    if (all_required) {
        offset = 0;
        page_size = partition_names.size();
    } else {
        offset = std::min(static_cast<size_t>(offset), partition_names.size());
        page_size = std::min(partition_names.size() - offset, static_cast<size_t>(page_size));
    }

    partition_list_dto->count = partition_names.size();
    partition_list_dto->partitions = partition_list_dto->partitions.createShared();

    if (offset < static_cast<int64_t>(partition_names.size())) {
        for (int64_t i = offset; i < page_size + offset; i++) {
            auto partition_dto = PartitionFieldsDto::createShared();
            partition_dto->partition_tag = partition_names.at(i).c_str();
            partition_list_dto->partitions->push_back(partition_dto);
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDtoT
WebRequestHandler::DropPartition(const OString& collection_name, const OString& body) {
    std::string tag;
    try {
        auto json = nlohmann::json::parse(body->std_str());
        tag = json["partition_tag"].get<std::string>();
    } catch (nlohmann::detail::parse_error& e) {
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, e.what())
    } catch (nlohmann::detail::type_error& e) {
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, e.what())
    }
    auto status = req_handler_.DropPartition(context_ptr_, collection_name->std_str(), tag);

    ASSIGN_RETURN_STATUS_DTO(status)
}

/***********
 *
 * Segment {
 */
StatusDtoT
WebRequestHandler::ShowSegments(const OString& collection_name, const OQueryParams& query_params, OString& response) {
    int64_t offset = 0;
    auto status = ParseQueryInteger(query_params, "offset", offset);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    int64_t page_size = 10;
    status = ParseQueryInteger(query_params, "page_size", page_size);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    if (offset < 0 || page_size < 0) {
        RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, "Query param 'offset' or 'page_size' should equal or bigger than 0");
    }

    bool all_required = false;
    auto required = query_params.get("all_required");
    if (nullptr != required.get()) {
        auto required_str = required->std_str();
        if (!ValidateStringIsBool(required_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, "Query param \'all_required\' must be a bool")
        }
        all_required = required_str == "True" || required_str == "true";
    }

    std::string tag;
    if (nullptr != query_params.get("partition_tag").get()) {
        tag = query_params.get("partition_tag")->std_str();
    }

    std::string stats;
    status = req_handler_.GetCollectionStats(context_ptr_, collection_name->std_str(), stats);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    nlohmann::json info_json = nlohmann::json::parse(stats);
    nlohmann::json segments_json = nlohmann::json::array();
    for (auto& par : info_json["partitions"]) {
        if (!(all_required || tag.empty() || tag == par["tag"])) {
            continue;
        }

        auto segments = par["segments"];
        if (!segments.is_null()) {
            for (auto& seg : segments) {
                seg["partition_tag"] = par["tag"];
                segments_json.push_back(seg);
            }
        }
    }
    nlohmann::json result_json;
    if (!all_required) {
        int64_t size = segments_json.size();
        int iter_begin = std::min(size, offset);
        int iter_end = std::min(size, offset + page_size);

        nlohmann::json segments_slice_json = nlohmann::json::array();
        segments_slice_json.insert(segments_slice_json.begin(), segments_json.begin() + iter_begin,
                                   segments_json.begin() + iter_end);
        result_json["segments"] = segments_slice_json;  // segments_json;
    } else {
        result_json["segments"] = segments_json;
    }
    result_json["count"] = segments_json.size();
    AddStatusToJson(result_json, status.code(), status.message());
    response = result_json.dump().c_str();

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDtoT
WebRequestHandler::GetSegmentInfo(const OString& collection_name, const OString& segment_name, const OString& info,
                                  const OQueryParams& query_params, OString& result) {
    int64_t offset = 0;
    auto status = ParseQueryInteger(query_params, "offset", offset);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    int64_t page_size = 10;
    status = ParseQueryInteger(query_params, "page_size", page_size);
    if (!status.ok()) {
        RETURN_STATUS_DTO(status.code(), status.message().c_str());
    }

    if (offset < 0 || page_size < 0) {
        ASSIGN_RETURN_STATUS_DTO(
            Status(SERVER_UNEXPECTED_ERROR, "Query param 'offset' or 'page_size' should equal or bigger than 0"));
    }

    std::string id_str = segment_name->std_str();
    int64_t segment_id = atol(id_str.c_str());
    std::string re = info->std_str();
    status = Status::OK();
    nlohmann::json json;
    // Get vectors
    if (re == "vectors") {
        status = GetSegmentVectors(collection_name->std_str(), segment_id, page_size, offset, json);
        // Get vector ids
    } else if (re == "ids") {
        status = GetSegmentIds(collection_name->std_str(), segment_id, page_size, offset, json);
    }

    result = status.ok() ? json.dump().c_str() : "NULL";

    ASSIGN_RETURN_STATUS_DTO(status)
}

/**
 *
 * Vector
 */
StatusDtoT
WebRequestHandler::InsertEntity(const OString& collection_name, const milvus::server::web::OString& body,
                                EntityIdsDtoT& ids_dto) {
    if (nullptr == body.get() || body->getSize() == 0) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Request payload is required.")
    }

    auto body_json = nlohmann::json::parse(body->c_str());
    std::string partition_name;
    if (body_json.contains("partition_tag")) {
        partition_name = body_json["partition_tag"];
    }

    CollectionSchema collection_schema;
    std::unordered_map<std::string, engine::DataType> field_types;
    auto status = req_handler_.GetCollectionInfo(context_ptr_, collection_name->std_str(), collection_schema);
    if (!status.ok()) {
        auto msg = "Collection " + collection_name->std_str() + " not exist";
        RETURN_STATUS_DTO(COLLECTION_NOT_EXISTS, msg.c_str());
    }
    for (const auto& field : collection_schema.fields_) {
        field_types.insert({field.first, field.second.field_type_});
    }

    std::unordered_map<std::string, std::vector<uint8_t>> chunk_data;
    int64_t row_num;

    auto entities_json = body_json["entities"];
    if (!entities_json.is_array()) {
        RETURN_STATUS_DTO(ILLEGAL_ARGUMENT, "Entities is not an array");
    }
    row_num = entities_json.size();
    int64_t offset = 0;
    std::vector<uint8_t> ids;
    for (auto& one_entity : entities_json) {
        for (auto& entity : one_entity.items()) {
            std::string field_name = entity.key();
            if (field_name == NAME_ID) {
                if (ids.empty()) {
                    ids.resize(row_num * sizeof(int64_t));
                }
                auto id = entity.value().get<int64_t>();
                int64_t id_offset = offset * sizeof(int64_t);
                memcpy(ids.data() + id_offset, &id, sizeof(int64_t));
                continue;
            }
            std::vector<uint8_t> temp_data;
            switch (field_types.at(field_name)) {
                case engine::DataType::INT32: {
                    CopyRowStructuredData<int32_t>(entity.value(), field_name, offset, row_num, chunk_data);
                    break;
                }
                case engine::DataType::INT64: {
                    CopyRowStructuredData<int64_t>(entity.value(), field_name, offset, row_num, chunk_data);
                    break;
                }
                case engine::DataType::FLOAT: {
                    CopyRowStructuredData<float>(entity.value(), field_name, offset, row_num, chunk_data);
                    break;
                }
                case engine::DataType::DOUBLE: {
                    CopyRowStructuredData<double>(entity.value(), field_name, offset, row_num, chunk_data);
                    break;
                }
                case engine::DataType::VECTOR_FLOAT:
                case engine::DataType::VECTOR_BINARY: {
                    bool is_bin = !(field_types.at(field_name) == engine::DataType::VECTOR_FLOAT);
                    CopyRowVectorFromJson(entity.value(), temp_data, is_bin);
                    auto size = temp_data.size();
                    if (chunk_data.find(field_name) == chunk_data.end()) {
                        std::vector<uint8_t> vector_data(row_num * size, 0);
                        memcpy(vector_data.data(), temp_data.data(), size);
                        chunk_data.insert({field_name, vector_data});
                    } else {
                        int64_t vector_offset = offset * size;
                        memcpy(chunk_data.at(field_name).data() + vector_offset, temp_data.data(), size);
                    }
                    break;
                }
                default: {}
            }
        }
        offset++;
    }

    if (!ids.empty()) {
        chunk_data.insert({engine::FIELD_UID, ids});
    }

#if 0
    for (auto& entity : body_json["entities"].items()) {
        std::string field_name = entity.key();
        auto field_value = entity.value();
        if (!field_value.is_array()) {
            RETURN_STATUS_DTO(ILLEGAL_ROWRECORD, "Field value is not an array");
        }
        if (field_name == NAME_ID) {
            std::vector<uint8_t> temp_data(field_value.size() * sizeof(int64_t), 0);
            CopyStructuredData<int64_t>(field_value, temp_data);
            chunk_data.insert({engine::FIELD_UID, temp_data});
            continue;
        }
        row_num = field_value.size();

        std::vector<uint8_t> temp_data;
        switch (field_types.at(field_name)) {
            case engine::DataType::INT32: {
                CopyStructuredData<int32_t>(field_value, temp_data);
                break;
            }
            case engine::DataType::INT64: {
                CopyStructuredData<int64_t>(field_value, temp_data);
                break;
            }
            case engine::DataType::FLOAT: {
                CopyStructuredData<float>(field_value, temp_data);
                break;
            }
            case engine::DataType::DOUBLE: {
                CopyStructuredData<double>(field_value, temp_data);
                break;
            }
            case engine::DataType::VECTOR_FLOAT: {
                CopyRecordsFromJson(field_value, temp_data, false);
                break;
            }
            case engine::DataType::VECTOR_BINARY: {
                CopyRecordsFromJson(field_value, temp_data, true);
                break;
            }
            default: {}
        }

        chunk_data.insert(std::make_pair(field_name, temp_data));
    }
#endif

    status = req_handler_.Insert(context_ptr_, collection_name->c_str(), partition_name, row_num, chunk_data);
    if (!status.ok()) {
        RETURN_STATUS_DTO(UNEXPECTED_ERROR, "Failed to insert data");
    }

    // return generated ids
    auto pair = chunk_data.find(engine::FIELD_UID);
    if (pair != chunk_data.end()) {
        int64_t count = pair->second.size() / 8;
        auto pdata = reinterpret_cast<int64_t*>(pair->second.data());
        ids_dto->ids = ids_dto->ids.createShared();
        for (int64_t i = 0; i < count; ++i) {
            ids_dto->ids->push_back(std::to_string(pdata[i]).c_str());
        }
    }
    ids_dto->code = status.code();
    ids_dto->message = status.message().c_str();

    ASSIGN_RETURN_STATUS_DTO(status)
}

Status
WebRequestHandler::GetEntity(const milvus::server::web::OString& collection_name,
                             const milvus::server::web::OQueryParams& query_params,
                             milvus::server::web::OString& response) {
    auto status = Status::OK();
    try {
        if (query_params.get("offset") && query_params.get("page_size")) {
            nlohmann::json json_out;
            auto offset = std::stoi(query_params.get("offset")->std_str(), nullptr);
            auto page_size = std::stoi(query_params.get("page_size")->std_str(), nullptr);
            std::string partition_tag;
            if (query_params.get("partition_tag")) {
                partition_tag = query_params.get("partition_tag")->std_str();
            }
            status = GetPageEntities(collection_name->std_str(), partition_tag, page_size, offset, json_out);
            if (!status.ok()) {
                json_out["entities"] = json::array();
            }
            AddStatusToJson(json_out, status.code(), status.message());
            response = json_out.dump().c_str();
            return status;
        }

        auto query_ids = query_params.get("ids");
        if (query_ids == nullptr || query_ids.get() == nullptr) {
            return Status(QUERY_PARAM_LOSS, "Query param ids is required.");
        }

        std::vector<std::string> ids;
        StringHelpFunctions::SplitStringByDelimeter(query_ids->c_str(), ",", ids);
        std::vector<int64_t> entity_ids;

        entity_ids.reserve(ids.size());
        for (auto& id : ids) {
            entity_ids.push_back(std::stol(id));
        }

        std::vector<std::string> field_names;
        auto query_fields = query_params.get("fields");
        if (query_fields != nullptr && query_fields.get() != nullptr) {
            StringHelpFunctions::SplitStringByDelimeter(query_fields->c_str(), ",", field_names);
        }

        std::vector<bool> valid_row;
        nlohmann::json entity_result_json;
        status = GetEntityByIDs(collection_name->std_str(), entity_ids, field_names, entity_result_json);
        if (!status.ok()) {
            response = "NULL";
            return status;
        }

        nlohmann::json json;
        AddStatusToJson(json, status.code(), status.message());
        if (entity_result_json.empty()) {
            json = std::vector<int64_t>();
        } else {
            json = entity_result_json;
        }
        response = json.dump().c_str();
    } catch (std::exception& e) {
        return Status(SERVER_UNEXPECTED_ERROR, e.what());
    }

    return status;
}

StatusDtoT
WebRequestHandler::EntityOp(const OString& collection_name, const OQueryParams& query_params, const OString& payload,
                            OString& response) {
    auto status = Status::OK();
    std::string result_str;

    try {
        nlohmann::json payload_json;
        if (!payload->std_str().empty()) {
            payload_json = nlohmann::json::parse(payload->std_str());
        }
        if (query_params.get("offset") || query_params.get("page_size") || query_params.get("ids")) {
            status = GetEntity(collection_name, query_params, response);
            ASSIGN_RETURN_STATUS_DTO(status);
        } else if (!payload_json.empty()) {
            if (payload_json.contains("query")) {
                status = Search(collection_name->c_str(), payload_json, result_str);
            } else {
                status = Status(ILLEGAL_BODY, "Unknown payload");
            }
        } else {
            OQueryParams self_params;
            self_params.put("offset", "0");
            self_params.put("page_size", "10");
            status = GetEntity(collection_name, self_params, response);
            ASSIGN_RETURN_STATUS_DTO(status)
        }
    } catch (nlohmann::detail::parse_error& e) {
        std::string emsg = "json error: code=" + std::to_string(e.id) + ", reason=" + e.what();
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, emsg.c_str());
    } catch (nlohmann::detail::type_error& e) {
        std::string emsg = "json error: code=" + std::to_string(e.id) + ", reason=" + e.what();
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, emsg.c_str());
    } catch (std::exception& e) {
        RETURN_STATUS_DTO(SERVER_UNEXPECTED_ERROR, e.what());
    }

    response = status.ok() ? result_str.c_str() : "NULL";

    ASSIGN_RETURN_STATUS_DTO(status)
}

/**********
 *
 * System {
 */
StatusDtoT
WebRequestHandler::SystemInfo(const OString& cmd, const OQueryParams& query_params, OString& response_str) {
    std::string info = cmd->std_str();

    auto status = Status::OK();
    std::string result_str;

    try {
        if (info == "config") {
            status = GetConfig(result_str);
        } else {
            if ("info" == info) {
                info = "get_system_info";
            }
            status = Cmd(info, result_str);
        }
    } catch (nlohmann::detail::parse_error& e) {
        std::string emsg = "json error: code=" + std::to_string(e.id) + ", reason=" + e.what();
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, emsg.c_str());
    } catch (nlohmann::detail::type_error& e) {
        std::string emsg = "json error: code=" + std::to_string(e.id) + ", reason=" + e.what();
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, emsg.c_str());
    }

    response_str = status.ok() ? result_str.c_str() : "NULL";

    ASSIGN_RETURN_STATUS_DTO(status);
}

StatusDtoT
WebRequestHandler::SystemOp(const OString& op, const OString& body_str, OString& response_str) {
    if (nullptr == body_str.get() || body_str->getSize() == 0) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Payload is empty.");
    }

    Status status = Status::OK();
    std::string result_str;
    try {
        fiu_do_on("WebRequestHandler.SystemOp.raise_parse_error",
                  throw nlohmann::detail::parse_error::create(0, 0, ""));
        fiu_do_on("WebRequestHandler.SystemOp.raise_type_error", throw nlohmann::detail::type_error::create(0, ""));
        nlohmann::json j = nlohmann::json::parse(body_str->c_str());
        if (op->equals("task")) {
            if (j.contains("load")) {
                status = PreLoadCollection(j["load"], result_str);
            } else if (j.contains("flush")) {
                status = Flush(j["flush"], result_str);
            }
            if (j.contains("compact")) {
                status = Compact(j["compact"], result_str);
            }
        } else if (op->equals("config")) {
            status = SetConfig(j, result_str);
        } else {
            status = Status(UNKNOWN_PATH, "Unknown path: /system/" + op->std_str());
        }
    } catch (nlohmann::detail::parse_error& e) {
        std::string emsg = "json error: code=" + std::to_string(e.id) + ", reason=" + e.what();
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, emsg.c_str());
    } catch (nlohmann::detail::type_error& e) {
        std::string emsg = "json error: code=" + std::to_string(e.id) + ", reason=" + e.what();
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, emsg.c_str());
    }

    response_str = status.ok() ? result_str.c_str() : "NULL";

    ASSIGN_RETURN_STATUS_DTO(status);
}

StatusDtoT
WebRequestHandler::ServerStatus(OString& response_str) {
    std::string result_str;
    auto status = CommandLine("status", result_str);
    response_str = status.ok() ? result_str.c_str() : "NULL";
    ASSIGN_RETURN_STATUS_DTO(status);
}

}  // namespace web
}  // namespace server
}  // namespace milvus
