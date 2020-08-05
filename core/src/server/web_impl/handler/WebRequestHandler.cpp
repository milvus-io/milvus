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
#include <string>
#include <unordered_map>
#include <vector>

#include <fiu-local.h>

#include "config/ServerConfig.h"
#include "db/Utils.h"
#include "metrics/SystemInfo.h"
#include "query/BinaryQuery.h"
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
    for (auto data : json) {
        values[offset] = data.get<T>();
        ++offset;
    }
    memcpy(raw.data(), values.data(), size * sizeof(T));
}

using FloatJson = nlohmann::basic_json<std::map, std::vector, std::string, bool, std::int64_t, std::uint64_t, float>;

/////////////////////////////////// Private methods ///////////////////////////////////////
void
WebRequestHandler::AddStatusToJson(nlohmann::json& json, int64_t code, const std::string& msg) {
    json["code"] = (int64_t)code;
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
WebRequestHandler::CopyRecordsFromJson(const nlohmann::json& json, engine::VectorsData& vectors, bool bin) {
    if (!json.is_array()) {
        return Status(ILLEGAL_BODY, "field \"vectors\" must be a array");
    }

    vectors.vector_count_ = json.size();

    if (!bin) {
        for (auto& vec : json) {
            if (!vec.is_array()) {
                return Status(ILLEGAL_BODY, "A vector in field \"vectors\" must be a float array");
            }
            for (auto& data : vec) {
                vectors.float_data_.emplace_back(data.get<float>());
            }
        }
    } else {
        for (auto& vec : json) {
            if (!vec.is_array()) {
                return Status(ILLEGAL_BODY, "A vector in field \"vectors\" must be a float array");
            }
            for (auto& data : vec) {
                vectors.binary_data_.emplace_back(data.get<uint8_t>());
            }
        }
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
    json_out["dimension"] = schema.extra_params_[engine::PARAM_DIMENSION].get<int64_t>();
    json_out["segment_row_count"] = schema.extra_params_[engine::PARAM_SEGMENT_ROW_COUNT].get<int64_t>();
    json_out["metric_type"] = schema.extra_params_[engine::PARAM_INDEX_METRIC_TYPE].get<int64_t>();
    json_out["index_params"] = schema.extra_params_[engine::PARAM_INDEX_EXTRA_PARAMS].get<std::string>();
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
WebRequestHandler::GetSegmentVectors(const std::string& collection_name, int64_t segment_id, int64_t page_size,
                                     int64_t offset, nlohmann::json& json_out) {
    engine::IDNumbers vector_ids;
    STATUS_CHECK(req_handler_.ListIDInSegment(context_ptr_, 0, segment_id, vector_ids));

    auto ids_begin = std::min(vector_ids.size(), (size_t)offset);
    auto ids_end = std::min(vector_ids.size(), (size_t)(offset + page_size));

    auto new_ids = std::vector<int64_t>(vector_ids.begin() + ids_begin, vector_ids.begin() + ids_end);
    nlohmann::json vectors_json;
    auto status = GetVectorsByIDs(collection_name, new_ids, vectors_json);

    nlohmann::json result_json;
    if (vectors_json.empty()) {
        json_out["vectors"] = std::vector<int64_t>();
    } else {
        json_out["vectors"] = vectors_json;
    }
    json_out["count"] = vector_ids.size();

    AddStatusToJson(json_out, status.code(), status.message());

    return Status::OK();
}

Status
WebRequestHandler::GetSegmentIds(const std::string& collection_name, int64_t segment_id, int64_t page_size,
                                 int64_t offset, nlohmann::json& json_out) {
    std::vector<int64_t> ids;
    auto status = req_handler_.ListIDInSegment(context_ptr_, collection_name, segment_id, ids);
    if (status.ok()) {
        auto ids_begin = std::min(ids.size(), (size_t)offset);
        auto ids_end = std::min(ids.size(), (size_t)(offset + page_size));

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

    double compact_threshold = 0.1;  // compact trigger threshold: delete_counts/segment_counts
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
    std::string cmd = "get_config *";
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
        // check if server require start
        bool required = false;
        // TODO: Use new cofnig mgr
        // Config::GetInstance().GetServerRestartRequired(required);
        j["restart_required"] = required;
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
        auto evalue = el.value();
        if (!evalue.is_object()) {
            return Status(ILLEGAL_BODY, "Invalid payload format, the root value must be json map");
        }

        for (auto& iel : el.value().items()) {
            auto ievalue = iel.value();
            if (!(ievalue.is_string() || ievalue.is_number() || ievalue.is_boolean())) {
                return Status(ILLEGAL_BODY, "Config value must be one of string, numeric or boolean");
            }
            std::ostringstream ss;
            if (ievalue.is_string()) {
                std::string vle = ievalue;
                ss << "set_config " << el.key() << "." << iel.key() << " " << vle;
            } else {
                ss << "set_config " << el.key() << "." << iel.key() << " " << ievalue;
            }
            cmds.emplace_back(ss.str());
        }
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

    bool required = false;
    // Config::GetInstance().GetServerRestartRequired(required);
    result["restart_required"] = required;

    result_str = result.dump();

    return Status::OK();
}

Status
WebRequestHandler::ProcessLeafQueryJson(const nlohmann::json& json, milvus::query::BooleanQueryPtr& query) {
    if (json.contains("term")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto term_json = json["term"];
        std::string field_name = term_json["field_name"];
        auto term_value_json = term_json["values"];
        if (!term_value_json.is_array()) {
            std::string msg = "Term json string is not an array";
            return Status{BODY_PARSE_FAIL, msg};
        }

        //        auto term_size = term_value_json.size();
        //        auto term_query = std::make_shared<query::TermQuery>();
        //        term_query->field_name = field_name;
        //        term_query->field_value.resize(term_size * sizeof(int64_t));
        //
        //        switch (field_type_.at(field_name)) {
        //            case engine::DataType::INT8:
        //            case engine::DataType::INT16:
        //            case engine::DataType::INT32:
        //            case engine::DataType::INT64: {
        //                std::vector<int64_t> term_value(term_size, 0);
        //                for (uint64_t i = 0; i < term_size; ++i) {
        //                    term_value[i] = term_value_json[i].get<int64_t>();
        //                }
        //                memcpy(term_query->field_value.data(), term_value.data(), term_size * sizeof(int64_t));
        //                break;
        //            }
        //            case engine::DataType::FLOAT:
        //            case engine::DataType::DOUBLE: {
        //                std::vector<double> term_value(term_size, 0);
        //                for (uint64_t i = 0; i < term_size; ++i) {
        //                    term_value[i] = term_value_json[i].get<double>();
        //                }
        //                memcpy(term_query->field_value.data(), term_value.data(), term_size * sizeof(double));
        //                break;
        //            }
        //            default:
        //                break;
        //        }
        //
        //        leaf_query->term_query = term_query;
        //        query->AddLeafQuery(leaf_query);
        //    } else if (json.contains("range")) {
        //        auto leaf_query = std::make_shared<query::LeafQuery>();
        //        auto range_query = std::make_shared<query::RangeQuery>();
        //
        //        auto range_json = json["range"];
        //        std::string field_name = range_json["field_name"];
        //        range_query->field_name = field_name;
        //
        //        auto range_value_json = range_json["values"];
        //        if (range_value_json.contains("lt")) {
        //            query::CompareExpr compare_expr;
        //            compare_expr.compare_operator = query::CompareOperator::LT;
        //            compare_expr.operand = range_value_json["lt"].get<std::string>();
        //            range_query->compare_expr.emplace_back(compare_expr);
        //        }
        //        if (range_value_json.contains("lte")) {
        //            query::CompareExpr compare_expr;
        //            compare_expr.compare_operator = query::CompareOperator::LTE;
        //            compare_expr.operand = range_value_json["lte"].get<std::string>();
        //            range_query->compare_expr.emplace_back(compare_expr);
        //        }
        //        if (range_value_json.contains("eq")) {
        //            query::CompareExpr compare_expr;
        //            compare_expr.compare_operator = query::CompareOperator::EQ;
        //            compare_expr.operand = range_value_json["eq"].get<std::string>();
        //            range_query->compare_expr.emplace_back(compare_expr);
        //        }
        //        if (range_value_json.contains("ne")) {
        //            query::CompareExpr compare_expr;
        //            compare_expr.compare_operator = query::CompareOperator::NE;
        //            compare_expr.operand = range_value_json["ne"].get<std::string>();
        //            range_query->compare_expr.emplace_back(compare_expr);
        //        }
        //        if (range_value_json.contains("gt")) {
        //            query::CompareExpr compare_expr;
        //            compare_expr.compare_operator = query::CompareOperator::GT;
        //            compare_expr.operand = range_value_json["gt"].get<std::string>();
        //            range_query->compare_expr.emplace_back(compare_expr);
        //        }
        //        if (range_value_json.contains("gte")) {
        //            query::CompareExpr compare_expr;
        //            compare_expr.compare_operator = query::CompareOperator::GTE;
        //            compare_expr.operand = range_value_json["gte"].get<std::string>();
        //            range_query->compare_expr.emplace_back(compare_expr);
        //        }
        //
        //        leaf_query->range_query = range_query;
        //        query->AddLeafQuery(leaf_query);
        //    } else if (json.contains("vector")) {
        //        auto leaf_query = std::make_shared<query::LeafQuery>();
        //        auto vector_query = std::make_shared<query::VectorQuery>();
        //
        //        auto vector_json = json["vector"];
        //        std::string field_name = vector_json["field_name"];
        //        vector_query->field_name = field_name;
        //
        //        engine::VectorsData vectors;
        //        // TODO(yukun): process binary vector
        //        CopyRecordsFromJson(vector_json["values"], vectors, false);
        //
        //        vector_query->query_vector.float_data = vectors.float_data_;
        //        vector_query->query_vector.binary_data = vectors.binary_data_;
        //
        //        vector_query->topk = vector_json["topk"].get<int64_t>();
        //        vector_query->extra_params = vector_json["extra_params"];
        //
        //        // TODO(yukun): remove hardcode here
        //        std::string vector_placeholder = "placeholder_1";
        //        query_ptr_->vectors.insert(std::make_pair(vector_placeholder, vector_query));
        //        leaf_query->vector_placeholder = vector_placeholder;
        //        query->AddLeafQuery(leaf_query);
    }
    return Status::OK();
}

Status
WebRequestHandler::ProcessBoolQueryJson(const nlohmann::json& query_json, query::BooleanQueryPtr& boolean_query) {
    if (query_json.contains("must")) {
        boolean_query->SetOccur(query::Occur::MUST);
        auto must_json = query_json["must"];
        if (!must_json.is_array()) {
            std::string msg = "Must json string is not an array";
            return Status{BODY_PARSE_FAIL, msg};
        }

        for (auto& json : must_json) {
            auto must_query = std::make_shared<query::BooleanQuery>();
            if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                ProcessBoolQueryJson(json, must_query);
                boolean_query->AddBooleanQuery(must_query);
            } else {
                ProcessLeafQueryJson(json, boolean_query);
            }
        }
        return Status::OK();
    } else if (query_json.contains("should")) {
        boolean_query->SetOccur(query::Occur::SHOULD);
        auto should_json = query_json["should"];
        if (!should_json.is_array()) {
            std::string msg = "Should json string is not an array";
            return Status{BODY_PARSE_FAIL, msg};
        }

        for (auto& json : should_json) {
            if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                auto should_query = std::make_shared<query::BooleanQuery>();
                ProcessBoolQueryJson(json, should_query);
                boolean_query->AddBooleanQuery(should_query);
            } else {
                ProcessLeafQueryJson(json, boolean_query);
            }
        }
        return Status::OK();
    } else if (query_json.contains("must_not")) {
        boolean_query->SetOccur(query::Occur::MUST_NOT);
        auto should_json = query_json["must_not"];
        if (!should_json.is_array()) {
            std::string msg = "Must_not json string is not an array";
            return Status{BODY_PARSE_FAIL, msg};
        }

        for (auto& json : should_json) {
            if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                auto must_not_query = std::make_shared<query::BooleanQuery>();
                ProcessBoolQueryJson(json, must_not_query);
                boolean_query->AddBooleanQuery(must_not_query);
            } else {
                ProcessLeafQueryJson(json, boolean_query);
            }
        }
        return Status::OK();
    } else {
        std::string msg = "Must json string doesnot include right query";
        return Status{BODY_PARSE_FAIL, msg};
    }
}

void
ConvertRowToColumnJson(const std::vector<engine::AttrsData>& row_attrs, const std::vector<std::string>& field_names,
                       const int64_t row_num, nlohmann::json& column_attrs_json) {
    //    if (field_names.size() == 0) {
    //        if (row_attrs.size() > 0) {
    //            auto attr_it = row_attrs[0].attr_type_.begin();
    //            for (; attr_it != row_attrs[0].attr_type_.end(); attr_it++) {
    //                field_names.emplace_back(attr_it->first);
    //            }
    //        }
    //    }

    for (uint64_t i = 0; i < field_names.size() - 1; i++) {
        std::vector<int64_t> int_data;
        std::vector<double> double_data;
        for (auto& attr : row_attrs) {
            int64_t int_value;
            double double_value;
            auto attr_data = attr.attr_data_.at(field_names[i]);
            switch (attr.attr_type_.at(field_names[i])) {
                case engine::DataType::INT8: {
                    if (attr_data.size() == sizeof(int8_t)) {
                        int_value = attr_data[0];
                        int_data.emplace_back(int_value);
                    }
                    break;
                }
                case engine::DataType::INT16: {
                    if (attr_data.size() == sizeof(int16_t)) {
                        memcpy(&int_value, attr_data.data(), sizeof(int16_t));
                        int_data.emplace_back(int_value);
                    }
                    break;
                }
                case engine::DataType::INT32: {
                    if (attr_data.size() == sizeof(int32_t)) {
                        memcpy(&int_value, attr_data.data(), sizeof(int32_t));
                        int_data.emplace_back(int_value);
                    }
                    break;
                }
                case engine::DataType::INT64: {
                    if (attr_data.size() == sizeof(int64_t)) {
                        memcpy(&int_value, attr_data.data(), sizeof(int64_t));
                        int_data.emplace_back(int_value);
                    }
                    break;
                }
                case engine::DataType::FLOAT: {
                    if (attr_data.size() == sizeof(float)) {
                        float float_value;
                        memcpy(&float_value, attr_data.data(), sizeof(float));
                        double_value = float_value;
                        double_data.emplace_back(double_value);
                    }
                    break;
                }
                case engine::DataType::DOUBLE: {
                    if (attr_data.size() == sizeof(double)) {
                        memcpy(&double_value, attr_data.data(), sizeof(double));
                        double_data.emplace_back(double_value);
                    }
                    break;
                }
                default: { return; }
            }
        }
        if (int_data.size() > 0) {
            if (row_num == -1) {
                nlohmann::json int_data_json(int_data);
                column_attrs_json[field_names[i]] = int_data_json;
            } else {
                nlohmann::json topk_int_result;
                int64_t topk = int_data.size() / row_num;
                for (int64_t j = 0; j < row_num; j++) {
                    std::vector<int64_t> one_int_result(topk);
                    memcpy(one_int_result.data(), int_data.data() + j * topk, sizeof(int64_t) * topk);
                    nlohmann::json one_int_result_json(one_int_result);
                    std::string tag = "top" + std::to_string(j);
                    topk_int_result[tag] = one_int_result_json;
                }
                column_attrs_json[field_names[i]] = topk_int_result;
            }
        } else if (double_data.size() > 0) {
            if (row_num == -1) {
                nlohmann::json double_data_json(double_data);
                column_attrs_json[field_names[i]] = double_data_json;
            } else {
                nlohmann::json topk_double_result;
                int64_t topk = int_data.size() / row_num;
                for (int64_t j = 0; j < row_num; j++) {
                    std::vector<double> one_double_result(topk);
                    memcpy(one_double_result.data(), double_data.data() + j * topk, sizeof(double) * topk);
                    nlohmann::json one_double_result_json(one_double_result);
                    std::string tag = "top" + std::to_string(j);
                    topk_double_result[tag] = one_double_result_json;
                }
                column_attrs_json[field_names[i]] = topk_double_result;
            }
        }
    }
}

Status
WebRequestHandler::Search(const std::string& collection_name, const nlohmann::json& json, std::string& result_str) {
    Status status;

    milvus::server::CollectionSchema collection_schema;
    status = req_handler_.GetCollectionInfo(context_ptr_, collection_name, collection_schema);
    if (!status.ok()) {
        return Status{UNEXPECTED_ERROR, "DescribeHybridCollection failed"};
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

        status = ProcessBoolQueryJson(boolean_query_json, boolean_query);
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

        auto step = result->result_ids_.size() / result->row_num_;
        nlohmann::json search_result_json;
        for (int64_t i = 0; i < result->row_num_; i++) {
            nlohmann::json raw_result_json;
            for (size_t j = 0; j < step; j++) {
                nlohmann::json one_result_json;
                one_result_json["id"] = std::to_string(result->result_ids_.at(i * step + j));
                one_result_json["distance"] = std::to_string(result->result_distances_.at(i * step + j));
                raw_result_json.emplace_back(one_result_json);
            }
            search_result_json.emplace_back(raw_result_json);
        }
        nlohmann::json attr_json;
        //        ConvertRowToColumnJson(result->attrs_, query_ptr_->field_names, result->row_num_, attr_json);
        result_json["Entity"] = attr_json;
        result_json["result"] = search_result_json;
        result_str = result_json.dump();
    }

    return Status::OK();
}

Status
WebRequestHandler::DeleteByIDs(const std::string& collection_name, const nlohmann::json& json,
                               std::string& result_str) {
    std::vector<int64_t> vector_ids;
    if (!json.contains("ids")) {
        return Status(BODY_FIELD_LOSS, "Field \"delete\" must contains \"ids\"");
    }
    auto ids = json["ids"];
    if (!ids.is_array()) {
        return Status(BODY_FIELD_LOSS, "\"ids\" must be an array");
    }

    for (auto& id : ids) {
        auto id_str = id.get<std::string>();
        if (!ValidateStringIsNumber(id_str).ok()) {
            return Status(ILLEGAL_BODY, "Members in \"ids\" must be integer string");
        }
        vector_ids.emplace_back(std::stol(id_str));
    }

    auto status = req_handler_.DeleteEntityByID(context_ptr_, collection_name, vector_ids);

    nlohmann::json result_json;
    AddStatusToJson(result_json, status.code(), status.message());
    result_str = result_json.dump();

    return status;
}

Status
WebRequestHandler::GetEntityByIDs(const std::string& collection_name, const std::vector<int64_t>& ids,
                                  std::vector<std::string>& field_names, nlohmann::json& json_out) {
    std::vector<bool> valid_row;
    engine::DataChunkPtr data_chunk;
    engine::snapshot::FieldElementMappings field_mappings;

    std::vector<engine::AttrsData> attr_batch;
    std::vector<engine::VectorsData> vector_batch;
    auto status = req_handler_.GetEntityByID(context_ptr_, collection_name, ids, field_names, valid_row, field_mappings,
                                             data_chunk);
    if (!status.ok()) {
        return status;
    }
    std::vector<uint8_t> id_array = data_chunk->fixed_fields_[engine::DEFAULT_UID_NAME]->data_;

    for (const auto& it : field_mappings) {
        std::string name = it.first->GetName();
        uint64_t type = it.first->GetFtype();
        std::vector<uint8_t>& data = data_chunk->fixed_fields_[name]->data_;
        if (type == engine::DataType::VECTOR_BINARY) {
            engine::VectorsData vectors_data;
            memcpy(vectors_data.binary_data_.data(), data.data(), data.size());
            memcpy(vectors_data.id_array_.data(), id_array.data(), id_array.size());
            vector_batch.emplace_back(vectors_data);
        } else if (type == engine::DataType::VECTOR_FLOAT) {
            engine::VectorsData vectors_data;
            memcpy(vectors_data.float_data_.data(), data.data(), data.size());
            memcpy(vectors_data.id_array_.data(), id_array.data(), id_array.size());
            vector_batch.emplace_back(vectors_data);
        } else {
            engine::AttrsData attrs_data;
            attrs_data.attr_type_[name] = static_cast<engine::DataType>(type);
            attrs_data.attr_data_[name] = data;
            memcpy(attrs_data.id_array_.data(), id_array.data(), id_array.size());
            attr_batch.emplace_back(attrs_data);
        }
    }

    bool bin;
    status = IsBinaryCollection(collection_name, bin);
    if (!status.ok()) {
        return status;
    }

    nlohmann::json vectors_json, attrs_json;
    for (size_t i = 0; i < vector_batch.size(); i++) {
        nlohmann::json vector_json;
        if (bin) {
            vector_json["vector"] = vector_batch.at(i).binary_data_;
        } else {
            vector_json["vector"] = vector_batch.at(i).float_data_;
        }
        vector_json["id"] = std::to_string(ids[i]);
        vectors_json.push_back(vector_json);
    }
    ConvertRowToColumnJson(attr_batch, field_names, -1, attrs_json);
    json_out["vectors"] = vectors_json;
    json_out["attributes"] = attrs_json;
    return Status::OK();
}

Status
WebRequestHandler::GetVectorsByIDs(const std::string& collection_name, const std::vector<int64_t>& ids,
                                   nlohmann::json& json_out) {
    std::vector<engine::VectorsData> vector_batch;
    auto status = Status::OK();
    //    auto status = req_handler_.GetVectorsByID(context_ptr_, collection_name, ids, vector_batch);
    if (!status.ok()) {
        return status;
    }

    bool bin;
    status = IsBinaryCollection(collection_name, bin);
    if (!status.ok()) {
        return status;
    }

    nlohmann::json vectors_json;
    for (size_t i = 0; i < vector_batch.size(); i++) {
        nlohmann::json vector_json;
        if (bin) {
            vector_json["vector"] = vector_batch.at(i).binary_data_;
        } else {
            vector_json["vector"] = vector_batch.at(i).float_data_;
        }
        vector_json["id"] = std::to_string(ids[i]);
        json_out.push_back(vector_json);
    }

    return Status::OK();
}

////////////////////////////////// Router methods ////////////////////////////////////////////
StatusDto::ObjectWrapper
WebRequestHandler::GetDevices(DevicesDto::ObjectWrapper& devices_dto) {
    auto system_info = SystemInfo::GetInstance();

    devices_dto->cpu = devices_dto->cpu->createShared();
    devices_dto->cpu->memory = system_info.GetPhysicalMemory() >> 30;

    devices_dto->gpus = devices_dto->gpus->createShared();

#ifdef MILVUS_GPU_VERSION
    size_t count = system_info.num_device();
    std::vector<int64_t> device_mems = system_info.GPUMemoryTotal();

    if (count != device_mems.size()) {
        RETURN_STATUS_DTO(UNEXPECTED_ERROR, "Can't obtain GPU info");
    }

    for (size_t i = 0; i < count; i++) {
        auto device_dto = DeviceInfoDto::createShared();
        device_dto->memory = device_mems.at(i) >> 30;
        devices_dto->gpus->put("GPU" + OString(std::to_string(i).c_str()), device_dto);
    }
#endif

    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}

StatusDto::ObjectWrapper
WebRequestHandler::GetAdvancedConfig(AdvancedConfigDto::ObjectWrapper& advanced_config) {
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

StatusDto::ObjectWrapper
WebRequestHandler::SetAdvancedConfig(const AdvancedConfigDto::ObjectWrapper& advanced_config) {
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

StatusDto::ObjectWrapper
WebRequestHandler::GetGpuConfig(GPUConfigDto::ObjectWrapper& gpu_config_dto) {
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

StatusDto::ObjectWrapper
WebRequestHandler::SetGpuConfig(const GPUConfigDto::ObjectWrapper& gpu_config_dto) {
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
StatusDto::ObjectWrapper
WebRequestHandler::CreateCollection(const CollectionRequestDto::ObjectWrapper& collection_schema) {
    if (nullptr == collection_schema->collection_name.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'collection_name\' is missing")
    }

    if (nullptr == collection_schema->dimension.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'dimension\' is missing")
    }

    if (nullptr == collection_schema->index_file_size.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'index_file_size\' is missing")
    }

    if (nullptr == collection_schema->metric_type.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'metric_type\' is missing")
    }

    auto status = Status::OK();
    //    auto status = req_handler_.CreateCollection(
    //        context_ptr_, collection_schema->collection_name->std_str(), collection_schema->dimension,
    //        collection_schema->index_file_size,
    //        static_cast<int64_t>(MetricNameMap.at(collection_schema->metric_type->std_str())));

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::CreateHybridCollection(const milvus::server::web::OString& body) {
    auto json_str = nlohmann::json::parse(body->c_str());
    std::string collection_name = json_str["collection_name"];

    // TODO(yukun): do checking
    std::unordered_map<std::string, FieldSchema> fields;
    for (auto& field : json_str["fields"]) {
        FieldSchema field_schema;
        std::string field_name = field["field_name"];

        field_schema.field_params_ = field["extra_params"];

        const std::string& field_type = field["field_type"];
        if (field_type == "int8") {
            field_schema.field_type_ = engine::DataType::INT8;
        } else if (field_type == "int16") {
            field_schema.field_type_ = engine::DataType::INT16;
        } else if (field_type == "int32") {
            field_schema.field_type_ = engine::DataType::INT32;
        } else if (field_type == "int64") {
            field_schema.field_type_ = engine::DataType::INT64;
        } else if (field_type == "float") {
            field_schema.field_type_ = engine::DataType::FLOAT;
        } else if (field_type == "double") {
            field_schema.field_type_ = engine::DataType::DOUBLE;
        } else if (field_type == "vector") {
        } else {
            std::string msg = field_name + " has wrong field_type";
            RETURN_STATUS_DTO(BODY_PARSE_FAIL, msg.c_str());
        }

        fields[field_name] = field_schema;
    }

    milvus::json json_params;

    auto status = req_handler_.CreateCollection(context_ptr_, collection_name, fields, json_params);

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
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
        offset = std::min((size_t)offset, collections.size());
        page_size = std::min(collections.size() - offset, (size_t)page_size);
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

    result = result_json.dump().c_str();

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
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

StatusDto::ObjectWrapper
WebRequestHandler::DropCollection(const OString& collection_name) {
    auto status = req_handler_.DropCollection(context_ptr_, collection_name->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

/***********
 *
 * Index {
 */

StatusDto::ObjectWrapper
WebRequestHandler::CreateIndex(const OString& collection_name, const OString& body) {
    try {
        auto request_json = nlohmann::json::parse(body->std_str());
        std::string field_name, index_name;
        if (!request_json.contains("index_type")) {
            RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'index_type\' is required");
        }

        auto status = Status::OK();
        //        auto status =
        //            req_handler_.CreateIndex(context_ptr_, collection_name->std_str(), index,
        //            request_json["params"]);
        ASSIGN_RETURN_STATUS_DTO(status);
    } catch (nlohmann::detail::parse_error& e) {
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, e.what())
    } catch (nlohmann::detail::type_error& e) {
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, e.what())
    }

    ASSIGN_RETURN_STATUS_DTO(Status::OK())
}

StatusDto::ObjectWrapper
WebRequestHandler::DropIndex(const OString& collection_name) {
    auto status = Status::OK();
    //    auto status = req_handler_.DropIndex(context_ptr_, collection_name->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::CreatePartition(const OString& collection_name, const PartitionRequestDto::ObjectWrapper& param) {
    if (nullptr == param->partition_tag.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'partition_tag\' is required")
    }

    auto status =
        req_handler_.CreatePartition(context_ptr_, collection_name->std_str(), param->partition_tag->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::ShowPartitions(const OString& collection_name, const OQueryParams& query_params,
                                  PartitionListDto::ObjectWrapper& partition_list_dto) {
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
        offset = std::min((size_t)offset, partition_names.size());
        page_size = std::min(partition_names.size() - offset, (size_t)page_size);
    }

    partition_list_dto->count = partition_names.size();
    partition_list_dto->partitions = partition_list_dto->partitions->createShared();

    if (offset < (int64_t)(partition_names.size())) {
        for (int64_t i = offset; i < page_size + offset; i++) {
            auto partition_dto = PartitionFieldsDto::createShared();
            partition_dto->partition_tag = partition_names.at(i).c_str();
            partition_list_dto->partitions->pushBack(partition_dto);
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
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
StatusDto::ObjectWrapper
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

StatusDto::ObjectWrapper
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
StatusDto::ObjectWrapper
WebRequestHandler::InsertEntity(const OString& collection_name, const milvus::server::web::OString& body,
                                VectorIdsDto::ObjectWrapper& ids_dto) {
    if (nullptr == body.get() || body->getSize() == 0) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Request payload is required.")
    }

    auto body_json = nlohmann::json::parse(body->c_str());
    std::string partition_name = body_json["partition_tag"];
    int32_t row_num = body_json["row_num"];

    std::unordered_map<std::string, engine::DataType> field_types;
    auto status = Status::OK();
    // auto status = req_handler_.DescribeHybridCollection(context_ptr_, collection_name->c_str(), field_types);

    auto entities = body_json["entity"];
    if (!entities.is_array()) {
        RETURN_STATUS_DTO(ILLEGAL_BODY, "An entity must be an array");
    }

    std::unordered_map<std::string, std::vector<uint8_t>> chunk_data;

    for (auto& entity : entities) {
        std::string field_name = entity["field_name"];
        auto field_value = entity["field_value"];
        auto size = field_value.size();
        if (size != row_num) {
            RETURN_STATUS_DTO(ILLEGAL_ROWRECORD, "Field row count inconsist");
        }

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
                bool bin_flag;
                status = IsBinaryCollection(collection_name->c_str(), bin_flag);
                if (!status.ok()) {
                    ASSIGN_RETURN_STATUS_DTO(status)
                }

                // engine::VectorsData vectors;
                // CopyRecordsFromJson(field_value, vectors, bin_flag);
                // vector_datas.insert(std::make_pair(field_name, vectors));
            }
            default: {}
        }

        chunk_data.insert(std::make_pair(field_name, temp_data));
    }

    status = req_handler_.Insert(context_ptr_, collection_name->c_str(), partition_name, row_num, chunk_data);
    if (!status.ok()) {
        RETURN_STATUS_DTO(UNEXPECTED_ERROR, "Failed to insert data");
    }

    // return generated ids
    auto pair = chunk_data.find(engine::DEFAULT_UID_NAME);
    if (pair != chunk_data.end()) {
        int64_t count = pair->second.size() / 8;
        int64_t* pdata = reinterpret_cast<int64_t*>(pair->second.data());
        for (int64_t i = 0; i < count; ++i) {
            ids_dto->ids->pushBack(std::to_string(pdata[i]).c_str());
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::GetEntity(const milvus::server::web::OString& collection_name,
                             const milvus::server::web::OQueryParams& query_params,
                             milvus::server::web::OString& response) {
    auto status = Status::OK();
    try {
        auto query_ids = query_params.get("ids");
        if (query_ids == nullptr || query_ids.get() == nullptr) {
            RETURN_STATUS_DTO(QUERY_PARAM_LOSS, "Query param ids is required.");
        }

        std::vector<std::string> ids;
        StringHelpFunctions::SplitStringByDelimeter(query_ids->c_str(), ",", ids);
        std::vector<int64_t> entity_ids;
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
            ASSIGN_RETURN_STATUS_DTO(status)
        }

        nlohmann::json json;
        AddStatusToJson(json, status.code(), status.message());
        if (entity_result_json.empty()) {
            json["entities"] = std::vector<int64_t>();
        } else {
            json["entities"] = entity_result_json;
        }
    } catch (std::exception& e) {
        RETURN_STATUS_DTO(SERVER_UNEXPECTED_ERROR, e.what());
    }

    ASSIGN_RETURN_STATUS_DTO(status);
}

StatusDto::ObjectWrapper
WebRequestHandler::GetVector(const OString& collection_name, const OQueryParams& query_params, OString& response) {
    auto status = Status::OK();
    try {
        auto query_ids = query_params.get("ids");
        if (query_ids == nullptr || query_ids.get() == nullptr) {
            RETURN_STATUS_DTO(QUERY_PARAM_LOSS, "Query param ids is required.");
        }

        std::vector<std::string> ids;
        StringHelpFunctions::SplitStringByDelimeter(query_ids->c_str(), ",", ids);

        std::vector<int64_t> vector_ids;
        for (auto& id : ids) {
            vector_ids.push_back(std::stol(id));
        }
        engine::VectorsData vectors;
        nlohmann::json vectors_json;
        status = GetVectorsByIDs(collection_name->std_str(), vector_ids, vectors_json);
        if (!status.ok()) {
            response = "NULL";
            ASSIGN_RETURN_STATUS_DTO(status)
        }

        FloatJson json;
        json["code"] = (int64_t)status.code();
        json["message"] = status.message();
        if (vectors_json.empty()) {
            json["vectors"] = std::vector<int64_t>();
        } else {
            json["vectors"] = vectors_json;
        }
        response = json.dump().c_str();
    } catch (std::exception& e) {
        RETURN_STATUS_DTO(SERVER_UNEXPECTED_ERROR, e.what());
    }

    ASSIGN_RETURN_STATUS_DTO(status);
}

StatusDto::ObjectWrapper
WebRequestHandler::VectorsOp(const OString& collection_name, const OString& payload, OString& response) {
    auto status = Status::OK();
    std::string result_str;

    try {
        nlohmann::json payload_json = nlohmann::json::parse(payload->std_str());

        if (payload_json.contains("delete")) {
            status = DeleteByIDs(collection_name->std_str(), payload_json["delete"], result_str);
        } else if (payload_json.contains("query")) {
            status = Search(collection_name->c_str(), payload_json, result_str);
        } else {
            status = Status(ILLEGAL_BODY, "Unknown body");
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
StatusDto::ObjectWrapper
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

StatusDto::ObjectWrapper
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
            //        } else if (op->equals("config")) {
            //            status = SetConfig(j, result_str);
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

}  // namespace web
}  // namespace server
}  // namespace milvus
