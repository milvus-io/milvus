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
#include <cmath>
#include <ctime>
#include <string>
#include <vector>

#include "metrics/SystemInfo.h"
#include "server/Config.h"
#include "server/delivery/request/BaseRequest.h"
#include "server/web_impl/Constants.h"
#include "server/web_impl/Types.h"
#include "server/web_impl/dto/PartitionDto.hpp"
#include "server/web_impl/utils/Util.h"
#include "thirdparty/nlohmann/json.hpp"
#include "utils/StringHelpFunctions.h"
#include "utils/ValidationUtil.h"

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
        {SERVER_TABLE_NOT_EXIST, StatusCode::TABLE_NOT_EXISTS},
        {SERVER_INVALID_TABLE_NAME, StatusCode::ILLEGAL_TABLE_NAME},
        {SERVER_INVALID_TABLE_DIMENSION, StatusCode::ILLEGAL_DIMENSION},
        {SERVER_INVALID_VECTOR_DIMENSION, StatusCode::ILLEGAL_DIMENSION},

        {SERVER_INVALID_INDEX_TYPE, StatusCode::ILLEGAL_INDEX_TYPE},
        {SERVER_INVALID_ROWRECORD, StatusCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_ROWRECORD_ARRAY, StatusCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_TOPK, StatusCode::ILLEGAL_TOPK},
        {SERVER_INVALID_NPROBE, StatusCode::ILLEGAL_ARGUMENT},
        {SERVER_INVALID_INDEX_NLIST, StatusCode::ILLEGAL_NLIST},
        {SERVER_INVALID_INDEX_METRIC_TYPE, StatusCode::ILLEGAL_METRIC_TYPE},
        {SERVER_INVALID_INDEX_FILE_SIZE, StatusCode::ILLEGAL_ARGUMENT},
        {SERVER_ILLEGAL_VECTOR_ID, StatusCode::ILLEGAL_VECTOR_ID},
        {SERVER_ILLEGAL_SEARCH_RESULT, StatusCode::ILLEGAL_SEARCH_RESULT},
        {SERVER_CACHE_FULL, StatusCode::CACHE_FAILED},
        {SERVER_BUILD_INDEX_ERROR, StatusCode::BUILD_INDEX_ERROR},
        {SERVER_OUT_OF_MEMORY, StatusCode::OUT_OF_MEMORY},

        {DB_NOT_FOUND, StatusCode::TABLE_NOT_EXISTS},
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

/////////////////////////////////// Private methods ///////////////////////////////////////
void
WebRequestHandler::AddStatusToJson(nlohmann::json& json, int64_t code, const std::string& msg) {
    json["code"] = (int64_t)code;
    json["message"] = msg;
}

Status
WebRequestHandler::ParseSegmentStat(const milvus::server::SegmentStat& seg_stat, nlohmann::json& json) {
    json["segment_name"] = seg_stat.name_;
    json["index"] = seg_stat.index_name_;
    json["count"] = seg_stat.row_num_;
    json["size"] = seg_stat.data_size_;

    return Status::OK();
}

Status
WebRequestHandler::ParsePartitionStat(const milvus::server::PartitionStat& par_stat, nlohmann::json& json) {
    json["partition_tag"] = par_stat.tag_;
    json["count"] = par_stat.total_row_num_;

    std::vector<nlohmann::json> seg_stat_json;
    for (auto& seg : par_stat.segments_stat_) {
        nlohmann::json seg_json;
        ParseSegmentStat(seg, seg_json);
        seg_stat_json.push_back(seg_json);
    }
    json["segments_stat"] = seg_stat_json;

    return Status::OK();
}

Status
WebRequestHandler::IsBinaryTable(const std::string& table_name, bool& bin) {
    TableSchema schema;
    auto status = request_handler_.DescribeTable(context_ptr_, table_name, schema);
    if (status.ok()) {
        auto metric = engine::MetricType(schema.metric_type_);
        bin = engine::MetricType::HAMMING == metric || engine::MetricType::JACCARD == metric ||
              engine::MetricType::TANIMOTO == metric;
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
WebRequestHandler::GetTableMetaInfo(const std::string& table_name, nlohmann::json& json_out) {
    TableSchema schema;
    auto status = request_handler_.DescribeTable(context_ptr_, table_name, schema);
    if (!status.ok()) {
        return status;
    }

    int64_t count;
    status = request_handler_.CountTable(context_ptr_, table_name, count);
    if (!status.ok()) {
        return status;
    }

    IndexParam index_param;
    status = request_handler_.DescribeIndex(context_ptr_, table_name, index_param);
    if (!status.ok()) {
        return status;
    }

    json_out["table_name"] = schema.table_name_;
    json_out["dimension"] = schema.dimension_;
    json_out["index_file_size"] = schema.index_file_size_;
    json_out["index"] = IndexMap.at(engine::EngineType(index_param.index_type_));
    json_out["nlist"] = index_param.nlist_;
    json_out["metric_type"] = MetricMap.at(engine::MetricType(schema.metric_type_));
    json_out["count"] = count;

    return Status::OK();
}

Status
WebRequestHandler::GetTableStat(const std::string& table_name, nlohmann::json& json_out) {
    struct TableInfo table_info;
    auto status = request_handler_.ShowTableInfo(context_ptr_, table_name, table_info);

    if (status.ok()) {
        json_out["count"] = table_info.total_row_num_;

        std::vector<nlohmann::json> par_stat_json;
        for (auto& par : table_info.partitions_stat_) {
            nlohmann::json par_json;
            ParsePartitionStat(par, par_json);
            par_stat_json.push_back(par_json);
        }
        json_out["partitions_stat"] = par_stat_json;
    }

    return status;
}

Status
WebRequestHandler::GetSegmentVectors(const std::string& table_name, const std::string& segment_name, int64_t page_size,
                                     int64_t offset, nlohmann::json& json_out) {
    std::vector<int64_t> vector_ids;
    auto status = request_handler_.GetVectorIDs(context_ptr_, table_name, segment_name, vector_ids);
    if (!status.ok()) {
        return status;
    }

    auto ids_begin = std::min(vector_ids.size(), (size_t)offset);
    auto ids_end = std::min(vector_ids.size(), (size_t)(offset + page_size));

    auto ids = std::vector<int64_t>(vector_ids.begin() + ids_begin, vector_ids.begin() + ids_end);
    nlohmann::json vectors_json;
    status = GetVectorsByIDs(table_name, ids, vectors_json);

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
WebRequestHandler::GetSegmentIds(const std::string& table_name, const std::string& segment_name, int64_t page_size,
                                 int64_t offset, nlohmann::json& json_out) {
    std::vector<int64_t> vector_ids;
    auto status = request_handler_.GetVectorIDs(context_ptr_, table_name, segment_name, vector_ids);
    if (status.ok()) {
        auto ids_begin = std::min(vector_ids.size(), (size_t)offset);
        auto ids_end = std::min(vector_ids.size(), (size_t)(offset + page_size));

        if (ids_begin >= ids_end) {
            json_out["ids"] = std::vector<int64_t>();
        } else {
            for (size_t i = ids_begin; i < ids_end; i++) {
                json_out["ids"].push_back(std::to_string(vector_ids.at(i)));
            }
        }
        json_out["count"] = vector_ids.size();
    }

    return status;
}

Status
WebRequestHandler::CommandLine(const std::string& cmd, std::string& reply) {
    return request_handler_.Cmd(context_ptr_, cmd, reply);
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
WebRequestHandler::PreLoadTable(const nlohmann::json& json, std::string& result_str) {
    if (!json.contains("table_name")) {
        return Status(BODY_FIELD_LOSS, "Field \"load\" must contains table_name");
    }

    auto table_name = json["table_name"];
    auto status = request_handler_.PreloadTable(context_ptr_, table_name.get<std::string>());
    if (status.ok()) {
        nlohmann::json result;
        AddStatusToJson(result, status.code(), status.message());
        result_str = result.dump();
    }

    return status;
}

Status
WebRequestHandler::Flush(const nlohmann::json& json, std::string& result_str) {
    if (!json.contains("table_names")) {
        return Status(BODY_FIELD_LOSS, "Field \"flush\" must contains table_names");
    }

    auto table_names = json["table_names"];
    if (!table_names.is_array()) {
        return Status(BODY_FIELD_LOSS, "Field \"table_names\" must be and array");
    }

    std::vector<std::string> names;
    for (auto& name : table_names) {
        names.emplace_back(name.get<std::string>());
    }

    auto status = request_handler_.Flush(context_ptr_, names);
    if (status.ok()) {
        nlohmann::json result;
        AddStatusToJson(result, status.code(), status.message());
        //        result["code"] = status.code();
        //        result["message"] = status.message();
        result_str = result.dump();
    }

    return status;
}

Status
WebRequestHandler::Compact(const nlohmann::json& json, std::string& result_str) {
    if (!json.contains("table_name")) {
        return Status(BODY_FIELD_LOSS, "Field \"compact\" must contains table_names");
    }

    auto table_name = json["table_name"];
    if (!table_name.is_string()) {
        return Status(BODY_FIELD_LOSS, "Field \"table_names\" must be a string");
    }

    auto name = table_name.get<std::string>();

    auto status = request_handler_.Compact(context_ptr_, name);

    if (status.ok()) {
        nlohmann::json result;
        result["code"] = status.code();
        result["message"] = status.message();
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
        Config& config = Config::GetInstance();
        bool required = false;
        config.GetServerRestartRequired(required);
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

    bool required = false;
    Config& config = Config::GetInstance();
    config.GetServerRestartRequired(required);

    nlohmann::json result;
    result["code"] = StatusCode::SUCCESS;
    result["message"] = msg;
    result["restart_required"] = required;

    result_str = result.dump();

    return Status::OK();
}

Status
WebRequestHandler::Search(const std::string& table_name, const nlohmann::json& json, std::string& result_str) {
    if (!json.contains("topk")) {
        return Status(BODY_FIELD_LOSS, "Field \'topk\' is required");
    }
    int64_t topk = json["topk"];

    if (!json.contains("nprobe")) {
        return Status(BODY_FIELD_LOSS, "Field \'nprobe\' is required");
    }
    int64_t nprobe = json["nprobe"];

    std::vector<std::string> partition_tags;
    if (json.contains("partition_tags")) {
        auto tags = json["partition_tags"];
        if (!tags.is_null() && !tags.is_array()) {
            return Status(BODY_PARSE_FAIL, "Field \"partition_tags\" must be a array");
        }

        for (auto& tag : tags) {
            partition_tags.emplace_back(tag.get<std::string>());
        }
    }

    TopKQueryResult result;
    if (json.contains("vector_id")) {
        auto vec_id = json["vector_id"].get<int64_t>();
        auto status =
            request_handler_.SearchByID(context_ptr_, table_name, vec_id, topk, nprobe, partition_tags, result);
        if (!status.ok()) {
            return status;
        }
    } else {
        std::vector<std::string> file_id_vec;
        if (json.contains("file_ids")) {
            auto ids = json["file_ids"];
            if (!ids.is_null() && !ids.is_array()) {
                return Status(BODY_PARSE_FAIL, "Field \"file_ids\" must be a array");
            }
            for (auto& id : ids) {
                file_id_vec.emplace_back(id.get<std::string>());
            }
        }

        bool bin_flag = false;
        auto status = IsBinaryTable(table_name, bin_flag);
        if (!status.ok()) {
            return status;
        }

        if (!json.contains("vectors")) {
            return Status(BODY_FIELD_LOSS, "Field \"vectors\" is required");
        }

        engine::VectorsData vectors_data;
        status = CopyRecordsFromJson(json["vectors"], vectors_data, bin_flag);
        if (!status.ok()) {
            return status;
        }

        status = request_handler_.Search(context_ptr_, table_name, vectors_data, topk, nprobe, partition_tags,
                                         file_id_vec, result);
        if (!status.ok()) {
            return status;
        }
    }

    nlohmann::json result_json;
    result_json["num"] = result.row_num_;
    if (result.row_num_ == 0) {
        result_json["result"] = std::vector<int64_t>();
        result_str = result_json.dump();
        return Status::OK();
    }

    auto step = result.id_list_.size() / result.row_num_;
    nlohmann::json search_result_json;
    for (size_t i = 0; i < result.row_num_; i++) {
        nlohmann::json raw_result_json;
        for (size_t j = 0; j < step; j++) {
            nlohmann::json one_result_json;
            one_result_json["id"] = std::to_string(result.id_list_.at(i * step + j));
            one_result_json["distance"] = std::to_string(result.distance_list_.at(i * step + j));
            raw_result_json.emplace_back(one_result_json);
        }
        search_result_json.emplace_back(raw_result_json);
    }
    result_json["result"] = search_result_json;
    result_str = result_json.dump();

    return Status::OK();
}

Status
WebRequestHandler::DeleteByIDs(const std::string& table_name, const nlohmann::json& json, std::string& result_str) {
    std::vector<int64_t> vector_ids;
    if (!json.contains("ids")) {
        return Status(BODY_FIELD_LOSS, "Field \"delete\" must contains \"ids\"");
    }
    auto ids = json["ids"];
    if (!ids.is_array()) {
        return Status(BODY_FIELD_LOSS, "\"ids\" must be an array");
    }

    for (auto& id : ids) {
        vector_ids.emplace_back(id.get<int64_t>());
    }

    auto status = request_handler_.DeleteByID(context_ptr_, table_name, vector_ids);
    if (status.ok()) {
        nlohmann::json result_json;
        result_json["code"] = status.code();
        result_json["message"] = status.message();
        result_str = result_json.dump();
    }

    return status;
}

Status
WebRequestHandler::GetVectorsByIDs(const std::string& table_name, const std::vector<int64_t>& ids,
                                   nlohmann::json& json_out) {
    std::vector<engine::VectorsData> vector_batch;
    for (size_t i = 0; i < ids.size(); i++) {
        auto vec_ids = std::vector<int64_t>(ids.begin() + i, ids.begin() + i + 1);
        engine::VectorsData vectors_data;
        auto status = request_handler_.GetVectorByID(context_ptr_, table_name, ids, vectors_data);
        if (!status.ok()) {
            return status;
        }
        vector_batch.push_back(vectors_data);
    }

    bool bin;
    auto status = IsBinaryTable(table_name, bin);
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

/************
 * Device {
 *
 */
StatusDto::ObjectWrapper
WebRequestHandler::GetDevices(DevicesDto::ObjectWrapper& devices_dto) {
    auto system_info = SystemInfo::GetInstance();

    devices_dto->cpu = devices_dto->cpu->createShared();
    devices_dto->cpu->memory = system_info.GetPhysicalMemory() >> 30;

    devices_dto->gpus = devices_dto->gpus->createShared();

#ifdef MILVUS_GPU_VERSION
    size_t count = system_info.num_device();
    std::vector<uint64_t> device_mems = system_info.GPUMemoryTotal();

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
    Config& config = Config::GetInstance();
    std::string reply;
    std::string cache_cmd_prefix = "get_config " + std::string(CONFIG_CACHE) + ".";

    std::string cache_cmd_string = cache_cmd_prefix + std::string(CONFIG_CACHE_CPU_CACHE_CAPACITY);
    auto status = CommandLine(cache_cmd_string, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }
    advanced_config->cpu_cache_capacity = std::stol(reply);

    cache_cmd_string = cache_cmd_prefix + std::string(CONFIG_CACHE_CACHE_INSERT_DATA);
    CommandLine(cache_cmd_string, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }
    advanced_config->cache_insert_data = ("1" == reply || "true" == reply);

    auto engine_cmd_prefix = "get_config " + std::string(CONFIG_ENGINE) + ".";

    auto engine_cmd_string = engine_cmd_prefix + std::string(CONFIG_ENGINE_USE_BLAS_THRESHOLD);
    CommandLine(engine_cmd_string, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }
    advanced_config->use_blas_threshold = std::stol(reply);

#ifdef MILVUS_GPU_VERSION
    engine_cmd_string = engine_cmd_prefix + std::string(CONFIG_ENGINE_GPU_SEARCH_THRESHOLD);
    CommandLine(engine_cmd_string, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }
    advanced_config->gpu_search_threshold = std::stol(reply);
#endif

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::SetAdvancedConfig(const AdvancedConfigDto::ObjectWrapper& advanced_config) {
    if (nullptr == advanced_config->cpu_cache_capacity.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'cpu_cache_capacity\' miss.");
    }

    if (nullptr == advanced_config->cache_insert_data.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'cache_insert_data\' miss.");
    }

    if (nullptr == advanced_config->use_blas_threshold.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'use_blas_threshold\' miss.");
    }

#ifdef MILVUS_GPU_VERSION
    if (nullptr == advanced_config->gpu_search_threshold.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'gpu_search_threshold\' miss.");
    }
#endif

    std::string reply;
    std::string cache_cmd_prefix = "set_config " + std::string(CONFIG_CACHE) + ".";

    std::string cache_cmd_string = cache_cmd_prefix + std::string(CONFIG_CACHE_CPU_CACHE_CAPACITY) + " " +
                                   std::to_string(advanced_config->cpu_cache_capacity->getValue());
    auto status = CommandLine(cache_cmd_string, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    cache_cmd_string = cache_cmd_prefix + std::string(CONFIG_CACHE_CACHE_INSERT_DATA) + " " +
                       std::to_string(advanced_config->cache_insert_data->getValue());
    status = CommandLine(cache_cmd_string, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    auto engine_cmd_prefix = "set_config " + std::string(CONFIG_ENGINE) + ".";

    auto engine_cmd_string = engine_cmd_prefix + std::string(CONFIG_ENGINE_USE_BLAS_THRESHOLD) + " " +
                             std::to_string(advanced_config->use_blas_threshold->getValue());
    status = CommandLine(engine_cmd_string, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

#ifdef MILVUS_GPU_VERSION
    engine_cmd_string = engine_cmd_prefix + std::string(CONFIG_ENGINE_GPU_SEARCH_THRESHOLD) + " " +
                        std::to_string(advanced_config->gpu_search_threshold->getValue());
    CommandLine(engine_cmd_string, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }
#endif

    ASSIGN_RETURN_STATUS_DTO(status)
}

#ifdef MILVUS_GPU_VERSION
StatusDto::ObjectWrapper
WebRequestHandler::GetGpuConfig(GPUConfigDto::ObjectWrapper& gpu_config_dto) {
    std::string reply;
    std::string gpu_cmd_prefix = "get_config " + std::string(CONFIG_GPU_RESOURCE) + ".";

    std::string gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_ENABLE);
    auto status = CommandLine(gpu_cmd_request, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }
    gpu_config_dto->enable = reply == "1" || reply == "true";

    if (!gpu_config_dto->enable->getValue()) {
        ASSIGN_RETURN_STATUS_DTO(Status::OK());
    }

    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_CACHE_CAPACITY);
    status = CommandLine(gpu_cmd_request, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }
    gpu_config_dto->cache_capacity = std::stol(reply);

    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_SEARCH_RESOURCES);
    status = CommandLine(gpu_cmd_request, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    std::vector<std::string> gpu_entry;
    StringHelpFunctions::SplitStringByDelimeter(reply, ",", gpu_entry);

    gpu_config_dto->search_resources = gpu_config_dto->search_resources->createShared();
    for (auto& device_id : gpu_entry) {
        gpu_config_dto->search_resources->pushBack(OString(device_id.c_str())->toUpperCase());
    }
    gpu_entry.clear();

    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES);
    status = CommandLine(gpu_cmd_request, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    StringHelpFunctions::SplitStringByDelimeter(reply, ",", gpu_entry);
    gpu_config_dto->build_index_resources = gpu_config_dto->build_index_resources->createShared();
    for (auto& device_id : gpu_entry) {
        gpu_config_dto->build_index_resources->pushBack(OString(device_id.c_str())->toUpperCase());
    }

    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}

StatusDto::ObjectWrapper
WebRequestHandler::SetGpuConfig(const GPUConfigDto::ObjectWrapper& gpu_config_dto) {
    // Step 1: Check config param
    if (nullptr == gpu_config_dto->enable.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'enable\' miss")
    }

    if (nullptr == gpu_config_dto->cache_capacity.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'cache_capacity\' miss")
    }

    if (nullptr == gpu_config_dto->search_resources.get()) {
        gpu_config_dto->search_resources = gpu_config_dto->search_resources->createShared();
        gpu_config_dto->search_resources->pushBack("GPU0");
    }

    if (nullptr == gpu_config_dto->build_index_resources.get()) {
        gpu_config_dto->build_index_resources = gpu_config_dto->build_index_resources->createShared();
        gpu_config_dto->build_index_resources->pushBack("GPU0");
    }

    // Step 2: Set config
    std::string reply;
    std::string gpu_cmd_prefix = "set_config " + std::string(CONFIG_GPU_RESOURCE) + ".";
    std::string gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_ENABLE) + " " +
                                  std::to_string(gpu_config_dto->enable->getValue());
    auto status = CommandLine(gpu_cmd_request, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    if (!gpu_config_dto->enable->getValue()) {
        RETURN_STATUS_DTO(SUCCESS, "Set Gpu resources to false");
    }

    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_CACHE_CAPACITY) + " " +
                      std::to_string(gpu_config_dto->cache_capacity->getValue());
    status = CommandLine(gpu_cmd_request, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    std::vector<std::string> search_resources;
    gpu_config_dto->search_resources->forEach(
        [&search_resources](const OString& res) { search_resources.emplace_back(res->toLowerCase()->std_str()); });

    std::string search_resources_value;
    for (auto& res : search_resources) {
        search_resources_value += res + ",";
    }
    auto len = search_resources_value.size();
    if (len > 0) {
        search_resources_value.erase(len - 1);
    }

    gpu_cmd_request = gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_SEARCH_RESOURCES) + " " + search_resources_value;
    status = CommandLine(gpu_cmd_request, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    std::vector<std::string> build_resources;
    gpu_config_dto->build_index_resources->forEach(
        [&build_resources](const OString& res) { build_resources.emplace_back(res->toLowerCase()->std_str()); });

    std::string build_resources_value;
    for (auto& res : build_resources) {
        build_resources_value += res + ",";
    }
    len = build_resources_value.size();
    if (len > 0) {
        build_resources_value.erase(len - 1);
    }

    gpu_cmd_request =
        gpu_cmd_prefix + std::string(CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES) + " " + build_resources_value;
    status = CommandLine(gpu_cmd_request, reply);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}
#endif

/*************
 *
 * Table {
 */
StatusDto::ObjectWrapper
WebRequestHandler::CreateTable(const TableRequestDto::ObjectWrapper& table_schema) {
    if (nullptr == table_schema->table_name.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'table_name\' is missing")
    }

    if (nullptr == table_schema->dimension.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'dimension\' is missing")
    }

    if (nullptr == table_schema->index_file_size.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'index_file_size\' is missing")
    }

    if (nullptr == table_schema->metric_type.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'metric_type\' is missing")
    }

    if (MetricNameMap.find(table_schema->metric_type->std_str()) == MetricNameMap.end()) {
        RETURN_STATUS_DTO(ILLEGAL_METRIC_TYPE, "metric_type is illegal")
    }

    auto status = request_handler_.CreateTable(
        context_ptr_, table_schema->table_name->std_str(), table_schema->dimension, table_schema->index_file_size,
        static_cast<int64_t>(MetricNameMap.at(table_schema->metric_type->std_str())));

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::ShowTables(const OQueryParams& query_params, OString& result) {
    int64_t offset_value = 0;
    auto offset = query_params.get("offset");
    if (nullptr != offset.get() && offset->getSize() > 0) {
        std::string offset_str = offset->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(offset_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'offset\' is illegal, only non-negative integer supported");
        }
        offset_value = std::stol(offset_str);
    }

    int64_t page_size_value = 10;
    auto page_size = query_params.get("page_size");
    if (nullptr != page_size.get() && page_size->getSize() > 0) {
        std::string page_size_str = page_size->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(page_size_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'page_size\' is illegal, only non-negative integer supported");
        }
        page_size_value = std::stol(page_size_str);
    }

    if (offset_value < 0 || page_size_value < 0) {
        RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, "Query param 'offset' or 'page_size' should equal or bigger than 0");
    }

    bool all_required = false;
    auto required = query_params.get("all_required");
    if (nullptr != required.get()) {
        auto required_str = required->std_str();
        if (!ValidationUtil::ValidateStringIsBool(required_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, "Query param \'all_required\' must be a bool")
        }
        all_required = required_str == "True" || required_str == "true";
    }

    std::vector<std::string> tables;
    auto status = request_handler_.ShowTables(context_ptr_, tables);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    if (all_required) {
        offset_value = 0;
        page_size_value = tables.size();
    } else {
        offset_value = std::min((size_t)offset_value, tables.size());
        page_size_value = std::min(tables.size() - offset_value, (size_t)page_size_value);
    }

    nlohmann::json tables_json;
    for (int64_t i = offset_value; i < page_size_value + offset_value; i++) {
        nlohmann::json table_json;
        status = GetTableMetaInfo(tables.at(i), table_json);
        if (!status.ok()) {
            ASSIGN_RETURN_STATUS_DTO(status)
        }
        tables_json.push_back(table_json);
    }

    nlohmann::json result_json;
    result_json["count"] = tables.size();
    if (tables_json.empty()) {
        result_json["tables"] = std::vector<int64_t>();
    } else {
        result_json["tables"] = tables_json;
    }

    result = result_json.dump().c_str();

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::GetTable(const OString& table_name, const OQueryParams& query_params, OString& result) {
    if (nullptr == table_name.get()) {
        RETURN_STATUS_DTO(PATH_PARAM_LOSS, "Path param \'table_name\' is required!");
    }

    auto status = Status::OK();

    auto stat = query_params.get("info");
    if (nullptr != stat.get() && stat->std_str() == "stat") {
        nlohmann::json json;
        status = GetTableStat(table_name->std_str(), json);
        if (status.ok()) {
            result = json.dump().c_str();
        }
    } else {
        nlohmann::json json;
        status = GetTableMetaInfo(table_name->std_str(), json);
        if (status.ok()) {
            result = json.dump().c_str();
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status);
}

StatusDto::ObjectWrapper
WebRequestHandler::DropTable(const OString& table_name) {
    auto status = request_handler_.DropTable(context_ptr_, table_name->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

/***********
 *
 * Index {
 */

StatusDto::ObjectWrapper
WebRequestHandler::CreateIndex(const OString& table_name, const IndexRequestDto::ObjectWrapper& index_param) {
    if (nullptr == index_param->index_type.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'index_type\' is required")
    }
    std::string index_type = index_param->index_type->std_str();
    if (IndexNameMap.find(index_type) == IndexNameMap.end()) {
        RETURN_STATUS_DTO(ILLEGAL_INDEX_TYPE, "The index type is invalid.")
    }

    if (nullptr == index_param->nlist.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'nlist\' is required")
    }

    auto status =
        request_handler_.CreateIndex(context_ptr_, table_name->std_str(),
                                     static_cast<int64_t>(IndexNameMap.at(index_type)), index_param->nlist->getValue());
    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::GetIndex(const OString& table_name, IndexDto::ObjectWrapper& index_dto) {
    IndexParam param;
    auto status = request_handler_.DescribeIndex(context_ptr_, table_name->std_str(), param);

    if (status.ok()) {
        index_dto->index_type = IndexMap.at(engine::EngineType(param.index_type_)).c_str();
        index_dto->nlist = param.nlist_;
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::DropIndex(const OString& table_name) {
    auto status = request_handler_.DropIndex(context_ptr_, table_name->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

/***********
 *
 * Partition {
 */
StatusDto::ObjectWrapper
WebRequestHandler::CreatePartition(const OString& table_name, const PartitionRequestDto::ObjectWrapper& param) {
    if (nullptr == param->partition_tag.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'partition_tag\' is required")
    }

    auto status =
        request_handler_.CreatePartition(context_ptr_, table_name->std_str(), param->partition_tag->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::ShowPartitions(const OString& table_name, const OQueryParams& query_params,
                                  PartitionListDto::ObjectWrapper& partition_list_dto) {
    int64_t offset_value = 0;
    auto offset = query_params.get("offset");
    if (nullptr != offset.get() && offset->getSize() > 0) {
        std::string offset_str = offset->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(offset_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'offset\' is illegal, only non-negative integer supported");
        }
        offset_value = std::stol(offset_str);
    }

    int64_t page_size_value = 10;
    auto page_size = query_params.get("page_size");
    if (nullptr != page_size.get() && page_size->getSize() > 0) {
        std::string page_size_str = page_size->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(page_size_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'page_size\' is illegal, only non-negative integer supported");
        }
        page_size_value = std::stol(page_size_str);
    }

    if (offset_value < 0 || page_size_value < 0) {
        ASSIGN_RETURN_STATUS_DTO(
            Status(SERVER_UNEXPECTED_ERROR, "Query param 'offset' or 'page_size' should equal or bigger than 0"));
    }

    bool all_required = false;
    auto required = query_params.get("all_required");
    if (nullptr != required.get()) {
        auto required_str = required->std_str();
        if (!ValidationUtil::ValidateStringIsBool(required_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, "Query param \'all_required\' must be a bool")
        }
        all_required = required_str == "True" || required_str == "true";
    }

    std::vector<PartitionParam> partitions;
    auto status = request_handler_.ShowPartitions(context_ptr_, table_name->std_str(), partitions);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    if (all_required) {
        offset_value = 0;
        page_size_value = partitions.size();
    } else {
        offset_value = std::min((size_t)offset_value, partitions.size());
        page_size_value = std::min(partitions.size() - offset_value, (size_t)page_size_value);
    }

    partition_list_dto->count = partitions.size();
    partition_list_dto->partitions = partition_list_dto->partitions->createShared();

    if (offset_value < partitions.size()) {
        for (int64_t i = offset_value; i < page_size_value + offset_value; i++) {
            auto partition_dto = PartitionFieldsDto::createShared();
            partition_dto->partition_tag = partitions.at(i).tag_.c_str();
            partition_list_dto->partitions->pushBack(partition_dto);
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::DropPartition(const OString& table_name, const OString& body) {
    std::string tag;
    try {
        auto json = nlohmann::json::parse(body->std_str());
        tag = json["partition_tag"].get<std::string>();
    } catch (nlohmann::detail::parse_error& e) {
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, e.what())
    } catch (nlohmann::detail::type_error& e) {
        RETURN_STATUS_DTO(BODY_PARSE_FAIL, e.what())
    }
    auto status = request_handler_.DropPartition(context_ptr_, table_name->std_str(), tag);

    ASSIGN_RETURN_STATUS_DTO(status)
}

/***********
 *
 * Segment {
 */
StatusDto::ObjectWrapper
WebRequestHandler::ShowSegments(const OString& table_name, const OQueryParams& query_params, OString& response) {
    int64_t offset_value = 0;
    auto offset = query_params.get("offset");
    if (nullptr != offset.get() && offset->getSize() > 0) {
        std::string offset_str = offset->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(offset_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'offset\' is illegal, only non-negative integer supported");
        }
        offset_value = std::stol(offset_str);
    }

    int64_t page_size_value = 10;
    auto page_size = query_params.get("page_size");
    if (nullptr != page_size.get() && page_size->getSize() > 0) {
        std::string page_size_str = page_size->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(page_size_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'page_size\' is illegal, only non-negative integer supported");
        }
        page_size_value = std::stol(page_size_str);
    }

    if (offset_value < 0 || page_size_value < 0) {
        RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, "Query param 'offset' or 'page_size' should equal or bigger than 0");
    }

    std::string tag;
    if (nullptr != query_params.get("partition_tag").get()) {
        tag = query_params.get("partition_tag")->std_str();
    }

    struct TableInfo info;
    auto status = request_handler_.ShowTableInfo(context_ptr_, table_name->std_str(), info);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    typedef std::pair<std::string, SegmentStat> Pair;
    std::vector<Pair> segments;
    for (auto& par_stat : info.partitions_stat_) {
        if (!tag.empty() && tag != par_stat.tag_) {
            continue;
        }
        for (auto& seg_stat : par_stat.segments_stat_) {
            auto segment_stat = std::pair<std::string, SegmentStat>(par_stat.tag_, seg_stat);
            segments.push_back(segment_stat);
        }
    }

    int64_t size = segments.size();
    auto iter_begin = std::min(size, offset_value);
    auto iter_end = std::min(size, offset_value + page_size_value);

    auto segments_out = std::vector<Pair>(segments.begin() + iter_begin, segments.begin() + iter_end);

    // sort with segment name
    auto compare = [](Pair& a, Pair& b) -> bool { return a.second.name_ >= b.second.name_; };
    std::sort(segments_out.begin(), segments_out.end(), compare);

    nlohmann::json result_json;
    if (segments_out.empty()) {
        result_json["segments"] = std::vector<int64_t>();
    } else {
        nlohmann::json segs_json;
        for (auto& s : segments_out) {
            nlohmann::json seg_json;
            ParseSegmentStat(s.second, seg_json);
            seg_json["partition_tag"] = s.first;
            segs_json.push_back(seg_json);
        }
        result_json["segments"] = segs_json;
    }

    result_json["count"] = size;
    AddStatusToJson(result_json, status.code(), status.message());

    response = result_json.dump().c_str();

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::GetSegmentInfo(const OString& table_name, const OString& segment_name, const OString& info,
                                  const OQueryParams& query_params, OString& result) {
    int64_t offset_value = 0;
    auto offset = query_params.get("offset");
    if (nullptr != offset.get()) {
        std::string offset_str = offset->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(offset_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'offset\' is illegal, only non-negative integer supported");
        }
        offset_value = std::stol(offset_str);
    }

    int64_t page_size_value = 10;
    auto page_size = query_params.get("page_size");
    if (nullptr != page_size.get()) {
        std::string page_size_str = page_size->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(page_size_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'page_size\' is illegal, only non-negative integer supported");
        }
        page_size_value = std::stol(page_size_str);
    }

    if (offset_value < 0 || page_size_value < 0) {
        ASSIGN_RETURN_STATUS_DTO(
            Status(SERVER_UNEXPECTED_ERROR, "Query param 'offset' or 'page_size' should equal or bigger than 0"));
    }

    std::string re = info->std_str();
    auto status = Status::OK();
    nlohmann::json json;
    // Get vectors
    if (re == "vectors") {
        status = GetSegmentVectors(table_name->std_str(), segment_name->std_str(), page_size_value, offset_value, json);
        // Get vector ids
    } else if (re == "ids") {
        status = GetSegmentIds(table_name->std_str(), segment_name->std_str(), page_size_value, offset_value, json);
    }

    if (status.ok()) {
        result = json.dump().c_str();
    } else {
        result = "NULL";
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

/**********
 *
 * Vector {
 */
StatusDto::ObjectWrapper
WebRequestHandler::Insert(const OString& table_name, const OString& body, VectorIdsDto::ObjectWrapper& ids_dto) {
    if (nullptr == body.get() || body->getSize() == 0) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Request payload is required.")
    }

    // step 1: copy vectors
    bool bin_flag;
    auto status = IsBinaryTable(table_name->std_str(), bin_flag);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    auto body_json = nlohmann::json::parse(body->std_str());
    if (!body_json.contains("vectors")) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'vectors\' is required");
    }
    engine::VectorsData vectors;
    CopyRecordsFromJson(body_json["vectors"], vectors, bin_flag);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    // step 2: copy id array
    if (body_json.contains("ids")) {
        auto& ids_json = body_json["ids"];
        if (!ids_json.is_array()) {
            RETURN_STATUS_DTO(ILLEGAL_BODY, "Field \"ids\" must be a array");
        }
        auto& id_array = vectors.id_array_;
        id_array.clear();
        for (auto& id : ids_json) {
            id_array.emplace_back(id.get<int64_t>());
        }
    }

    // step 3: copy partition tag
    std::string tag;
    if (body_json.contains("partition_tag")) {
        tag = body_json["partition_tag"];
    }

    // step 4: construct result
    status = request_handler_.Insert(context_ptr_, table_name->std_str(), vectors, tag);
    if (status.ok()) {
        ids_dto->ids = ids_dto->ids->createShared();
        for (auto& id : vectors.id_array_) {
            ids_dto->ids->pushBack(std::to_string(id).c_str());
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::GetVector(const OString& table_name, const OQueryParams& query_params, OString& response) {
    auto id_str = query_params.get("id");
    if (id_str.get() == nullptr) {
        RETURN_STATUS_DTO(QUERY_PARAM_LOSS, "Need to specify vector id in query string")
    }
    if (!ValidationUtil::ValidateStringIsNumber(id_str->std_str()).ok()) {
        RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, "Query param \'id\' is illegal, only non-negative integer supported");
    }

    auto id = std::stol(id_str->c_str());

    std::vector<int64_t> ids = {id};
    engine::VectorsData vectors;
    nlohmann::json vectors_json;
    auto status = GetVectorsByIDs(table_name->std_str(), ids, vectors_json);
    if (!status.ok()) {
        response = "NULL";
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    nlohmann::json json;
    json["code"] = status.code();
    json["message"] = status.message();
    if (vectors_json.empty()) {
        json["vectors"] = std::vector<int64_t>();
    } else {
        json["vectors"] = vectors_json;
    }

    response = json.dump().c_str();

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::VectorsOp(const OString& table_name, const OString& payload, OString& response) {
    auto status = Status::OK();
    std::string result_str;

    try {
        nlohmann::json payload_json = nlohmann::json::parse(payload->std_str());

        if (payload_json.contains("delete")) {
            status = DeleteByIDs(table_name->std_str(), payload_json["delete"], result_str);
        } else if (payload_json.contains("search")) {
            status = Search(table_name->std_str(), payload_json["search"], result_str);
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

    if (status.ok()) {
        response = result_str.c_str();
    } else {
        response = "NULL";
    }

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
        RETURN_STATUS_DTO(ILLEGAL_BODY, emsg.c_str());
    } catch (nlohmann::detail::type_error& e) {
        std::string emsg = "json error: code=" + std::to_string(e.id) + ", reason=" + e.what();
        RETURN_STATUS_DTO(ILLEGAL_BODY, emsg.c_str());
    }

    if (status.ok()) {
        response_str = result_str.c_str();
    } else {
        response_str = "NULL";
    }

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
        nlohmann::json j = nlohmann::json::parse(body_str->c_str());
        if (op->equals("task")) {
            if (j.contains("load")) {
                status = PreLoadTable(j["load"], result_str);
            } else if (j.contains("flush")) {
                status = Flush(j["flush"], result_str);
            }
            if (j.contains("compact")) {
                status = Compact(j["compact"], result_str);
            }
        } else if (op->equals("config")) {
            SetConfig(j, result_str);
        } else {
            status = Status(UNKNOWN_PATH, "Unknown path: /system/" + op->std_str());
        }
    } catch (nlohmann::detail::parse_error& e) {
        std::string emsg = "json error: code=" + std::to_string(e.id) + ", reason=" + e.what();
        RETURN_STATUS_DTO(ILLEGAL_BODY, emsg.c_str());
    } catch (nlohmann::detail::type_error& e) {
        std::string emsg = "json error: code=" + std::to_string(e.id) + ", reason=" + e.what();
        RETURN_STATUS_DTO(ILLEGAL_BODY, emsg.c_str());
    }

    if (status.ok()) {
        response_str = result_str.c_str();
    } else {
        response_str = "NULL";
    }

    ASSIGN_RETURN_STATUS_DTO(status);
}

}  // namespace web
}  // namespace server
}  // namespace milvus
