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
#include "utils/StringHelpFunctions.h"
#include "utils/TimeRecorder.h"
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

    if (code_map.find(code) != code_map.end()) {
        return code_map.at(code);
    } else {
        return StatusCode::UNEXPECTED_ERROR;
    }
}

///////////////////////// WebRequestHandler methods ///////////////////////////////////////
Status
WebRequestHandler::GetTableInfo(const std::string& table_name, TableFieldsDto::ObjectWrapper& table_fields) {
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

    table_fields->table_name = schema.table_name_.c_str();
    table_fields->dimension = schema.dimension_;
    table_fields->index_file_size = schema.index_file_size_;
    table_fields->index = IndexMap.at(engine::EngineType(index_param.index_type_)).c_str();
    table_fields->nlist = index_param.nlist_;
    table_fields->metric_type = MetricMap.at(engine::MetricType(schema.metric_type_)).c_str();
    table_fields->count = count;
}

Status
WebRequestHandler::CommandLine(const std::string& cmd, std::string& reply) {
    return request_handler_.Cmd(context_ptr_, cmd, reply);
}

/////////////////////////////////////////// Router methods ////////////////////////////////////////////

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

#endif

#ifdef MILVUS_GPU_VERSION

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
WebRequestHandler::GetTable(const OString& table_name, const OQueryParams& query_params,
                            TableFieldsDto::ObjectWrapper& fields_dto) {
    if (nullptr == table_name.get()) {
        RETURN_STATUS_DTO(PATH_PARAM_LOSS, "Path param \'table_name\' is required!");
    }

    // TODO: query string field `fields` npt used here
    auto status = GetTableInfo(table_name->std_str(), fields_dto);

    ASSIGN_RETURN_STATUS_DTO(status);
}

StatusDto::ObjectWrapper
WebRequestHandler::ShowTables(const OString& offset, const OString& page_size,
                              TableListFieldsDto::ObjectWrapper& response_dto) {
    int64_t offset_value = 0;
    int64_t page_size_value = 10;

    if (nullptr != offset.get()) {
        std::string offset_str = offset->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(offset_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'offset\' is illegal, only non-negative integer supported");
        }
        offset_value = std::stol(offset_str);
    }

    if (nullptr != page_size.get()) {
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

    std::vector<std::string> tables;
    auto status = request_handler_.ShowTables(context_ptr_, tables);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    response_dto->tables = response_dto->tables->createShared();

    if (offset_value >= tables.size()) {
        ASSIGN_RETURN_STATUS_DTO(Status::OK());
    }

    response_dto->count = tables.size();

    int64_t size = page_size_value + offset_value > tables.size() ? tables.size() - offset_value : page_size_value;
    for (int64_t i = offset_value; i < size + offset_value; i++) {
        auto table_fields_dto = TableFieldsDto::createShared();
        status = GetTableInfo(tables.at(i), table_fields_dto);
        if (!status.ok()) {
            break;
        }

        response_dto->tables->pushBack(table_fields_dto);
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::DropTable(const OString& table_name) {
    auto status = request_handler_.DropTable(context_ptr_, table_name->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

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

StatusDto::ObjectWrapper
WebRequestHandler::CreatePartition(const OString& table_name, const PartitionRequestDto::ObjectWrapper& param) {
    if (nullptr == param->partition_name.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'partition_name\' is required")
    }

    if (nullptr == param->partition_tag.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'partition_tag\' is required")
    }

    auto status =
        request_handler_.CreatePartition(context_ptr_, table_name->std_str(), param->partition_tag->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::ShowPartitions(const OString& offset, const OString& page_size, const OString& table_name,
                                  PartitionListDto::ObjectWrapper& partition_list_dto) {
    int64_t offset_value = 0;
    int64_t page_size_value = 10;

    if (nullptr != offset.get()) {
        std::string offset_str = offset->std_str();
        if (!ValidationUtil::ValidateStringIsNumber(offset_str).ok()) {
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM,
                              "Query param \'offset\' is illegal, only non-negative integer supported");
        }
        offset_value = std::stol(offset_str);
    }

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

    std::vector<PartitionParam> partitions;
    auto status = request_handler_.ShowPartitions(context_ptr_, table_name->std_str(), partitions);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    partition_list_dto->partitions = partition_list_dto->partitions->createShared();

    if (offset_value < partitions.size()) {
        int64_t size =
            offset_value + page_size_value > partitions.size() ? partitions.size() - offset_value : page_size_value;
        for (int64_t i = offset_value; i < size + offset_value; i++) {
            auto partition_dto = PartitionFieldsDto::createShared();
            partition_dto->partition_tag = partitions.at(i).tag_.c_str();
            partition_list_dto->partitions->pushBack(partition_dto);
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::DropPartition(const OString& table_name, const OString& tag) {
    auto status = request_handler_.DropPartition(context_ptr_, table_name->std_str(), tag->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::Insert(const OString& table_name, const InsertRequestDto::ObjectWrapper& request,
                          VectorIdsDto::ObjectWrapper& ids_dto) {
    TableSchema schema;
    auto status = request_handler_.DescribeTable(context_ptr_, table_name->std_str(), schema);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    auto metric = engine::MetricType(schema.metric_type_);
    engine::VectorsData vectors;
    bool bin_flag = engine::MetricType::HAMMING == metric || engine::MetricType::JACCARD == metric ||
                    engine::MetricType::TANIMOTO == metric;

    if (!bin_flag) {
        if (nullptr == request->records.get()) {
            RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'records\' is required to fill vectors");
        }
        vectors.vector_count_ = request->records->count();
        status = CopyRowRecords(request->records, vectors.float_data_);
    } else {
        if (nullptr == request->records_bin.get()) {
            RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'records_bin\' is required to fill vectors");
        }
        vectors.vector_count_ = request->records_bin->count();
        status = CopyBinRowRecords(request->records_bin, vectors.binary_data_);
    }

    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    // step 2: copy id array
    if (nullptr != request->ids.get()) {
        auto& id_array = vectors.id_array_;
        id_array.resize(request->ids->count());

        size_t i = 0;
        request->ids->forEach([&id_array, &i](const OInt64& item) { id_array[i++] = item->getValue(); });
    }

    status = request_handler_.Insert(context_ptr_, table_name->std_str(), vectors, request->tag->std_str());

    if (status.ok()) {
        ids_dto->ids = ids_dto->ids->createShared();
        for (auto& id : vectors.id_array_) {
            ids_dto->ids->pushBack(std::to_string(id).c_str());
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::Search(const OString& table_name, const SearchRequestDto::ObjectWrapper& request,
                          TopkResultsDto::ObjectWrapper& results_dto) {
    if (nullptr == request->topk.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'topk\' is required in request body")
    }
    int64_t topk_t = request->topk->getValue();

    if (nullptr == request->nprobe.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'nprobe\' is required in request body")
    }
    int64_t nprobe_t = request->nprobe->getValue();

    std::vector<std::string> tag_list;
    if (nullptr != request->tags.get()) {
        request->tags->forEach([&tag_list](const OString& tag) { tag_list.emplace_back(tag->std_str()); });
    }

    std::vector<std::string> file_id_list;
    if (nullptr != request->file_ids.get()) {
        request->file_ids->forEach([&file_id_list](const OString& id) { file_id_list.emplace_back(id->std_str()); });
    }

    TableSchema schema;
    auto status = request_handler_.DescribeTable(context_ptr_, table_name->std_str(), schema);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    auto metric = engine::MetricType(schema.metric_type_);
    bool bin_flag = engine::MetricType::HAMMING == metric || engine::MetricType::JACCARD == metric ||
                    engine::MetricType::TANIMOTO == metric;
    engine::VectorsData vectors;

    if (!bin_flag) {
        if (nullptr == request->records.get()) {
            RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'records\' is required to fill vectors");
        }
        vectors.vector_count_ = request->records->count();
        status = CopyRowRecords(request->records, vectors.float_data_);
    } else {
        if (nullptr == request->records_bin.get()) {
            RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'records_bin\' is required to fill vectors");
        }
        vectors.vector_count_ = request->records_bin->count();
        status = CopyBinRowRecords(request->records_bin, vectors.binary_data_);
    }

    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    TopKQueryResult result;
    auto context_ptr = GenContextPtr("Web Handler");
    status = request_handler_.Search(context_ptr, table_name->std_str(), vectors, topk_t, nprobe_t, tag_list,
                                     file_id_list, result);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    results_dto->num = result.row_num_;
    results_dto->results = results_dto->results->createShared();
    if (0 == result.row_num_) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    auto step = result.id_list_.size() / result.row_num_;
    for (size_t i = 0; i < result.row_num_; i++) {
        auto row_result_dto = OList<ResultDto::ObjectWrapper>::createShared();
        for (size_t j = 0; j < step; j++) {
            auto result_dto = ResultDto::createShared();
            result_dto->id = std::to_string(result.id_list_.at(i * step + j)).c_str();
            result_dto->dit = std::to_string(result.distance_list_.at(i * step + j)).c_str();
            row_result_dto->pushBack(result_dto);
        }
        results_dto->results->pushBack(row_result_dto);
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::Cmd(const OString& cmd, const OQueryParams& query_params, CommandDto::ObjectWrapper& cmd_dto) {
    std::string info = cmd->std_str();
    auto status = Status::OK();

    // TODO: (yhz) now only support load table into memory, may remove in the future
    if ("task" == info) {
        auto action = query_params.get("action");
        if (nullptr == action.get()) {
            RETURN_STATUS_DTO(QUERY_PARAM_LOSS, "Query param \'action\' is required in url \'/system/task\'");
        }
        std::string action_str = action->std_str();

        auto target = query_params.get("target");
        if (nullptr == target.get()) {
            RETURN_STATUS_DTO(QUERY_PARAM_LOSS, "Query param \'target\' is required in url \'/system/task\'");
        }
        std::string target_str = target->std_str();

        if ("load" == action_str) {
            status = request_handler_.PreloadTable(context_ptr_, target_str);
        } else {
            std::string error_msg = std::string("Unknown action value \'") + action_str + "\'";
            RETURN_STATUS_DTO(ILLEGAL_QUERY_PARAM, error_msg.c_str());
        }

        ASSIGN_RETURN_STATUS_DTO(status)
    }

    if ("info" == info) {
        info = "get_system_info";
    }

    std::string reply_str;
    status = CommandLine(info, reply_str);

    if (status.ok()) {
        cmd_dto->reply = reply_str.c_str();
    }

    ASSIGN_RETURN_STATUS_DTO(status);
}

}  // namespace web
}  // namespace server
}  // namespace milvus
