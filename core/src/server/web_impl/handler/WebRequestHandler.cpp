// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "server/web_impl/handler/WebRequestHandler.h"

#include <boost/algorithm/string.hpp>
#include <cmath>
#include <string>
#include <vector>

#include "metrics/SystemInfo.h"
#include "utils/Log.h"

#include "server/Config.h"
#include "server/delivery/request/BaseRequest.h"
#include "server/web_impl/Constants.h"
#include "server/web_impl/Types.h"
#include "server/web_impl/dto/PartitionDto.hpp"

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
        {SERVER_INVALID_TIME_RANGE, StatusCode::ILLEGAL_RANGE},
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

namespace {
Status
CopyRowRecords(const InsertRequestDto::ObjectWrapper& param, engine::VectorsData& vectors) {
    vectors.float_data_.clear();
    vectors.binary_data_.clear();
    vectors.id_array_.clear();
    vectors.vector_count_ = param->records->count();

    // step 1: copy vector data
    if (nullptr == param->records.get()) {
        return Status(SERVER_INVALID_ROWRECORD_ARRAY, "");
    }

    size_t tal_size = 0;
    for (int64_t i = 0; i < param->records->count(); i++) {
        tal_size += param->records->get(i)->count();
    }

    std::vector<float>& datas = vectors.float_data_;
    datas.resize(tal_size);
    size_t index_offset = 0;
    param->records->forEach([&datas, &index_offset](const OList<OFloat32>::ObjectWrapper& row_item) {
        row_item->forEach([&datas, &index_offset](const OFloat32& item) {
            datas[index_offset] = item->getValue();
            index_offset++;
        });
    });

    // step 2: copy id array
    if (nullptr == param->ids.get()) {
        return Status(SERVER_ILLEGAL_VECTOR_ID, "");
    }

    for (int64_t i = 0; i < param->ids->count(); i++) {
        vectors.id_array_.emplace_back(param->ids->get(i)->getValue());
    }

    return Status::OK();
}

Status
CopyRowRecords(const SearchRequestDto::ObjectWrapper& param, engine::VectorsData& vectors) {
    vectors.float_data_.clear();
    vectors.binary_data_.clear();
    vectors.id_array_.clear();
    vectors.vector_count_ = param->records->count();

    // step 1: copy vector data
    if (nullptr == param->records.get()) {
        return Status(SERVER_INVALID_ROWRECORD_ARRAY, "");
    }

    size_t tal_size = 0;
    for (int64_t i = 0; i < param->records->count(); i++) {
        tal_size += param->records->get(i)->count();
    }

    std::vector<float>& datas = vectors.float_data_;
    datas.resize(tal_size);
    size_t index_offset = 0;
    param->records->forEach([&datas, &index_offset](const OList<OFloat32>::ObjectWrapper& row_item) {
        row_item->forEach([&datas, &index_offset](const OFloat32& item) {
            datas[index_offset] = item->getValue();
            index_offset++;
        });
    });

    return Status::OK();
}

}  // namespace

///////////////////////// WebRequestHandler methods ///////////////////////////////////////

Status
WebRequestHandler::GetTaleInfo(const std::shared_ptr<Context>& context, const std::string& table_name,
                               std::map<std::string, std::string>& table_info) {
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

    table_info[KEY_TABLE_TABLE_NAME] = schema.table_name_;
    table_info[KEY_TABLE_DIMENSION] = std::to_string(schema.dimension_);
    table_info[KEY_TABLE_INDEX_METRIC_TYPE] = std::string(MetricMap.at(engine::MetricType(schema.metric_type_)));
    table_info[KEY_TABLE_INDEX_FILE_SIZE] = std::to_string(schema.index_file_size_);

    table_info[KEY_INDEX_INDEX_TYPE] = std::string(IndexMap.at(engine::EngineType(index_param.index_type_)));
    table_info[KEY_INDEX_NLIST] = std::to_string(index_param.nlist_);

    table_info[KEY_TABLE_COUNT] = std::to_string(count);
}

/////////////////////////////////////////// Router methods ////////////////////////////////////////////

StatusDto::ObjectWrapper
WebRequestHandler::GetDevices(DevicesDto::ObjectWrapper& devices_dto) {
    auto getgb = [](uint64_t x) -> uint64_t { return x / 1024 / 1024 / 1024; };
    auto system_info = SystemInfo::GetInstance();

    devices_dto->cpu = devices_dto->cpu->createShared();
    devices_dto->cpu->memory = getgb(system_info.GetPhysicalMemory());

    devices_dto->gpus = devices_dto->gpus->createShared();

#ifdef MILVUS_GPU_VERSION

    size_t count = system_info.num_device();
    std::vector<uint64_t> device_mems = system_info.GPUMemoryTotal();

    if (count != device_mems.size()) {
        ASSIGN_RETURN_STATUS_DTO(Status(UNEXPECTED_ERROR, "Can't obtain GPU info"));
    }

    for (size_t i = 0; i < count; i++) {
        auto device_dto = DeviceInfoDto::createShared();
        device_dto->memory = getgb(device_mems.at(i));
        devices_dto->gpus->put("GPU" + OString(std::to_string(i).c_str()), device_dto);
    }

#endif

    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}

StatusDto::ObjectWrapper
WebRequestHandler::GetAdvancedConfig(AdvancedConfigDto::ObjectWrapper& advanced_config) {
    Config& config = Config::GetInstance();

    int64_t value;
    auto status = config.GetCacheConfigCpuCacheCapacity(value);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }
    advanced_config->cpu_cache_capacity = value;

    bool ok;
    status = config.GetCacheConfigCacheInsertData(ok);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }
    advanced_config->cache_insert_data = ok;

    status = config.GetEngineConfigUseBlasThreshold(value);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }
    advanced_config->use_blas_threshold = value;

#ifdef MILVUS_GPU_VERSION

    status = config.GetEngineConfigGpuSearchThreshold(value);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }
    advanced_config->gpu_search_threshold = value;

#endif

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::SetAdvancedConfig(const AdvancedConfigDto::ObjectWrapper& advanced_config) {
    Config& config = Config::GetInstance();

    if (nullptr == advanced_config->cpu_cache_capacity.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'cpu_cache_capacity\' miss.");
    }
    auto status =
        config.SetCacheConfigCpuCacheCapacity(std::to_string(advanced_config->cpu_cache_capacity->getValue()));
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    if (nullptr == advanced_config->cache_insert_data.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'cache_insert_data\' miss.");
    }
    status = config.SetCacheConfigCacheInsertData(std::to_string(advanced_config->cache_insert_data->getValue()));
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    if (nullptr == advanced_config->use_blas_threshold.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'use_blas_threshold\' miss.");
    }
    status = config.SetEngineConfigUseBlasThreshold(std::to_string(advanced_config->use_blas_threshold->getValue()));
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

#ifdef MILVUS_GPU_VERSION

    if (nullptr == advanced_config->gpu_search_threshold.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'gpu_search_threshold\' miss.");
    }
    status =
        config.SetEngineConfigGpuSearchThreshold(std::to_string(advanced_config->gpu_search_threshold->getValue()));
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

#endif

    ASSIGN_RETURN_STATUS_DTO(status)
}

#ifdef MILVUS_GPU_VERSION

StatusDto::ObjectWrapper
WebRequestHandler::GetGpuConfig(GPUConfigDto::ObjectWrapper& gpu_config_dto) {
    Config& config = Config::GetInstance();

    bool enable;
    auto status = config.GetGpuResourceConfigEnable(enable);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }
    gpu_config_dto->enable = enable;

    if (!enable) {
        ASSIGN_RETURN_STATUS_DTO(Status::OK());
    }

    int64_t capacity;
    status = config.GetGpuResourceConfigCacheCapacity(capacity);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }
    gpu_config_dto->cache_capacity = capacity;

    std::vector<int64_t> values;
    status = config.GetGpuResourceConfigSearchResources(values);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    gpu_config_dto->search_resources = gpu_config_dto->search_resources->createShared();
    for (auto& device_id : values) {
        gpu_config_dto->search_resources->pushBack("GPU" + OString(std::to_string(device_id).c_str()));
    }

    values.clear();
    status = config.GetGpuResourceConfigBuildIndexResources(values);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    gpu_config_dto->build_index_resources = gpu_config_dto->build_index_resources->createShared();
    for (auto& device_id : values) {
        gpu_config_dto->build_index_resources->pushBack("GPU" + OString(std::to_string(device_id).c_str()));
    }

    ASSIGN_RETURN_STATUS_DTO(Status::OK());
}

#endif

#ifdef MILVUS_GPU_VERSION

StatusDto::ObjectWrapper
WebRequestHandler::SetGpuConfig(const GPUConfigDto::ObjectWrapper& gpu_config_dto) {
    Config& config = Config::GetInstance();

    if (nullptr == gpu_config_dto->enable.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'enable\' miss")
    }
    auto status = config.SetGpuResourceConfigEnable(std::to_string(gpu_config_dto->enable->getValue()));
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    if (!gpu_config_dto->enable->getValue()) {
        RETURN_STATUS_DTO(SUCCESS, "Set Gpu resources false");
    }

    if (nullptr == gpu_config_dto->cache_capacity.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'cache_capacity\' miss")
    }
    status = config.SetGpuResourceConfigCacheCapacity(std::to_string(gpu_config_dto->cache_capacity->getValue()));
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    if (nullptr == gpu_config_dto->search_resources.get()) {
        gpu_config_dto->search_resources = gpu_config_dto->search_resources->createShared();
        gpu_config_dto->search_resources->pushBack("GPU0");
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
    status = config.SetGpuResourceConfigSearchResources(search_resources_value);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status);
    }

    if (nullptr == gpu_config_dto->build_index_resources.get()) {
        gpu_config_dto->build_index_resources = gpu_config_dto->build_index_resources->createShared();
        gpu_config_dto->build_index_resources->pushBack("GPU0");
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

    status = config.SetGpuResourceConfigBuildIndexResources(build_resources_value);
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

    Status status = Status::OK();

    // TODO: query string field `fields` npt used here
    std::map<std::string, std::string> table_info;
    status = GetTaleInfo(context_ptr_, table_name->std_str(), table_info);
    if (!status.ok()) {
        ASSIGN_RETURN_STATUS_DTO(status)
    }

    fields_dto->table_name = table_info[KEY_TABLE_TABLE_NAME].c_str();
    fields_dto->dimension = std::stol(table_info[KEY_TABLE_DIMENSION]);
    fields_dto->index = table_info[KEY_INDEX_INDEX_TYPE].c_str();
    fields_dto->nlist = std::stol(table_info[KEY_INDEX_NLIST]);
    fields_dto->metric_type = table_info[KEY_TABLE_INDEX_METRIC_TYPE].c_str();
    fields_dto->index_file_size = std::stol(table_info[KEY_TABLE_INDEX_FILE_SIZE]);
    fields_dto->count = std::stol(table_info[KEY_TABLE_COUNT]);

    ASSIGN_RETURN_STATUS_DTO(status);
}

StatusDto::ObjectWrapper
WebRequestHandler::ShowTables(const OInt64& offset, const OInt64& page_size,
                              TableListFieldsDto::ObjectWrapper& response_dto) {
    if (nullptr == offset.get()) {
        RETURN_STATUS_DTO(QUERY_PARAM_LOSS, "Query param \'offset\' is required");
    }

    if (nullptr == page_size.get()) {
        RETURN_STATUS_DTO(QUERY_PARAM_LOSS, "Query param \'page_size\' is required");
    }
    std::vector<std::string> tables;
    Status status = Status::OK();

    response_dto->tables = response_dto->tables->createShared();

    if (offset < 0 || page_size < 0) {
        ASSIGN_RETURN_STATUS_DTO(
            Status(SERVER_UNEXPECTED_ERROR, "Query param 'offset' or 'page_size' should bigger than 0"));
    } else {
        status = request_handler_.ShowTables(context_ptr_, tables);
        if (!status.ok()) {
            ASSIGN_RETURN_STATUS_DTO(status)
        }
        if (offset < tables.size()) {
            int64_t size = (page_size->getValue() + offset->getValue() > tables.size()) ? tables.size() - offset
                                                                                        : page_size->getValue();
            for (int64_t i = offset->getValue(); i < size + offset->getValue(); i++) {
                std::map<std::string, std::string> table_info;

                status = GetTaleInfo(context_ptr_, tables.at(i), table_info);
                if (!status.ok()) {
                    break;
                }

                auto table_fields_dto = TableFieldsDto::createShared();
                table_fields_dto->table_name = table_info[KEY_TABLE_TABLE_NAME].c_str();
                table_fields_dto->dimension = std::stol(table_info[std::string(KEY_TABLE_DIMENSION)]);
                table_fields_dto->index_file_size = std::stol(table_info[std::string(KEY_TABLE_INDEX_FILE_SIZE)]);
                table_fields_dto->index = table_info[KEY_INDEX_INDEX_TYPE].c_str();
                table_fields_dto->nlist = std::stol(table_info[KEY_INDEX_NLIST]);
                table_fields_dto->metric_type = table_info[KEY_TABLE_INDEX_METRIC_TYPE].c_str();
                table_fields_dto->count = std::stol(table_info[KEY_TABLE_COUNT]);

                response_dto->tables->pushBack(table_fields_dto);
            }

            response_dto->count = tables.size();
        }
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

    auto status = request_handler_.CreatePartition(context_ptr_, table_name->std_str(),
                                                   param->partition_name->std_str(), param->partition_tag->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::ShowPartitions(const OInt64& offset, const OInt64& page_size, const OString& table_name,
                                  PartitionListDto::ObjectWrapper& partition_list_dto) {
    if (nullptr == offset.get()) {
        RETURN_STATUS_DTO(QUERY_PARAM_LOSS, "Query param \'offset\' is required!");
    }

    if (nullptr == page_size.get()) {
        RETURN_STATUS_DTO(QUERY_PARAM_LOSS, "Query param \'page_size\' is required!");
    }

    std::vector<PartitionParam> partitions;
    auto status = request_handler_.ShowPartitions(context_ptr_, table_name->std_str(), partitions);

    if (status.ok()) {
        partition_list_dto->partitions = partition_list_dto->partitions->createShared();

        if (offset->getValue() < partitions.size()) {
            int64_t size = (offset->getValue() + page_size->getValue() > partitions.size()) ? partitions.size() - offset
                                                                                            : page_size->getValue();
            for (int64_t i = offset->getValue(); i < size + offset->getValue(); i++) {
                auto partition_dto = PartitionFieldsDto::createShared();
                partition_dto->partition_name = partitions.at(i).partition_name_.c_str();
                partition_dto->partition_tag = partitions.at(i).tag_.c_str();
                partition_list_dto->partitions->pushBack(partition_dto);
            }
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::DropPartition(const OString& table_name, const OString& tag) {
    auto status = request_handler_.DropPartition(context_ptr_, table_name->std_str(), "", tag->std_str());

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::Insert(const OString& table_name, const InsertRequestDto::ObjectWrapper& param,
                          VectorIdsDto::ObjectWrapper& ids_dto) {
    engine::VectorsData vectors;
    auto status = CopyRowRecords(param, vectors);
    if (status.code() == SERVER_INVALID_ROWRECORD_ARRAY) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'records\' is required to fill vectors")
    }

    status = request_handler_.Insert(context_ptr_, table_name->std_str(), vectors, param->tag->std_str());

    if (status.ok()) {
        ids_dto->ids = ids_dto->ids->createShared();
        for (auto& id : vectors.id_array_) {
            ids_dto->ids->pushBack(std::to_string(id).c_str());
        }
    }

    ASSIGN_RETURN_STATUS_DTO(status)
}

StatusDto::ObjectWrapper
WebRequestHandler::Search(const OString& table_name, const SearchRequestDto::ObjectWrapper& search_request,
                          TopkResultsDto::ObjectWrapper& results_dto) {
    if (nullptr == search_request->topk.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'topk\' is required in request body")
    }
    int64_t topk_t = search_request->topk->getValue();

    if (nullptr == search_request->nprobe.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'nprobe\' is required in request body")
    }
    int64_t nprobe_t = search_request->nprobe->getValue();

    std::vector<std::string> tag_list;
    std::vector<std::string> file_id_list;

    if (nullptr != search_request->tags.get()) {
        search_request->tags->forEach([&tag_list](const OString& tag) { tag_list.emplace_back(tag->std_str()); });
    }

    if (nullptr != search_request->file_ids.get()) {
        search_request->file_ids->forEach(
            [&file_id_list](const OString& id) { file_id_list.emplace_back(id->std_str()); });
    }

    if (nullptr == search_request->records.get()) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'records\' is required to fill query vectors")
    }

    engine::VectorsData vectors;
    auto status = CopyRowRecords(search_request, vectors);
    if (status.code() == SERVER_INVALID_ROWRECORD_ARRAY) {
        RETURN_STATUS_DTO(BODY_FIELD_LOSS, "Field \'records\' is required to fill vectors")
    }

    std::vector<Range> range_list;

    TopKQueryResult result;
    auto context_ptr = GenContextPtr("Web Handler");
    status = request_handler_.Search(context_ptr, table_name->std_str(), vectors, range_list, topk_t, nprobe_t,
                                     tag_list, file_id_list, result);
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
WebRequestHandler::Cmd(const OString& cmd, CommandDto::ObjectWrapper& cmd_dto) {
    std::string reply_str;
    auto status = request_handler_.Cmd(context_ptr_, cmd->std_str(), reply_str);

    if (status.ok()) {
        cmd_dto->reply = reply_str.c_str();
    }

    ASSIGN_RETURN_STATUS_DTO(status);
}

}  // namespace web
}  // namespace server
}  // namespace milvus
