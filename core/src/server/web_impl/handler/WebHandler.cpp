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

#include "server/web_impl/handler/WebHandler.h"

#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>

#include "server/delivery/request/BaseRequest.h"
#include "server/web_impl/dto/PartitionDto.hpp"

namespace milvus {
namespace server {
namespace web {

void
WebHandler::CreateTable(const TableRequestDto::ObjectWrapper& table_schema, StatusDto::ObjectWrapper& status_dto) {
    auto status =
        request_handler_.CreateTable(context_ptr_, table_schema->table_name->std_str(), table_schema->dimension,
                                     table_schema->index_file_size, table_schema->metric_type);

    ASSIGN_STATUS_DTO(status_dto, status);
}

/**
 * fields:
 *  - ALL: request all fields
 *  - NULL: Just check whether table exists
 *  - COUNT: request number of vectors
 *  - *,*,*...:
 */
void
WebHandler::GetTable(const OString& table_name, const OQueryParams& query_params, StatusDto::ObjectWrapper& status_dto,
                     TableFieldsDto::ObjectWrapper& fields_dto) {
    Status status = Status::OK();

    fields_dto->schema = fields_dto->schema->createShared();

    if (query_params.getSize() == 0) {
        TableSchema schema;
        status = request_handler_.DescribeTable(context_ptr_, table_name->std_str(), schema);
        if (status.ok()) {
            fields_dto->schema->put("table_name", schema.table_name_.c_str());
            fields_dto->schema->put("dimension", OString(std::to_string(schema.dimension_).c_str()));
            fields_dto->schema->put("index_file_size", OString(std::to_string(schema.index_file_size_).c_str()));
            fields_dto->schema->put("metric_type", OString(std::to_string(schema.metric_type_).c_str()));
        }
    } else {
        for (auto& param : query_params.getAll()) {
            std::string key = param.first.std_str();
            std::string value = param.second.std_str();

            if ("fields" == key) {
                if ("num" == value) {
                    int64_t count;
                    status = request_handler_.CountTable(context_ptr_, table_name->std_str(), count);
                    if (status.ok()) {
                        fields_dto->schema->put("num", OString(std::to_string(count).c_str()));
                    }
                }
            }
        }
    }

    ASSIGN_STATUS_DTO(status_dto, status);
}

void
WebHandler::ShowTables(const OInt64& offset, const OInt64& page_size, StatusDto::ObjectWrapper& status_dto,
                       TableListDto::ObjectWrapper& table_list_dto) {
    std::vector<std::string> tables;
    Status status = Status::OK();

    if (offset < 0 || page_size < 0) {
        status = Status(SERVER_UNEXPECTED_ERROR, " Query param offset or page_size should bigger than 0");
    } else {
        status = request_handler_.ShowTables(context_ptr_, tables);
        if (status.ok()) {
            table_list_dto->tables = table_list_dto->tables->createShared();
            if (offset + 1 < tables.size()) {
                int64_t size = (page_size->getValue() + offset->getValue() > tables.size()) ? tables.size()
                                                                                            : page_size->getValue();
                for (int64_t i = 0; i < size; i++) {
                    table_list_dto->tables->pushBack(tables.at(i).c_str());
                }
            }
        }
    }

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::DropTable(const OString& table_name, StatusDto::ObjectWrapper& status_dto) {
    auto status = request_handler_.DropTable(context_ptr_, table_name->std_str());

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::CreateIndex(const OString& table_name, const IndexRequestDto::ObjectWrapper& index_param,
                        StatusDto::ObjectWrapper& status_dto) {
    auto status = request_handler_.CreateIndex(context_ptr_, table_name->std_str(), index_param->index_type->getValue(),
                                               index_param->nlist->getValue());

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::GetIndex(const OString& table_name, StatusDto::ObjectWrapper& status_dto,
                     IndexDto::ObjectWrapper& index_dto) {
    IndexParam param;
    auto status = request_handler_.DescribeIndex(context_ptr_, table_name->std_str(), param);

    if (status.ok()) {
        index_dto->index_type = param.index_type_;
        index_dto->nlist = param.nlist_;
    }

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::DropIndex(const OString& table_name, StatusDto::ObjectWrapper& status_dto) {
    auto status = request_handler_.DropIndex(context_ptr_, table_name->std_str());

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::CreatePartition(const OString& table_name, const PartitionRequestDto::ObjectWrapper& param,
                            StatusDto::ObjectWrapper& status_dto) {
    auto status = request_handler_.CreatePartition(context_ptr_, table_name->std_str(),
                                                   param->partition_name->std_str(), param->tag->std_str());

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::ShowPartitions(const OInt64& offset, const OInt64& page_size, const OString& table_name,
                           StatusDto::ObjectWrapper& status_dto, PartitionListDto::ObjectWrapper& partition_list_dto) {
    std::vector<PartitionParam> partitions;
    auto status = request_handler_.ShowPartitions(context_ptr_, table_name->std_str(), partitions);

    if (status.ok()) {
        partition_list_dto->partitions = partition_list_dto->partitions->createShared();

        if (offset->getValue() + 1 < partitions.size()) {
            int64_t size = (offset->getValue() + page_size->getValue() > partitions.size()) ? partitions.size()
                                                                                            : page_size->getValue();

            for (int64_t i = 0; i < size; i++) {
                auto partition_dto = PartitionParamDto::createShared();
                partition_dto->table_name = partitions.at(i + offset).table_name_.c_str();
                partition_dto->partition_name = partitions.at(i + offset).partition_name_.c_str();
                partition_dto->tag = partitions.at(i + offset).tag_.c_str();

                partition_list_dto->partitions->pushBack(partition_dto);
            }
        }
    }

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::DropPartition(const OString& table_name, const OString& tag, StatusDto::ObjectWrapper& status_dto) {
    auto status = request_handler_.DropPartition(context_ptr_, table_name->std_str(), "", tag->std_str());

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::Insert(const OQueryParams& query_params, const InsertRequestDto::ObjectWrapper& param,
                   StatusDto::ObjectWrapper& status_dto, VectorIdsDto::ObjectWrapper& ids_dto) {
    std::vector<int64_t> ids;
    if (param->ids->count() > 0) {
        for (int64_t i = 0; i < param->ids->count(); i++) {
            ids.emplace_back(param->ids->get(i)->getValue());
        }
    }

    std::vector<float> datas;
    for (int64_t j = 0; j < param->records->count(); j++) {
        for (int64_t k = 0; k < param->records->get(j)->record->count(); k++) {
            datas.emplace_back(param->records->get(j)->record->get(k)->getValue());
        }
    }

    std::string table_name;
    std::string tag;
    for (auto& query_param : query_params.getAll()) {
        std::string key = query_param.first.std_str();
        std::string value = query_param.second.std_str();

        if ("table_name" == key) {
            table_name = value;
        } else if ("tag" == key) {
            tag = value;
        }
    }

    auto status = request_handler_.Insert(context_ptr_, table_name, param->records->count(), datas, tag, ids);

    if (status.ok()) {
        ids_dto->ids = ids_dto->ids->createShared();
        for (auto& id : ids) {
            ids_dto->ids->pushBack(id);
        }
    }

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::Search(const OString& table_name, const OInt64& topk, const OInt64& nprobe,
                   const OQueryParams& query_params, const RecordsDto::ObjectWrapper& records,
                   StatusDto::ObjectWrapper& status_dto, ResultDto::ObjectWrapper& results_dto) {
    int64_t topk_t = topk->getValue();
    int64_t nprobe_t = nprobe->getValue();

    std::vector<std::string> tag_list;
    std::vector<std::string> file_id_list;

    for (auto& param : query_params.getAll()) {
        std::string key = param.first.std_str();
        std::string value = param.second.std_str();

        if ("tags" == key) {
            boost::split(tag_list, value, boost::is_any_of(","), boost::token_compress_on);
        } else if ("file_ids" == key) {
            boost::split(file_id_list, value, boost::is_any_of(","), boost::token_compress_on);
        }
    }

    std::vector<float> datas;
    for (int64_t j = 0; j < records->records->count(); j++) {
        for (int64_t k = 0; k < records->records->get(j)->record->count(); k++) {
            datas.emplace_back(records->records->get(j)->record->get(k)->getValue());
        }
    }

    std::vector<Range> range_list;

    TopKQueryResult result;
    auto context_ptr = MockContextPtr("Search");
    auto status = request_handler_.Search(context_ptr, table_name->std_str(), records->records->count(), datas,
                                          range_list, topk_t, nprobe_t, tag_list, file_id_list, result);

    if (status.ok()) {
        results_dto->ids = results_dto->ids->createShared();
        for (auto& id : result.id_list_) {
            results_dto->ids->pushBack(id);
        }
        results_dto->dits = results_dto->dits->createShared();
        for (auto& dit : result.distance_list_) {
            results_dto->dits->pushBack(dit);
        }
        results_dto->num = result.row_num_;
    }

    ASSIGN_STATUS_DTO(status_dto, status)
}

void
WebHandler::Cmd(const OString& cmd, StatusDto::ObjectWrapper& status_dto, CommandDto::ObjectWrapper& cmd_dto) {
    std::string reply_str;
    auto status = request_handler_.Cmd(context_ptr_, cmd->std_str(), reply_str);

    if (status.ok()) {
        cmd_dto->reply = reply_str.c_str();
    }

    ASSIGN_STATUS_DTO(status_dto, status)
}

}  // namespace web
}  // namespace server
}  // namespace milvus
