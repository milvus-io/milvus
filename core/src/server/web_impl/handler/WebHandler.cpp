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

#include <string>
#include <boost/algorithm/string.hpp>
#include <src/server/web_impl/dto/PartitionDto.hpp>

#include "server/delivery/request/BaseRequest.h"

namespace milvus {
namespace server {
namespace web {

Status
WebHandler::CreateTable(TableRequestDto::ObjectWrapper table_schema) {
    return request_handler_.CreateTable(context_ptr_,
                                        table_schema->table_name->std_str(),
                                        table_schema->dimension,
                                        table_schema->index_file_size,
                                        table_schema->metric_type);
}

/**
 * fields:
 *  - ALL: request all fields
 *  - NULL: Just check whether table exists
 *  - COUNT: request number of vectors
 *  - *,*,*...:
 */
Status
WebHandler::GetTable(const OString& table_name, const OString& fields, TableFieldsDto::ObjectWrapper& fields_dto) {
    TableSchema schema;
    Status status = request_handler_.DescribeTable(context_ptr_, table_name->std_str(), schema);

    if (!status.ok()) {
        return status;
    }

    fields_dto->schema = fields_dto->schema->createShared();

    std::vector<std::string> field_list;
    std::string fields_str = fields->std_str();
    boost::split(field_list, fields_str, boost::is_any_of(","), boost::token_compress_on);
    for (auto& str : field_list) {
        if (!str.empty()) {
            if ("table_name" == str) {
                fields_dto->schema->put("table_name", schema.table_name_.c_str());
            } else if ("dimension" == str) {
                fields_dto->schema->put("dimension", OString(schema.dimension_));
            } else if ("index_file_size" == str) {
                fields_dto->schema->put("index_file_size", OString(schema.index_file_size_));
            } else if ("metric_type" == str) {
                fields_dto->schema->put("", OString(schema.metric_type_));
            } else {
                return Status(SERVER_UNEXPECTED_ERROR, "field illegal");
            }
        }
    }

    return status;
}

BoolReplyDto::ObjectWrapper
WebHandler::hasTable(const std::string& table_name) {
    auto status_dto = StatusDto::createShared();
    bool has_table = false;
    Status status = request_handler_.HasTable(context_ptr_, table_name, has_table);

    status_dto->code = status.code();
    status_dto->message = status.message().c_str();

    auto hasTable = BoolReplyDto::createShared();
    hasTable->reply = has_table;
    hasTable->status = hasTable->status->createShared();
    hasTable->status = status_dto;

    return hasTable;
}

TableSchemaDto::ObjectWrapper
WebHandler::DescribeTable(const std::string& table_name) {
    TableSchema table_schema;
    Status status = request_handler_.DescribeTable(context_ptr_, table_name, table_schema);

    auto status_dto = StatusDto::createShared();
    status_dto->code = status.code();
    status_dto->message = status.message().c_str();

    auto describeTable = TableSchemaDto::createShared();
    describeTable->table_name = table_schema.table_name_.c_str();
    describeTable->dimension = table_schema.dimension_;
    describeTable->index_file_size = table_schema.index_file_size_;
    describeTable->metric_type = table_schema.metric_type_;

    return describeTable;
}

TableRowCountDto::ObjectWrapper
WebHandler::CountTable(const std::string& table_name) {
    int64_t count;
    Status status = request_handler_.CountTable(context_ptr_, table_name, count);

    auto status_dto = StatusDto::createShared();
    status_dto->code = status.code();
    status_dto->message = status.message().c_str();

    auto countTable = TableRowCountDto::createShared();
    countTable->status = status_dto;
    countTable->count = count;

    return countTable;
}

Status
WebHandler::ShowTables(TableNameListDto::ObjectWrapper& table_list_dto) {
    std::vector<std::string> tables;
    Status status = request_handler_.ShowTables(context_ptr_, tables);

    if (!status.ok()) {
        return status;
    }

    table_list_dto->tables = table_list_dto->tables->createShared();
    for (auto& table : tables) {
        table_list_dto->tables->pushBack(table.c_str());
    }

    return status;
}

Status
WebHandler::DropTable(const OString& table_name) {
    return request_handler_.DropTable(context_ptr_, table_name->std_str());
}

Status
WebHandler::CreateIndex(IndexRequestDto::ObjectWrapper index_param) {
//    Status status = request_handler_.CreateIndex(context_ptr_, );
}

Status
WebHandler::GetIndex(const OString& table_name, IndexDto::ObjectWrapper& index_dto) {
    IndexParam param;
    auto status = request_handler_.DescribeIndex(context_ptr_, table_name->std_str(), param);

    if (status.ok()) {
        index_dto->index_type = param.index_type_;
        index_dto->nlist = param.nlist_;
    }

    return status;
}

Status
WebHandler::DropIndex(const OString& table_name) {
    return request_handler_.DropIndex(context_ptr_, table_name->std_str());
}

Status
WebHandler::CreatePartition(const PartitionParamDto::ObjectWrapper& param) {
    return request_handler_.CreatePartition(context_ptr_,
                                            param->table_name->std_str(),
                                            param->partition_name->std_str(),
                                            param->tag->std_str());
}

Status
WebHandler::ShowPartitions(const OString& table_name, PartitionListDto::ObjectWrapper& partition_list_dto) {
    std::vector<PartitionParam> partitions;
    auto status = request_handler_.ShowPartitions(context_ptr_, table_name->std_str(), partitions);

    if (status.ok()) {
        partition_list_dto->partitions = partition_list_dto->partitions->createShared();
        for (auto& partition : partitions) {
            auto partition_dto = PartitionParamDto::createShared();
            partition_dto->table_name = partition.table_name_.c_str();
            partition_dto->partition_name = partition.partition_name_.c_str();
            partition_dto->tag = partition.tag_.c_str();

            partition_list_dto->partitions->pushBack(partition_dto);
        }
    }

    return status;
}

Status
WebHandler::DropPartition(const OString& table_name, const OString& tag) {
    return request_handler_.DropPartition(context_ptr_, table_name->std_str(), "", tag->std_str());
}

Status
WebHandler::Insert(const OString& table_name,
                   const InsertRequestDto::ObjectWrapper& param,
                   VectorIdsDto::ObjectWrapper& ids_dto) {

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

    auto status = request_handler_.Insert(context_ptr_,
                                          table_name->std_str(),
                                          param->records->count(),
                                          datas,
                                          param->tag->std_str(),
                                          ids);

    if (status.ok()) {
        ids_dto->ids = ids_dto->ids->createShared();
        for (auto& id : ids) {
            ids_dto->ids->pushBack(id);
        }
    }

    return status;
}

Status
WebHandler::Search(const OString& table_name,
                   OInt64 topk, OInt64 nprobe, OString tags,
                   const RecordsDto::ObjectWrapper& records,
                   ResultDto::ObjectWrapper& results_dto) {

    int64_t topk_t = topk->getValue();
    int64_t nprobe_t = nprobe->getValue();
    std::vector<std::string> tags_t;
    std::vector<std::string> file_ids;

//    for (auto& param : query_params.getAll()) {
//        const std::string key = param.first.std_str();
//        const std::string value = param.second.std_str();
//        if (key == "topk") {
//            topk = std::stol(value);
//        } else if (key == "nprobe") {
//            nprobe = std::stol(value);
//        } else if ("tags" == key) {
//            boost::split(tags, value, boost::is_any_of(","), boost::token_compress_on);
//        } else if ("file_ids" == key) {
//            boost::split(file_ids, value, boost::is_any_of(","), boost::token_compress_on);
//        } else {
//            return Status(SERVER_UNEXPECTED_ERROR, "Unsupported query key");
//        }
//    }

    auto tags_value = tags->std_str();
    boost::split(tags_t, tags_value, boost::is_any_of(","), boost::token_compress_on);


    boost::split(file_ids, tags_value, boost::is_any_of(","), boost::token_compress_on);

    if (-1 == topk) {
        // TODO: topk not passed. use default or return error status
        topk = 10;
    }
    if (-1 == nprobe) {
        nprobe = 16;
    }

    std::vector<float> datas;
    for (int64_t j = 0; j < records->records->count(); j++) {
        for (int64_t k = 0; k < records->records->get(j)->record->count(); k++) {
            datas.emplace_back(records->records->get(j)->record->get(k)->getValue());
        }
    }

    std::vector<Range> range_list;

    TopKQueryResult result;
    auto status = request_handler_.Search(context_ptr_,
                                          table_name->std_str(),
                                          records->records->count(),
                                          datas,
                                          range_list,
                                          topk,
                                          nprobe,
                                          tags_t,
                                          file_ids,
                                          result);

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

    return status;
}

Status
WebHandler::Cmd(const OString& cmd, OString& reply) {
    std::string reply_str;
    auto status = request_handler_.Cmd(context_ptr_, cmd->std_str(), reply_str);
    reply = reply_str.c_str();

    return status;
}

} // namespace web
} // namespace server
} // namespace milvus
