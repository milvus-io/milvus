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

#include "server/delivery/request/BaseRequest.h"
#include "utils/Status.h"

namespace milvus {
namespace server {
namespace web {

StatusDto::ObjectWrapper
WebHandler::CreateTable(TableSchemaDto::ObjectWrapper table_schema) {
    Status status = request_handler_.CreateTable(context_ptr_,
                                                 table_schema->table_name->std_str(),
                                                 table_schema->dimension,
                                                 table_schema->index_file_size,
                                                 table_schema->metric_type);

    auto status_dto = StatusDto::createShared();

    status_dto->errorCode = status.code();
    status_dto->reason = status.message().c_str();

    return status_dto;
}

BoolReplyDto::ObjectWrapper
WebHandler::hasTable(const std::string& table_name) {
    auto status_dto = StatusDto::createShared();
    bool has_table = false;
    Status status = request_handler_.HasTable(context_ptr_, table_name, has_table);

    status_dto->errorCode = status.code();
    status_dto->reason = status.message().c_str();

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
    status_dto->errorCode = status.code();
    status_dto->reason = status.message().c_str();

    auto describeTable = TableSchemaDto::createShared();
    describeTable->status = status_dto;
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
    status_dto->errorCode = status.code();
    status_dto->reason = status.message().c_str();

    auto countTable = TableRowCountDto::createShared();
    countTable->status = status_dto;
    countTable->count = count;

    return countTable;
}

TableNameListDto::ObjectWrapper
WebHandler::ShowTables() {
    std::vector<std::string> tables;
    Status status = request_handler_.ShowTables(context_ptr_, tables);

    auto status_dto = StatusDto::createShared();
    status_dto->errorCode = status.code();
    status_dto->reason = status.message().c_str();

    auto showTables = TableNameListDto::createShared();
    showTables->status = status_dto;

}

StatusDto::ObjectWrapper
WebHandler::DropTable(const std::string& table_name) {
    Status status = request_handler_.DropTable(context_ptr_, table_name);

    auto status_dto = StatusDto::createShared();
    status_dto->errorCode = status.code();
    status_dto->reason = status.message().c_str();

    return status_dto;
}

StatusDto::ObjectWrapper
WebHandler::CreateIndex(IndexParamDto::ObjectWrapper index_param) {
    Status status = request_handler_.CreateIndex(context_ptr_, );
}

} // namespace web
} // namespace server
} // namespace milvus
