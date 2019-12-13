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

#include "utils/Status.h"

namespace milvus {
namespace server {
namespace web {

StatusDto::ObjectWrapper
WebHandler::CreateTable(const std::string& table_name, int64_t dimension,
                        int64_t index_file_size, int64_t metric_type) {
    auto status_dto = StatusDto::createShared();

    Status status = request_handler_.CreateTable(context_ptr_, table_name, dimension, index_file_size, metric_type);
    status_dto->errorCode = status.code();
    status_dto->reason = status.message().c_str();

    return status_dto;
}

HasTableDto::ObjectWrapper
WebHandler::hasTable(const std::string& tableName) {
    auto status_dto = StatusDto::createShared();
    bool has_table = false;
    Status status = request_handler_.HasTable(context_ptr_, tableName, has_table);

    status_dto->errorCode = status.code();
    status_dto->reason = status.message().c_str();

    auto hasTable = HasTableDto::createShared();
    hasTable->reply = has_table;
    hasTable->status = hasTable->status->createShared();
    hasTable->status = status_dto;

    return hasTable;
}

} // namespace web
} // namespace server
} // namespace milvus
