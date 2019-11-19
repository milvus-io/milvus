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

#include "server/grpc_impl/request/ShowTablesRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {
namespace grpc {

ShowTablesRequest::ShowTablesRequest(::milvus::grpc::TableNameList* table_name_list)
    : GrpcBaseRequest(INFO_REQUEST_GROUP), table_name_list_(table_name_list) {
}

BaseRequestPtr
ShowTablesRequest::Create(::milvus::grpc::TableNameList* table_name_list) {
    return std::shared_ptr<GrpcBaseRequest>(new ShowTablesRequest(table_name_list));
}

Status
ShowTablesRequest::OnExecute() {
    std::vector<engine::meta::TableSchema> schema_array;
    auto statuts = DBWrapper::DB()->AllTables(schema_array);
    if (!statuts.ok()) {
        return statuts;
    }

    for (auto& schema : schema_array) {
        table_name_list_->add_table_names(schema.table_id_);
    }
    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
