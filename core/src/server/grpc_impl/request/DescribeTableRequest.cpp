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

#include "server/grpc_impl/request/DescribeTableRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>

namespace milvus {
namespace server {
namespace grpc {

DescribeTableRequest::DescribeTableRequest(const std::string& table_name, ::milvus::grpc::TableSchema* schema)
    : GrpcBaseRequest(INFO_REQUEST_GROUP), table_name_(table_name), schema_(schema) {
}

BaseRequestPtr
DescribeTableRequest::Create(const std::string& table_name, ::milvus::grpc::TableSchema* schema) {
    return std::shared_ptr<GrpcBaseRequest>(new DescribeTableRequest(table_name, schema));
}

Status
DescribeTableRequest::OnExecute() {
    std::string hdr = "DescribeTableRequest(table=" + table_name_ + ")";
    TimeRecorder rc(hdr);

    try {
        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: get table info
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            return status;
        }

        schema_->set_table_name(table_info.table_id_);
        schema_->set_dimension(table_info.dimension_);
        schema_->set_index_file_size(table_info.index_file_size_);
        schema_->set_metric_type(table_info.metric_type_);
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    rc.ElapseFromBegin("totally cost");

    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
