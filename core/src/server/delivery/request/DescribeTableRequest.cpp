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

#include "server/delivery/request/DescribeTableRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>

namespace milvus {
namespace server {

DescribeTableRequest::DescribeTableRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                           TableSchema& schema)
    : BaseRequest(context, INFO_REQUEST_GROUP), table_name_(table_name), schema_(schema) {
}

BaseRequestPtr
DescribeTableRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name,
                             TableSchema& schema) {
    return std::shared_ptr<BaseRequest>(new DescribeTableRequest(context, table_name, schema));
}

Status
DescribeTableRequest::OnExecute() {
    std::string hdr = "DescribeTableRequest(table=" + table_name_ + ")";
    TimeRecorderAuto rc(hdr);

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

        schema_.table_name_ = table_info.table_id_;
        schema_.dimension_ = static_cast<int64_t>(table_info.dimension_);
        schema_.index_file_size_ = table_info.index_file_size_;
        schema_.metric_type_ = table_info.metric_type_;
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
