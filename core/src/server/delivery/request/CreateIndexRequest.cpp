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

#include "server/delivery/request/CreateIndexRequest.h"
#include "server/Config.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>

namespace milvus {
namespace server {

CreateIndexRequest::CreateIndexRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                       int64_t index_type, int64_t nlist)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP), table_name_(table_name), index_type_(index_type), nlist_(nlist) {
}

BaseRequestPtr
CreateIndexRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name, int64_t index_type,
                           int64_t nlist) {
    return std::shared_ptr<BaseRequest>(new CreateIndexRequest(context, table_name, index_type, nlist));
}

Status
CreateIndexRequest::OnExecute() {
    try {
        std::string hdr = "CreateIndexRequest(table=" + table_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        bool has_table = false;
        status = DBWrapper::DB()->HasTable(table_name_, has_table);
        if (!status.ok()) {
            return status;
        }

        if (!has_table) {
            return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
        }

        status = ValidationUtil::ValidateTableIndexType(index_type_);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexNlist(nlist_);
        if (!status.ok()) {
            return status;
        }

#ifdef MILVUS_GPU_VERSION
        Status s;
        bool enable_gpu = false;
        server::Config& config = server::Config::GetInstance();
        s = config.GetGpuResourceConfigEnable(enable_gpu);
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (s.ok() && index_type_ == (int)engine::EngineType::FAISS_PQ &&
            table_info.metric_type_ == (int)engine::MetricType::IP) {
            return Status(SERVER_UNEXPECTED_ERROR, "PQ not support IP in GPU version!");
        }
#endif

        // step 2: check table existence
        engine::TableIndex index;
        index.engine_type_ = index_type_;
        index.nlist_ = nlist_;
        status = DBWrapper::DB()->CreateIndex(table_name_, index);
        if (!status.ok()) {
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
