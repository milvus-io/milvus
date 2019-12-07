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

#include "server/grpc_impl/request/CreateIndexRequest.h"
#include "server/Config.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>

namespace milvus {
namespace server {
namespace grpc {

CreateIndexRequest::CreateIndexRequest(const std::shared_ptr<Context>& context,
                                       const ::milvus::grpc::IndexParam* index_param)
    : GrpcBaseRequest(context, DDL_DML_REQUEST_GROUP), index_param_(index_param) {
}

BaseRequestPtr
CreateIndexRequest::Create(const std::shared_ptr<Context>& context, const ::milvus::grpc::IndexParam* index_param) {
    if (index_param == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }
    return std::shared_ptr<GrpcBaseRequest>(new CreateIndexRequest(context, index_param));
}

Status
CreateIndexRequest::OnExecute() {
    try {
        std::string hdr = "CreateIndexRequest(table=" + index_param_->table_name() + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        std::string table_name_ = index_param_->table_name();
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

        auto& grpc_index = index_param_->index();
        status = ValidationUtil::ValidateTableIndexType(grpc_index.index_type());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexNlist(grpc_index.nlist());
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
        if (s.ok() && grpc_index.index_type() == (int)engine::EngineType::FAISS_PQ &&
            table_info.metric_type_ == (int)engine::MetricType::IP) {
            return Status(SERVER_UNEXPECTED_ERROR, "PQ not support IP in GPU version!");
        }
#endif

        // step 2: check table existence
        engine::TableIndex index;
        index.engine_type_ = grpc_index.index_type();
        index.nlist_ = grpc_index.nlist();
        status = DBWrapper::DB()->CreateIndex(table_name_, index);
        if (!status.ok()) {
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
