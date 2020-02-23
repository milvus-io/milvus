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

        // only process root table, ignore partition table
        engine::meta::TableSchema table_schema;
        table_schema.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
            } else {
                return status;
            }
        } else {
            if (!table_schema.owner_table_.empty()) {
                return Status(SERVER_INVALID_TABLE_NAME, TableNotExistMsg(table_name_));
            }
        }

        status = ValidationUtil::ValidateTableIndexType(index_type_);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexNlist(nlist_);
        if (!status.ok()) {
            return status;
        }

        // step 2: binary and float vector support different index/metric type, need to adapt here
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_info);

        int32_t adapter_index_type = index_type_;
        if (ValidationUtil::IsBinaryMetricType(table_info.metric_type_)) {  // binary vector not allow
            if (adapter_index_type == static_cast<int32_t>(engine::EngineType::FAISS_IDMAP)) {
                adapter_index_type = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IDMAP);
            } else if (adapter_index_type == static_cast<int32_t>(engine::EngineType::FAISS_IVFFLAT)) {
                adapter_index_type = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IVFFLAT);
            } else {
                return Status(SERVER_INVALID_INDEX_TYPE, "Invalid index type for table metric type");
            }
        }

#ifdef MILVUS_GPU_VERSION
        Status s;
        bool enable_gpu = false;
        server::Config& config = server::Config::GetInstance();
        s = config.GetGpuResourceConfigEnable(enable_gpu);
        if (s.ok() && adapter_index_type == (int)engine::EngineType::FAISS_PQ &&
            table_info.metric_type_ == (int)engine::MetricType::IP) {
            return Status(SERVER_UNEXPECTED_ERROR, "PQ not support IP in GPU version!");
        }
#endif

        rc.RecordSection("check validation");

        // step 3: create index
        engine::TableIndex index;
        index.engine_type_ = adapter_index_type;
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
