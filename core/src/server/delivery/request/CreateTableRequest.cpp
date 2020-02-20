// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "server/delivery/request/CreateTableRequest.h"
#include "server/DBWrapper.h"
#include "server/delivery/request/BaseRequest.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>
#include <string>

namespace milvus {
namespace server {

CreateTableRequest::CreateTableRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                       int64_t dimension, int64_t index_file_size, int64_t metric_type)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP),
      table_name_(table_name),
      dimension_(dimension),
      index_file_size_(index_file_size),
      metric_type_(metric_type) {
}

BaseRequestPtr
CreateTableRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name, int64_t dimension,
                           int64_t index_file_size, int64_t metric_type) {
    return std::shared_ptr<BaseRequest>(
        new CreateTableRequest(context, table_name, dimension, index_file_size, metric_type));
}

Status
CreateTableRequest::OnExecute() {
    std::string hdr = "CreateTableRequest(table=" + table_name_ + ", dimension=" + std::to_string(dimension_) + ")";
    TimeRecorderAuto rc(hdr);

    try {
        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableDimension(dimension_);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexFileSize(index_file_size_);
        fiu_do_on("CreateTableRequest.OnExecute.invalid_index_file_size",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexMetricType(metric_type_);
        if (!status.ok()) {
            return status;
        }

        // step 2: construct table schema
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        table_info.dimension_ = static_cast<uint16_t>(dimension_);
        table_info.index_file_size_ = index_file_size_;
        table_info.metric_type_ = metric_type_;

        // some metric type only support binary vector, adapt the index type
        if (ValidationUtil::IsBinaryMetricType(metric_type_)) {
            if (table_info.engine_type_ == static_cast<int32_t>(engine::EngineType::FAISS_IDMAP)) {
                table_info.engine_type_ = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IDMAP);
            } else if (table_info.engine_type_ == static_cast<int32_t>(engine::EngineType::FAISS_IVFFLAT)) {
                table_info.engine_type_ = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IVFFLAT);
            }
        }

        // step 3: create table
        status = DBWrapper::DB()->CreateTable(table_info);
        fiu_do_on("CreateTableRequest.OnExecute.db_already_exist", status = Status(milvus::DB_ALREADY_EXIST, ""));
        fiu_do_on("CreateTableRequest.OnExecute.create_table_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        fiu_do_on("CreateTableRequest.OnExecute.throw_std_exception", throw std::exception());
        if (!status.ok()) {
            // table could exist
            if (status.code() == DB_ALREADY_EXIST) {
                return Status(SERVER_INVALID_TABLE_NAME, status.message());
            }
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
