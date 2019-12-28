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

#include "server/delivery/request/InsertRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {

InsertRequest::InsertRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                             int64_t record_size, std::vector<float>& data_list, const std::string& partition_tag,
                             std::vector<int64_t>& id_array)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP),
      table_name_(table_name),
      record_size_(record_size),
      data_list_(data_list),
      partition_tag_(partition_tag),
      id_array_(id_array) {
}

BaseRequestPtr
InsertRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name, int64_t record_size,
                      std::vector<float>& data_list, const std::string& partition_tag, std::vector<int64_t>& id_array) {
    return std::shared_ptr<BaseRequest>(
        new InsertRequest(context, table_name, record_size, data_list, partition_tag, id_array));
}

Status
InsertRequest::OnExecute() {
    try {
        std::string hdr = "InsertRequest(table=" + table_name_ + ", n=" + std::to_string(record_size_) +
                          ", partition_tag=" + partition_tag_ + ")";
        TimeRecorder rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }
        if (data_list_.empty()) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                          "The vector array is empty. Make sure you have entered vector records.");
        }

        if (!id_array_.empty()) {
            if (id_array_.size() != record_size_) {
                return Status(SERVER_ILLEGAL_VECTOR_ID,
                              "The size of vector ID array must be equal to the size of the vector.");
            }
        }

        // step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
            } else {
                return status;
            }
        }

        // step 3: check table flag
        // all user provide id, or all internal id
        bool user_provide_ids = !id_array_.empty();
        // user already provided id before, all insert action require user id
        if ((table_info.flag_ & engine::meta::FLAG_MASK_HAS_USERID) != 0 && !user_provide_ids) {
            return Status(SERVER_ILLEGAL_VECTOR_ID,
                          "Table vector IDs are user-defined. Please provide IDs for all vectors of this table.");
        }

        // user didn't provided id before, no need to provide user id
        if ((table_info.flag_ & engine::meta::FLAG_MASK_NO_USERID) != 0 && user_provide_ids) {
            return Status(
                SERVER_ILLEGAL_VECTOR_ID,
                "Table vector IDs are auto-generated. All vectors of this table must use auto-generated IDs.");
        }

        rc.RecordSection("check validation");

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname =
            "/tmp/insert_" + std::to_string(this->insert_param_->row_record_array_size()) + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        // step 4: check prepared float data
        if (data_list_.size() % record_size_ != 0) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY, "The vector dimension must be equal to the table dimension.");
        }

        if (data_list_.size() / record_size_ != table_info.dimension_) {
            return Status(SERVER_INVALID_VECTOR_DIMENSION,
                          "The vector dimension must be equal to the table dimension.");
        }

        // step 5: insert vectors
        auto vec_count = static_cast<uint64_t>(record_size_);

        rc.RecordSection("prepare vectors data");
        status = DBWrapper::DB()->InsertVectors(table_name_, partition_tag_, vec_count, data_list_.data(), id_array_);
        if (!status.ok()) {
            return status;
        }

        auto ids_size = id_array_.size();
        if (ids_size != vec_count) {
            std::string msg =
                "Add " + std::to_string(vec_count) + " vectors but only return " + std::to_string(ids_size) + " id";
            return Status(SERVER_ILLEGAL_VECTOR_ID, msg);
        }

        // step 6: update table flag
        user_provide_ids ? table_info.flag_ |= engine::meta::FLAG_MASK_HAS_USERID
                         : table_info.flag_ |= engine::meta::FLAG_MASK_NO_USERID;
        status = DBWrapper::DB()->UpdateTableFlag(table_name_, table_info.flag_);

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        rc.RecordSection("add vectors to engine");
        rc.ElapseFromBegin("total cost");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
