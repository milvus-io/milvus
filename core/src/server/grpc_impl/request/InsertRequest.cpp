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

#include "server/grpc_impl/request/InsertRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace server {
namespace grpc {

InsertRequest::InsertRequest(const ::milvus::grpc::InsertParam* insert_param, ::milvus::grpc::VectorIds* record_ids)
    : GrpcBaseRequest(DDL_DML_REQUEST_GROUP), insert_param_(insert_param), record_ids_(record_ids) {
}

BaseRequestPtr
InsertRequest::Create(const ::milvus::grpc::InsertParam* insert_param, ::milvus::grpc::VectorIds* record_ids) {
    if (insert_param == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }
    return std::shared_ptr<GrpcBaseRequest>(new InsertRequest(insert_param, record_ids));
}

Status
InsertRequest::OnExecute() {
    try {
        std::string hdr = "InsertRequest(table=" + insert_param_->table_name() +
                          ", n=" + std::to_string(insert_param_->row_record_array_size()) + ")";
        TimeRecorder rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(insert_param_->table_name());
        if (!status.ok()) {
            return status;
        }
        if (insert_param_->row_record_array().empty()) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                          "The vector array is empty. Make sure you have entered vector records.");
        }

        if (!insert_param_->row_id_array().empty()) {
            if (insert_param_->row_id_array().size() != insert_param_->row_record_array_size()) {
                return Status(SERVER_ILLEGAL_VECTOR_ID,
                              "The size of vector ID array must be equal to the size of the vector.");
            }
        }

        // step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = insert_param_->table_name();
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(insert_param_->table_name()));
            } else {
                return status;
            }
        }

        // step 3: check table flag
        // all user provide id, or all internal id
        bool user_provide_ids = !insert_param_->row_id_array().empty();
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

        // step 4: prepare float data
        std::vector<float> vec_f(insert_param_->row_record_array_size() * table_info.dimension_, 0);

        // TODO(yk): change to one dimension array or use multiple-thread to copy the data
        for (size_t i = 0; i < insert_param_->row_record_array_size(); i++) {
            if (insert_param_->row_record_array(i).vector_data().empty()) {
                return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                              "The vector dimension must be equal to the table dimension.");
            }
            uint64_t vec_dim = insert_param_->row_record_array(i).vector_data().size();
            if (vec_dim != table_info.dimension_) {
                ErrorCode error_code = SERVER_INVALID_VECTOR_DIMENSION;
                std::string error_msg = "The vector dimension must be equal to the table dimension.";
                return Status(error_code, error_msg);
            }
            memcpy(&vec_f[i * table_info.dimension_], insert_param_->row_record_array(i).vector_data().data(),
                   table_info.dimension_ * sizeof(float));
        }

        rc.ElapseFromBegin("prepare vectors data");

        // step 5: insert vectors
        auto vec_count = static_cast<uint64_t>(insert_param_->row_record_array_size());
        std::vector<int64_t> vec_ids(insert_param_->row_id_array_size(), 0);
        if (!insert_param_->row_id_array().empty()) {
            const int64_t* src_data = insert_param_->row_id_array().data();
            int64_t* target_data = vec_ids.data();
            memcpy(target_data, src_data, static_cast<size_t>(sizeof(int64_t) * insert_param_->row_id_array_size()));
        }

        status = DBWrapper::DB()->InsertVectors(insert_param_->table_name(), insert_param_->partition_tag(), vec_count,
                                                vec_f.data(), vec_ids);
        rc.ElapseFromBegin("add vectors to engine");
        if (!status.ok()) {
            return status;
        }
        for (int64_t id : vec_ids) {
            record_ids_->add_vector_id_array(id);
        }

        auto ids_size = record_ids_->vector_id_array_size();
        if (ids_size != vec_count) {
            std::string msg =
                "Add " + std::to_string(vec_count) + " vectors but only return " + std::to_string(ids_size) + " id";
            return Status(SERVER_ILLEGAL_VECTOR_ID, msg);
        }

        // step 6: update table flag
        user_provide_ids ? table_info.flag_ |= engine::meta::FLAG_MASK_HAS_USERID
                         : table_info.flag_ |= engine::meta::FLAG_MASK_NO_USERID;
        status = DBWrapper::DB()->UpdateTableFlag(insert_param_->table_name(), table_info.flag_);

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

}  // namespace grpc
}  // namespace server
}  // namespace milvus
