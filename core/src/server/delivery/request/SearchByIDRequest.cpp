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

#include "server/delivery/request/SearchByIDRequest.h"

#include <memory>

#include "server/Config.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace server {

SearchByIDRequest::SearchByIDRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                     int64_t vector_id, int64_t topk, int64_t nprobe,
                                     const std::vector<std::string>& partition_list, TopKQueryResult& result)
    : BaseRequest(context, DQL_REQUEST_GROUP),
      table_name_(table_name),
      vector_id_(vector_id),
      topk_(topk),
      nprobe_(nprobe),
      partition_list_(partition_list),
      result_(result) {
}

BaseRequestPtr
SearchByIDRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name, int64_t vector_id,
                          int64_t topk, int64_t nprobe, const std::vector<std::string>& partition_list,
                          TopKQueryResult& result) {
    return std::shared_ptr<BaseRequest>(
        new SearchByIDRequest(context, table_name, vector_id, topk, nprobe, partition_list, result));
}

Status
SearchByIDRequest::OnExecute() {
    try {
        auto pre_query_ctx = context_->Child("Pre query");

        std::string hdr = "SearchByIDRequest(table=" + table_name_ + ", id=" + std::to_string(vector_id_) +
                          ", k=" + std::to_string(topk_) + ", nprob=" + std::to_string(nprobe_) + ")";

        TimeRecorder rc(hdr);

        // step 1: check empty id

        // step 2: check table name
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // step 3: check table existence
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

        // Check whether GPU search resource is enabled
#ifdef MILVUS_GPU_VERSION
        Config& config = Config::GetInstance();
        bool gpu_enable;
        config.GetGpuResourceConfigEnable(gpu_enable);
        if (gpu_enable) {
            std::vector<int64_t> search_resources;
            config.GetGpuResourceConfigSearchResources(search_resources);
            if (!search_resources.empty()) {
                std::string err_msg = "SearchByID cannot be executed on GPU";
                SERVER_LOG_ERROR << err_msg;
                return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
            }
        }
#endif

        // Check table's index type supports search by id
        if (table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_IDMAP &&
            table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_BIN_IDMAP &&
            table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_IVFFLAT &&
            table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_BIN_IVFFLAT &&
            table_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_IVFSQ8) {
            std::string err_msg =
                "Index type " + std::to_string(table_schema.engine_type_) + " does not support SearchByID operation";
            SERVER_LOG_ERROR << err_msg;
            return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
        }

        // step 4: check search parameter
        status = ValidationUtil::ValidateSearchTopk(topk_, table_schema);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateSearchNprobe(nprobe_, table_schema);
        if (!status.ok()) {
            return status;
        }

        rc.RecordSection("check validation");

        // step 5: search vectors
        engine::ResultIds result_ids;
        engine::ResultDistances result_distances;

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname =
            "/tmp/search_nq_" + std::to_string(this->search_param_->query_record_array_size()) + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        pre_query_ctx->GetTraceContext()->GetSpan()->Finish();

        status = DBWrapper::DB()->QueryByID(context_, table_name_, partition_list_, (size_t)topk_, nprobe_, vector_id_,
                                            result_ids, result_distances);

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        rc.RecordSection("search vectors from engine");
        if (!status.ok()) {
            return status;
        }

        if (result_ids.empty()) {
            return Status::OK();  // empty table
        }

        auto post_query_ctx = context_->Child("Constructing result");

        // step 7: construct result array
        result_.row_num_ = 1;
        result_.distance_list_ = result_distances;
        result_.id_list_ = result_ids;

        post_query_ctx->GetTraceContext()->GetSpan()->Finish();

        // step 8: print time cost percent
        rc.RecordSection("construct result and send");
        rc.ElapseFromBegin("totally cost");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
