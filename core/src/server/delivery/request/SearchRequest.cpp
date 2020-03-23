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

#include "server/delivery/request/SearchRequest.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>

#ifdef MILVUS_ENABLE_PROFILING
#include <gperftools/profiler.h>
#endif

namespace milvus {
namespace server {

SearchRequest::SearchRequest(const std::shared_ptr<milvus::server::Context>& context, const std::string& table_name,
                             const engine::VectorsData& vectors, int64_t topk, const milvus::json& extra_params,
                             const std::vector<std::string>& partition_list,
                             const std::vector<std::string>& file_id_list, TopKQueryResult& result)
    : BaseRequest(context, BaseRequest::kSearch),
      table_name_(table_name),
      vectors_data_(vectors),
      topk_(topk),
      extra_params_(extra_params),
      partition_list_(partition_list),
      file_id_list_(file_id_list),
      result_(result) {
}

BaseRequestPtr
SearchRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& table_name,
                      const engine::VectorsData& vectors, int64_t topk, const milvus::json& extra_params,
                      const std::vector<std::string>& partition_list, const std::vector<std::string>& file_id_list,
                      TopKQueryResult& result) {
    return std::shared_ptr<BaseRequest>(
        new SearchRequest(context, table_name, vectors, topk, extra_params, partition_list, file_id_list, result));
}

Status
SearchRequest::OnExecute() {
    try {
        fiu_do_on("SearchRequest.OnExecute.throw_std_exception", throw std::exception());
        uint64_t vector_count = vectors_data_.vector_count_;
        auto pre_query_ctx = context_->Child("Pre query");

        SERVER_LOG_DEBUG << "SearchRequest begin execute, extra_params=" << extra_params_.dump();
        std::string hdr = "SearchRequest(table=" + table_name_ + ", nq=" + std::to_string(vector_count) +
                          ", k=" + std::to_string(topk_) + ")";

        TimeRecorder rc(hdr);

        // step 1: check table name
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check search topk
        status = ValidationUtil::ValidateSearchTopk(topk_);
        if (!status.ok()) {
            return status;
        }

        // step 3: check table existence
        // only process root table, ignore partition table
        engine::meta::TableSchema table_schema;
        table_schema.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_schema);
        fiu_do_on("SearchRequest.OnExecute.describe_table_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
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

        // step 4: check search parameters
        status = ValidationUtil::ValidateSearchParams(extra_params_, table_schema, topk_);
        if (!status.ok()) {
            return status;
        }

        // step 5: check vector data according to metric type
        status = ValidationUtil::ValidateVectorData(vectors_data_, table_schema);
        if (!status.ok()) {
            return status;
        }

        // step 6: check partition tags
        status = ValidationUtil::ValidatePartitionTags(partition_list_);
        fiu_do_on("SearchRequest.OnExecute.invalid_partition_tags",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }

        rc.RecordSection("check validation");

        // step 7: search vectors
        engine::ResultIds result_ids;
        engine::ResultDistances result_distances;

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_" + CommonUtil::GetCurrentTimeStr() + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        pre_query_ctx->GetTraceContext()->GetSpan()->Finish();

        if (file_id_list_.empty()) {
            status = DBWrapper::DB()->Query(context_, table_name_, partition_list_, (size_t)topk_, extra_params_,
                                            vectors_data_, result_ids, result_distances);
        } else {
            status = DBWrapper::DB()->QueryByFileID(context_, file_id_list_, (size_t)topk_, extra_params_,
                                                    vectors_data_, result_ids, result_distances);
        }

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        rc.RecordSection("search vectors from engine");
        fiu_do_on("SearchRequest.OnExecute.query_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
        fiu_do_on("SearchRequest.OnExecute.empty_result_ids", result_ids.clear());
        if (result_ids.empty()) {
            return Status::OK();  // empty table
        }

        auto post_query_ctx = context_->Child("Constructing result");

        // step 8: construct result array
        result_.row_num_ = vector_count;
        result_.distance_list_ = result_distances;
        result_.id_list_ = result_ids;

        post_query_ctx->GetTraceContext()->GetSpan()->Finish();

        rc.RecordSection("construct result and send");
        rc.ElapseFromBegin("totally cost");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
