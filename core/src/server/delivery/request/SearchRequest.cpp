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

#include "server/delivery/request/SearchRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>

namespace milvus {
namespace server {

SearchRequest::SearchRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                             const engine::VectorsData& vectors, int64_t topk, int64_t nprobe,
                             const std::vector<std::string>& partition_list,
                             const std::vector<std::string>& file_id_list, TopKQueryResult& result)
    : BaseRequest(context, DQL_REQUEST_GROUP),
      table_name_(table_name),
      vectors_data_(vectors),
      topk_(topk),
      nprobe_(nprobe),
      partition_list_(partition_list),
      file_id_list_(file_id_list),
      result_(result) {
}

BaseRequestPtr
SearchRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name,
                      const engine::VectorsData& vectors, int64_t topk, int64_t nprobe,
                      const std::vector<std::string>& partition_list, const std::vector<std::string>& file_id_list,
                      TopKQueryResult& result) {
    return std::shared_ptr<BaseRequest>(
        new SearchRequest(context, table_name, vectors, topk, nprobe, partition_list, file_id_list, result));
}

Status
SearchRequest::OnExecute() {
    try {
        uint64_t vector_count = vectors_data_.vector_count_;
        auto pre_query_ctx = context_->Child("Pre query");

        std::string hdr = "SearchRequest(table=" + table_name_ + ", nq=" + std::to_string(vector_count) +
                          ", k=" + std::to_string(topk_) + ", nprob=" + std::to_string(nprobe_) + ")";

        TimeRecorder rc(hdr);

        // step 1: check table name
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
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

        // step 3: check search parameter
        status = ValidationUtil::ValidateSearchTopk(topk_, table_schema);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateSearchNprobe(nprobe_, table_schema);
        if (!status.ok()) {
            return status;
        }

        if (vectors_data_.float_data_.empty() && vectors_data_.binary_data_.empty()) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                          "The vector array is empty. Make sure you have entered vector records.");
        }

        rc.RecordSection("check validation");

        // step 4: check metric type
        if (ValidationUtil::IsBinaryMetricType(table_schema.metric_type_)) {
            // check prepared binary data
            if (vectors_data_.binary_data_.size() % vector_count != 0) {
                return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                              "The vector dimension must be equal to the table dimension.");
            }

            if (vectors_data_.binary_data_.size() * 8 / vector_count != table_schema.dimension_) {
                return Status(SERVER_INVALID_VECTOR_DIMENSION,
                              "The vector dimension must be equal to the table dimension.");
            }
        } else {
            // check prepared float data
            if (vectors_data_.float_data_.size() % vector_count != 0) {
                return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                              "The vector dimension must be equal to the table dimension.");
            }

            if (vectors_data_.float_data_.size() / vector_count != table_schema.dimension_) {
                return Status(SERVER_INVALID_VECTOR_DIMENSION,
                              "The vector dimension must be equal to the table dimension.");
            }
        }

        rc.RecordSection("prepare vector data");

        // step 5: search vectors
        engine::ResultIds result_ids;
        engine::ResultDistances result_distances;

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname =
            "/tmp/search_nq_" + std::to_string(this->search_param_->query_record_array_size()) + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        pre_query_ctx->GetTraceContext()->GetSpan()->Finish();

        if (file_id_list_.empty()) {
            status = ValidationUtil::ValidatePartitionTags(partition_list_);
            if (!status.ok()) {
                return status;
            }

            status = DBWrapper::DB()->Query(context_, table_name_, partition_list_, (size_t)topk_, nprobe_,
                                            vectors_data_, result_ids, result_distances);
        } else {
            status = DBWrapper::DB()->QueryByFileID(context_, table_name_, file_id_list_, (size_t)topk_, nprobe_,
                                                    vectors_data_, result_ids, result_distances);
        }

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
        result_.row_num_ = vector_count;
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
