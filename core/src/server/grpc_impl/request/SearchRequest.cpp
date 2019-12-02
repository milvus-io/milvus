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

#include "server/grpc_impl/request/SearchRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>

namespace milvus {
namespace server {
namespace grpc {

SearchRequest::SearchRequest(const ::milvus::grpc::SearchParam* search_vector_infos,
                             const std::vector<std::string>& file_id_array, ::milvus::grpc::TopKQueryResult* response)
    : GrpcBaseRequest(DQL_REQUEST_GROUP),
      search_param_(search_vector_infos),
      file_id_array_(file_id_array),
      topk_result_(response) {
}

BaseRequestPtr
SearchRequest::Create(const ::milvus::grpc::SearchParam* search_vector_infos,
                      const std::vector<std::string>& file_id_array, ::milvus::grpc::TopKQueryResult* response) {
    if (search_vector_infos == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }
    return std::shared_ptr<GrpcBaseRequest>(new SearchRequest(search_vector_infos, file_id_array, response));
}

Status
SearchRequest::OnExecute() {
    try {
        int64_t top_k = search_param_->topk();
        int64_t nprobe = search_param_->nprobe();

        std::string hdr = "SearchRequest(table=" + search_param_->table_name() +
                          ", nq=" + std::to_string(search_param_->query_record_array_size()) +
                          ", k=" + std::to_string(top_k) + ", nprob=" + std::to_string(nprobe) + ")";
        TimeRecorder rc(hdr);

        // step 1: check table name
        std::string table_name_ = search_param_->table_name();
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
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

        // step 3: check search parameter
        status = ValidationUtil::ValidateSearchTopk(top_k, table_info);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateSearchNprobe(nprobe, table_info);
        if (!status.ok()) {
            return status;
        }

        if (search_param_->query_record_array().empty()) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                          "The vector array is empty. Make sure you have entered vector records.");
        }

        // step 4: check date range, and convert to db dates
        std::vector<DB_DATE> dates;
        std::vector<::milvus::grpc::Range> range_array;
        for (size_t i = 0; i < search_param_->query_range_array_size(); i++) {
            range_array.emplace_back(search_param_->query_range_array(i));
        }

        status = ConvertTimeRangeToDBDates(range_array, dates);
        if (!status.ok()) {
            return status;
        }

        rc.RecordSection("check validation");

        // step 5: prepare float data
        auto record_array_size = search_param_->query_record_array_size();
        std::vector<float> vec_f(record_array_size * table_info.dimension_, 0);
        for (size_t i = 0; i < record_array_size; i++) {
            if (search_param_->query_record_array(i).vector_data().empty()) {
                return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                              "The vector dimension must be equal to the table dimension.");
            }
            uint64_t query_vec_dim = search_param_->query_record_array(i).vector_data().size();
            if (query_vec_dim != table_info.dimension_) {
                ErrorCode error_code = SERVER_INVALID_VECTOR_DIMENSION;
                std::string error_msg = "The vector dimension must be equal to the table dimension.";
                return Status(error_code, error_msg);
            }

            memcpy(&vec_f[i * table_info.dimension_], search_param_->query_record_array(i).vector_data().data(),
                   table_info.dimension_ * sizeof(float));
        }
        rc.RecordSection("prepare vector data");

        // step 6: search vectors
        engine::ResultIds result_ids;
        engine::ResultDistances result_distances;
        auto record_count = (uint64_t)search_param_->query_record_array().size();

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname =
            "/tmp/search_nq_" + std::to_string(this->search_param_->query_record_array_size()) + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        if (file_id_array_.empty()) {
            std::vector<std::string> partition_tags;
            for (size_t i = 0; i < search_param_->partition_tag_array_size(); i++) {
                partition_tags.emplace_back(search_param_->partition_tag_array(i));
            }

            status = ValidationUtil::ValidatePartitionTags(partition_tags);
            if (!status.ok()) {
                return status;
            }

            status = DBWrapper::DB()->Query(table_name_, partition_tags, (size_t)top_k, record_count, nprobe,
                                            vec_f.data(), dates, result_ids, result_distances);
        } else {
            status = DBWrapper::DB()->QueryByFileID(table_name_, file_id_array_, (size_t)top_k, record_count, nprobe,
                                                    vec_f.data(), dates, result_ids, result_distances);
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

        // step 7: construct result array
        topk_result_->set_row_num(record_count);
        topk_result_->mutable_ids()->Resize(static_cast<int>(result_ids.size()), -1);
        memcpy(topk_result_->mutable_ids()->mutable_data(), result_ids.data(), result_ids.size() * sizeof(int64_t));
        topk_result_->mutable_distances()->Resize(static_cast<int>(result_distances.size()), 0.0);
        memcpy(topk_result_->mutable_distances()->mutable_data(), result_distances.data(),
               result_distances.size() * sizeof(float));

        // step 8: print time cost percent
        rc.RecordSection("construct result and send");
        rc.ElapseFromBegin("totally cost");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
