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

#include <memory>

#include <fiu-local.h>

#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#ifdef MILVUS_ENABLE_PROFILING
#include <gperftools/profiler.h>
#endif

namespace milvus {
namespace server {

SearchRequest::SearchRequest(const std::shared_ptr<milvus::server::Context>& context,
                             const std::string& collection_name, const engine::VectorsData& vectors, int64_t topk,
                             const milvus::json& extra_params, const std::vector<std::string>& partition_list,
                             const std::vector<std::string>& file_id_list, TopKQueryResult& result)
    : BaseRequest(context, BaseRequest::kSearch),
      collection_name_(collection_name),
      vectors_data_(vectors),
      topk_(topk),
      extra_params_(extra_params),
      partition_list_(partition_list),
      file_id_list_(file_id_list),
      result_(result) {
}

BaseRequestPtr
SearchRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                      const engine::VectorsData& vectors, int64_t topk, const milvus::json& extra_params,
                      const std::vector<std::string>& partition_list, const std::vector<std::string>& file_id_list,
                      TopKQueryResult& result) {
    return std::shared_ptr<BaseRequest>(
        new SearchRequest(context, collection_name, vectors, topk, extra_params, partition_list, file_id_list, result));
}

Status
SearchRequest::OnPreExecute() {
    LOG_SERVER_INFO_ << LogOut("[%s][%ld] ", "search", 0) << "Search pre-execute. Check search parameters";
    std::string hdr = "SearchRequest pre-execute(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(LogOut("[%s][%ld] %s", "search", 0, hdr.c_str()));

    milvus::server::ContextChild tracer_pre(context_, "Pre Query");
    // step 1: check collection name
    auto status = ValidationUtil::ValidateCollectionName(collection_name_);
    if (!status.ok()) {
        LOG_SERVER_ERROR_ << LogOut("[%s][%d] %s", "search", 0, status.message().c_str());
        return status;
    }

    // step 2: check search topk
    status = ValidationUtil::ValidateSearchTopk(topk_);
    if (!status.ok()) {
        LOG_SERVER_ERROR_ << LogOut("[%s][%d] %s", "search", 0, status.message().c_str());
        return status;
    }

    // step 3: check partition tags
    status = ValidationUtil::ValidatePartitionTags(partition_list_);
    fiu_do_on("SearchRequest.OnExecute.invalid_partition_tags", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        LOG_SERVER_ERROR_ << LogOut("[%s][%d] %s", "search", 0, status.message().c_str());
        return status;
    }

    return Status::OK();
}

Status
SearchRequest::OnExecute() {
    LOG_SERVER_INFO_ << LogOut("[%s][%ld] ", "search", 0) << "Search execute.";
    try {
        uint64_t vector_count = vectors_data_.vector_count_;
        fiu_do_on("SearchRequest.OnExecute.throw_std_exception", throw std::exception());
        std::string hdr = "SearchRequest execute(collection=" + collection_name_ +
                          ", nq=" + std::to_string(vector_count) + ", k=" + std::to_string(topk_) + ")";
        TimeRecorderAuto rc(LogOut("[%s][%d] %s", "search", 0, hdr.c_str()));

        // step 4: check collection existence
        // only process root collection, ignore partition collection
        collection_schema_.collection_id_ = collection_name_;
        auto status = DBWrapper::DB()->DescribeCollection(collection_schema_);

        fiu_do_on("SearchRequest.OnExecute.describe_collection_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                LOG_SERVER_ERROR_ << LogOut("[%s][%d] Collection %s not found: %s", "search", 0,
                                            collection_name_.c_str(), status.message().c_str());
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                LOG_SERVER_ERROR_ << LogOut("[%s][%d] Error occurred when describing collection %s: %s", "search", 0,
                                            collection_name_.c_str(), status.message().c_str());
                return status;
            }
        } else {
            if (!collection_schema_.owner_collection_.empty()) {
                LOG_SERVER_ERROR_ << LogOut("[%s][%d] %s", "search", 0,
                                            CollectionNotExistMsg(collection_name_).c_str());
                return Status(SERVER_INVALID_COLLECTION_NAME, CollectionNotExistMsg(collection_name_));
            }
        }

        // step 5: check search parameters
        status = ValidationUtil::ValidateSearchParams(extra_params_, collection_schema_, topk_);
        if (!status.ok()) {
            LOG_SERVER_ERROR_ << LogOut("[%s][%d] Invalid search params: %s", "search", 0, status.message().c_str());
            return status;
        }

        // step 6: check vector data according to metric type
        status = ValidationUtil::ValidateVectorData(vectors_data_, collection_schema_);
        if (!status.ok()) {
            LOG_SERVER_ERROR_ << LogOut("[%s][%d] Invalid vector data: %s", "search", 0, status.message().c_str());
            return status;
        }

        rc.RecordSection("check validation");

        // step 7: search vectors
#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_" + CommonUtil::GetCurrentTimeStr() + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        engine::ResultIds result_ids;
        engine::ResultDistances result_distances;

        if (file_id_list_.empty()) {
            status = DBWrapper::DB()->Query(context_, collection_name_, partition_list_, (size_t)topk_, extra_params_,
                                            vectors_data_, result_ids, result_distances);
        } else {
            status = DBWrapper::DB()->QueryByFileID(context_, file_id_list_, (size_t)topk_, extra_params_,
                                                    vectors_data_, result_ids, result_distances);
        }

        rc.RecordSection("query vectors from engine");

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif
        fiu_do_on("SearchRequest.OnExecute.query_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            LOG_SERVER_ERROR_ << LogOut("[%s][%d] Query fail: %s", "search", 0, status.message().c_str());
            return status;
        }
        fiu_do_on("SearchRequest.OnExecute.empty_result_ids", result_ids.clear());
        if (result_ids.empty()) {
            return Status::OK();  // empty collection
        }

        // step 8: construct result array
        milvus::server::ContextChild tracer(context_, "Constructing result");
        result_.row_num_ = vectors_data_.vector_count_;
        result_.id_list_.swap(result_ids);
        result_.distance_list_.swap(result_distances);
        rc.RecordSection("construct result");
    } catch (std::exception& ex) {
        LOG_SERVER_ERROR_ << LogOut("[%s][%d] Encounter exception: %s", "search", 0, ex.what());
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}
}  // namespace server
}  // namespace milvus
