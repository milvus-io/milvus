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

#include "config/Config.h"
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

SearchByIDRequest::SearchByIDRequest(const std::shared_ptr<milvus::server::Context>& context,
                                     const std::string& collection_name, int64_t vector_id, int64_t topk,
                                     const milvus::json& extra_params, const std::vector<std::string>& partition_list,
                                     TopKQueryResult& result)
    : BaseRequest(context, BaseRequest::kSearchByID),
      collection_name_(collection_name),
      vector_id_(vector_id),
      topk_(topk),
      extra_params_(extra_params),
      partition_list_(partition_list),
      result_(result) {
}

BaseRequestPtr
SearchByIDRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                          int64_t vector_id, int64_t topk, const milvus::json& extra_params,
                          const std::vector<std::string>& partition_list, TopKQueryResult& result) {
    return std::shared_ptr<BaseRequest>(
        new SearchByIDRequest(context, collection_name, vector_id, topk, extra_params, partition_list, result));
}

Status
SearchByIDRequest::OnExecute() {
    try {
        auto pre_query_ctx = context_->Child("Pre query");

        std::string hdr = "SearchByIDRequest(collection=" + collection_name_ + ", id=" + std::to_string(vector_id_) +
                          ", k=" + std::to_string(topk_) + ", extra_params=" + extra_params_.dump() + ")";

        TimeRecorder rc(hdr);

        // step 1: check empty id

        // step 2: check collection name
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // step 3: check search topk
        status = ValidationUtil::ValidateSearchTopk(topk_);
        if (!status.ok()) {
            return status;
        }

        // step 4: check collection existence
        // only process root collection, ignore partition collection
        engine::meta::CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeCollection(collection_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        } else {
            if (!collection_schema.owner_collection_.empty()) {
                return Status(SERVER_INVALID_COLLECTION_NAME, CollectionNotExistMsg(collection_name_));
            }
        }

        // step 5: check search parameters
        status = ValidationUtil::ValidateSearchParams(extra_params_, collection_schema, topk_);
        if (!status.ok()) {
            return status;
        }

        // step 6: check whether GPU search resource is enabled
#ifdef MILVUS_GPU_VERSION
        Config& config = Config::GetInstance();
        bool gpu_enable;
        config.GetGpuResourceConfigEnable(gpu_enable);
        if (gpu_enable) {
            std::vector<int64_t> search_resources;
            config.GetGpuResourceConfigSearchResources(search_resources);
            if (!search_resources.empty()) {
                std::string err_msg = "SearchByID cannot be executed on GPU";
                LOG_SERVER_ERROR_ << err_msg;
                return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
            }
        }
#endif

        // step 7: check collection's index type supports search by id
        if (collection_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_IDMAP &&
            collection_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_BIN_IDMAP &&
            collection_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_IVFFLAT &&
            collection_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_BIN_IVFFLAT &&
            collection_schema.engine_type_ != (int32_t)engine::EngineType::FAISS_IVFSQ8) {
            std::string err_msg = "Index type " + std::to_string(collection_schema.engine_type_) +
                                  " does not support SearchByID operation";
            LOG_SERVER_ERROR_ << err_msg;
            return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
        }

        rc.RecordSection("check validation");

        // step 8: search vectors
        engine::ResultIds result_ids;
        engine::ResultDistances result_distances;

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_by_id_" + CommonUtil::GetCurrentTimeStr() + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        pre_query_ctx->GetTraceContext()->GetSpan()->Finish();

        status = DBWrapper::DB()->QueryByID(context_, collection_name_, partition_list_, (size_t)topk_, extra_params_,
                                            vector_id_, result_ids, result_distances);

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        rc.RecordSection("search vectors from engine");
        if (!status.ok()) {
            return status;
        }

        if (result_ids.empty()) {
            return Status::OK();  // empty collection
        }

        auto post_query_ctx = context_->Child("Constructing result");

        // step 9: construct result array
        result_.row_num_ = 1;
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
