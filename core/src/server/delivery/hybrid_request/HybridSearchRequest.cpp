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

#include "server/delivery/hybrid_request/HybridSearchRequest.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#ifdef ENABLE_CPU_PROFILING
#include <gperftools/profiler.h>
#endif

namespace milvus {
namespace server {

HybridSearchRequest::HybridSearchRequest(const std::shared_ptr<milvus::server::Context>& context,
                                         const query::QueryPtr& query_ptr, const milvus::json& json_params,
                                         engine::QueryResultPtr& result)
    : BaseRequest(context, BaseRequest::kHybridSearch),
      query_ptr_(query_ptr),
      json_params_(json_params),
      result_(result) {
}

BaseRequestPtr
HybridSearchRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const query::QueryPtr& query_ptr,
                            const milvus::json& json_params, engine::QueryResultPtr& result) {
    return std::shared_ptr<BaseRequest>(new HybridSearchRequest(context, query_ptr, json_params, result));
}

Status
HybridSearchRequest::OnExecute() {
    try {
        fiu_do_on("HybridSearchRequest.OnExecute.throw_std_exception", throw std::exception());
        std::string hdr = "SearchRequest(table=" + query_ptr_->collection_id;

        TimeRecorder rc(hdr);

        // step 1: check table name
        auto status = ValidateCollectionName(query_ptr_->collection_id);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
        // only process root table, ignore partition table
        engine::snapshot::CollectionPtr collection;
        engine::snapshot::CollectionMappings fields_schema;
        status = DBWrapper::SSDB()->DescribeCollection(query_ptr_->collection_id, collection, fields_schema);
        fiu_do_on("HybridSearchRequest.OnExecute.describe_table_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(query_ptr_->collection_id));
            } else {
                return status;
            }
        }

        int64_t dimension = collection->GetParams()[engine::DIMENSION].get<int64_t>();

        // step 3: check partition tags
        status = ValidatePartitionTags(query_ptr_->partitions);
        fiu_do_on("HybridSearchRequest.OnExecute.invalid_partition_tags",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            LOG_SERVER_ERROR_ << LogOut("[%s][%ld] %s", "search", 0, status.message().c_str());
            return status;
        }

        // step 4: Get field info
        std::unordered_map<std::string, engine::meta::hybrid::DataType> field_types;
        for (auto& schema : fields_schema) {
            field_types.insert(
                std::make_pair(schema.first->GetName(), (engine::meta::hybrid::DataType)schema.first->GetFtype()));
        }

        // step 5: check field names
        if (json_params_.contains("fields")) {
            if (json_params_["fields"].is_array()) {
                for (auto& name : json_params_["fields"]) {
                    status = ValidateFieldName(name.get<std::string>());
                    if (!status.ok()) {
                        return status;
                    }
                    bool find_field_name = false;
                    for (const auto& schema : field_types) {
                        if (name.get<std::string>() == schema.first) {
                            find_field_name = true;
                            break;
                        }
                    }
                    if (not find_field_name) {
                        return Status{SERVER_INVALID_FIELD_NAME, "Field: " + name.get<std::string>() + " not exist"};
                    }
                    query_ptr_->field_names.emplace_back(name.get<std::string>());
                }
            }
        }

        result_->row_num_ = 0;
        status = DBWrapper::SSDB()->Query(context_, query_ptr_, result_);

#ifdef ENABLE_CPU_PROFILING
        ProfilerStop();
#endif

        fiu_do_on("HybridSearchRequest.OnExecute.query_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
        fiu_do_on("HybridSearchRequest.OnExecute.empty_result_ids", result_->result_ids_.clear());
        if (result_->result_ids_.empty()) {
            auto vector_query = query_ptr_->vectors.begin()->second;
            if (!vector_query->query_vector.binary_data.empty()) {
                result_->row_num_ = vector_query->query_vector.binary_data.size() * 8 / dimension;
            } else if (!vector_query->query_vector.float_data.empty()) {
                result_->row_num_ = vector_query->query_vector.float_data.size() / dimension;
            }
            return Status::OK();  // empty table
        }

        auto post_query_ctx = context_->Child("Constructing result");

        // step 7: construct result array
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
