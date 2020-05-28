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
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

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
                                         const std::string& collection_name, std::vector<std::string>& partition_list,
                                         query::GeneralQueryPtr& general_query, query::QueryPtr& query_ptr,
                                         milvus::json& json_params, std::vector<std::string>& field_names,
                                         engine::QueryResult& result)
    : BaseRequest(context, BaseRequest::kHybridSearch),
      collection_name_(collection_name),
      partition_list_(partition_list),
      general_query_(general_query),
      query_ptr_(query_ptr),
      field_names_(field_names),
      result_(result) {
}

BaseRequestPtr
HybridSearchRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                            std::vector<std::string>& partition_list, query::GeneralQueryPtr& general_query,
                            query::QueryPtr& query_ptr, milvus::json& json_params,
                            std::vector<std::string>& field_names, engine::QueryResult& result) {
    return std::shared_ptr<BaseRequest>(new HybridSearchRequest(context, collection_name, partition_list, general_query,
                                                                query_ptr, json_params, field_names, result));
}

Status
HybridSearchRequest::OnExecute() {
    try {
        fiu_do_on("SearchRequest.OnExecute.throw_std_exception", throw std::exception());
        std::string hdr = "SearchRequest(table=" + collection_name_;

        TimeRecorder rc(hdr);

        // step 1: check table name
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
        // only process root table, ignore partition table
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;
        collection_schema.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeHybridCollection(collection_schema, fields_schema);
        fiu_do_on("SearchRequest.OnExecute.describe_table_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
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

        std::unordered_map<std::string, engine::meta::hybrid::DataType> attr_type;
        for (auto& field_schema : fields_schema.fields_schema_) {
            attr_type.insert(
                std::make_pair(field_schema.field_name_, (engine::meta::hybrid::DataType)field_schema.field_type_));
        }

        if (json_params.contains("field_names")) {
            if (json_params["field_names"].is_array()) {
                for (auto& name : json_params["field_names"]) {
                    field_names_.emplace_back(name.get<std::string>());
                }
            }
        } else {
            for (auto& field_schema : fields_schema.fields_schema_) {
                field_names_.emplace_back(field_schema.field_name_);
            }
        }

        status = DBWrapper::DB()->HybridQuery(context_, collection_name_, partition_list_, general_query_, query_ptr_,
                                              field_names_, attr_type, result_);

#ifdef ENABLE_CPU_PROFILING
        ProfilerStop();
#endif

        fiu_do_on("SearchRequest.OnExecute.query_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
        fiu_do_on("SearchRequest.OnExecute.empty_result_ids", result_.result_ids_.clear());
        if (result_.result_ids_.empty()) {
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
