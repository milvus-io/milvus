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
                                         const std::string& collection_name, std::vector<std::string>& partition_list,
                                         query::GeneralQueryPtr& general_query, query::QueryPtr& query_ptr,
                                         milvus::json& json_params, std::vector<std::string>& field_names,
                                         engine::QueryResult& result)
    : BaseRequest(context, BaseRequest::kHybridSearch),
      collection_name_(collection_name),
      partition_list_(partition_list),
      general_query_(general_query),
      query_ptr_(query_ptr),
      json_params_(json_params),
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
        fiu_do_on("HybridSearchRequest.OnExecute.throw_std_exception", throw std::exception());
        std::string hdr = "SearchRequest(table=" + collection_name_;

        TimeRecorder rc(hdr);

        // step 1: check table name
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
        // only process root table, ignore partition table
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;
        collection_schema.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeHybridCollection(collection_schema, fields_schema);
        fiu_do_on("HybridSearchRequest.OnExecute.describe_table_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
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

        // step 3: check partition tags
        status = ValidatePartitionTags(partition_list_);
        fiu_do_on("HybridSearchRequest.OnExecute.invalid_partition_tags",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            LOG_SERVER_ERROR_ << LogOut("[%s][%ld] %s", "search", 0, status.message().c_str());
            return status;
        }

        // step 3: check field names
        if (json_params_.contains("fields")) {
            if (json_params_["fields"].is_array()) {
                for (auto& name : json_params_["fields"]) {
                    status = ValidateFieldName(name.get<std::string>());
                    if (!status.ok()) {
                        return status;
                    }
                    bool find_field_name = false;
                    for (const auto& schema : fields_schema.fields_schema_) {
                        if (name.get<std::string>() == schema.field_name_) {
                            find_field_name = true;
                            break;
                        }
                    }
                    if (not find_field_name) {
                        return Status{SERVER_INVALID_FIELD_NAME, "Field: " + name.get<std::string>() + " not exist"};
                    }
                    field_names_.emplace_back(name.get<std::string>());
                }
            }
        }

        std::unordered_map<std::string, engine::meta::hybrid::DataType> attr_type;
        for (auto& field_schema : fields_schema.fields_schema_) {
            attr_type.insert(
                std::make_pair(field_schema.field_name_, (engine::meta::hybrid::DataType)field_schema.field_type_));
        }

        result_.row_num_ = 0;
        status = DBWrapper::DB()->HybridQuery(context_, collection_name_, partition_list_, general_query_, query_ptr_,
                                              field_names_, attr_type, result_);

#ifdef ENABLE_CPU_PROFILING
        ProfilerStop();
#endif

        fiu_do_on("HybridSearchRequest.OnExecute.query_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
        fiu_do_on("HybridSearchRequest.OnExecute.empty_result_ids", result_.result_ids_.clear());
        if (result_.result_ids_.empty()) {
            auto vector_query = query_ptr_->vectors.begin()->second;
            if (!vector_query->query_vector.binary_data.empty()) {
                result_.row_num_ = vector_query->query_vector.binary_data.size() * 8 / collection_schema.dimension_;
            } else if (!vector_query->query_vector.float_data.empty()) {
                result_.row_num_ = vector_query->query_vector.float_data.size() / collection_schema.dimension_;
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
