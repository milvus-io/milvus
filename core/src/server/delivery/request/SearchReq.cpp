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

#include "server/delivery/request/SearchReq.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu/fiu-local.h>
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

SearchReq::SearchReq(const ContextPtr& context, const query::QueryPtr& query_ptr, const milvus::json& json_params,
                     engine::snapshot::FieldElementMappings& field_mappings, engine::QueryResultPtr& result)
    : BaseReq(context, ReqType::kSearch),
      query_ptr_(query_ptr),
      json_params_(json_params),
      field_mappings_(field_mappings),
      result_(result) {
}

BaseReqPtr
SearchReq::Create(const ContextPtr& context, const query::QueryPtr& query_ptr, const milvus::json& json_params,
                  engine::snapshot::FieldElementMappings& field_mappings, engine::QueryResultPtr& result) {
    return std::shared_ptr<BaseReq>(new SearchReq(context, query_ptr, json_params, field_mappings, result));
}

Status
SearchReq::OnExecute() {
    try {
        fiu_do_on("SearchReq.OnExecute.throw_std_exception", throw std::exception());
        std::string hdr = "SearchReq(table=" + query_ptr_->collection_id;
        TimeRecorder rc(hdr);

        STATUS_CHECK(ValidateCollectionName(query_ptr_->collection_id));
        STATUS_CHECK(ValidatePartitionTags(query_ptr_->partitions));

        // step 2: check table existence
        // only process root table, ignore partition table
        engine::snapshot::CollectionPtr collection;
        engine::snapshot::FieldElementMappings fields_schema;
        auto status = DBWrapper::DB()->GetCollectionInfo(query_ptr_->collection_id, collection, fields_schema);
        fiu_do_on("SearchReq.OnExecute.describe_table_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, "Collection not exist: " + query_ptr_->collection_id);
            } else {
                return status;
            }
        }

        // step 4: Get field info
        std::unordered_map<std::string, engine::DataType> field_types;
        for (auto& schema : fields_schema) {
            auto field = schema.first;
            field_types.insert(std::make_pair(field->GetName(), field->GetFtype()));
            if (field->GetFtype() == engine::DataType::VECTOR_FLOAT ||
                field->GetFtype() == engine::DataType::VECTOR_BINARY) {
                // check dim
                int64_t dimension = field->GetParams()[engine::PARAM_DIMENSION];
                auto vector_query = query_ptr_->vectors.begin()->second;
                if (!vector_query->query_vector.binary_data.empty()) {
                    if (vector_query->query_vector.binary_data.size() !=
                        vector_query->query_vector.vector_count * dimension / 8) {
                        return Status(SERVER_INVALID_ARGUMENT, "query vector dim not match");
                    }
                } else if (!vector_query->query_vector.float_data.empty()) {
                    if (vector_query->query_vector.float_data.size() !=
                        vector_query->query_vector.vector_count * dimension) {
                        return Status(SERVER_INVALID_ARGUMENT, "query vector dim not match");
                    }
                }

                // validate search metric type and DataType match
                bool is_binary = (field->GetFtype() == engine::DataType::VECTOR_FLOAT) ? false : true;
                if (query_ptr_->metric_types.find(field->GetName()) != query_ptr_->metric_types.end()) {
                    auto metric_type = query_ptr_->metric_types.at(field->GetName());
                    STATUS_CHECK(ValidateSearchMetricType(metric_type, is_binary));
                }
            }
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
                    for (const auto& schema : fields_schema) {
                        if (name.get<std::string>() == schema.first->GetName()) {
                            find_field_name = true;
                            field_mappings_.insert(schema);
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
        status = DBWrapper::DB()->Query(context_, query_ptr_, result_);

#ifdef ENABLE_CPU_PROFILING
        ProfilerStop();
#endif

        fiu_do_on("SearchReq.OnExecute.query_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
        fiu_do_on("SearchReq.OnExecute.empty_result_ids", result_->result_ids_.clear());
        if (result_->result_ids_.empty()) {
            auto vector_query = query_ptr_->vectors.begin()->second;
            result_->row_num_ = vector_query->query_vector.vector_count;
            return Status::OK();  // empty table
        }

        auto post_query_ctx = context_->Child("Constructing result");

        // step 7: construct result array
        post_query_ctx->GetTraceContext()->GetSpan()->Finish();

        // step 8: print time cost percent
        rc.RecordSection("construct result and send");
        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
