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
#ifdef MILVUS_ENABLE_PROFILING
#include <gperftools/profiler.h>
#endif

namespace milvus {
namespace server {

HybridSearchRequest::HybridSearchRequest(const std::shared_ptr<milvus::server::Context>& context,
                                         context::HybridSearchContextPtr& hybrid_search_context,
                                         const std::string& collection_name, std::vector<std::string>& partition_list,
                                         milvus::query::GeneralQueryPtr& general_query, TopKQueryResult& result)
    : BaseRequest(context, BaseRequest::kHybridSearch),
      hybrid_search_contxt_(hybrid_search_context),
      collection_name_(collection_name),
      partition_list_(partition_list),
      general_query_(general_query),
      result_(result) {
}

BaseRequestPtr
HybridSearchRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                            context::HybridSearchContextPtr& hybrid_search_context, const std::string& collection_name,
                            std::vector<std::string>& partition_list, milvus::query::GeneralQueryPtr& general_query,
                            TopKQueryResult& result) {
    return std::shared_ptr<BaseRequest>(new HybridSearchRequest(context, hybrid_search_context, collection_name,
                                                                partition_list, general_query, result));
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
        for (uint64_t i = 0; i < fields_schema.fields_schema_.size(); ++i) {
            attr_type.insert(
                std::make_pair(fields_schema.fields_schema_[i].field_name_,
                               (engine::meta::hybrid::DataType)fields_schema.fields_schema_[i].field_type_));
        }

        engine::ResultIds result_ids;
        engine::ResultDistances result_distances;
        uint64_t nq;

        status = DBWrapper::DB()->HybridQuery(context_, collection_name_, partition_list_, hybrid_search_contxt_,
                                              general_query_, attr_type, nq, result_ids, result_distances);

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        fiu_do_on("SearchRequest.OnExecute.query_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
        fiu_do_on("SearchRequest.OnExecute.empty_result_ids", result_ids.clear());
        if (result_ids.empty()) {
            return Status::OK();  // empty table
        }

        auto post_query_ctx = context_->Child("Constructing result");

        // step 7: construct result array
        result_.row_num_ = nq;
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
