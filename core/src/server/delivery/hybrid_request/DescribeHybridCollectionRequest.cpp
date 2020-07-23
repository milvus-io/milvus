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

#include "server/delivery/hybrid_request/DescribeHybridCollectionRequest.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/delivery/request/BaseRequest.h"
#include "server/web_impl/Constants.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace milvus {
namespace server {

DescribeHybridCollectionRequest::DescribeHybridCollectionRequest(
    const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
    HybridCollectionSchema& collection_schema)
    : BaseRequest(context, BaseRequest::kDescribeHybridCollection),
      collection_name_(collection_name),
      collection_schema_(collection_schema) {
}

BaseRequestPtr
DescribeHybridCollectionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                        const std::string& collection_name, HybridCollectionSchema& collection_schema) {
    return std::shared_ptr<BaseRequest>(
        new DescribeHybridCollectionRequest(context, collection_name, collection_schema));
}

Status
DescribeHybridCollectionRequest::OnExecute() {
    std::string hdr = "CreateCollectionRequest(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    try {
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;
        collection_schema.collection_id_ = collection_name_;
        auto status = DBWrapper::DB()->DescribeHybridCollection(collection_schema, fields_schema);
        fiu_do_on("DescribeHybridCollectionRequest.OnExecute.invalid_db_execute",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }

        for (const auto& schema : fields_schema.fields_schema_) {
            auto field_name = schema.field_name_;
            collection_schema_.field_types_.insert(
                std::make_pair(field_name, (engine::meta::hybrid::DataType)schema.field_type_));
            milvus::json json_index_param = milvus::json::parse(schema.index_param_);
            collection_schema_.index_params_.insert(std::make_pair(field_name, json_index_param));
            milvus::json json_extra_param = milvus::json::parse(schema.field_params_);
            collection_schema_.field_params_.insert(std::make_pair(field_name, json_extra_param));
        }
        collection_schema_.extra_params_["segment_size"] = collection_schema.index_file_size_ / engine::MB;
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
