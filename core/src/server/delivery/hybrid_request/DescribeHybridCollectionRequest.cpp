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
#include "utils/ValidationUtil.h"

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
    std::unordered_map<std::string, engine::meta::hybrid::DataType>& field_types)
    : BaseRequest(context, BaseRequest::kDescribeHybridCollection),
      collection_name_(collection_name),
      field_types_(field_types) {
}

BaseRequestPtr
DescribeHybridCollectionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                        const std::string& collection_name,
                                        std::unordered_map<std::string, engine::meta::hybrid::DataType>& field_types) {
    return std::shared_ptr<BaseRequest>(new DescribeHybridCollectionRequest(context, collection_name, field_types));
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
        if (!status.ok()) {
            return status;
        }

        for (auto schema : fields_schema.fields_schema_) {
            field_types_.insert(std::make_pair(schema.field_name_, (engine::meta::hybrid::DataType)schema.field_type_));
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
