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

#include "server/delivery/request/DescribeCollectionRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>

namespace milvus {
namespace server {

DescribeCollectionRequest::DescribeCollectionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                                     const std::string& collection_name, CollectionSchema& schema)
    : BaseRequest(context, BaseRequest::kDescribeCollection), collection_name_(collection_name), schema_(schema) {
}

BaseRequestPtr
DescribeCollectionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                  const std::string& collection_name, CollectionSchema& schema) {
    return std::shared_ptr<BaseRequest>(new DescribeCollectionRequest(context, collection_name, schema));
}

Status
DescribeCollectionRequest::OnExecute() {
    std::string hdr = "DescribeCollectionRequest(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    try {
        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: get collection info
        // only process root collection, ignore partition collection
        engine::meta::CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeCollection(collection_schema);
        fiu_do_on("DescribeCollectionRequest.OnExecute.describe_collection_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        fiu_do_on("DescribeCollectionRequest.OnExecute.throw_std_exception", throw std::exception());
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

        schema_.collection_name_ = collection_schema.collection_id_;
        schema_.dimension_ = static_cast<int64_t>(collection_schema.dimension_);
        schema_.index_file_size_ = collection_schema.index_file_size_;
        schema_.metric_type_ = collection_schema.metric_type_;
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
