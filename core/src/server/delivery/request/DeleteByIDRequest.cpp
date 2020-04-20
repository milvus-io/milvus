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

#include "server/delivery/request/DeleteByIDRequest.h"

#include <memory>
#include <string>
#include <vector>

#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace server {

DeleteByIDRequest::DeleteByIDRequest(const std::shared_ptr<milvus::server::Context>& context,
                                     const std::string& collection_name, const std::vector<int64_t>& vector_ids)
    : BaseRequest(context, BaseRequest::kDeleteByID), collection_name_(collection_name), vector_ids_(vector_ids) {
}

BaseRequestPtr
DeleteByIDRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                          const std::vector<int64_t>& vector_ids) {
    return std::shared_ptr<BaseRequest>(new DeleteByIDRequest(context, collection_name, vector_ids));
}

Status
DeleteByIDRequest::OnExecute() {
    try {
        TimeRecorderAuto rc("DeleteByIDRequest");

        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check collection existence
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

        // Check collection's index type supports delete
        if (collection_schema.engine_type_ == (int32_t)engine::EngineType::SPTAG_BKT ||
            collection_schema.engine_type_ == (int32_t)engine::EngineType::SPTAG_KDT) {
            std::string err_msg =
                "Index type " + std::to_string(collection_schema.engine_type_) + " does not support delete operation";
            LOG_SERVER_ERROR_ << err_msg;
            return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
        }

        rc.RecordSection("check validation");

        status = DBWrapper::DB()->DeleteVectors(collection_name_, vector_ids_);
        if (!status.ok()) {
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
