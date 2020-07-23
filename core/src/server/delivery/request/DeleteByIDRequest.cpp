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
#include <unordered_map>
#include <vector>

#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace server {

DeleteByIDRequest::DeleteByIDRequest(const std::shared_ptr<milvus::server::Context>& context,
                                     const std::string& collection_name, const std::vector<int64_t>& entity_ids)
    : BaseRequest(context, BaseRequest::kDeleteByID), collection_name_(collection_name), entity_ids_(entity_ids) {
}

BaseRequestPtr
DeleteByIDRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                          const std::vector<int64_t>& entity_ids) {
    return std::shared_ptr<BaseRequest>(new DeleteByIDRequest(context, collection_name, entity_ids));
}

Status
DeleteByIDRequest::OnExecute() {
    try {
        TimeRecorderAuto rc("DeleteByIDRequest");

        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check collection existence
        engine::snapshot::CollectionPtr collection;
        std::unordered_map<engine::snapshot::FieldPtr, std::vector<engine::snapshot::FieldElementPtr>> fields_schema;
        status = DBWrapper::SSDB()->DescribeCollection(collection_name_, collection, fields_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        }
        // Check collection's index type supports delete
#ifdef MILVUS_SUPPORT_SPTAG
        if (collection_schema.engine_type_ == (int32_t)engine::EngineType::SPTAG_BKT ||
            collection_schema.engine_type_ == (int32_t)engine::EngineType::SPTAG_KDT) {
            std::string err_msg =
                "Index type " + std::to_string(collection_schema.engine_type_) + " does not support delete operation";
            LOG_SERVER_ERROR_ << err_msg;
            return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
        }
#endif

        rc.RecordSection("check validation");

        status = DBWrapper::SSDB()->DeleteEntities(collection_name_, entity_ids_);
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
