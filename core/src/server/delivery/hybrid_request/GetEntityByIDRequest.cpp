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

#include "server/delivery/hybrid_request/GetEntityByIDRequest.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

constexpr uint64_t MAX_COUNT_RETURNED = 1000;

GetEntityByIDRequest::GetEntityByIDRequest(const std::shared_ptr<milvus::server::Context>& context,
                                           const std::string& collection_name, std::vector<std::string>& field_names,
                                           const std::vector<int64_t>& ids, std::vector<engine::AttrsData>& attrs,
                                           std::vector<engine::VectorsData>& vectors)
    : BaseRequest(context, BaseRequest::kGetVectorByID),
      collection_name_(collection_name),
      field_names_(field_names),
      ids_(ids),
      attrs_(attrs),
      vectors_(vectors) {
}

BaseRequestPtr
GetEntityByIDRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                             const std::string& collection_name, std::vector<std::string>& field_names,
                             const std::vector<int64_t>& ids, std::vector<engine::AttrsData>& attrs,
                             std::vector<engine::VectorsData>& vectors) {
    return std::shared_ptr<BaseRequest>(
        new GetEntityByIDRequest(context, collection_name, field_names, ids, attrs, vectors));
}

Status
GetEntityByIDRequest::OnExecute() {
    try {
        std::string hdr = "GetEntitiesByIDRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        if (ids_.empty()) {
            return Status(SERVER_INVALID_ARGUMENT, "No entity id specified");
        }

        if (ids_.size() > MAX_COUNT_RETURNED) {
            std::string msg = "Input id array size cannot exceed: " + std::to_string(MAX_COUNT_RETURNED);
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }

        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // TODO(yukun) ValidateFieldNames

        // only process root collection, ignore partition collection
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;
        collection_schema.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeHybridCollection(collection_schema, fields_schema);
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

        if (field_names_.empty()) {
            for (const auto& schema : fields_schema.fields_schema_) {
                field_names_.emplace_back(schema.field_name_);
            }
        } else {
            for (const auto& name : field_names_) {
                bool find_field_name = false;
                for (const auto& schema : fields_schema.fields_schema_) {
                    if (name == schema.field_name_) {
                        find_field_name = true;
                        break;
                    }
                }
                if (not find_field_name) {
                    return Status{SERVER_INVALID_FIELD_NAME, "Field name: " + name + " is wrong"};
                }
            }
        }

        // step 2: get vector data, now only support get one id
        return DBWrapper::DB()->GetEntitiesByID(collection_name_, ids_, field_names_, vectors_, attrs_);
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
