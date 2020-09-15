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

#include "server/delivery/request/GetEntityByIDReq.h"

#include "db/Types.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

constexpr uint64_t MAX_COUNT_RETURNED = 1000;

GetEntityByIDReq::GetEntityByIDReq(const ContextPtr& context, const std::string& collection_name,
                                   const engine::IDNumbers& id_array, std::vector<std::string>& field_names,
                                   std::vector<bool>& valid_row, engine::snapshot::FieldElementMappings& field_mappings,
                                   engine::DataChunkPtr& data_chunk)
    : BaseReq(context, ReqType::kGetEntityByID),
      collection_name_(collection_name),
      id_array_(id_array),
      field_names_(field_names),
      valid_row_(valid_row),
      field_mappings_(field_mappings),
      data_chunk_(data_chunk) {
}

BaseReqPtr
GetEntityByIDReq::Create(const ContextPtr& context, const std::string& collection_name,
                         const engine::IDNumbers& id_array, std::vector<std::string>& field_names_,
                         std::vector<bool>& valid_row, engine::snapshot::FieldElementMappings& field_mappings,
                         engine::DataChunkPtr& data_chunk) {
    return std::shared_ptr<BaseReq>(
        new GetEntityByIDReq(context, collection_name, id_array, field_names_, valid_row, field_mappings, data_chunk));
}

Status
GetEntityByIDReq::OnExecute() {
    try {
        std::string hdr = "GetEntityByIDReq(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        if (id_array_.empty()) {
            return Status(SERVER_INVALID_ARGUMENT, "No entity id specified");
        }

        if (id_array_.size() > MAX_COUNT_RETURNED) {
            std::string msg = "Input id array size cannot exceed: " + std::to_string(MAX_COUNT_RETURNED);
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }

        STATUS_CHECK(ValidateCollectionName(collection_name_));

        // TODO(yukun) ValidateFieldNames

        // only process root collection, ignore partition collection
        engine::snapshot::CollectionPtr collectionPtr;
        engine::snapshot::FieldElementMappings field_mappings;
        STATUS_CHECK(DBWrapper::DB()->GetCollectionInfo(collection_name_, collectionPtr, field_mappings));
        if (collectionPtr == nullptr) {
            return Status(SERVER_INVALID_COLLECTION_NAME, "Collection not exist: " + collection_name_);
        }

        if (field_names_.empty()) {
            for (const auto& schema : field_mappings) {
                if (schema.first->GetName() == engine::FIELD_UID) {
                    continue;
                }
                field_mappings_.insert(schema);
                field_names_.emplace_back(schema.first->GetName());
            }
        } else {
            for (const auto& name : field_names_) {
                bool find_field_name = false;
                for (const auto& schema : field_mappings) {
                    if (name == schema.first->GetName()) {
                        find_field_name = true;
                        field_mappings_.insert(schema);
                        break;
                    }
                }
                if (not find_field_name) {
                    return Status{SERVER_INVALID_FIELD_NAME, "Field name: " + name + " is wrong"};
                }
            }
        }

        // step 2: get vector data, now only support get one id
        STATUS_CHECK(
            DBWrapper::DB()->GetEntityByID(collection_name_, id_array_, field_names_, valid_row_, data_chunk_));
        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
