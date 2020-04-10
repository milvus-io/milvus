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

#include "server/delivery/hybrid_request/CreateCollectionRequest.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/delivery/request/BaseRequest.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>
#include <string>

namespace milvus {
namespace server {

CreateCollectionRequest::CreateCollectionRequest(const std::shared_ptr<Context>& context,
                                                 std::string& collection_name,
                                                 std::vector<std::string>& field_names,
                                                 std::vector<std::string>& field_types,
                                                 const std::vector<std::string>& field_params)
    : BaseRequest(context, DDL_DML_REQUEST_GROUP),
      collection_name_(collection_name),
      field_names_(field_names),
      field_types_(field_types),
      field_params_(field_params) {
}

BaseRequestPtr
CreateCollectionRequest::Create(const std::shared_ptr<Context>& context,
                                std::string& collection_name,
                                std::vector<std::string>& field_names,
                                std::vector<std::string>& field_types,
                                const std::vector<std::string>& field_params) {
    return std::shared_ptr<BaseRequest>(
        new CreateCollectionRequest(context, collection_name, field_names, field_types, field_params));
}

Status
CreateCollectionRequest::OnExecute() {
    std::string hdr = "CreateCollectionRequest(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    try {
        // step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        rc.RecordSection("check validation");

        // step 2: construct collection schema and vector schema
        engine::meta::hybrid::CollectionSchema collection_info;
        engine::meta::hybrid::FieldsSchema fields_schema;

        collection_info.collection_id_ = collection_name_;
        collection_info.field_num = field_names_.size();
        fields_schema.fields_schema_.resize(field_names_.size());
        for (uint64_t i = 0; i < field_names_.size(); ++i) {
            fields_schema.fields_schema_[i].field_name_ = field_names_[i];
            fields_schema.fields_schema_[i].field_type_ = field_types_[i];
            fields_schema.fields_schema_[i].field_params_ = field_params_[i];
        }

        // TODO(yukun): check dimension, metric_type, and assign engine_type

        // step 3: create collection
        status = DBWrapper::DB()->CreateHybridCollection(collection_info, fields_schema);
        if (!status.ok()) {
            // collection could exist
            if (status.code() == DB_ALREADY_EXIST) {
                return Status(SERVER_INVALID_TABLE_NAME, status.message());
            }
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
