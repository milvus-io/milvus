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

#include "server/delivery/request/GetCollectionInfoReq.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "server/web_impl/Constants.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <utility>

namespace milvus {
namespace server {

GetCollectionInfoReq::GetCollectionInfoReq(const ContextPtr& context, const std::string& collection_name,
                                           CollectionSchema& collection_schema)
    : BaseReq(context, ReqType::kGetCollectionInfo),
      collection_name_(collection_name),
      collection_schema_(collection_schema) {
}

BaseReqPtr
GetCollectionInfoReq::Create(const ContextPtr& context, const std::string& collection_name,
                             CollectionSchema& collection_schema) {
    return std::shared_ptr<BaseReq>(new GetCollectionInfoReq(context, collection_name, collection_schema));
}

Status
GetCollectionInfoReq::OnExecute() {
    try {
        std::string hdr = "GetCollectionInfoReq(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        STATUS_CHECK(ValidateCollectionName(collection_name_));

        bool exist = false;
        STATUS_CHECK(DBWrapper::DB()->HasCollection(collection_name_, exist));
        if (!exist) {
            return Status(SERVER_COLLECTION_NOT_EXIST, "Collection not exist: " + collection_name_);
        }

        engine::snapshot::CollectionPtr collection;
        engine::snapshot::FieldElementMappings field_mappings;
        STATUS_CHECK(DBWrapper::DB()->GetCollectionInfo(collection_name_, collection, field_mappings));

        collection_schema_.collection_name_ = collection_name_;
        collection_schema_.extra_params_ = collection->GetParams();
        for (auto& field_kv : field_mappings) {
            auto field = field_kv.first;

            FieldSchema field_schema;
            milvus::json field_index_param;
            auto field_elements = field_kv.second;
            for (const auto& element : field_elements) {
                if (element->GetFEtype() == engine::FieldElementType::FET_INDEX) {
                    field_index_param = element->GetParams();
                    auto type = element->GetTypeName();
                    field_schema.index_params_ = field_index_param;
                    field_schema.index_params_[engine::PARAM_INDEX_TYPE] = element->GetTypeName();
                    break;
                }
            }

            auto field_name = field->GetName();
            field_schema.field_type_ = field->GetFtype();
            field_schema.field_params_ = field->GetParams();

            collection_schema_.fields_.insert(std::make_pair(field_name, field_schema));
        }

        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
