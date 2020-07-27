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
#include "server/delivery/request/BaseReq.h"
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

GetCollectionInfoRequest::GetCollectionInfoRequest(const std::shared_ptr<milvus::server::Context>& context,
                                                   const std::string& collection_name,
                                                   CollectionSchema& collection_schema)
    : BaseRequest(context, BaseRequest::kGetCollectionInfo),
      collection_name_(collection_name),
      collection_schema_(collection_schema) {
}

BaseRequestPtr
GetCollectionInfoRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                 const std::string& collection_name, CollectionSchema& collection_schema) {
    return std::shared_ptr<BaseRequest>(new GetCollectionInfoRequest(context, collection_name, collection_schema));
}

Status
GetCollectionInfoRequest::OnExecute() {
    std::string hdr = "GetCollectionInfoRequest(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    try {
        engine::snapshot::CollectionPtr collection;
        engine::snapshot::CollectionMappings collection_mappings;
        auto status = DBWrapper::DB()->GetCollectionInfo(collection_name_, collection, collection_mappings);
        if (!status.ok()) {
            return status;
        }
        collection_schema_.collection_name_ = collection_name_;
        collection_schema_.extra_params_ = collection->GetParams();
        engine::meta::hybrid::FieldsSchema fields_schema;
        for (auto field_it = collection_mappings.begin(); field_it != collection_mappings.end(); field_it++) {
            engine::meta::hybrid::FieldSchema schema;
            auto field = field_it->first;
            if (field->GetFtype() == (int)engine::meta::hybrid::DataType::UID) {
                continue;
            }
            schema.field_name_ = field->GetName();
            schema.field_type_ = (int)field->GetFtype();
            schema.field_params_ = field->GetParams().dump();
            auto field_elements = field_it->second;
            for (const auto& element : field_elements) {
                if (element->GetFtype() == (int)engine::FieldElementType::FET_INDEX) {
                    schema.index_name_ = element->GetName();
                    schema.index_param_ = element->GetParams().dump();
                    break;
                }
            }
            fields_schema.fields_schema_.emplace_back(schema);
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

        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
